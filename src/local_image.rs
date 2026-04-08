use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::path::{Component, Path, PathBuf};

use base64::Engine as _;
use bytes::Bytes;
use flate2::read::GzDecoder;
use oci_spec::image::{Descriptor, ImageConfiguration, ImageManifest};
use serde::Deserialize;
use serde_json::Value;
use smol::fs;

use crate::common::{NixStoragePluginError, sha256_blob_file_name, sha256_digest};
use crate::metadata::{
	LayerDiffEntry, LayerDiffEntryKind, LayerSource, NixClosureMetadata, ResolvedImage,
	ResolvedLayer,
};
use crate::nix_metadata::{NIX_STORE_PATH_PREFIX, ParsedNixMetadata, path_to_string};
use crate::oci::{descriptor_annotations_btree, image_source_ref};
use crate::skopeo::{
	export_source_to_temp_dir, host_command, inspect_config_raw, inspect_manifest_raw,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PodmanInfo {
	store: PodmanStore,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PodmanStore {
	graph_root: PathBuf,
}

async fn storage_graph_root() -> Result<PathBuf, NixStoragePluginError> {
	let output = host_command(&["podman", "info", "--format", "json"]).await?;
	let info: PodmanInfo = serde_json::from_str(&output)?;
	Ok(info.store.graph_root)
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LocalStorageLayerRecord {
	#[serde(default)]
	compressed_diff_digest: Option<String>,
	#[serde(default)]
	diff_digest: Option<String>,
	#[serde(default)]
	diff_size: Option<u64>,
	#[serde(default)]
	compression: Option<u32>,
	#[serde(default)]
	uidset: Vec<u32>,
	#[serde(default)]
	gidset: Vec<u32>,
	#[serde(default, skip)]
	raw_info: Value,
}

pub(crate) async fn resolve_local_image(
	image_ref: &str,
) -> Result<ResolvedImage, NixStoragePluginError> {
	let manifest_raw = inspect_image_source_manifest_raw(image_ref).await?;
	let manifest: ImageManifest = serde_json::from_str(&manifest_raw)?;
	let config: ImageConfiguration =
		serde_json::from_str(&inspect_image_source_config_raw(image_ref).await?)?;
	let exported_blobs = export_image_source_blobs(image_ref, manifest.layers()).await?;
	let local_layer_records = read_local_layer_records().await?;
	let mut resolved_layers = Vec::with_capacity(manifest.layers().len());

	for (index, layer) in manifest.layers().iter().enumerate() {
		resolved_layers.push(
			resolve_local_layer(
				layer,
				config.rootfs().diff_ids().get(index),
				exported_blobs.get(layer.digest().as_ref()),
				&local_layer_records,
			)
			.await?,
		)
	}

	Ok(ResolvedImage {
		image_ref: image_ref.to_owned(),
		encoded_ref: base64::engine::general_purpose::STANDARD.encode(image_ref),
		manifest_digest: sha256_digest(manifest_raw.as_bytes()),
		config_digest: manifest.config().digest().to_string(),
		layers: resolved_layers,
		command: image_command(&config),
	})
}

pub(crate) fn projected_store_paths(layer: &ResolvedLayer) -> Vec<PathBuf> {
	if let Some(nix_closure) = &layer.nix_closure {
		return nix_closure.store_paths.iter().map(PathBuf::from).collect();
	}

	let mut store_paths = layer
		.annotations
		.iter()
		.filter(|(key, _)| key.starts_with(NIX_STORE_PATH_PREFIX))
		.map(|(_, value)| PathBuf::from(value))
		.collect::<Vec<_>>();
	store_paths.sort();
	store_paths.dedup();
	store_paths
}

async fn resolve_local_layer(
	layer: &Descriptor,
	diff_id: Option<&String>,
	exported_blob: Option<&Bytes>,
	local_layer_records: &[LocalStorageLayerRecord],
) -> Result<ResolvedLayer, NixStoragePluginError> {
	let annotations = descriptor_annotations_btree(layer);
	let nix_metadata = ParsedNixMetadata::parse_annotations(&annotations).await?;
	let nix_closure = match &nix_metadata {
		ParsedNixMetadata::Closure(info) => Some(NixClosureMetadata {
			closure_path: path_to_string(&info.closure_path),
			store_paths: info
				.store_paths
				.iter()
				.map(|path| path_to_string(path))
				.collect(),
		}),
		_ => None,
	};
	let compressed_digest = layer.digest().to_string();
	let diff_digest = diff_id
		.cloned()
		.unwrap_or_else(|| compressed_digest.clone());
	let local_layer = local_layer_records.iter().find(|candidate| {
		candidate.compressed_diff_digest.as_deref() == Some(compressed_digest.as_str())
			|| candidate.diff_digest.as_deref() == Some(diff_digest.as_str())
	});
	let blob = exported_blob.cloned().unwrap_or_default();
	let diff_entries = parse_layer_diff_entries(&blob)?;

	Ok(ResolvedLayer {
		compressed_digest,
		compressed_size: layer.size(),
		diff_digest,
		diff_size: local_layer
			.and_then(|candidate| candidate.diff_size)
			.unwrap_or(layer.size()),
		annotations,
		raw_info: local_layer.map(|candidate| candidate.raw_info.clone()),
		compression: local_layer.and_then(|candidate| candidate.compression),
		uidset: local_layer
			.map(|candidate| candidate.uidset.clone())
			.unwrap_or_default(),
		gidset: local_layer
			.map(|candidate| candidate.gidset.clone())
			.unwrap_or_default(),
		source: LayerSource::Registry,
		nix_closure,
		blob,
		diff_entries,
	})
}

async fn inspect_image_source_manifest_raw(
	image_ref: &str,
) -> Result<String, NixStoragePluginError> {
	let source = image_source_ref(image_ref);
	inspect_manifest_raw(&source).await
}

async fn inspect_image_source_config_raw(image_ref: &str) -> Result<String, NixStoragePluginError> {
	let source = image_source_ref(image_ref);
	inspect_config_raw(&source).await
}

async fn export_image_source_blobs(
	image_ref: &str,
	layers: &[Descriptor],
) -> Result<BTreeMap<String, Bytes>, NixStoragePluginError> {
	if layers.is_empty() {
		return Ok(BTreeMap::new());
	}

	let source = image_source_ref(image_ref);
	let export_dir = export_source_to_temp_dir(&source, "nix-storage-plugin-").await?;
	read_exported_blobs_by_layer_order(export_dir.path(), layers, &source).await
}

async fn read_exported_blobs_by_layer_order(
	export_dir: &Path,
	original_layers: &[Descriptor],
	source: &str,
) -> Result<BTreeMap<String, Bytes>, NixStoragePluginError> {
	let exported_manifest: ImageManifest =
		serde_json::from_str(&fs::read_to_string(export_dir.join("manifest.json")).await?)?;
	let mut blobs = BTreeMap::new();

	for (original_layer, exported_layer) in original_layers
		.iter()
		.zip(exported_manifest.layers().iter())
	{
		let Some(file_name) = sha256_blob_file_name(exported_layer.digest().as_ref()) else {
			continue;
		};
		let path = export_dir.join(file_name);
		if let Ok(bytes) = fs::read(path).await {
			blobs.insert(original_layer.digest().to_string(), Bytes::from(bytes));
		}
	}

	if blobs.len() == original_layers.len() {
		return Ok(blobs);
	}

	Err(NixStoragePluginError::InvalidLocalStorageState(format!(
		"copied {source} but only materialized {}/{} layer blobs",
		blobs.len(),
		original_layers.len(),
	)))
}

fn parse_layer_diff_entries(blob: &Bytes) -> Result<Vec<LayerDiffEntry>, NixStoragePluginError> {
	if blob.is_empty() {
		return Ok(Vec::new());
	}

	let tar_bytes = maybe_decompress_layer(blob)?;
	let mut archive = tar::Archive::new(Cursor::new(tar_bytes));
	let mut entries = Vec::new();

	for entry in archive.entries()? {
		let mut entry = entry?;
		let Some(path) = normalize_tar_path(entry.path()?.as_ref()) else {
			continue;
		};
		let mode = (entry.header().mode().unwrap_or(0o555) & 0o777) as u16;
		match entry.header().entry_type() {
			tar::EntryType::Directory => entries.push(LayerDiffEntry {
				path,
				perm: if mode == 0 { 0o555 } else { mode },
				kind: LayerDiffEntryKind::Directory,
			}),
			tar::EntryType::Regular => {
				let mut contents = Vec::new();
				entry.read_to_end(&mut contents)?;
				entries.push(LayerDiffEntry {
					path,
					perm: if mode == 0 { 0o444 } else { mode },
					kind: LayerDiffEntryKind::Regular {
						contents: Bytes::from(contents),
					},
				})
			}
			tar::EntryType::Symlink => {
				let Some(target) = entry.link_name()? else {
					continue;
				};
				entries.push(LayerDiffEntry {
					path,
					perm: 0o777,
					kind: LayerDiffEntryKind::Symlink {
						target: target.into_owned(),
					},
				})
			}
			_ => continue,
		}
	}

	entries.sort_by(|left, right| left.path.cmp(&right.path));
	Ok(entries)
}

fn maybe_decompress_layer(blob: &Bytes) -> Result<Vec<u8>, NixStoragePluginError> {
	if blob.starts_with(&[0x1f, 0x8b]) {
		let mut decoder = GzDecoder::new(Cursor::new(blob.as_ref()));
		let mut decoded = Vec::new();
		decoder.read_to_end(&mut decoded)?;
		return Ok(decoded);
	}

	Ok(blob.to_vec())
}

fn normalize_tar_path(path: &Path) -> Option<PathBuf> {
	let mut normalized = PathBuf::new();
	for component in path.components() {
		match component {
			Component::Prefix(_) | Component::RootDir | Component::ParentDir => return None,
			Component::CurDir => continue,
			Component::Normal(value) => normalized.push(value),
		}
	}
	if normalized.as_os_str().is_empty() {
		None
	} else {
		Some(normalized)
	}
}

async fn read_local_layer_records() -> Result<Vec<LocalStorageLayerRecord>, NixStoragePluginError> {
	let path = storage_graph_root()
		.await?
		.join("overlay-layers")
		.join("layers.json");
	let data = fs::read_to_string(path).await?;
	let raw_records = serde_json::from_str::<Vec<Value>>(&data)?;
	let mut records = Vec::with_capacity(raw_records.len());

	for raw_info in raw_records {
		let mut record: LocalStorageLayerRecord = serde_json::from_value(raw_info.clone())?;
		record.raw_info = raw_info;
		records.push(record);
	}

	Ok(records)
}

fn image_command(config: &ImageConfiguration) -> Vec<String> {
	let config = config.config();
	let entrypoint = config
		.as_ref()
		.and_then(|config| config.entrypoint().as_ref());
	let cmd = config.as_ref().and_then(|config| config.cmd().as_ref());

	if let (Some(entrypoint), Some(cmd)) = (entrypoint, cmd) {
		return entrypoint.iter().chain(cmd.iter()).cloned().collect();
	}

	if let Some(entrypoint) = entrypoint {
		return entrypoint.clone();
	}

	cmd.cloned().unwrap_or_default()
}
