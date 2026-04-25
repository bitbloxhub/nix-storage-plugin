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
use tempfile::NamedTempFile;

use crate::common::{NixStoragePluginError, sha256_blob_file_name, sha256_digest};
use crate::metadata::{
	LayerDiffEntry, LayerDiffEntryKind, LayerSource, NixClosureMetadata, ResolvedImage,
	ResolvedLayer,
};
use crate::nix::try_realize_nix_archive_path;
use crate::nix_metadata::{NIX_STORE_PATH_PREFIX, ParsedNixMetadata, path_to_string};
use crate::oci::{
	archive_path_from_image_ref, containers_storage_ref, descriptor_annotations_btree,
};
use crate::skopeo::{export_source_to_temp_dir, inspect_config_raw, inspect_manifest_raw};
use crate::storage_config::{StorageConfig, load_storage_config};

#[derive(Debug)]
struct LocalStorageSource {
	graph_root: PathBuf,
}

impl LocalStorageSource {
	fn source_ref(&self, image_ref: &str) -> Result<String, NixStoragePluginError> {
		Ok(containers_storage_ref(image_ref))
	}
}

async fn local_storage_source() -> Result<LocalStorageSource, NixStoragePluginError> {
	let StorageConfig { graph_root, .. } = load_storage_config().await?;
	Ok(LocalStorageSource { graph_root })
}

async fn helper_storage_conf() -> Result<NamedTempFile, NixStoragePluginError> {
	let StorageConfig {
		driver,
		graph_root,
		run_root,
	} = load_storage_config().await?;
	let file = NamedTempFile::new()?;
	let mut contents = String::from("[storage]\n");
	if let Some(driver) = driver {
		contents.push_str(&format!("driver = \"{driver}\"\n"));
	}
	contents.push_str(&format!(
		"graphroot = \"{}\"\nrunroot = \"{}\"\n",
		graph_root.display(),
		run_root.display(),
	));
	std::fs::write(file.path(), contents)?;
	Ok(file)
}

fn is_local_storage_image_miss(error: &NixStoragePluginError) -> bool {
	matches!(
		error,
		NixStoragePluginError::HostCommandFailed { stderr, .. }
			if stderr.contains("does not resolve to an image ID") || stderr.contains("identifier is not an image")
	)
}

fn remote_image_source(image_ref: &str) -> String {
	format!("docker://{image_ref}")
}

async fn image_source_for_skopeo(image_ref: &str) -> Result<String, NixStoragePluginError> {
	if let Some(archive_path) = archive_path_from_image_ref(image_ref) {
		try_realize_nix_archive_path(&archive_path).await;
		return Ok(format!("oci-archive:{}", archive_path.display()));
	}

	local_storage_source().await?.source_ref(image_ref)
}

async fn storage_graph_root() -> Result<PathBuf, NixStoragePluginError> {
	Ok(local_storage_source().await?.graph_root)
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

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::*;
	use hegel::generators::{self, Generator};
	use oci_spec::image::{DescriptorBuilder, Digest, MediaType};

	fn store_path() -> impl hegel::generators::Generator<String> {
		generators::from_regex(r"/nix/store/[A-Za-z0-9][A-Za-z0-9._+-]{0,23}").fullmatch(true)
	}

	fn tar_path_component() -> impl hegel::generators::Generator<String> {
		generators::from_regex(r"[A-Za-z0-9][A-Za-z0-9._-]{0,11}")
			.fullmatch(true)
			.filter(|value| value != "." && value != "..")
	}

	fn resolved_layer(
		annotations: BTreeMap<String, String>,
		nix_closure: Option<NixClosureMetadata>,
	) -> ResolvedLayer {
		ResolvedLayer {
			compressed_digest: "sha256:compressed".to_owned(),
			compressed_size: 1,
			diff_digest: "sha256:diff".to_owned(),
			diff_size: 1,
			annotations,
			raw_info: None,
			compression: None,
			uidset: Vec::new(),
			gidset: Vec::new(),
			source: LayerSource::Registry,
			nix_closure,
			blob: Bytes::new(),
			diff_entries: Vec::new(),
		}
	}

	#[hegel::test(derandomize = true)]
	fn projected_store_paths_prefers_nix_closure(tc: hegel::TestCase) {
		let store_paths = tc.draw(generators::vecs(store_path()).max_size(8));
		let layer = resolved_layer(
			BTreeMap::from([(
				format!("{NIX_STORE_PATH_PREFIX}legacy"),
				"/nix/store/legacy-path".to_owned(),
			)]),
			Some(NixClosureMetadata {
				closure_path: "/nix/store/closure".to_owned(),
				store_paths: store_paths.clone(),
			}),
		);

		assert_eq!(
			projected_store_paths(&layer),
			store_paths
				.into_iter()
				.map(PathBuf::from)
				.collect::<Vec<_>>()
		);
	}

	#[hegel::test(derandomize = true)]
	fn projected_store_paths_sorts_and_dedups_legacy_annotations(tc: hegel::TestCase) {
		let store_paths = tc.draw(generators::vecs(store_path()).max_size(12));
		let annotations = store_paths
			.iter()
			.enumerate()
			.map(|(index, path)| (format!("{NIX_STORE_PATH_PREFIX}{index}"), path.clone()))
			.collect::<BTreeMap<_, _>>();
		let layer = resolved_layer(annotations, None);
		let mut expected = store_paths
			.into_iter()
			.map(PathBuf::from)
			.collect::<Vec<_>>();
		expected.sort();
		expected.dedup();

		assert_eq!(projected_store_paths(&layer), expected);
	}
	fn tar_blob(entries: &[(&str, &[u8], u32)]) -> Bytes {
		let mut buffer = Vec::new();
		{
			let mut builder = tar::Builder::new(&mut buffer);
			for (path, contents, mode) in entries {
				let mut header = tar::Header::new_gnu();
				header.set_size(contents.len() as u64);
				header.set_mode(*mode);
				header.set_cksum();
				builder
					.append_data(&mut header, path, *contents)
					.expect("tar entry should append");
			}
			builder.finish().expect("tar builder should finish");
		}
		Bytes::from(buffer)
	}

	#[test]
	fn parse_layer_diff_entries_empty_blob_is_empty() {
		assert!(
			parse_layer_diff_entries(&Bytes::new())
				.expect("empty blob should parse")
				.is_empty()
		);
	}

	#[hegel::test(derandomize = true)]
	fn maybe_decompress_layer_roundtrips_plain_bytes(tc: hegel::TestCase) {
		let bytes = tc.draw(generators::binary().filter(|bytes| !bytes.starts_with(&[0x1f, 0x8b])));

		assert_eq!(
			maybe_decompress_layer(&Bytes::from(bytes.clone()))
				.expect("plain bytes should pass through"),
			bytes
		);
	}

	#[hegel::test(derandomize = true)]
	fn normalize_tar_path_rejects_parent_root_and_empty_paths(tc: hegel::TestCase) {
		let path = tc.draw(generators::sampled_from(vec![
			"",
			".",
			"..",
			"/etc/passwd",
			"../x",
			"a/../../b",
		]));

		assert_eq!(normalize_tar_path(Path::new(path)), None);
	}

	#[hegel::test(derandomize = true)]
	fn normalize_tar_path_removes_curdir_segments(tc: hegel::TestCase) {
		let left: String = tc.draw(tar_path_component());
		let right: String = tc.draw(tar_path_component());
		let path = format!("./{left}/./{right}");

		assert_eq!(
			normalize_tar_path(Path::new(&path)),
			Some(PathBuf::from(format!("{left}/{right}")))
		);
	}

	#[hegel::test(derandomize = true)]
	fn parse_layer_diff_entries_sorts_paths_and_preserves_contents(tc: hegel::TestCase) {
		let first: String = tc.draw(tar_path_component());
		let second: String = tc.draw(tar_path_component().filter(|value| value != &first));
		let first_contents = tc.draw(generators::binary().max_size(16));
		let second_contents = tc.draw(generators::binary().max_size(16));
		let blob = tar_blob(&[
			(&second, second_contents.as_slice(), 0),
			(&first, first_contents.as_slice(), 0o640),
		]);
		let entries = parse_layer_diff_entries(&blob).expect("tar blob should parse");

		assert_eq!(entries.len(), 2);
		let (first_path, first_perm, first_bytes, second_path, second_perm, second_bytes) =
			if first <= second {
				(
					&first,
					0o640,
					first_contents.as_slice(),
					&second,
					0o444,
					second_contents.as_slice(),
				)
			} else {
				(
					&second,
					0o444,
					second_contents.as_slice(),
					&first,
					0o640,
					first_contents.as_slice(),
				)
			};
		assert_eq!(entries[0].path, PathBuf::from(first_path));
		assert_eq!(entries[0].perm, first_perm);
		assert_eq!(entries[1].path, PathBuf::from(second_path));
		assert_eq!(entries[1].perm, second_perm);
		assert!(
			matches!(&entries[0].kind, LayerDiffEntryKind::Regular { contents } if contents.as_ref() == first_bytes)
		);
		assert!(
			matches!(&entries[1].kind, LayerDiffEntryKind::Regular { contents } if contents.as_ref() == second_bytes)
		);
	}

	#[hegel::test(derandomize = true)]
	fn image_command_prefers_entrypoint_then_cmd(tc: hegel::TestCase) {
		let entrypoint = tc.draw(
			generators::vecs(generators::from_regex(r"[A-Za-z0-9._/-]{1,12}").fullmatch(true))
				.max_size(4),
		);
		let cmd = tc.draw(
			generators::vecs(generators::from_regex(r"[A-Za-z0-9._/-]{1,12}").fullmatch(true))
				.max_size(4),
		);
		let json = serde_json::json!({
			"architecture": "amd64",
			"os": "linux",
			"rootfs": { "type": "layers", "diff_ids": [] },
			"config": {
				"Entrypoint": entrypoint,
				"Cmd": cmd,
			}
		});
		let config: ImageConfiguration =
			serde_json::from_value(json).expect("image config should parse");
		let expected = config
			.config()
			.as_ref()
			.and_then(|cfg| cfg.entrypoint().as_ref())
			.into_iter()
			.flatten()
			.chain(
				config
					.config()
					.as_ref()
					.and_then(|cfg| cfg.cmd().as_ref())
					.into_iter()
					.flatten(),
			)
			.cloned()
			.collect::<Vec<_>>();

		assert_eq!(image_command(&config), expected);
	}

	fn layer_descriptor(digest: &str, size: u64) -> Descriptor {
		DescriptorBuilder::default()
			.media_type(MediaType::ImageLayer)
			.size(size)
			.digest(digest.parse::<Digest>().expect("digest should parse"))
			.build()
			.expect("descriptor should build")
	}

	#[test]
	fn export_image_source_blobs_returns_empty_when_no_layers() {
		smol::block_on(async {
			let blobs = export_image_source_blobs("example:latest", &[])
				.await
				.expect("empty layer list should short-circuit");
			assert!(blobs.is_empty());
		})
	}

	#[hegel::test(derandomize = true)]
	fn resolve_local_layer_uses_matching_local_record_by_diff_digest(tc: hegel::TestCase) {
		smol::block_on(async {
			let digests = tc.draw(
				generators::vecs(generators::from_regex(r"[a-f0-9]{64}").fullmatch(true))
					.min_size(3)
					.max_size(3)
					.unique(true),
			);
			let compressed_digest = format!("sha256:{}", digests[0]);
			let diff_digest = format!("sha256:{}", digests[1]);
			let mismatched_compressed = format!("sha256:{}", digests[2]);
			let descriptor = layer_descriptor(&compressed_digest, 42);
			let raw_info = serde_json::json!({
				"compressed-diff-digest": compressed_digest,
				"diff-digest": diff_digest,
				"diff-size": 777,
			});
			let records = vec![LocalStorageLayerRecord {
				compressed_diff_digest: Some(mismatched_compressed),
				diff_digest: Some(diff_digest.clone()),
				diff_size: Some(777),
				compression: Some(2),
				uidset: vec![1, 2],
				gidset: vec![3],
				raw_info: raw_info.clone(),
			}];

			let resolved = resolve_local_layer(
				&descriptor,
				Some(&diff_digest),
				Some(&Bytes::new()),
				&records,
			)
			.await
			.expect("layer should resolve");

			assert_eq!(resolved.compressed_digest, compressed_digest);
			assert_eq!(resolved.diff_digest, diff_digest);
			assert_eq!(resolved.diff_size, 777);
			assert_eq!(resolved.compression, Some(2));
			assert_eq!(resolved.uidset, vec![1, 2]);
			assert_eq!(resolved.gidset, vec![3]);
			assert_eq!(resolved.raw_info, Some(raw_info));
		})
	}

	#[hegel::test(derandomize = true)]
	fn read_exported_blobs_by_layer_order_reports_missing_or_unsupported_layer_blobs(
		tc: hegel::TestCase,
	) {
		smol::block_on(async {
			let digests = tc.draw(
				generators::vecs(generators::from_regex(r"[a-f0-9]{64}").fullmatch(true))
					.min_size(4)
					.max_size(4)
					.unique(true),
			);
			let first_original = layer_descriptor(&format!("sha256:{}", digests[0]), 1);
			let second_original = layer_descriptor(&format!("sha256:{}", digests[1]), 1);
			let config_digest = format!("sha256:{}", digests[2]);
			let exported_sha256 = format!("sha256:{}", digests[3]);
			let exported_sha512 = format!("sha512:{}", "a".repeat(128));
			let dir = tempfile::tempdir().expect("tempdir should exist");
			let manifest = serde_json::json!({
				"schemaVersion": 2,
				"config": {
					"mediaType": "application/vnd.oci.image.config.v1+json",
					"digest": config_digest,
					"size": 1,
				},
				"layers": [
					{
						"mediaType": "application/vnd.oci.image.layer.v1.tar",
						"digest": exported_sha512,
						"size": 1,
					},
					{
						"mediaType": "application/vnd.oci.image.layer.v1.tar",
						"digest": exported_sha256,
						"size": 1,
					},
				],
			});
			fs::write(
				dir.path().join("manifest.json"),
				serde_json::to_vec(&manifest).expect("manifest should serialize"),
			)
			.await
			.expect("manifest should be written");

			let error = read_exported_blobs_by_layer_order(
				dir.path(),
				&[first_original, second_original],
				"docker://example/test:latest",
			)
			.await
			.expect_err("missing blobs should fail");

			assert!(matches!(
				error,
				NixStoragePluginError::InvalidLocalStorageState(message)
					if message.contains("only materialized 0/2 layer blobs")
			));
		})
	}

	#[test]
	fn parse_layer_diff_entries_skips_symlink_without_target_and_unsupported_entries() {
		let mut archive_bytes = Vec::new();
		{
			let mut builder = tar::Builder::new(&mut archive_bytes);

			let mut regular = tar::Header::new_gnu();
			regular.set_size(2);
			regular.set_mode(0o644);
			regular.set_cksum();
			builder
				.append_data(&mut regular, "ok", std::io::Cursor::new(b"hi".as_slice()))
				.expect("regular entry should append");

			let mut symlink_without_target = tar::Header::new_gnu();
			symlink_without_target.set_entry_type(tar::EntryType::Symlink);
			symlink_without_target.set_size(0);
			symlink_without_target.set_mode(0o777);
			symlink_without_target.set_cksum();
			builder
				.append_data(
					&mut symlink_without_target,
					"skip-link",
					std::io::Cursor::new(Vec::<u8>::new()),
				)
				.expect("symlink without target should append");

			let mut hard_link = tar::Header::new_gnu();
			hard_link.set_entry_type(tar::EntryType::Link);
			hard_link
				.set_link_name("ok")
				.expect("hard link target should set");
			hard_link.set_size(0);
			hard_link.set_mode(0o777);
			hard_link.set_cksum();
			builder
				.append_data(
					&mut hard_link,
					"skip-hard-link",
					std::io::Cursor::new(Vec::<u8>::new()),
				)
				.expect("hard link entry should append");

			builder.finish().expect("tar builder should finish");
		}

		let entries =
			parse_layer_diff_entries(&Bytes::from(archive_bytes)).expect("tar should parse");
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].path, PathBuf::from("ok"));
		assert!(matches!(
			entries[0].kind,
			LayerDiffEntryKind::Regular { .. }
		));
	}

	#[test]
	fn image_command_returns_cmd_when_entrypoint_missing_and_empty_when_both_missing() {
		let cmd_only: ImageConfiguration = serde_json::from_value(serde_json::json!({
			"architecture": "amd64",
			"os": "linux",
			"rootfs": { "type": "layers", "diff_ids": [] },
			"config": {
				"Cmd": ["echo", "hello"],
			}
		}))
		.expect("cmd-only config should parse");
		assert_eq!(
			image_command(&cmd_only),
			vec!["echo".to_owned(), "hello".to_owned()]
		);

		let empty: ImageConfiguration = serde_json::from_value(serde_json::json!({
			"architecture": "amd64",
			"os": "linux",
			"rootfs": { "type": "layers", "diff_ids": [] },
		}))
		.expect("empty config should parse");
		assert!(image_command(&empty).is_empty());
	}
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

async fn ensure_image_manifest_raw(
	source: &str,
	storage_conf_path: Option<&str>,
	manifest_raw: String,
) -> Result<String, NixStoragePluginError> {
	if serde_json::from_str::<ImageManifest>(&manifest_raw).is_ok() {
		return Ok(manifest_raw);
	}

	let export_dir = match storage_conf_path {
		Some(path) => {
			export_source_to_temp_dir(
				source,
				"nix-storage-plugin-manifest-",
				&[("CONTAINERS_STORAGE_CONF", path)],
			)
			.await?
		}
		None => export_source_to_temp_dir(source, "nix-storage-plugin-manifest-", &[]).await?,
	};
	fs::read_to_string(export_dir.path().join("manifest.json"))
		.await
		.map_err(Into::into)
}

async fn inspect_image_source_manifest_raw(
	image_ref: &str,
) -> Result<String, NixStoragePluginError> {
	let source = image_source_for_skopeo(image_ref).await?;
	let storage_conf = helper_storage_conf().await?;
	let storage_conf_path = storage_conf.path().display().to_string();
	let (manifest_source, manifest_storage_conf_path, manifest_raw) = match inspect_manifest_raw(
		&source,
		&[("CONTAINERS_STORAGE_CONF", storage_conf_path.as_str())],
	)
	.await
	{
		Ok(manifest) => (source.clone(), Some(storage_conf_path.as_str()), manifest),
		Err(error) if is_local_storage_image_miss(&error) => {
			let remote_source = remote_image_source(image_ref);
			let manifest = inspect_manifest_raw(&remote_source, &[]).await?;
			(remote_source, None, manifest)
		}
		Err(error) => return Err(error),
	};
	ensure_image_manifest_raw(&manifest_source, manifest_storage_conf_path, manifest_raw).await
}

async fn inspect_image_source_config_raw(image_ref: &str) -> Result<String, NixStoragePluginError> {
	let source = image_source_for_skopeo(image_ref).await?;
	let storage_conf = helper_storage_conf().await?;
	let storage_conf_path = storage_conf.path().display().to_string();
	match inspect_config_raw(
		&source,
		&[("CONTAINERS_STORAGE_CONF", storage_conf_path.as_str())],
	)
	.await
	{
		Ok(config) => Ok(config),
		Err(error) if is_local_storage_image_miss(&error) => {
			inspect_config_raw(&remote_image_source(image_ref), &[]).await
		}
		Err(error) => Err(error),
	}
}

async fn export_image_source_blobs(
	image_ref: &str,
	layers: &[Descriptor],
) -> Result<BTreeMap<String, Bytes>, NixStoragePluginError> {
	if layers.is_empty() {
		return Ok(BTreeMap::new());
	}

	let source = image_source_for_skopeo(image_ref).await?;
	let storage_conf = helper_storage_conf().await?;
	let storage_conf_path = storage_conf.path().display().to_string();
	let (source, export_dir) = match export_source_to_temp_dir(
		&source,
		"nix-storage-plugin-",
		&[("CONTAINERS_STORAGE_CONF", storage_conf_path.as_str())],
	)
	.await
	{
		Ok(export_dir) => (source, export_dir),
		Err(error) if is_local_storage_image_miss(&error) => {
			let source = remote_image_source(image_ref);
			let export_dir = export_source_to_temp_dir(&source, "nix-storage-plugin-", &[]).await?;
			(source, export_dir)
		}
		Err(error) => return Err(error),
	};
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

	let data = match fs::read_to_string(&path).await {
		Ok(data) => data,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
		Err(error) => return Err(error.into()),
	};
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
