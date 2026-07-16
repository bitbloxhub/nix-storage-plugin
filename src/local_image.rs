use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::path::{Component, Path, PathBuf};

use base64::Engine as _;
use bytes::Bytes;
use flate2::read::GzDecoder;
use oci_spec::image::{Arch, Descriptor, ImageConfiguration, ImageIndex, ImageManifest, Os};
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
use crate::nix_metadata::{
	NIX_CLOSURE_ANNOTATION_KEY, NIX_STORE_PATH_PREFIX, ParsedNixMetadata, path_to_string,
};
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

fn docker_source_ref_with_digest(source: &str, digest: &str) -> Option<String> {
	let reference = source.strip_prefix("docker://")?;
	let name = reference
		.split_once('@')
		.map(|(name, _)| name)
		.unwrap_or(reference);

	let name = match name.rsplit_once('/') {
		Some((prefix, last)) => match last.rsplit_once(':') {
			Some((repo, _tag)) => format!("{prefix}/{repo}"),
			None => name.to_owned(),
		},
		None => name
			.rsplit_once(':')
			.map(|(repo, _tag)| repo.to_owned())
			.unwrap_or_else(|| name.to_owned()),
	};

	Some(format!("docker://{name}@{digest}"))
}

fn current_architecture() -> Arch {
	Arch::default()
}

fn current_os() -> Os {
	Os::default()
}

fn select_current_platform(index: &ImageIndex) -> Option<&Descriptor> {
	let arch = current_architecture();
	let os = current_os();

	index
		.manifests()
		.iter()
		.find(|descriptor| {
			descriptor
				.platform()
				.as_ref()
				.is_some_and(|platform| platform.architecture() == &arch && platform.os() == &os)
		})
		.or_else(|| match index.manifests().as_slice() {
			[descriptor] if descriptor.platform().is_none() => Some(descriptor),
			_ => None,
		})
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
) -> Result<Option<ResolvedImage>, NixStoragePluginError> {
	let started = std::time::Instant::now();
	let manifest_started = std::time::Instant::now();
	let manifest_raw = inspect_image_source_manifest_raw(image_ref).await?;
	tracing::debug!(
		%image_ref,
		elapsed = ?manifest_started.elapsed(),
		manifest_bytes = manifest_raw.len(),
		"resolved image manifest raw"
	);
	let manifest: ImageManifest = serde_json::from_str(&manifest_raw)?;

	if !manifest_maybe_nix_backed(&manifest) {
		tracing::debug!(
			%image_ref,
			elapsed = ?started.elapsed(),
			layer_count = manifest.layers().len(),
			"resolved local image as ordinary image; skipping ALS resolution"
		);
		return Ok(None);
	}

	let config_started = std::time::Instant::now();
	let config: ImageConfiguration =
		serde_json::from_str(&inspect_image_source_config_raw(image_ref).await?)?;
	tracing::debug!(
		%image_ref,
		elapsed = ?config_started.elapsed(),
		"resolved image config raw"
	);
	let export_started = std::time::Instant::now();
	let exported_blobs = export_image_source_blobs(image_ref, manifest.layers()).await?;
	tracing::debug!(
		%image_ref,
		elapsed = ?export_started.elapsed(),
		layer_count = manifest.layers().len(),
		exported_blob_count = exported_blobs.len(),
		"exported image blobs"
	);
	let layer_records_started = std::time::Instant::now();
	let local_layer_records = read_local_layer_records().await?;
	tracing::debug!(
		%image_ref,
		elapsed = ?layer_records_started.elapsed(),
		local_layer_record_count = local_layer_records.len(),
		"read local layer records"
	);
	let resolve_layers_started = std::time::Instant::now();
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
	tracing::debug!(
		%image_ref,
		elapsed = ?resolve_layers_started.elapsed(),
		resolved_layer_count = resolved_layers.len(),
		"resolved image layers"
	);

	let image = ResolvedImage {
		image_ref: image_ref.to_owned(),
		encoded_ref: base64::engine::general_purpose::STANDARD.encode(image_ref),
		manifest_digest: sha256_digest(manifest_raw.as_bytes()),
		config_digest: manifest.config().digest().to_string(),
		layers: resolved_layers,
		command: image_command(&config),
	};

	if !image.is_nix_backed() {
		tracing::debug!(
			%image_ref,
			elapsed = ?started.elapsed(),
			resolved_layer_count = image.layers.len(),
			"resolved image layers but image not nix-backed; skipping ALS resolution"
		);
		return Ok(None);
	}

	tracing::debug!(
		%image_ref,
		elapsed = ?started.elapsed(),
		resolved_layer_count = image.layers.len(),
		"resolved nix-backed local image"
	);
	Ok(Some(image))
}

fn manifest_maybe_nix_backed(manifest: &ImageManifest) -> bool {
	manifest.layers().iter().any(layer_maybe_nix_backed)
}

fn layer_maybe_nix_backed(layer: &Descriptor) -> bool {
	let annotations = descriptor_annotations_btree(layer);

	annotations.contains_key(NIX_CLOSURE_ANNOTATION_KEY)
		|| annotations
			.keys()
			.any(|key| key.starts_with(NIX_STORE_PATH_PREFIX))
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
	use std::collections::{BTreeMap, HashMap};

	use super::*;
	use hegel::generators::{self, Generator};
	use oci_spec::image::{DescriptorBuilder, Digest, ImageManifest, MediaType};

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
	fn tar_blob(entries: &[(&str, &[u8], u32, u32, u32)]) -> Bytes {
		let mut buffer = Vec::new();
		{
			let mut builder = tar::Builder::new(&mut buffer);
			for (path, contents, mode, uid, gid) in entries {
				let mut header = tar::Header::new_gnu();
				header.set_size(contents.len() as u64);
				header.set_mode(*mode);
				header.set_uid(u64::from(*uid));
				header.set_gid(u64::from(*gid));
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
	fn parse_layer_diff_entries_sorts_paths_and_preserves_metadata(tc: hegel::TestCase) {
		let first: String = tc.draw(tar_path_component());
		let second: String = tc.draw(tar_path_component().filter(|value| value != &first));
		let first_contents = tc.draw(generators::binary().max_size(16));
		let second_contents = tc.draw(generators::binary().max_size(16));
		let first_uid = tc.draw(generators::integers::<u32>());
		let first_gid = tc.draw(generators::integers::<u32>());
		let second_uid = tc.draw(generators::integers::<u32>());
		let second_gid = tc.draw(generators::integers::<u32>());
		let blob = tar_blob(&[
			(
				&second,
				second_contents.as_slice(),
				0,
				second_uid,
				second_gid,
			),
			(
				&first,
				first_contents.as_slice(),
				0o640,
				first_uid,
				first_gid,
			),
		]);
		let entries = parse_layer_diff_entries(&blob).expect("tar blob should parse");

		assert_eq!(entries.len(), 2);
		let expected = if first <= second {
			vec![
				(
					&first,
					0o640,
					first_uid,
					first_gid,
					first_contents.as_slice(),
				),
				(
					&second,
					0o444,
					second_uid,
					second_gid,
					second_contents.as_slice(),
				),
			]
		} else {
			vec![
				(
					&second,
					0o444,
					second_uid,
					second_gid,
					second_contents.as_slice(),
				),
				(
					&first,
					0o640,
					first_uid,
					first_gid,
					first_contents.as_slice(),
				),
			]
		};
		for (entry, (path, perm, uid, gid, contents)) in entries.iter().zip(expected) {
			assert_eq!(entry.path, PathBuf::from(path));
			assert_eq!(entry.perm, perm);
			assert_eq!(entry.uid, uid);
			assert_eq!(entry.gid, gid);
			assert!(
				matches!(&entry.kind, LayerDiffEntryKind::Regular { contents: actual } if actual.as_ref() == contents),
			);
		}
	}

	#[test]
	fn parse_layer_diff_entries_preserves_owned_dirs_and_synthesizes_parents() {
		let mut archive_bytes = Vec::new();
		{
			let mut builder = tar::Builder::new(&mut archive_bytes);

			let mut file = tar::Header::new_gnu();
			file.set_size(1);
			file.set_mode(0o644);
			file.set_uid(1000);
			file.set_gid(1000);
			file.set_cksum();
			builder
				.append_data(&mut file, "home/user/file", std::io::Cursor::new(b"x"))
				.expect("file should append");

			for (path, mode, uid, gid) in
				[("home/user/", 0o755, 1000, 1000), ("tmp/", 0o1777, 0, 0)]
			{
				let mut directory = tar::Header::new_gnu();
				directory.set_entry_type(tar::EntryType::Directory);
				directory.set_size(0);
				directory.set_mode(mode);
				directory.set_uid(uid);
				directory.set_gid(gid);
				directory.set_cksum();
				builder
					.append_data(&mut directory, path, std::io::empty())
					.expect("directory should append");
			}
			builder.finish().expect("tar should finish");
		}

		let entries =
			parse_layer_diff_entries(&Bytes::from(archive_bytes)).expect("tar should parse");
		let metadata = |path: &str| {
			entries
				.iter()
				.find(|entry| entry.path == Path::new(path))
				.map(|entry| (entry.perm, entry.uid, entry.gid))
				.expect("entry should exist")
		};

		assert_eq!(metadata("home"), (0o755, 0, 0));
		assert_eq!(metadata("home/user"), (0o755, 1000, 1000));
		assert_eq!(metadata("home/user/file"), (0o644, 1000, 1000));
		assert_eq!(metadata("tmp"), (0o1777, 0, 0));
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

	fn layer_descriptor_with_annotations(
		digest: &str,
		size: u64,
		annotations: BTreeMap<String, String>,
	) -> Descriptor {
		DescriptorBuilder::default()
			.media_type(MediaType::ImageLayer)
			.size(size)
			.digest(digest.parse::<Digest>().expect("digest should parse"))
			.annotations(annotations.into_iter().collect::<HashMap<_, _>>())
			.build()
			.expect("descriptor should build")
	}

	fn manifest_with_layers(layers: Vec<Descriptor>) -> ImageManifest {
		serde_json::from_value(serde_json::json!({
			"schemaVersion": 2,
			"config": {
				"mediaType": "application/vnd.oci.image.config.v1+json",
				"digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"size": 1,
			},
			"layers": layers,
		}))
		.expect("manifest should parse")
	}

	fn index_with_manifests(manifests: serde_json::Value) -> ImageIndex {
		serde_json::from_value(serde_json::json!({
			"schemaVersion": 2,
			"mediaType": "application/vnd.oci.image.index.v1+json",
			"manifests": manifests,
		}))
		.expect("index should parse")
	}

	#[test]
	fn select_current_platform_accepts_single_platformless_manifest() {
		let index = index_with_manifests(serde_json::json!([{
			"mediaType": "application/vnd.oci.image.manifest.v1+json",
			"digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"size": 1,
		}]));

		assert_eq!(
			select_current_platform(&index)
				.expect("singleton platformless manifest should be selected")
				.digest()
				.to_string(),
			"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		);
	}

	#[test]
	fn select_current_platform_prefers_exact_match_over_platformless_manifest() {
		let index = index_with_manifests(serde_json::json!([
			{
				"mediaType": "application/vnd.oci.image.manifest.v1+json",
				"digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"size": 1,
			},
			{
				"mediaType": "application/vnd.oci.image.manifest.v1+json",
				"digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"size": 1,
				"platform": {
					"architecture": current_architecture().to_string(),
					"os": current_os().to_string(),
				},
			},
		]));

		assert_eq!(
			select_current_platform(&index)
				.expect("exact platform manifest should be selected")
				.digest()
				.to_string(),
			"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		);
	}

	#[test]
	fn select_current_platform_rejects_ambiguous_platformless_manifests() {
		let index = index_with_manifests(serde_json::json!([
			{
				"mediaType": "application/vnd.oci.image.manifest.v1+json",
				"digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"size": 1,
			},
			{
				"mediaType": "application/vnd.oci.image.manifest.v1+json",
				"digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"size": 1,
			},
		]));

		assert!(select_current_platform(&index).is_none());
	}

	#[test]
	fn layer_maybe_nix_backed_detects_closure_annotation() {
		let layer = layer_descriptor_with_annotations(
			"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			1,
			BTreeMap::from([(
				NIX_CLOSURE_ANNOTATION_KEY.to_owned(),
				"/nix/store/closure".to_owned(),
			)]),
		);

		assert!(layer_maybe_nix_backed(&layer));
	}

	#[test]
	fn manifest_maybe_nix_backed_rejects_ordinary_layers() {
		let manifest = manifest_with_layers(vec![layer_descriptor(
			"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			1,
		)]);

		assert!(!manifest_maybe_nix_backed(&manifest));
	}

	#[test]
	fn manifest_maybe_nix_backed_accepts_legacy_store_path_annotations() {
		let manifest = manifest_with_layers(vec![layer_descriptor_with_annotations(
			"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			1,
			BTreeMap::from([(
				format!("{NIX_STORE_PATH_PREFIX}0"),
				"/nix/store/example-path".to_owned(),
			)]),
		)]);

		assert!(manifest_maybe_nix_backed(&manifest));
	}

	#[test]
	fn docker_source_ref_with_digest_replaces_tag_and_preserves_registry_port() {
		assert_eq!(
			docker_source_ref_with_digest(
				"docker://ghcr.io:443/nicolaka/netshoot:v0.15",
				"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			),
			Some(
				"docker://ghcr.io:443/nicolaka/netshoot@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
					.to_owned(),
			),
		);
	}

	#[test]
	fn docker_source_ref_with_digest_keeps_untagged_name() {
		assert_eq!(
			docker_source_ref_with_digest(
				"docker://ghcr.io/nicolaka/netshoot",
				"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			),
			Some(
				"docker://ghcr.io/nicolaka/netshoot@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
					.to_owned(),
			),
		);
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

	tracing::debug!(%source, "raw manifest was an index; selecting platform manifest by digest");
	let index: ImageIndex = serde_json::from_str(&manifest_raw)?;
	let descriptor = select_current_platform(&index).ok_or_else(|| {
		NixStoragePluginError::InvalidLocalStorageState(format!(
			"image source {source} is an index but has no manifest for {}/{}",
			current_os(),
			current_architecture()
		))
	})?;
	let digest_source = docker_source_ref_with_digest(source, descriptor.digest().as_ref())
		.ok_or_else(|| {
			NixStoragePluginError::InvalidLocalStorageState(format!(
				"cannot select platform manifest by digest for source {source}"
			))
		})?;

	let selected_raw = match storage_conf_path {
		Some(path) => {
			inspect_manifest_raw(&digest_source, &[("CONTAINERS_STORAGE_CONF", path)]).await?
		}
		None => inspect_manifest_raw(&digest_source, &[]).await?,
	};

	if serde_json::from_str::<ImageManifest>(&selected_raw).is_ok() {
		return Ok(selected_raw);
	}

	Err(NixStoragePluginError::InvalidLocalStorageState(format!(
		"selected platform manifest {digest_source} did not resolve to an image manifest"
	)))
}

async fn inspect_image_source_manifest_raw(
	image_ref: &str,
) -> Result<String, NixStoragePluginError> {
	let started = std::time::Instant::now();
	let source = image_source_for_skopeo(image_ref).await?;
	let storage_conf = helper_storage_conf().await?;
	let storage_conf_path = storage_conf.path().display().to_string();
	let inspect_started = std::time::Instant::now();
	let (manifest_source, manifest_storage_conf_path, manifest_raw) = match inspect_manifest_raw(
		&source,
		&[("CONTAINERS_STORAGE_CONF", storage_conf_path.as_str())],
	)
	.await
	{
		Ok(manifest) => {
			tracing::debug!(
				%image_ref,
				%source,
				elapsed = ?inspect_started.elapsed(),
				manifest_bytes = manifest.len(),
				"inspected manifest from local storage source"
			);
			(source.clone(), Some(storage_conf_path.as_str()), manifest)
		}
		Err(error) if is_local_storage_image_miss(&error) => {
			tracing::debug!(
				%image_ref,
				%source,
				elapsed = ?inspect_started.elapsed(),
				"local storage manifest inspect missed; falling back to remote source"
			);
			let remote_source = remote_image_source(image_ref);
			let remote_started = std::time::Instant::now();
			let manifest = inspect_manifest_raw(&remote_source, &[]).await?;
			tracing::debug!(
				%image_ref,
				source = %remote_source,
				elapsed = ?remote_started.elapsed(),
				manifest_bytes = manifest.len(),
				"inspected manifest from remote source"
			);
			(remote_source, None, manifest)
		}
		Err(error) => return Err(error),
	};
	let ensure_started = std::time::Instant::now();
	let manifest_raw =
		ensure_image_manifest_raw(&manifest_source, manifest_storage_conf_path, manifest_raw)
			.await?;
	tracing::debug!(
		%image_ref,
		source = %manifest_source,
		elapsed = ?started.elapsed(),
		ensure_elapsed = ?ensure_started.elapsed(),
		manifest_bytes = manifest_raw.len(),
		"resolved final image manifest raw"
	);
	Ok(manifest_raw)
}

async fn inspect_image_source_config_raw(image_ref: &str) -> Result<String, NixStoragePluginError> {
	let source = image_source_for_skopeo(image_ref).await?;
	let storage_conf = helper_storage_conf().await?;
	let storage_conf_path = storage_conf.path().display().to_string();
	let inspect_started = std::time::Instant::now();
	match inspect_config_raw(
		&source,
		&[("CONTAINERS_STORAGE_CONF", storage_conf_path.as_str())],
	)
	.await
	{
		Ok(config) => {
			tracing::debug!(
				%image_ref,
				%source,
				elapsed = ?inspect_started.elapsed(),
				config_bytes = config.len(),
				"inspected config from local storage source"
			);
			Ok(config)
		}
		Err(error) if is_local_storage_image_miss(&error) => {
			tracing::debug!(
				%image_ref,
				%source,
				elapsed = ?inspect_started.elapsed(),
				"local storage config inspect missed; falling back to remote source"
			);
			let remote_source = remote_image_source(image_ref);
			let remote_started = std::time::Instant::now();
			let config = inspect_config_raw(&remote_source, &[]).await?;
			tracing::debug!(
				%image_ref,
				source = %remote_source,
				elapsed = ?remote_started.elapsed(),
				config_bytes = config.len(),
				"inspected config from remote source"
			);
			Ok(config)
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

	let started = std::time::Instant::now();
	let source = image_source_for_skopeo(image_ref).await?;
	let storage_conf = helper_storage_conf().await?;
	let storage_conf_path = storage_conf.path().display().to_string();
	let export_started = std::time::Instant::now();
	let (source, export_dir) = match export_source_to_temp_dir(
		&source,
		"nix-storage-plugin-",
		&[("CONTAINERS_STORAGE_CONF", storage_conf_path.as_str())],
	)
	.await
	{
		Ok(export_dir) => {
			tracing::debug!(
				%image_ref,
				%source,
				elapsed = ?export_started.elapsed(),
				layer_count = layers.len(),
				"exported blobs from local storage source"
			);
			(source, export_dir)
		}
		Err(error) if is_local_storage_image_miss(&error) => {
			tracing::debug!(
				%image_ref,
				%source,
				elapsed = ?export_started.elapsed(),
				"local storage blob export missed; falling back to remote source"
			);
			let source = remote_image_source(image_ref);
			let remote_started = std::time::Instant::now();
			let export_dir = export_source_to_temp_dir(&source, "nix-storage-plugin-", &[]).await?;
			tracing::debug!(
				%image_ref,
				source = %source,
				elapsed = ?remote_started.elapsed(),
				layer_count = layers.len(),
				"exported blobs from remote source"
			);
			(source, export_dir)
		}
		Err(error) => return Err(error),
	};
	let read_started = std::time::Instant::now();
	let blobs = read_exported_blobs_by_layer_order(export_dir.path(), layers, &source).await?;
	tracing::debug!(
		%image_ref,
		source = %source,
		elapsed = ?started.elapsed(),
		read_elapsed = ?read_started.elapsed(),
		layer_count = layers.len(),
		exported_blob_count = blobs.len(),
		"materialized exported blobs by layer order"
	);
	Ok(blobs)
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
	let mut entries = BTreeMap::new();

	for entry in archive.entries()? {
		let mut entry = entry?;
		let Some(path) = normalize_tar_path(entry.path()?.as_ref()) else {
			continue;
		};
		let entry_type = entry.header().entry_type();
		let mode = entry
			.header()
			.mode()
			.ok()
			.map(|mode| (mode & 0o7777) as u16);
		let uid = entry
			.header()
			.uid()
			.ok()
			.and_then(|uid| u32::try_from(uid).ok())
			.unwrap_or(0);
		let gid = entry
			.header()
			.gid()
			.ok()
			.and_then(|gid| u32::try_from(gid).ok())
			.unwrap_or(0);

		let parsed = match entry_type {
			tar::EntryType::Directory => LayerDiffEntry {
				path: path.clone(),
				perm: mode.filter(|mode| *mode != 0).unwrap_or(0o755),
				uid,
				gid,
				kind: LayerDiffEntryKind::Directory,
			},
			tar::EntryType::Regular => {
				let mut contents = Vec::new();
				entry.read_to_end(&mut contents)?;
				LayerDiffEntry {
					path: path.clone(),
					perm: mode.filter(|mode| *mode != 0).unwrap_or(0o444),
					uid,
					gid,
					kind: LayerDiffEntryKind::Regular {
						contents: Bytes::from(contents),
					},
				}
			}
			tar::EntryType::Symlink => {
				let Some(target) = entry.link_name()? else {
					continue;
				};
				LayerDiffEntry {
					path: path.clone(),
					perm: mode.filter(|mode| *mode != 0).unwrap_or(0o777),
					uid,
					gid,
					kind: LayerDiffEntryKind::Symlink {
						target: target.into_owned(),
					},
				}
			}
			_ => continue,
		};

		let mut parent = path.parent();
		while let Some(parent_path) = parent.filter(|parent| !parent.as_os_str().is_empty()) {
			entries
				.entry(parent_path.to_path_buf())
				.or_insert_with(|| LayerDiffEntry {
					path: parent_path.to_path_buf(),
					perm: 0o755,
					uid: 0,
					gid: 0,
					kind: LayerDiffEntryKind::Directory,
				});
			parent = parent_path.parent();
		}
		entries.insert(path, parsed);
	}

	Ok(entries.into_values().collect())
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
