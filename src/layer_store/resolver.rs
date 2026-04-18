use std::collections::BTreeMap;
use std::sync::RwLock;

use base64::Engine as _;
use bytes::Bytes;
use serde::Serialize;

use crate::local_image::{projected_store_paths, resolve_local_image};
use crate::metadata::{ResolvedImage, ResolvedLayer};

use super::{LayerStoreDiff, LayerStoreImage, LayerStoreLayer};

#[derive(Debug, Default)]
pub struct LayerStoreResolver {
	images: RwLock<BTreeMap<String, LayerStoreImage>>,
}

impl LayerStoreResolver {
	pub fn new() -> Self {
		Self::default()
	}

	pub(super) async fn images(&self) -> Vec<LayerStoreImage> {
		self.images
			.read()
			.expect("dynamic layer store read lock")
			.values()
			.cloned()
			.collect()
	}

	pub(super) async fn image_by_encoded_ref(&self, encoded_ref: &str) -> Option<LayerStoreImage> {
		if let Some(image) = self
			.images
			.read()
			.expect("dynamic layer store read lock")
			.get(encoded_ref)
			.cloned()
		{
			return Some(image);
		}

		let image = self.resolve_image(encoded_ref).await?;
		self.images
			.write()
			.expect("dynamic layer store write lock")
			.insert(encoded_ref.to_owned(), image.clone());
		Some(image)
	}

	fn decode_image_ref(encoded_ref: &str) -> Option<String> {
		let decoded = base64::engine::general_purpose::STANDARD
			.decode(encoded_ref)
			.ok()?;
		String::from_utf8(decoded).ok()
	}

	async fn resolve_image(&self, encoded_ref: &str) -> Option<LayerStoreImage> {
		let image_ref = Self::decode_image_ref(encoded_ref)?;
		match resolve_local_image(&image_ref).await {
			Ok(image) => {
				if !image.is_nix_backed() {
					tracing::debug!(%image_ref, "skipping non-nix image in dynamic ALS backend");
					return None;
				}
				Some(LayerStoreImage::from_resolved_image(image).await)
			}
			Err(error) => {
				tracing::warn!(%error, %image_ref, "failed to resolve image for dynamic ALS backend");
				None
			}
		}
	}
	#[cfg(test)]
	pub(super) fn insert_image_for_test(&self, image: LayerStoreImage) {
		self.images
			.write()
			.expect("dynamic layer store write lock")
			.insert(image.encoded_ref.clone(), image);
	}
}

impl LayerStoreImage {
	pub(super) async fn from_resolved_image(image: ResolvedImage) -> Self {
		let mut layers = Vec::with_capacity(image.layers.len());
		for layer in image.layers {
			layers.push(LayerStoreLayer::from_resolved_layer(layer).await);
		}

		Self {
			encoded_ref: image.encoded_ref,
			layers,
		}
	}

	pub(super) fn layer_by_key(&self, layer_key: &str) -> Option<&LayerStoreLayer> {
		self.layers
			.iter()
			.find(|layer| layer.keys.iter().any(|candidate| candidate == layer_key))
	}
}

fn resolved_layer_keys(layer: &ResolvedLayer) -> Vec<String> {
	let mut keys = vec![layer.compressed_digest.clone()];
	if layer.diff_digest != layer.compressed_digest {
		keys.push(layer.diff_digest.clone());
	}
	keys
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct ResolvedLayerInfoFallback<'a> {
	compressed_diff_digest: &'a str,
	compressed_size: u64,
	diff_digest: &'a str,
	diff_size: u64,
	compression: Option<u32>,
	uidset: &'a [u32],
	gidset: &'a [u32],
}

fn resolved_layer_info_bytes(layer: &ResolvedLayer) -> Bytes {
	let fallback = ResolvedLayerInfoFallback {
		compressed_diff_digest: &layer.compressed_digest,
		compressed_size: layer.compressed_size,
		diff_digest: &layer.diff_digest,
		diff_size: layer.diff_size,
		compression: layer.compression,
		uidset: &layer.uidset,
		gidset: &layer.gidset,
	};

	let info = if let Some(raw_info) = layer.raw_info.as_ref() {
		serde_json::to_vec(raw_info)
	} else {
		serde_json::to_vec(&fallback)
	};

	Bytes::from(info.expect("resolved layer info json"))
}

async fn resolved_layer_diff(layer: &ResolvedLayer) -> LayerStoreDiff {
	let roots = projected_store_paths(layer)
		.into_iter()
		.filter(|path| std::fs::metadata(path).is_ok())
		.collect::<Vec<_>>();
	LayerStoreDiff {
		tar_entries: layer.diff_entries.clone(),
		host_projection_roots: roots,
	}
}

impl LayerStoreLayer {
	async fn from_resolved_layer(layer: ResolvedLayer) -> Self {
		Self {
			keys: resolved_layer_keys(&layer),
			info: resolved_layer_info_bytes(&layer),
			blob: layer.blob.clone(),
			diff: resolved_layer_diff(&layer).await,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use super::*;
	use crate::metadata::{LayerSource, NixClosureMetadata};
	use hegel::generators;

	fn resolved_layer(
		compressed_digest: String,
		diff_digest: String,
		raw_info: Option<serde_json::Value>,
		nix_closure: Option<NixClosureMetadata>,
	) -> ResolvedLayer {
		ResolvedLayer {
			compressed_digest,
			compressed_size: 11,
			diff_digest,
			diff_size: 22,
			annotations: BTreeMap::new(),
			raw_info,
			compression: Some(3),
			uidset: vec![1, 2],
			gidset: vec![3],
			source: LayerSource::Registry,
			nix_closure,
			blob: Bytes::from_static(b"blob"),
			diff_entries: Vec::new(),
		}
	}

	#[hegel::test(derandomize = true)]
	fn decode_image_ref_roundtrips_base64_utf8_and_rejects_invalid_input(tc: hegel::TestCase) {
		let image_ref = tc.draw(generators::text());
		let encoded = base64::engine::general_purpose::STANDARD.encode(image_ref.as_bytes());
		assert_eq!(
			LayerStoreResolver::decode_image_ref(&encoded),
			Some(image_ref)
		);
		assert_eq!(LayerStoreResolver::decode_image_ref("%%%"), None);
	}

	#[hegel::test(derandomize = true)]
	fn resolved_layer_keys_include_diff_only_when_different(tc: hegel::TestCase) {
		let digests = tc.draw(
			generators::vecs(generators::from_regex(r"[a-f0-9]{64}").fullmatch(true))
				.min_size(2)
				.max_size(2)
				.unique(true),
		);
		let compressed = format!("sha256:{}", digests[0]);
		let diff = format!("sha256:{}", digests[1]);
		let same = resolved_layer_keys(&resolved_layer(
			compressed.clone(),
			compressed.clone(),
			None,
			None,
		));
		assert_eq!(same, vec![compressed.clone()]);

		let different = resolved_layer_keys(&resolved_layer(
			compressed.clone(),
			diff.clone(),
			None,
			None,
		));
		assert_eq!(different, vec![compressed, diff]);
	}

	#[test]
	fn resolved_layer_info_bytes_prefers_raw_info_and_falls_back_to_schema() {
		let raw_info = serde_json::json!({ "preserve": true });
		let from_raw = resolved_layer_info_bytes(&resolved_layer(
			"sha256:a".to_owned(),
			"sha256:b".to_owned(),
			Some(raw_info.clone()),
			None,
		));
		assert_eq!(
			serde_json::from_slice::<serde_json::Value>(&from_raw)
				.expect("raw info json should parse"),
			raw_info
		);

		let fallback = resolved_layer_info_bytes(&resolved_layer(
			"sha256:c".to_owned(),
			"sha256:d".to_owned(),
			None,
			None,
		));
		let parsed = serde_json::from_slice::<serde_json::Value>(&fallback)
			.expect("fallback info json should parse");
		assert_eq!(parsed["compressed-diff-digest"], "sha256:c");
		assert_eq!(parsed["diff-digest"], "sha256:d");
		assert_eq!(parsed["compressed-size"], 11);
		assert_eq!(parsed["diff-size"], 22);
	}

	#[test]
	fn resolved_layer_diff_keeps_existing_projection_roots_only() {
		smol::block_on(async {
			let existing = tempfile::tempdir().expect("tempdir should exist");
			let missing = existing.path().join("missing");
			let layer = resolved_layer(
				"sha256:a".to_owned(),
				"sha256:b".to_owned(),
				None,
				Some(NixClosureMetadata {
					closure_path: "/nix/store/closure".to_owned(),
					store_paths: vec![
						existing.path().display().to_string(),
						missing.display().to_string(),
					],
				}),
			);

			let diff = resolved_layer_diff(&layer).await;
			assert_eq!(
				diff.host_projection_roots,
				vec![PathBuf::from(existing.path())]
			);
		});
	}

	#[test]
	fn layer_by_key_matches_any_registered_layer_key() {
		smol::block_on(async {
			let image = LayerStoreImage::from_resolved_image(ResolvedImage {
				image_ref: "example:latest".to_owned(),
				encoded_ref: "encoded".to_owned(),
				manifest_digest: "sha256:manifest".to_owned(),
				config_digest: "sha256:config".to_owned(),
				layers: vec![resolved_layer(
					"sha256:compressed".to_owned(),
					"sha256:diff".to_owned(),
					None,
					None,
				)],
				command: Vec::new(),
			})
			.await;

			assert!(image.layer_by_key("sha256:compressed").is_some());
			assert!(image.layer_by_key("sha256:diff").is_some());
			assert!(image.layer_by_key("sha256:missing").is_none());
		});
	}

	#[test]
	fn image_by_encoded_ref_returns_none_for_invalid_base64_without_resolving() {
		smol::block_on(async {
			let resolver = LayerStoreResolver::new();
			assert!(
				resolver
					.image_by_encoded_ref("%%%not-base64%%%")
					.await
					.is_none()
			);
			assert!(resolver.images().await.is_empty());
		});
	}
}
