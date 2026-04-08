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
