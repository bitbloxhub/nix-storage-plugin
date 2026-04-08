use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::nix_metadata::{NIX_CLOSURE_ANNOTATION_KEY, NIX_STORE_PATH_PREFIX};
use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LayerSource {
	Registry,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NixClosureMetadata {
	pub closure_path: String,
	pub store_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub enum LayerDiffEntryKind {
	Directory,
	Regular {
		#[serde(skip_serializing)]
		contents: Bytes,
	},
	Symlink {
		target: PathBuf,
	},
}

#[derive(Debug, Clone, Serialize)]
pub struct LayerDiffEntry {
	pub path: PathBuf,
	pub perm: u16,
	pub kind: LayerDiffEntryKind,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedLayer {
	pub compressed_digest: String,
	pub compressed_size: u64,
	pub diff_digest: String,
	pub diff_size: u64,
	pub annotations: BTreeMap<String, String>,
	pub raw_info: Option<Value>,
	pub compression: Option<u32>,
	pub uidset: Vec<u32>,
	pub gidset: Vec<u32>,
	pub source: LayerSource,
	pub nix_closure: Option<NixClosureMetadata>,
	#[serde(skip_serializing)]
	pub blob: Bytes,
	#[serde(skip_serializing)]
	pub diff_entries: Vec<LayerDiffEntry>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedImage {
	pub image_ref: String,
	pub encoded_ref: String,
	pub manifest_digest: String,
	pub config_digest: String,
	pub layers: Vec<ResolvedLayer>,
	pub command: Vec<String>,
}

impl ResolvedImage {
	pub fn is_nix_backed(&self) -> bool {
		self.layers.iter().any(|layer| {
			layer.nix_closure.is_some()
				|| layer.annotations.keys().any(|key| {
					key == NIX_CLOSURE_ANNOTATION_KEY || key.starts_with(NIX_STORE_PATH_PREFIX)
				})
		})
	}
}
