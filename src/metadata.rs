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

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;
	use hegel::generators::{self, Generator};

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

	fn resolved_image(layers: Vec<ResolvedLayer>) -> ResolvedImage {
		ResolvedImage {
			image_ref: "example:latest".to_owned(),
			encoded_ref: "example/latest".to_owned(),
			manifest_digest: "sha256:manifest".to_owned(),
			config_digest: "sha256:config".to_owned(),
			layers,
			command: Vec::new(),
		}
	}

	#[hegel::test(derandomize = true)]
	fn is_nix_backed_false_without_nix_metadata(tc: hegel::TestCase) {
		let annotations = tc.draw(generators::hashmaps(
			generators::text().filter(|key| {
				key != NIX_CLOSURE_ANNOTATION_KEY && !key.starts_with(NIX_STORE_PATH_PREFIX)
			}),
			generators::text(),
		));
		let layers = tc.draw(generators::vecs(generators::just(annotations)).max_size(8));
		let image = resolved_image(
			layers
				.into_iter()
				.map(|annotations: HashMap<String, String>| {
					resolved_layer(annotations.into_iter().collect(), None)
				})
				.collect(),
		);

		assert!(!image.is_nix_backed());
	}

	#[hegel::test(derandomize = true)]
	fn is_nix_backed_true_when_any_layer_has_nix_closure(tc: hegel::TestCase) {
		let closure_path = tc.draw(generators::text());
		let store_paths = tc.draw(generators::vecs(generators::text()).max_size(8));
		let non_nix_layers = tc.draw(generators::integers::<usize>().max_value(4));
		let image = resolved_image(
			(0..non_nix_layers)
				.map(|_| resolved_layer(BTreeMap::new(), None))
				.chain(std::iter::once(resolved_layer(
					BTreeMap::new(),
					Some(NixClosureMetadata {
						closure_path: closure_path.clone(),
						store_paths: store_paths.clone(),
					}),
				)))
				.collect(),
		);

		assert!(image.is_nix_backed());
	}

	#[hegel::test(derandomize = true)]
	fn is_nix_backed_true_for_closure_annotation_key(tc: hegel::TestCase) {
		let value = tc.draw(generators::text());
		let image = resolved_image(vec![resolved_layer(
			BTreeMap::from([(NIX_CLOSURE_ANNOTATION_KEY.to_owned(), value)]),
			None,
		)]);

		assert!(image.is_nix_backed());
	}

	#[hegel::test(derandomize = true)]
	fn is_nix_backed_true_for_legacy_store_path_annotations(tc: hegel::TestCase) {
		let suffix = tc.draw(generators::text());
		let value = tc.draw(generators::text());
		let image = resolved_image(vec![resolved_layer(
			BTreeMap::from([(format!("{NIX_STORE_PATH_PREFIX}{suffix}"), value)]),
			None,
		)]);

		assert!(image.is_nix_backed());
	}
}
