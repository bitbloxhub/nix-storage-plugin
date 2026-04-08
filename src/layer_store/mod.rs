use std::path::PathBuf;

use bytes::Bytes;

use crate::metadata::LayerDiffEntry;

mod fs;
mod resolver;

pub use fs::LayerStoreFS;
pub use resolver::LayerStoreResolver;

#[derive(Debug, Clone, Default)]
struct LayerStoreDiff {
	tar_entries: Vec<LayerDiffEntry>,
	host_projection_roots: Vec<PathBuf>,
}

impl LayerStoreDiff {
	fn is_empty(&self) -> bool {
		self.tar_entries.is_empty() && self.host_projection_roots.is_empty()
	}
}

#[derive(Debug, Clone)]
struct LayerStoreLayer {
	keys: Vec<String>,
	info: Bytes,
	blob: Bytes,
	diff: LayerStoreDiff,
}

#[derive(Debug, Clone)]
struct LayerStoreImage {
	encoded_ref: String,
	layers: Vec<LayerStoreLayer>,
}
