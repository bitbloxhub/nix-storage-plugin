use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use smol::fs;
use thiserror::Error;

pub const NIX_CLOSURE_ANNOTATION_KEY: &str = "containerd.io/snapshot/nix-closure";
pub const NIX_STORE_PATH_PREFIX: &str = "containerd.io/snapshot/nix-store-path.";

#[derive(Debug, Error)]
pub enum NixMetadataError {
	#[error(transparent)]
	Io(#[from] std::io::Error),
	#[error("invalid nix closure path: {0}")]
	InvalidClosurePath(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClosureInfo {
	pub closure_path: PathBuf,
	pub store_paths: Vec<PathBuf>,
}

impl ClosureInfo {
	pub(crate) async fn read(closure_path: impl Into<PathBuf>) -> Result<Self, NixMetadataError> {
		let closure_path = closure_path.into();
		if !closure_path.is_absolute() {
			return Err(NixMetadataError::InvalidClosurePath(
				closure_path.display().to_string(),
			));
		}

		let store_paths_path = closure_path.join("store-paths");
		let store_paths = fs::read_to_string(&store_paths_path)
			.await?
			.lines()
			.map(str::trim)
			.filter(|line| !line.is_empty())
			.map(PathBuf::from)
			.collect();

		Ok(Self {
			closure_path,
			store_paths,
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedNixMetadata {
	Closure(ClosureInfo),
	LegacyStorePaths(Vec<PathBuf>),
	None,
}

impl ParsedNixMetadata {
	pub(crate) async fn parse_annotations(
		annotations: &BTreeMap<String, String>,
	) -> Result<Self, NixMetadataError> {
		if let Some(closure_path) = annotations.get(NIX_CLOSURE_ANNOTATION_KEY) {
			return Ok(Self::Closure(ClosureInfo::read(closure_path).await?));
		}

		let mut store_paths = annotations
			.iter()
			.filter(|(key, _)| key.starts_with(NIX_STORE_PATH_PREFIX))
			.map(|(_, value)| PathBuf::from(value))
			.collect::<Vec<_>>();
		store_paths.sort();
		store_paths.dedup();

		if store_paths.is_empty() {
			Ok(Self::None)
		} else {
			Ok(Self::LegacyStorePaths(store_paths))
		}
	}
}

pub(crate) fn path_to_string(path: &Path) -> String {
	path.display().to_string()
}
