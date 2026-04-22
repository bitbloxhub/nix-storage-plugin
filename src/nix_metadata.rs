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

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::*;
	use hegel::generators::{self, Generator};

	fn store_path() -> impl hegel::generators::Generator<String> {
		generators::from_regex(r"/nix/store/[A-Za-z0-9._+-]{1,24}").fullmatch(true)
	}

	#[hegel::test(derandomize = true)]
	fn path_to_string_roundtrips_displayable_paths(tc: hegel::TestCase) {
		let suffix = tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{0,24}").fullmatch(true));
		let path = PathBuf::from(format!("/{suffix}"));

		assert_eq!(path_to_string(&path), path.display().to_string());
	}

	#[hegel::test(derandomize = true)]
	fn parse_annotations_returns_none_without_nix_keys(tc: hegel::TestCase) {
		let annotations = tc.draw(generators::hashmaps(
			generators::from_regex(r"[A-Za-z0-9._/-]{1,24}")
				.fullmatch(true)
				.filter(|key| {
					key != NIX_CLOSURE_ANNOTATION_KEY && !key.starts_with(NIX_STORE_PATH_PREFIX)
				}),
			generators::text(),
		));

		let parsed = smol::block_on(ParsedNixMetadata::parse_annotations(
			&annotations.into_iter().collect::<BTreeMap<_, _>>(),
		))
		.expect("non-nix annotations should parse");

		assert_eq!(parsed, ParsedNixMetadata::None);
	}

	#[hegel::test(derandomize = true)]
	fn parse_annotations_dedups_and_sorts_legacy_store_paths(tc: hegel::TestCase) {
		let store_paths = tc.draw(generators::vecs(store_path()).min_size(1).max_size(12));
		let annotations = store_paths
			.iter()
			.enumerate()
			.map(|(index, path)| (format!("{NIX_STORE_PATH_PREFIX}{index}"), path.clone()))
			.collect::<BTreeMap<_, _>>();

		let parsed = smol::block_on(ParsedNixMetadata::parse_annotations(&annotations))
			.expect("legacy store path annotations should parse");
		let ParsedNixMetadata::LegacyStorePaths(parsed_paths) = parsed else {
			panic!("expected legacy store paths")
		};
		let mut expected = store_paths
			.into_iter()
			.map(PathBuf::from)
			.collect::<Vec<_>>();
		expected.sort();
		expected.dedup();

		assert_eq!(parsed_paths, expected);
	}

	#[hegel::test(derandomize = true)]
	fn closure_info_read_trims_blank_lines(tc: hegel::TestCase) {
		let closure_name = tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,12}").fullmatch(true));
		let entries = tc.draw(generators::vecs(store_path()).max_size(8));
		let temp_dir = tempfile::tempdir().expect("tempdir should exist");
		let closure_path = temp_dir.path().join(closure_name);
		smol::block_on(fs::create_dir_all(&closure_path)).expect("closure dir should be created");
		let file_contents = entries
			.iter()
			.map(|entry| format!("  {entry}  \n"))
			.collect::<String>()
			+ "\n\n";
		smol::block_on(fs::write(closure_path.join("store-paths"), file_contents))
			.expect("store-paths file should be written");

		let info = smol::block_on(ClosureInfo::read(&closure_path))
			.expect("absolute closure path should read");
		let expected = entries.into_iter().map(PathBuf::from).collect::<Vec<_>>();

		assert_eq!(info.closure_path, closure_path);
		assert_eq!(info.store_paths, expected);
	}

	#[hegel::test(derandomize = true)]
	fn closure_info_read_rejects_relative_paths(tc: hegel::TestCase) {
		let relative = tc.draw(
			generators::from_regex(r"[A-Za-z0-9._/-]{1,24}")
				.fullmatch(true)
				.filter(|path| !path.starts_with('/')),
		);

		let result = smol::block_on(ClosureInfo::read(PathBuf::from(relative.clone())));

		assert!(matches!(
			result,
			Err(NixMetadataError::InvalidClosurePath(path)) if path == relative
		));
	}
}
