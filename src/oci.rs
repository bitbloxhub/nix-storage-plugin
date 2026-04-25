use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::OnceLock;

use oci_spec::image::Descriptor;

fn descriptor_annotations(layer: &Descriptor) -> &HashMap<String, String> {
	layer.annotations().as_ref().unwrap_or_else(|| {
		static EMPTY: OnceLock<HashMap<String, String>> = OnceLock::new();
		EMPTY.get_or_init(HashMap::new)
	})
}

pub(crate) fn descriptor_annotations_btree(layer: &Descriptor) -> BTreeMap<String, String> {
	descriptor_annotations(layer)
		.iter()
		.map(|(key, value)| (key.clone(), value.clone()))
		.collect()
}

pub(crate) fn archive_path_from_image_ref(image_ref: &str) -> Option<PathBuf> {
	let suffix = image_ref.strip_prefix("nix:0/")?;
	let archive_end = suffix.find(".tar")? + ".tar".len();
	Some(PathBuf::from(format!("/{}", &suffix[..archive_end])))
}

pub(crate) fn containers_storage_ref(image_ref: &str) -> String {
	let normalized = if image_ref
		.rsplit('/')
		.next()
		.is_some_and(|segment| segment.contains(':'))
	{
		image_ref.to_owned()
	} else {
		format!("{image_ref}:latest")
	};
	format!("containers-storage:{normalized}")
}

#[cfg(test)]
mod tests {
	use std::collections::{BTreeMap, HashMap};
	use std::path::PathBuf;
	use std::str::FromStr;

	use super::*;
	use hegel::generators::{self, Generator};
	use oci_spec::image::{DescriptorBuilder, Digest, MediaType};

	fn descriptor_with_annotations(annotations: Option<HashMap<String, String>>) -> Descriptor {
		let builder = DescriptorBuilder::default()
			.media_type(MediaType::ImageLayer)
			.size(1u64)
			.digest(
				Digest::from_str(
					"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				)
				.expect("digest should parse"),
			);
		let builder = if let Some(annotations) = annotations {
			builder.annotations(annotations)
		} else {
			builder
		};
		builder.build().expect("descriptor should build")
	}

	#[hegel::test(derandomize = true)]
	fn descriptor_annotations_btree_matches_original_map(tc: hegel::TestCase) {
		let annotations = tc.draw(generators::hashmaps(
			generators::from_regex(r"[A-Za-z0-9._/-]{1,16}").fullmatch(true),
			generators::text(),
		));
		let descriptor = descriptor_with_annotations(Some(annotations.clone()));
		let as_btree = descriptor_annotations_btree(&descriptor);

		assert_eq!(
			as_btree,
			annotations.into_iter().collect::<BTreeMap<_, _>>()
		);
	}

	#[test]
	fn descriptor_annotations_btree_returns_empty_map_when_absent() {
		let descriptor = descriptor_with_annotations(None);

		assert!(descriptor_annotations_btree(&descriptor).is_empty());
	}

	#[hegel::test(derandomize = true)]
	fn archive_path_from_image_ref_extracts_first_tar_suffix(tc: hegel::TestCase) {
		let prefix = tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{1,20}").fullmatch(true));
		let suffix = tc.draw(generators::text());
		let image_ref = format!("nix:0/{prefix}.tar{suffix}");

		assert_eq!(
			archive_path_from_image_ref(&image_ref),
			Some(PathBuf::from(format!("/{prefix}.tar")))
		);
	}

	#[hegel::test(derandomize = true)]
	fn archive_path_from_image_ref_rejects_non_nix_or_non_tar_refs(tc: hegel::TestCase) {
		let image_ref = tc.draw(
			generators::text()
				.filter(|value| !value.starts_with("nix:0/") || !value.contains(".tar")),
		);

		assert_eq!(archive_path_from_image_ref(&image_ref), None);
	}

	#[hegel::test(derandomize = true)]
	fn containers_storage_ref_adds_latest_only_when_last_segment_has_no_tag(tc: hegel::TestCase) {
		let repo = tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{1,24}").fullmatch(true));
		let tagged_last_segment = tc.draw(generators::booleans());
		let image_ref = if tagged_last_segment {
			format!("{repo}:tag")
		} else {
			repo.clone()
		};

		let storage_ref = containers_storage_ref(&image_ref);

		assert_eq!(
			storage_ref,
			if tagged_last_segment {
				format!("containers-storage:{repo}:tag")
			} else {
				format!("containers-storage:{repo}:latest")
			},
		);
	}
}
