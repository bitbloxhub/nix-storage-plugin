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

pub(crate) fn image_source_ref(image_ref: &str) -> String {
	archive_source_ref(image_ref).unwrap_or_else(|| containers_storage_ref(image_ref))
}

fn archive_source_ref(image_ref: &str) -> Option<String> {
	archive_path_from_image_ref(image_ref)
		.map(|archive_path| format!("oci-archive:{}", archive_path.display()))
}

fn archive_path_from_image_ref(image_ref: &str) -> Option<PathBuf> {
	let suffix = image_ref.strip_prefix("nix:0/")?;
	let archive_end = suffix.find(".tar")? + ".tar".len();
	Some(PathBuf::from(format!("/{}", &suffix[..archive_end])))
}

fn containers_storage_ref(image_ref: &str) -> String {
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
