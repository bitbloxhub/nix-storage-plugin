use tempfile::TempDir;

use crate::common::{NixStoragePluginError, host_command_with_env};

fn skopeo_copy_args(source: &str, dest_dir: &str) -> Vec<String> {
	vec![
		"skopeo".to_owned(),
		"copy".to_owned(),
		source.to_owned(),
		format!("dir:{dest_dir}"),
	]
}

fn skopeo_inspect_manifest_args(source: &str) -> Vec<String> {
	vec![
		"skopeo".to_owned(),
		"inspect".to_owned(),
		"--raw".to_owned(),
		source.to_owned(),
	]
}

fn skopeo_inspect_config_args(source: &str) -> Vec<String> {
	vec![
		"skopeo".to_owned(),
		"inspect".to_owned(),
		"--config".to_owned(),
		source.to_owned(),
	]
}

pub(crate) async fn export_source_to_temp_dir(
	source: &str,
	prefix: &str,
	env: &[(&str, &str)],
) -> Result<TempDir, NixStoragePluginError> {
	let export_dir = TempDir::with_prefix(prefix)?;
	let export_dir_string = export_dir.path().to_string_lossy().into_owned();
	let args = skopeo_copy_args(source, &export_dir_string);
	let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
	host_command_with_env(&arg_refs, env).await?;
	Ok(export_dir)
}

pub(crate) async fn inspect_manifest_raw(
	source: &str,
	env: &[(&str, &str)],
) -> Result<String, NixStoragePluginError> {
	let args = skopeo_inspect_manifest_args(source);
	let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
	host_command_with_env(&arg_refs, env).await
}

pub(crate) async fn inspect_config_raw(
	source: &str,
	env: &[(&str, &str)],
) -> Result<String, NixStoragePluginError> {
	let args = skopeo_inspect_config_args(source);
	let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
	host_command_with_env(&arg_refs, env).await
}

#[cfg(test)]
mod tests {
	use super::*;
	use hegel::generators::{self};

	#[hegel::test(derandomize = true)]
	fn skopeo_copy_args_formats_dir_destination(tc: hegel::TestCase) {
		let source = tc.draw(generators::text());
		let dest_dir = tc.draw(generators::from_regex(r"/[A-Za-z0-9._/-]{1,32}").fullmatch(true));

		assert_eq!(
			skopeo_copy_args(&source, &dest_dir),
			vec![
				"skopeo".to_owned(),
				"copy".to_owned(),
				source,
				format!("dir:{dest_dir}"),
			],
		);
	}

	#[hegel::test(derandomize = true)]
	fn skopeo_inspect_manifest_args_match_raw_inspect_contract(tc: hegel::TestCase) {
		let source = tc.draw(generators::text());

		assert_eq!(
			skopeo_inspect_manifest_args(&source),
			vec![
				"skopeo".to_owned(),
				"inspect".to_owned(),
				"--raw".to_owned(),
				source,
			],
		);
	}

	#[hegel::test(derandomize = true)]
	fn skopeo_inspect_config_args_match_config_inspect_contract(tc: hegel::TestCase) {
		let source = tc.draw(generators::text());

		assert_eq!(
			skopeo_inspect_config_args(&source),
			vec![
				"skopeo".to_owned(),
				"inspect".to_owned(),
				"--config".to_owned(),
				source,
			],
		);
	}
}
