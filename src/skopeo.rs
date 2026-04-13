use tempfile::TempDir;

use crate::common::{NixStoragePluginError, host_command_with_env};
pub(crate) async fn export_source_to_temp_dir(
	source: &str,
	prefix: &str,
	env: &[(&str, &str)],
) -> Result<TempDir, NixStoragePluginError> {
	let export_dir = TempDir::with_prefix(prefix)?;
	let export_dir_string = export_dir.path().to_string_lossy().into_owned();
	host_command_with_env(
		&[
			"skopeo",
			"copy",
			source,
			&format!("dir:{export_dir_string}"),
		],
		env,
	)
	.await?;
	Ok(export_dir)
}

pub(crate) async fn inspect_manifest_raw(
	source: &str,
	env: &[(&str, &str)],
) -> Result<String, NixStoragePluginError> {
	host_command_with_env(&["skopeo", "inspect", "--raw", source], env).await
}

pub(crate) async fn inspect_config_raw(
	source: &str,
	env: &[(&str, &str)],
) -> Result<String, NixStoragePluginError> {
	host_command_with_env(&["skopeo", "inspect", "--config", source], env).await
}
