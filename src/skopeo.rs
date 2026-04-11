use async_process::Command;
use tempfile::TempDir;

use crate::common::NixStoragePluginError;

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

pub(crate) async fn host_command(args: &[&str]) -> Result<String, NixStoragePluginError> {
	host_command_with_env(args, &[]).await
}

pub(crate) async fn host_command_with_env(
	args: &[&str],
	env: &[(&str, &str)],
) -> Result<String, NixStoragePluginError> {
	if args.is_empty() {
		return Err(NixStoragePluginError::InvalidLocalStorageState(
			"host command requested without any argv".to_owned(),
		));
	}
	let mut command = Command::new(args[0]);
	command.args(&args[1..]);
	for (key, value) in env {
		command.env(key, value);
	}
	let output = command.output().await?;
	if !output.status.success() {
		return Err(NixStoragePluginError::HostCommandFailed {
			command: args.join(" "),
			stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
		});
	}
	Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}
