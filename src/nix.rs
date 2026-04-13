use std::path::Path;

use crate::common::host_command;

pub(crate) async fn try_realize_nix_archive_path(path: &Path) {
	if !path.starts_with("/nix/store/") {
		return;
	}

	let path_string = path.to_string_lossy().into_owned();
	if let Err(error) = host_command(&[
		"nix",
		"build",
		"--no-link",
		"--extra-experimental-features",
		"nix-command",
		"--",
		&path_string,
	])
	.await
	{
		tracing::debug!(%error, archive = %path.display(), "nix archive path not realized after substitution attempt");
	}
}
