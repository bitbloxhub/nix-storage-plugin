use std::path::Path;

use crate::common::host_command;

fn nix_realize_command_args(path: &Path) -> Option<Vec<String>> {
	if !path.starts_with("/nix/store/") {
		return None;
	}

	Some(vec![
		"nix".to_owned(),
		"build".to_owned(),
		"--no-link".to_owned(),
		"--extra-experimental-features".to_owned(),
		"nix-command".to_owned(),
		"--".to_owned(),
		path.to_string_lossy().into_owned(),
	])
}

pub(crate) async fn try_realize_nix_archive_path(path: &Path) {
	try_realize_nix_archive_path_with(path, |args| async move {
		let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
		host_command(&arg_refs).await
	})
	.await
}

async fn try_realize_nix_archive_path_with<F, Fut>(path: &Path, run: F)
where
	F: FnOnce(Vec<String>) -> Fut,
	Fut: std::future::Future<Output = Result<String, crate::common::NixStoragePluginError>>,
{
	let Some(args) = nix_realize_command_args(path) else {
		return;
	};
	if let Err(error) = run(args).await {
		tracing::debug!(%error, archive = %path.display(), "nix archive path not realized after substitution attempt");
	}
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use super::*;
	use hegel::generators::{self};

	#[hegel::test(derandomize = true)]
	fn nix_realize_command_args_only_builds_for_nix_store_paths(tc: hegel::TestCase) {
		let in_store = tc.draw(generators::booleans());
		let name = tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true));
		let ext = tc.draw(generators::sampled_from(vec!["tar", "txt", ""]));
		let suffix = if ext.is_empty() {
			name.clone()
		} else {
			format!("{name}.{ext}")
		};
		let path = if in_store {
			PathBuf::from(format!("/nix/store/{suffix}"))
		} else {
			PathBuf::from(format!("/tmp/{suffix}"))
		};
		let args = nix_realize_command_args(&path);

		if in_store {
			let args = args.expect("nix store path should build command");
			assert_eq!(
				args,
				vec![
					"nix".to_owned(),
					"build".to_owned(),
					"--no-link".to_owned(),
					"--extra-experimental-features".to_owned(),
					"nix-command".to_owned(),
					"--".to_owned(),
					path.to_string_lossy().into_owned(),
				],
			);
		} else {
			assert!(args.is_none());
		}
	}

	#[hegel::test(derandomize = true)]
	fn try_realize_nix_archive_path_with_skips_non_store_paths(tc: hegel::TestCase) {
		let path = PathBuf::from(format!(
			"/tmp/{}",
			tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
		));
		let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
		smol::block_on(try_realize_nix_archive_path_with(&path, {
			let called = called.clone();
			move |_| {
				called.store(true, std::sync::atomic::Ordering::SeqCst);
				async { Ok(String::new()) }
			}
		}));
		assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
	}

	#[hegel::test(derandomize = true)]
	fn try_realize_nix_archive_path_with_runs_and_tolerates_errors_for_store_paths(
		tc: hegel::TestCase,
	) {
		let path = PathBuf::from(format!(
			"/nix/store/{}.tar",
			tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
		));
		let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
		smol::block_on(try_realize_nix_archive_path_with(&path, {
			let calls = calls.clone();
			let expected = path.to_string_lossy().into_owned();
			move |args| {
				calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
				assert_eq!(args.last().map(String::as_str), Some(expected.as_str()));
				async {
					Err(crate::common::NixStoragePluginError::InvalidImageRef(
						"fail".to_owned(),
					))
				}
			}
		}));
		assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
	}

	#[hegel::test(derandomize = true)]
	fn try_realize_nix_archive_path_public_wrapper_skips_non_store_paths(tc: hegel::TestCase) {
		let path = PathBuf::from(format!(
			"/tmp/{}",
			tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
		));
		smol::block_on(try_realize_nix_archive_path(&path));
	}
}
