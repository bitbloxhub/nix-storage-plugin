use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_signal::{Signal, Signals};
use clap::{Parser, Subcommand};
use fuse3::{MountOptions, path::Session};
use nix_storage_plugin::{
	DEFAULT_REGISTRY_BIND_ADDR, LayerStoreFS, LayerStoreResolver, NixStoragePluginError,
	encode_flake_ref, run_registry_server,
};
use smol::fs;
use smol::stream::StreamExt;

#[derive(Parser)]
#[command(version, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[derive(Subcommand)]
enum Commands {
	MountStore {
		#[arg(long, default_value_os_t = default_mount_path())]
		mount_path: PathBuf,
	},
	ServeImage {
		#[arg(long, default_value = DEFAULT_REGISTRY_BIND_ADDR)]
		bind: SocketAddr,
	},
	EncodeFlakeRef {
		flake_ref: String,
	},
}

fn default_mount_path() -> PathBuf {
	if let Some(runtime_dir) = env::var_os("XDG_RUNTIME_DIR") {
		return PathBuf::from(runtime_dir).join("nix-storage-plugin/layer-store");
	}

	if nix::unistd::getuid().is_root() {
		return PathBuf::from("/run/nix-storage-plugin/layer-store");
	}

	PathBuf::from(format!(
		"/run/user/{}/nix-storage-plugin/layer-store",
		nix::unistd::getuid()
	))
}

fn main() -> Result<(), NixStoragePluginError> {
	smol::block_on(async {
		tracing_subscriber::fmt::init();

		match Cli::parse().command {
			Commands::MountStore { mount_path } => mount_store(mount_path).await,
			Commands::ServeImage { bind } => run_registry_server(bind).await,
			Commands::EncodeFlakeRef { flake_ref } => {
				println!("{}", encode_flake_ref(&flake_ref)?);
				Ok(())
			}
		}
	})
}

async fn mount_store(mount_path: PathBuf) -> Result<(), NixStoragePluginError> {
	let uid = nix::unistd::getuid();
	let gid = nix::unistd::getgid();

	if let Some(parent) = mount_path.parent()
		&& !parent.as_os_str().is_empty()
	{
		fs::create_dir_all(parent).await?;
	}
	fs::create_dir_all(&mount_path).await?;

	let mut mount_options = MountOptions::default();
	mount_options
		.uid(uid.into())
		.gid(gid.into())
		.fs_name("nix-storage-plugin-layer-store");

	let fs = LayerStoreFS::new(Arc::new(LayerStoreResolver::new()));
	let handle = Session::new(mount_options)
		.mount_with_unprivileged(fs, mount_path)
		.await?;

	let mut signals = Signals::new([Signal::Term, Signal::Quit, Signal::Int])?;
	if signals.next().await.is_some() {
		handle.unmount().await?;
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn with_env_var<T>(name: &str, value: Option<&str>, f: impl FnOnce() -> T) -> T {
		let previous = env::var_os(name);
		match value {
			Some(value) => unsafe { env::set_var(name, value) },
			None => unsafe { env::remove_var(name) },
		}
		let result = f();
		match previous {
			Some(value) => unsafe { env::set_var(name, value) },
			None => unsafe { env::remove_var(name) },
		}
		result
	}

	#[hegel::test(derandomize = true)]
	fn default_mount_path_prefers_xdg_runtime_dir(tc: hegel::TestCase) {
		let runtime_dir =
			tc.draw(hegel::generators::from_regex(r"/[A-Za-z0-9._/-]{1,24}").fullmatch(true));

		let mount_path = with_env_var("XDG_RUNTIME_DIR", Some(&runtime_dir), default_mount_path);

		assert_eq!(
			mount_path,
			PathBuf::from(runtime_dir).join("nix-storage-plugin/layer-store")
		);
	}

	#[hegel::test(derandomize = true)]
	fn cli_encode_flake_ref_subcommand_parses_input(tc: hegel::TestCase) {
		let flake_ref =
			tc.draw(hegel::generators::from_regex(r"github:[A-Za-z0-9._/-]{1,32}").fullmatch(true));
		let cli =
			Cli::try_parse_from(["nix-storage-plugin", "encode-flake-ref", flake_ref.as_str()])
				.expect("valid encode-flake-ref command should parse");

		assert!(matches!(
			cli.command,
			Commands::EncodeFlakeRef { flake_ref: parsed } if parsed == flake_ref
		));
	}

	#[hegel::test(derandomize = true)]
	fn cli_serve_image_subcommand_parses_socket_addr(tc: hegel::TestCase) {
		let port = tc.draw(hegel::generators::integers::<u16>().min_value(1));
		let bind = format!("127.0.0.1:{port}");
		let cli =
			Cli::try_parse_from(["nix-storage-plugin", "serve-image", "--bind", bind.as_str()])
				.expect("valid serve-image command should parse");

		assert!(matches!(
			cli.command,
			Commands::ServeImage { bind: parsed } if parsed == bind.parse().expect("socket addr should parse")
		));
	}

	#[hegel::test(derandomize = true)]
	fn cli_mount_store_subcommand_parses_path(tc: hegel::TestCase) {
		let mount_path =
			tc.draw(hegel::generators::from_regex(r"/[A-Za-z0-9._/-]{1,32}").fullmatch(true));
		let cli = Cli::try_parse_from([
			"nix-storage-plugin",
			"mount-store",
			"--mount-path",
			mount_path.as_str(),
		])
		.expect("valid mount-store command should parse");

		assert!(matches!(
			cli.command,
			Commands::MountStore { mount_path: parsed } if parsed == PathBuf::from(mount_path)
		));
	}

	#[hegel::test(derandomize = true)]
	fn default_mount_path_falls_back_to_uid_path_without_xdg_runtime_dir(_: hegel::TestCase) {
		let mount_path = with_env_var("XDG_RUNTIME_DIR", None, default_mount_path);

		assert_eq!(
			mount_path,
			PathBuf::from(format!(
				"/run/user/{}/nix-storage-plugin/layer-store",
				nix::unistd::getuid()
			)),
		);
	}

	#[hegel::test(derandomize = true)]
	fn cli_serve_image_subcommand_uses_default_bind(_: hegel::TestCase) {
		let cli = Cli::try_parse_from(["nix-storage-plugin", "serve-image"])
			.expect("serve-image without bind should parse");

		assert!(matches!(
			cli.command,
			Commands::ServeImage { bind } if bind.to_string() == DEFAULT_REGISTRY_BIND_ADDR
		));
	}

	#[test]
	fn cli_mount_store_subcommand_uses_default_mount_path() {
		let expected = with_env_var(
			"XDG_RUNTIME_DIR",
			Some("/tmp/pi-main-default"),
			default_mount_path,
		);
		let cli = with_env_var("XDG_RUNTIME_DIR", Some("/tmp/pi-main-default"), || {
			Cli::try_parse_from(["nix-storage-plugin", "mount-store"])
		})
		.expect("mount-store without path should parse");

		assert!(matches!(
			cli.command,
			Commands::MountStore { mount_path } if mount_path == expected
		));
	}
}
