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
