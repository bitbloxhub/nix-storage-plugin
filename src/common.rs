use std::io;
use std::time::Duration;

use async_process::Command;
use bytes::Bytes;
use fuse3::FileType;
use fuse3::path::reply::FileAttr;
use http_body_util::Full;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::nix_metadata::NixMetadataError;

pub const TTL: Duration = Duration::from_secs(60);
pub const DEFAULT_REGISTRY_BIND_ADDR: &str = "127.0.0.1:45123";
// Additional Layer Store entries are keyed by image reference plus layer digest.

#[derive(Debug, Error)]
pub enum NixStoragePluginError {
	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	Hyper(#[from] hyper::Error),
	#[error(transparent)]
	AddrParse(#[from] std::net::AddrParseError),
	#[error(transparent)]
	Json(#[from] serde_json::Error),
	#[error(transparent)]
	OciSpec(#[from] oci_spec::OciSpecError),
	#[error(transparent)]
	Procfs(#[from] procfs::ProcError),
	#[error("host command failed: {command}: {stderr}")]
	HostCommandFailed { command: String, stderr: String },
	#[error("invalid image reference: {0}")]
	InvalidImageRef(String),
	#[error("invalid local storage state: {0}")]
	InvalidLocalStorageState(String),
	#[error("local layer {layer_id} for {image_ref} is in use: {reasons:?}")]
	LocalLayerInUse {
		image_ref: String,
		layer_id: String,
		reasons: Vec<String>,
	},
	#[error(transparent)]
	NixMetadata(#[from] NixMetadataError),
}

pub fn dir_attr(perm: u16) -> FileAttr {
	let now = std::time::SystemTime::now();

	FileAttr {
		size: 0,
		blocks: 0,
		atime: now,
		mtime: now,
		ctime: now,
		kind: FileType::Directory,
		perm,
		nlink: 2,
		uid: 0,
		gid: 0,
		rdev: 0,
		blksize: 4096,
	}
}

pub fn file_attr(size: usize, perm: u16) -> FileAttr {
	let now = std::time::SystemTime::now();

	FileAttr {
		size: size as u64,
		blocks: 1,
		atime: now,
		mtime: now,
		ctime: now,
		kind: FileType::RegularFile,
		perm,
		nlink: 1,
		uid: 0,
		gid: 0,
		rdev: 0,
		blksize: 4096,
	}
}

pub fn sha256_blob_file_name(digest: &str) -> Option<&str> {
	digest.strip_prefix("sha256:")
}

pub fn sha256_digest(bytes: impl AsRef<[u8]>) -> String {
	format!("sha256:{:x}", Sha256::digest(bytes.as_ref()))
}

pub fn simple_response(status: StatusCode, body: Bytes, empty: bool) -> Response<Full<Bytes>> {
	data_response(
		status,
		if empty { Bytes::new() } else { body },
		"text/plain; charset=utf-8",
		None,
	)
}

pub fn data_response(
	status: StatusCode,
	body: Bytes,
	content_type: &str,
	digest: Option<&str>,
) -> Response<Full<Bytes>> {
	let mut builder = Response::builder()
		.status(status)
		.header(CONTENT_TYPE, content_type)
		.header(CONTENT_LENGTH, body.len().to_string())
		.header("docker-distribution-api-version", "registry/2.0");

	if let Some(digest) = digest {
		builder = builder.header("docker-content-digest", digest)
	}

	builder.body(Full::new(body)).expect("response")
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
