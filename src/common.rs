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
	dir_attr_owned(perm, 0, 0)
}

pub fn dir_attr_owned(perm: u16, uid: u32, gid: u32) -> FileAttr {
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
		uid,
		gid,
		rdev: 0,
		blksize: 4096,
	}
}

pub fn file_attr(size: usize, perm: u16) -> FileAttr {
	file_attr_owned(size, perm, 0, 0)
}

pub fn file_attr_owned(size: usize, perm: u16, uid: u32, gid: u32) -> FileAttr {
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
		uid,
		gid,
		rdev: 0,
		blksize: 4096,
	}
}

pub fn sha256_blob_file_name(digest: &str) -> Option<&str> {
	digest.strip_prefix("sha256:")
}

pub fn sha256_digest(bytes: impl AsRef<[u8]>) -> String {
	format!(
		"sha256:{}",
		base16ct::lower::encode_string(&Sha256::digest(bytes.as_ref()))
	)
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

#[cfg(test)]
mod tests {
	use super::*;
	use hegel::generators::{self, Generator};

	fn shell_single_quote(value: &str) -> String {
		format!("'{}'", value.replace('\'', "'\"'\"'"))
	}

	fn status_codes() -> impl hegel::generators::Generator<StatusCode> {
		generators::sampled_from(vec![
			StatusCode::OK,
			StatusCode::ACCEPTED,
			StatusCode::BAD_REQUEST,
			StatusCode::NOT_FOUND,
			StatusCode::INTERNAL_SERVER_ERROR,
		])
	}

	#[hegel::test(derandomize = true)]
	fn sha256_digest_roundtrips_to_blob_name(tc: hegel::TestCase) {
		let bytes = tc.draw(generators::binary());
		let digest = sha256_digest(&bytes);
		let blob_name =
			sha256_blob_file_name(&digest).expect("sha256 digest should have blob file name");

		assert_eq!(digest, format!("sha256:{blob_name}"));
		assert_eq!(blob_name.len(), 64);
		assert!(
			blob_name
				.chars()
				.all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
		);
	}

	#[hegel::test(derandomize = true)]
	fn sha256_blob_file_name_returns_none_for_non_sha256_prefix(tc: hegel::TestCase) {
		let digest = tc.draw(generators::text().filter(|value| !value.starts_with("sha256:")));

		assert_eq!(sha256_blob_file_name(&digest), None);
	}

	#[hegel::test(derandomize = true)]
	fn data_response_sets_headers_consistently(tc: hegel::TestCase) {
		let status = tc.draw(status_codes());
		let body = Bytes::from(tc.draw(generators::binary()));
		let content_type = tc.draw(generators::sampled_from(vec![
			"application/json",
			"application/octet-stream",
			"text/plain; charset=utf-8",
		]));
		let digest = tc.draw(generators::optional(
			generators::from_regex(r"[A-Za-z0-9._:-]{1,40}").fullmatch(true),
		));
		let response = data_response(status, body.clone(), content_type, digest.as_deref());

		assert_eq!(response.status(), status);
		assert_eq!(response.headers()[CONTENT_TYPE], content_type);
		assert_eq!(response.headers()[CONTENT_LENGTH], body.len().to_string());
		assert_eq!(
			response.headers()["docker-distribution-api-version"],
			"registry/2.0"
		);
		assert_eq!(
			response
				.headers()
				.get("docker-content-digest")
				.map(|value| value.to_str().expect("header should be utf-8")),
			digest.as_deref(),
		);
	}

	#[hegel::test(derandomize = true)]
	fn simple_response_empty_flag_controls_content_length(tc: hegel::TestCase) {
		let status = tc.draw(status_codes());
		let body = Bytes::from(tc.draw(generators::binary()));
		let empty = tc.draw(generators::booleans());
		let response = simple_response(status, body.clone(), empty);

		assert_eq!(response.status(), status);
		assert_eq!(
			response.headers()[CONTENT_TYPE],
			"text/plain; charset=utf-8"
		);
		assert_eq!(
			response.headers()[CONTENT_LENGTH],
			if empty {
				"0".to_owned()
			} else {
				body.len().to_string()
			},
		);
		assert!(response.headers().get("docker-content-digest").is_none());
	}

	#[hegel::test(derandomize = true)]
	fn owned_attrs_preserve_permissions_and_ownership(tc: hegel::TestCase) {
		let size = tc.draw(generators::integers::<u16>()) as usize;
		let perm = tc.draw(generators::integers::<u16>());
		let uid = tc.draw(generators::integers::<u32>());
		let gid = tc.draw(generators::integers::<u32>());

		let dir = dir_attr_owned(perm, uid, gid);
		assert_eq!(dir.kind, FileType::Directory);
		assert_eq!(dir.perm, perm);
		assert_eq!(dir.size, 0);
		assert_eq!(dir.blocks, 0);
		assert_eq!(dir.nlink, 2);
		assert_eq!(dir.uid, uid);
		assert_eq!(dir.gid, gid);
		assert_eq!(dir.blksize, 4096);

		let file = file_attr_owned(size, perm, uid, gid);
		assert_eq!(file.kind, FileType::RegularFile);
		assert_eq!(file.size, size as u64);
		assert_eq!(file.blocks, 1);
		assert_eq!(file.perm, perm);
		assert_eq!(file.nlink, 1);
		assert_eq!(file.uid, uid);
		assert_eq!(file.gid, gid);
		assert_eq!(file.blksize, 4096);
	}

	#[test]
	fn host_command_with_env_rejects_empty_argv() {
		let result = smol::block_on(host_command_with_env(&[], &[]));

		assert!(matches!(
			result,
			Err(NixStoragePluginError::InvalidLocalStorageState(message)) if message.contains("without any argv")
		));
	}

	#[hegel::test(derandomize = true)]
	fn host_command_with_env_runs_command_and_passes_env(tc: hegel::TestCase) {
		let value = tc.draw(generators::from_regex(r"[^\u0000]{0,32}").fullmatch(true));
		let result = smol::block_on(host_command_with_env(
			&["sh", "-c", "printf %s \"$NSP_TEST_VALUE\""],
			&[("NSP_TEST_VALUE", &value)],
		))
		.expect("command should succeed");

		assert_eq!(result, value);
	}

	#[hegel::test(derandomize = true)]
	fn host_command_returns_trimmed_stderr_on_failure(tc: hegel::TestCase) {
		let stderr = tc.draw(generators::from_regex(r"[^\u0000\n]{1,32}").fullmatch(true));
		let result = smol::block_on(host_command(&[
			"sh",
			"-c",
			&format!("printf %s {} >&2; exit 7", shell_single_quote(&stderr)),
		]));

		assert!(matches!(
			result,
			Err(NixStoragePluginError::HostCommandFailed { stderr: actual, .. }) if actual == stderr.trim()
		));
	}

	#[hegel::test(derandomize = true)]
	fn host_command_returns_stdout_on_success(tc: hegel::TestCase) {
		let stdout = tc.draw(generators::from_regex(r"[^\u0000]{0,32}").fullmatch(true));
		let result = smol::block_on(host_command(&[
			"sh",
			"-c",
			&format!("printf %s {}", shell_single_quote(&stdout)),
		]))
		.expect("command should succeed");

		assert_eq!(result, stdout);
	}
}
