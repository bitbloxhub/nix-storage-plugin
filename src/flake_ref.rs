use crate::common::NixStoragePluginError;

const FLAKE_REGISTRY_PREFIXES_LOG_VALUE: &str = "flake-github:0, flake-tarball-https:0, flake-tarball-http:0, flake-git-https:0, flake-git-http:0, flake-git-ssh:0";

#[derive(Debug, Clone, Copy)]
enum FlakeProtocol {
	Github,
	TarballHttps,
	TarballHttp,
	GitHttps,
	GitHttp,
	GitSsh,
}

impl FlakeProtocol {
	fn parse_repo(repo: &str) -> Option<(Self, &str)> {
		for (prefix, protocol) in [
			("flake-github/", Self::Github),
			("flake-tarball-https/", Self::TarballHttps),
			("flake-tarball-http/", Self::TarballHttp),
			("flake-git-https/", Self::GitHttps),
			("flake-git-http/", Self::GitHttp),
			("flake-git-ssh/", Self::GitSsh),
		] {
			if let Some(encoded_ref) = repo.strip_prefix(prefix) {
				return Some((protocol, encoded_ref));
			}
		}

		None
	}

	fn parse_flake_ref(flake_ref: &str) -> Option<(Self, &str)> {
		for (prefix, protocol) in [
			("github:", Self::Github),
			("tarball+https://", Self::TarballHttps),
			("tarball+http://", Self::TarballHttp),
			("git+https://", Self::GitHttps),
			("git+http://", Self::GitHttp),
			("git+ssh://", Self::GitSsh),
		] {
			if let Some(suffix) = flake_ref.strip_prefix(prefix) {
				return Some((protocol, suffix));
			}
		}

		None
	}

	fn flake_url_prefix(self) -> &'static str {
		match self {
			Self::Github => "github:",
			Self::TarballHttps => "tarball+https://",
			Self::TarballHttp => "tarball+http://",
			Self::GitHttps => "git+https://",
			Self::GitHttp => "git+http://",
			Self::GitSsh => "git+ssh://",
		}
	}

	fn registry_prefix(self) -> &'static str {
		match self {
			Self::Github => "flake-github:0",
			Self::TarballHttps => "flake-tarball-https:0",
			Self::TarballHttp => "flake-tarball-http:0",
			Self::GitHttps => "flake-git-https:0",
			Self::GitHttp => "flake-git-http:0",
			Self::GitSsh => "flake-git-ssh:0",
		}
	}
}

pub(crate) fn decode_flake_installable_from_repo(
	repo: &str,
) -> Result<Option<String>, NixStoragePluginError> {
	let Some((protocol, encoded_ref)) = FlakeProtocol::parse_repo(repo) else {
		return Ok(None);
	};
	if encoded_ref.is_empty() {
		return Err(NixStoragePluginError::InvalidImageRef(
			protocol.registry_prefix().to_owned(),
		));
	}

	let decoded_ref = decode_special_chars(encoded_ref)?;
	let installable = format!("{}{}", protocol.flake_url_prefix(), decoded_ref);
	if installable.contains('#') {
		return Ok(Some(installable));
	}

	Ok(Some(format!("{installable}#default")))
}

pub fn encode_flake_ref(flake_ref: &str) -> Result<String, NixStoragePluginError> {
	let Some((protocol, suffix)) = FlakeProtocol::parse_flake_ref(flake_ref) else {
		return Err(NixStoragePluginError::InvalidImageRef(format!(
			"unsupported flake protocol for encode-flake-ref: {flake_ref}",
		)));
	};
	if suffix.is_empty() {
		return Err(NixStoragePluginError::InvalidImageRef(format!(
			"flake ref is missing path: {flake_ref}",
		)));
	}
	let encoded_ref = encode_special_chars(suffix);
	if let Some(invalid) = encoded_ref
		.chars()
		.find(|ch| !is_valid_encoded_repo_char(*ch))
	{
		return Err(NixStoragePluginError::InvalidImageRef(format!(
			"flake ref contains unsupported OCI repository character after encoding: {invalid} in {flake_ref}",
		)));
	}

	Ok(format!("{}/{}", protocol.registry_prefix(), encoded_ref))
}

pub(crate) fn flake_registry_prefixes_log_value() -> &'static str {
	FLAKE_REGISTRY_PREFIXES_LOG_VALUE
}

fn encode_special_chars(value: &str) -> String {
	let mut encoded = String::new();

	for ch in value.chars() {
		if is_valid_unescaped_repo_char(ch) {
			encoded.push(ch)
		} else {
			for byte in ch.encode_utf8(&mut [0; 4]).as_bytes() {
				encoded.push_str(&format!("--x{byte:02x}--"))
			}
		}
	}

	encoded
}

fn decode_special_chars(value: &str) -> Result<String, NixStoragePluginError> {
	let mut decoded = String::new();
	let mut decoded_bytes = Vec::new();
	let mut remaining = value;

	while !remaining.is_empty() {
		if let Some((byte, rest)) = decode_hex_escape(remaining) {
			decoded_bytes.push(byte);
			remaining = rest;
			continue;
		}

		flush_decoded_bytes(&mut decoded, &mut decoded_bytes)?;

		let Some(ch) = remaining.chars().next() else {
			break;
		};
		decoded.push(ch);
		remaining = &remaining[ch.len_utf8()..];
	}

	flush_decoded_bytes(&mut decoded, &mut decoded_bytes)?;
	Ok(decoded)
}

fn decode_hex_escape(value: &str) -> Option<(u8, &str)> {
	let rest = value.strip_prefix("--x")?;
	if rest.len() < 4 {
		return None;
	}
	let hex = &rest[..2];
	let rest = rest.strip_prefix(hex)?.strip_prefix("--")?;
	let byte = u8::from_str_radix(hex, 16).ok()?;
	Some((byte, rest))
}

fn flush_decoded_bytes(
	decoded: &mut String,
	decoded_bytes: &mut Vec<u8>,
) -> Result<(), NixStoragePluginError> {
	if decoded_bytes.is_empty() {
		return Ok(());
	}

	let chunk = String::from_utf8(decoded_bytes.clone()).map_err(|error| {
		NixStoragePluginError::InvalidImageRef(format!("invalid encoded flake ref bytes: {error}",))
	})?;
	decoded.push_str(&chunk);
	decoded_bytes.clear();
	Ok(())
}

fn is_valid_encoded_repo_char(ch: char) -> bool {
	ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-' | '/')
}

fn is_valid_unescaped_repo_char(ch: char) -> bool {
	ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '/')
}
