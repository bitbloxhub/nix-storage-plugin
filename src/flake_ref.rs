use crate::common::NixStoragePluginError;

const FLAKE_REGISTRY_PREFIXES_LOG_VALUE: &str = "flake-github:0, flake-tarball-https:0, flake-tarball-http:0, flake-git-https:0, flake-git-http:0, flake-git-ssh:0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
	use super::*;
	use hegel::generators::{self, Generator};

	#[hegel::composite]
	fn supported_flake_ref(tc: hegel::TestCase) -> String {
		let protocol = tc.draw(generators::sampled_from(vec![
			"github:",
			"tarball+https://",
			"tarball+http://",
			"git+https://",
			"git+http://",
			"git+ssh://",
		]));
		let suffix = tc.draw(generators::text().min_size(1));
		format!("{protocol}{suffix}")
	}

	#[hegel::composite]
	fn supported_flake_ref_without_fragment(tc: hegel::TestCase) -> String {
		let protocol = tc.draw(generators::sampled_from(vec![
			"github:",
			"tarball+https://",
			"tarball+http://",
			"git+https://",
			"git+http://",
			"git+ssh://",
		]));
		let suffix = tc.draw(
			generators::text()
				.min_size(1)
				.filter(|suffix| !suffix.contains('#')),
		);
		format!("{protocol}{suffix}")
	}

	#[hegel::composite]
	fn supported_flake_ref_with_fragment(tc: hegel::TestCase) -> String {
		let protocol = tc.draw(generators::sampled_from(vec![
			"github:",
			"tarball+https://",
			"tarball+http://",
			"git+https://",
			"git+http://",
			"git+ssh://",
		]));
		let suffix = tc.draw(
			generators::text()
				.min_size(1)
				.filter(|suffix| !suffix.contains('#')),
		);
		let fragment = tc.draw(generators::text());
		format!("{protocol}{suffix}#{fragment}")
	}

	fn repo_path_from_encoded_image_ref(image_ref: &str) -> String {
		image_ref.replacen(":0/", "/", 1)
	}

	#[hegel::test(derandomize = true)]
	fn encode_output_uses_only_oci_repo_chars(tc: hegel::TestCase) {
		let flake_ref = tc.draw(supported_flake_ref());
		let encoded_image_ref =
			encode_flake_ref(&flake_ref).expect("supported flake ref should encode");
		let repo_path = repo_path_from_encoded_image_ref(&encoded_image_ref);
		let (protocol, _) = FlakeProtocol::parse_flake_ref(&flake_ref)
			.expect("generator should use supported protocol");
		let repo_suffix = repo_path
			.strip_prefix(&format!(
				"{}/",
				protocol.registry_prefix().replacen(":0", "", 1)
			))
			.expect("encoded image ref should use matching repo prefix");

		assert!(!repo_suffix.is_empty());
		assert!(repo_suffix.chars().all(is_valid_encoded_repo_char));
	}

	#[hegel::test(derandomize = true)]
	fn encode_then_decode_roundtrips_when_fragment_present(tc: hegel::TestCase) {
		let flake_ref = tc.draw(supported_flake_ref_with_fragment());
		let encoded_image_ref =
			encode_flake_ref(&flake_ref).expect("supported flake ref should encode");
		let repo_path = repo_path_from_encoded_image_ref(&encoded_image_ref);
		let decoded = decode_flake_installable_from_repo(&repo_path)
			.expect("encoded repo should decode without error")
			.expect("encoded repo should be recognized as flake repo");

		assert_eq!(decoded, flake_ref);
	}

	#[hegel::test(derandomize = true)]
	fn encode_then_decode_adds_default_fragment_when_missing(tc: hegel::TestCase) {
		let flake_ref = tc.draw(supported_flake_ref_without_fragment());
		let encoded_image_ref =
			encode_flake_ref(&flake_ref).expect("supported flake ref should encode");
		let repo_path = repo_path_from_encoded_image_ref(&encoded_image_ref);
		let decoded = decode_flake_installable_from_repo(&repo_path)
			.expect("encoded repo should decode without error")
			.expect("encoded repo should be recognized as flake repo");

		assert_eq!(decoded, format!("{flake_ref}#default"));
	}

	#[hegel::test(derandomize = true)]
	fn decode_rejects_invalid_utf8_escape_bytes(tc: hegel::TestCase) {
		let byte = tc.draw(generators::integers::<u8>().min_value(0x80).max_value(0xff));
		let repo = format!("flake-github/--x{byte:02x}--");

		assert!(matches!(
			decode_flake_installable_from_repo(&repo),
			Err(NixStoragePluginError::InvalidImageRef(_))
		));
	}

	#[hegel::test(derandomize = true)]
	fn decode_returns_none_for_non_flake_repos(tc: hegel::TestCase) {
		let repo = format!(
			"other/{}/{}",
			tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,16}").fullmatch(true)),
			tc.draw(generators::text()),
		);

		assert_eq!(
			decode_flake_installable_from_repo(&repo).expect("non-flake repo should not error"),
			None,
		);
	}

	#[hegel::test(derandomize = true)]
	fn decode_rejects_empty_encoded_repo_suffix(tc: hegel::TestCase) {
		let protocol = tc.draw(generators::sampled_from(vec![
			"flake-github",
			"flake-tarball-https",
			"flake-tarball-http",
			"flake-git-https",
			"flake-git-http",
			"flake-git-ssh",
		]));
		let repo = format!("{protocol}/");

		assert!(matches!(
			decode_flake_installable_from_repo(&repo),
			Err(NixStoragePluginError::InvalidImageRef(message)) if message == format!("{protocol}:0")
		));
	}

	#[hegel::test(derandomize = true)]
	fn encode_rejects_unsupported_protocols_and_missing_paths(tc: hegel::TestCase) {
		let unsupported = format!(
			"{}:{}",
			tc.draw(generators::from_regex(r"[a-z]{2,12}").fullmatch(true).filter(
				|value| !["github", "tarball+https", "tarball+http", "git+https", "git+http", "git+ssh"]
					.contains(&value.as_str()),
			)),
			tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{1,16}").fullmatch(true)),
		);
		let missing_path = tc.draw(generators::sampled_from(vec![
			"github:",
			"tarball+https://",
			"tarball+http://",
			"git+https://",
			"git+http://",
			"git+ssh://",
		]));

		assert!(matches!(
			encode_flake_ref(&unsupported),
			Err(NixStoragePluginError::InvalidImageRef(message)) if message.contains("unsupported flake protocol")
		));
		assert!(matches!(
			encode_flake_ref(missing_path),
			Err(NixStoragePluginError::InvalidImageRef(message)) if message.contains("missing path")
		));
	}

	#[hegel::test(derandomize = true)]
	fn encode_special_chars_roundtrips_through_decode_special_chars(tc: hegel::TestCase) {
		let value = tc.draw(generators::text());
		let encoded = encode_special_chars(&value);
		let decoded = decode_special_chars(&encoded).expect("encoded text should decode");

		assert_eq!(decoded, value);
	}

	#[hegel::test(derandomize = true)]
	fn decode_hex_escape_parses_exact_escape_prefix(tc: hegel::TestCase) {
		let byte = tc.draw(generators::integers::<u8>());
		let rest = tc.draw(generators::text());
		let value = format!("--x{byte:02x}--{rest}");

		assert_eq!(decode_hex_escape(&value), Some((byte, rest.as_str())));
	}

	#[hegel::test(derandomize = true)]
	fn flake_protocol_parse_and_prefix_helpers_agree(tc: hegel::TestCase) {
		let protocol = tc.draw(generators::sampled_from(vec![
			FlakeProtocol::Github,
			FlakeProtocol::TarballHttps,
			FlakeProtocol::TarballHttp,
			FlakeProtocol::GitHttps,
			FlakeProtocol::GitHttp,
			FlakeProtocol::GitSsh,
		]));
		let suffix = tc.draw(generators::text().min_size(1));
		let flake_ref = format!("{}{}", protocol.flake_url_prefix(), suffix);
		let repo = format!(
			"{}/{}",
			protocol.registry_prefix().replacen(":0", "", 1),
			encode_special_chars(&suffix),
		);

		assert_eq!(
			FlakeProtocol::parse_flake_ref(&flake_ref),
			Some((protocol, suffix.as_str()))
		);
		assert_eq!(
			FlakeProtocol::parse_repo(&repo),
			Some((protocol, encode_special_chars(&suffix).as_str()))
		);
	}

	#[test]
	fn flake_registry_prefixes_log_value_lists_all_protocols() {
		assert_eq!(
			flake_registry_prefixes_log_value(),
			"flake-github:0, flake-tarball-https:0, flake-tarball-http:0, flake-git-https:0, flake-git-http:0, flake-git-ssh:0",
		);
	}

	#[test]
	fn decode_special_chars_accepts_empty_input() {
		assert_eq!(
			decode_special_chars("").expect("empty input should decode"),
			""
		);
	}

	#[test]
	fn decode_hex_escape_rejects_too_short_escape_sequences() {
		assert_eq!(decode_hex_escape("--x0"), None);
		assert_eq!(decode_hex_escape("--x0-"), None);
	}
}
