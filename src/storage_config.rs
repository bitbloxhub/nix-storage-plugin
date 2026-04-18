use std::env;
use std::path::PathBuf;

use serde::Deserialize;

use crate::common::NixStoragePluginError;

const SYSTEM_STORAGE_CONF: &str = "/usr/share/containers/storage.conf";
const OVERRIDE_STORAGE_CONF: &str = "/etc/containers/storage.conf";

#[derive(Debug, Clone)]
pub(crate) struct StorageConfig {
	pub driver: Option<String>,
	pub graph_root: PathBuf,
	pub run_root: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
struct StorageToml {
	#[serde(default)]
	storage: StorageSection,
}

#[derive(Debug, Default, Deserialize)]
struct StorageSection {
	#[serde(default)]
	driver: Option<String>,
	#[serde(default)]
	runroot: Option<String>,
	#[serde(default)]
	graphroot: Option<String>,
	#[serde(default)]
	rootless_storage_path: Option<String>,
}

pub(crate) async fn load_storage_config() -> Result<StorageConfig, NixStoragePluginError> {
	let config_path = storage_config_path();
	let config = read_storage_config(&config_path).await?;
	let rootless = !nix::unistd::geteuid().is_root();
	let run_root_raw = config.storage.runroot.unwrap_or_else(|| {
		if rootless {
			rootless_run_root_default()
		} else {
			"/run/containers/storage".to_owned()
		}
	});
	let graph_root_raw = config.storage.graphroot.unwrap_or_else(|| {
		if rootless {
			config
				.storage
				.rootless_storage_path
				.clone()
				.unwrap_or_else(rootless_graph_root_default)
		} else {
			"/var/lib/containers/storage".to_owned()
		}
	});
	let run_root = expand_storage_path(&run_root_raw)?;
	let graph_root = expand_storage_path(&graph_root_raw)?;
	Ok(StorageConfig {
		driver: config.storage.driver,
		graph_root,
		run_root,
	})
}

fn storage_config_path() -> PathBuf {
	if let Some(path) = env::var_os("CONTAINERS_STORAGE_CONF") {
		return PathBuf::from(path);
	}
	if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
		let candidate = PathBuf::from(path).join("containers/storage.conf");
		if candidate.exists() {
			return candidate;
		}
	}
	if let Some(home) = env::var_os("HOME") {
		let candidate = PathBuf::from(home).join(".config/containers/storage.conf");
		if candidate.exists() {
			return candidate;
		}
	}
	let override_path = PathBuf::from(OVERRIDE_STORAGE_CONF);
	if override_path.exists() {
		return override_path;
	}
	PathBuf::from(SYSTEM_STORAGE_CONF)
}

async fn read_storage_config(path: &PathBuf) -> Result<StorageToml, NixStoragePluginError> {
	match smol::fs::read_to_string(path).await {
		Ok(contents) => Ok(toml::from_str(&contents).map_err(|error| {
			NixStoragePluginError::InvalidLocalStorageState(format!(
				"failed to parse storage config {}: {error}",
				path.display(),
			))
		})?),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(StorageToml::default()),
		Err(error) => Err(error.into()),
	}
}

fn rootless_run_root_default() -> String {
	if let Some(runtime_dir) = env::var_os("XDG_RUNTIME_DIR") {
		return PathBuf::from(runtime_dir)
			.join("containers")
			.display()
			.to_string();
	}

	format!("/run/user/{}/containers", nix::unistd::geteuid().as_raw())
}

fn rootless_graph_root_default() -> String {
	if let Some(data_dir) = env::var_os("XDG_DATA_HOME") {
		return PathBuf::from(data_dir)
			.join("containers/storage")
			.display()
			.to_string();
	}

	if let Some(home) = env::var_os("HOME") {
		return PathBuf::from(home)
			.join(".local/share/containers/storage")
			.display()
			.to_string();
	}

	"$HOME/.local/share/containers/storage".to_owned()
}

fn expand_storage_path(raw: &str) -> Result<PathBuf, NixStoragePluginError> {
	let expanded = expand_env_vars(raw)?;
	let path = PathBuf::from(expanded);
	if path.is_absolute() {
		Ok(path)
	} else {
		Err(NixStoragePluginError::InvalidLocalStorageState(format!(
			"storage path is not absolute: {raw}",
		)))
	}
}

fn expand_env_vars(raw: &str) -> Result<String, NixStoragePluginError> {
	let mut expanded = String::new();
	let mut chars = raw.chars().peekable();
	while let Some(ch) = chars.next() {
		if ch != '$' {
			expanded.push(ch);
			continue;
		}

		let name = if chars.peek() == Some(&'{') {
			chars.next();
			let mut name = String::new();
			while let Some(next) = chars.peek().copied() {
				if next == '}' {
					chars.next();
					break;
				}
				name.push(next);
				chars.next();
			}
			name
		} else {
			let mut name = String::new();
			while let Some(next) = chars.peek().copied() {
				if next.is_ascii_alphanumeric() || next == '_' {
					name.push(next);
					chars.next();
				} else {
					break;
				}
			}
			name
		};

		if name.is_empty() {
			expanded.push('$');
			continue;
		}

		let value = env::var(&name).map_err(|_| {
			NixStoragePluginError::InvalidLocalStorageState(format!(
				"storage path references unset environment variable ${name}",
			))
		})?;
		expanded.push_str(&value);
	}
	Ok(expanded)
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use super::*;
	use hegel::generators;
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

	fn env_var_name() -> impl hegel::generators::Generator<String> {
		generators::from_regex(r"[A-Z_][A-Z0-9_]{0,11}").fullmatch(true)
	}

	fn path_segment() -> impl hegel::generators::Generator<String> {
		generators::from_regex(r"[A-Za-z0-9._-]{1,12}").fullmatch(true)
	}

	#[hegel::test(derandomize = true)]
	fn expand_env_vars_expands_braced_and_unbraced_vars(tc: hegel::TestCase) {
		let name = tc.draw(env_var_name());
		let value = tc.draw(path_segment());
		let raw = format!("/prefix/${{{name}}}/suffix/${}/end", name);

		let expanded = with_env_var(&name, Some(&value), || expand_env_vars(&raw))
			.expect("set environment variable should expand");

		assert_eq!(expanded, format!("/prefix/{value}/suffix/{value}/end"));
	}

	#[hegel::test(derandomize = true)]
	fn expand_env_vars_leaves_lonely_dollar_signs_unchanged(tc: hegel::TestCase) {
		let suffix = tc.draw(path_segment());
		let raw = format!("/tmp/$/{suffix}/$$/tail");

		assert_eq!(
			expand_env_vars(&raw).expect("dollar literals should remain valid"),
			raw
		);
	}

	#[hegel::test(derandomize = true)]
	fn expand_env_vars_rejects_unset_variables(tc: hegel::TestCase) {
		let name = tc.draw(env_var_name());
		let raw = format!("/${{{name}}}/tail");

		let result = with_env_var(&name, None, || expand_env_vars(&raw));

		assert!(matches!(
			result,
			Err(NixStoragePluginError::InvalidLocalStorageState(message)) if message.contains(&format!("${name}"))
		));
	}

	#[hegel::test(derandomize = true)]
	fn expand_storage_path_accepts_absolute_paths_after_expansion(tc: hegel::TestCase) {
		let name = tc.draw(env_var_name());
		let head = tc.draw(path_segment());
		let tail = tc.draw(path_segment());
		let raw = format!("/${{{name}}}/{tail}");
		let value = format!("/{head}");

		let expanded = with_env_var(&name, Some(&value), || expand_storage_path(&raw))
			.expect("expanded path should stay absolute");

		assert_eq!(expanded, PathBuf::from(format!("{value}/{tail}")));
	}

	#[hegel::test(derandomize = true)]
	fn expand_storage_path_rejects_relative_paths_after_expansion(tc: hegel::TestCase) {
		let name = tc.draw(env_var_name());
		let value = tc.draw(path_segment());
		let raw = format!("${{{name}}}/tail");

		let result = with_env_var(&name, Some(&value), || expand_storage_path(&raw));

		assert!(matches!(
			result,
			Err(NixStoragePluginError::InvalidLocalStorageState(message)) if message.contains("storage path is not absolute")
		));
	}

	fn with_env_vars<T>(vars: &[(&str, Option<&str>)], f: impl FnOnce() -> T) -> T {
		let previous = vars
			.iter()
			.map(|(name, _)| ((*name).to_owned(), env::var_os(name)))
			.collect::<Vec<_>>();
		for (name, value) in vars {
			match value {
				Some(value) => unsafe { env::set_var(name, value) },
				None => unsafe { env::remove_var(name) },
			}
		}
		let result = f();
		for (name, value) in previous {
			match value {
				Some(value) => unsafe { env::set_var(&name, value) },
				None => unsafe { env::remove_var(&name) },
			}
		}
		result
	}

	#[hegel::test(derandomize = true)]
	fn rootless_run_root_default_prefers_xdg_runtime_dir(tc: hegel::TestCase) {
		let runtime_dir: String =
			tc.draw(generators::from_regex(r"/[A-Za-z0-9._/-]{1,24}").fullmatch(true));

		let result = with_env_var(
			"XDG_RUNTIME_DIR",
			Some(&runtime_dir),
			rootless_run_root_default,
		);

		assert_eq!(
			result,
			PathBuf::from(runtime_dir)
				.join("containers")
				.display()
				.to_string()
		);
	}

	#[hegel::test(derandomize = true)]
	fn rootless_graph_root_default_prefers_xdg_data_home(tc: hegel::TestCase) {
		let data_dir: String =
			tc.draw(generators::from_regex(r"/[A-Za-z0-9._/-]{1,24}").fullmatch(true));

		let result = with_env_var(
			"XDG_DATA_HOME",
			Some(&data_dir),
			rootless_graph_root_default,
		);

		assert_eq!(
			result,
			PathBuf::from(data_dir)
				.join("containers/storage")
				.display()
				.to_string()
		);
	}

	#[hegel::test(derandomize = true)]
	fn rootless_graph_root_default_falls_back_to_home(tc: hegel::TestCase) {
		let home: String =
			tc.draw(generators::from_regex(r"/[A-Za-z0-9._/-]{1,24}").fullmatch(true));

		let result = with_env_vars(
			&[("XDG_DATA_HOME", None), ("HOME", Some(&home))],
			rootless_graph_root_default,
		);

		assert_eq!(
			result,
			PathBuf::from(home)
				.join(".local/share/containers/storage")
				.display()
				.to_string()
		);
	}

	#[hegel::test(derandomize = true)]
	fn storage_config_path_prefers_explicit_env(tc: hegel::TestCase) {
		let path: String =
			tc.draw(generators::from_regex(r"/[A-Za-z0-9._/-]{1,24}").fullmatch(true));

		let result = with_env_var("CONTAINERS_STORAGE_CONF", Some(&path), storage_config_path);

		assert_eq!(result, PathBuf::from(path));
	}

	#[hegel::test(derandomize = true)]
	fn read_storage_config_returns_default_for_missing_file(tc: hegel::TestCase) {
		let path = PathBuf::from(format!(
			"/tmp/{}/storage.conf",
			tc.draw::<String>(
				generators::from_regex(r"[A-Za-z0-9][A-Za-z0-9._-]{0,23}").fullmatch(true)
			),
		));

		let parsed = smol::block_on(read_storage_config(&path))
			.expect("missing file should return default config");

		assert!(parsed.storage.driver.is_none());
		assert!(parsed.storage.runroot.is_none());
		assert!(parsed.storage.graphroot.is_none());
		assert!(parsed.storage.rootless_storage_path.is_none());
	}

	#[test]
	fn read_storage_config_rejects_invalid_toml() {
		let temp_dir = tempfile::tempdir().expect("tempdir should exist");
		let path = temp_dir.path().join("storage.conf");
		smol::block_on(smol::fs::write(&path, "[storage\ndriver = ["))
			.expect("invalid config fixture should be written");

		let result = smol::block_on(read_storage_config(&path));

		assert!(matches!(
			result,
			Err(NixStoragePluginError::InvalidLocalStorageState(message)) if message.contains("failed to parse storage config")
		));
	}

	#[hegel::test(derandomize = true)]
	fn rootless_run_root_default_falls_back_to_uid_path_when_xdg_missing(_: hegel::TestCase) {
		let result = with_env_var("XDG_RUNTIME_DIR", None, rootless_run_root_default);

		assert_eq!(
			result,
			format!("/run/user/{}/containers", nix::unistd::geteuid().as_raw()),
		);
	}

	#[hegel::test(derandomize = true)]
	fn rootless_graph_root_default_uses_literal_home_placeholder_without_home(_: hegel::TestCase) {
		let result = with_env_vars(
			&[("XDG_DATA_HOME", None), ("HOME", None)],
			rootless_graph_root_default,
		);

		assert_eq!(result, "$HOME/.local/share/containers/storage");
	}

	#[hegel::test(derandomize = true)]
	fn storage_config_path_prefers_existing_xdg_config_file(tc: hegel::TestCase) {
		let base = PathBuf::from(format!(
			"/tmp/{}",
			tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,24}").fullmatch(true))
		));
		let config_home = base.join("xdg");
		let config_dir = config_home.join("containers");
		std::fs::create_dir_all(&config_dir).expect("config dir should exist");
		let config_path = config_dir.join("storage.conf");
		std::fs::write(&config_path, "[storage]\n").expect("config file should exist");

		let result = with_env_vars(
			&[
				("CONTAINERS_STORAGE_CONF", None),
				("XDG_CONFIG_HOME", config_home.to_str()),
				("HOME", None),
			],
			storage_config_path,
		);

		assert_eq!(result, config_path);
	}

	#[hegel::test(derandomize = true)]
	fn storage_config_path_prefers_existing_home_config_file_when_xdg_missing(tc: hegel::TestCase) {
		let base = PathBuf::from(format!(
			"/tmp/{}",
			tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,24}").fullmatch(true))
		));
		let home = base.join("home");
		let config_dir = home.join(".config/containers");
		std::fs::create_dir_all(&config_dir).expect("config dir should exist");
		let config_path = config_dir.join("storage.conf");
		std::fs::write(&config_path, "[storage]\n").expect("config file should exist");

		let result = with_env_vars(
			&[
				("CONTAINERS_STORAGE_CONF", None),
				("XDG_CONFIG_HOME", None),
				("HOME", home.to_str()),
			],
			storage_config_path,
		);

		assert_eq!(result, config_path);
	}

	#[test]
	fn storage_config_path_falls_back_to_system_path_when_no_candidates_exist() {
		let temp_dir = tempfile::tempdir().expect("tempdir should exist");
		let xdg = temp_dir.path().join("xdg");

		let result = with_env_vars(
			&[
				("CONTAINERS_STORAGE_CONF", None),
				("XDG_CONFIG_HOME", xdg.to_str()),
				("HOME", None),
			],
			storage_config_path,
		);

		assert_eq!(result, PathBuf::from(SYSTEM_STORAGE_CONF));
	}

	#[hegel::test(derandomize = true)]
	fn read_storage_config_propagates_io_errors_other_than_not_found(tc: hegel::TestCase) {
		let base = PathBuf::from(format!(
			"/tmp/{}",
			tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,24}").fullmatch(true))
		));
		std::fs::create_dir_all(&base).expect("temp dir should exist");
		let result = smol::block_on(read_storage_config(&base));

		assert!(matches!(result, Err(NixStoragePluginError::Io(_))));
	}
}
