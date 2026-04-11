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
