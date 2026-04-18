use std::ffi::OsString;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use nix_storage_plugin::encode_flake_ref;
use tempfile::TempDir;

struct ChildGuard {
	child: Child,
}

impl ChildGuard {
	fn spawn(mut command: Command) -> Self {
		let child = command
			.stdout(Stdio::inherit())
			.stderr(Stdio::inherit())
			.spawn()
			.expect("child process should start");
		Self { child }
	}
}

impl Drop for ChildGuard {
	fn drop(&mut self) {
		let pid = self.child.id() as i32;
		unsafe {
			libc::kill(pid, libc::SIGTERM);
		}
		for _ in 0..20 {
			match self.child.try_wait() {
				Ok(Some(_)) => return,
				Ok(None) => thread::sleep(Duration::from_millis(100)),
				Err(_) => break,
			}
		}
		let _ = self.child.kill();
		let _ = self.child.wait();
	}
}

struct E2eEnv {
	_tempdir: TempDir,
	home_dir: PathBuf,
	runtime_dir: PathBuf,
	mount_path: PathBuf,
	storage_conf: PathBuf,
	registry_addr: SocketAddr,
}

impl E2eEnv {
	fn new() -> Self {
		let tempdir = tempfile::tempdir().expect("tempdir should exist");
		let home_dir = tempdir.path().join("home");
		let runtime_dir = tempdir.path().join("runtime");
		let xdg_config_home = home_dir.join(".config");
		let xdg_data_home = home_dir.join(".local/share");
		let mount_path = tempdir.path().join("layer-store");
		let graph_root = xdg_data_home.join("containers/storage");
		let run_root = runtime_dir.join("containers");
		let containers_dir = xdg_config_home.join("containers");
		let storage_conf = containers_dir.join("storage.conf");
		std::fs::create_dir_all(&containers_dir).expect("containers config dir should exist");
		std::fs::create_dir_all(&xdg_data_home).expect("xdg data dir should exist");
		std::fs::create_dir_all(&runtime_dir).expect("runtime dir should exist");
		std::fs::set_permissions(&runtime_dir, std::fs::Permissions::from_mode(0o700))
			.expect("runtime dir permissions should be set");
		std::fs::create_dir_all(&graph_root).expect("graph root should exist");
		std::fs::create_dir_all(&run_root).expect("run root should exist");
		std::fs::create_dir_all(&mount_path).expect("mount path should exist");
		let registry_addr = free_local_addr();
		std::fs::write(
			&storage_conf,
			format!(
				"[storage]\n",
			) + &format!(
				"driver = \"overlay\"\ngraphroot = \"{}\"\nrunroot = \"{}\"\n\n[storage.options]\nadditionallayerstores = [\"{}:ref\"]\n",
				graph_root.display(),
				run_root.display(),
				mount_path.display(),
			),
		)
		.expect("storage.conf should be written");

		std::fs::write(
			containers_dir.join("registries.conf"),
			format!(
				"short-name-mode = \"disabled\"\nunqualified-search-registries = []\n\n[[registry]]\nprefix = \"nix:0\"\nlocation = \"{}\"\ninsecure = true\n\n[[registry]]\nprefix = \"flake-github:0\"\nlocation = \"{}\"\ninsecure = true\n",
				registry_addr,
				registry_addr,
			),
		)
		.expect("registries.conf should be written");

		Self {
			_tempdir: tempdir,
			home_dir,
			runtime_dir,
			mount_path,
			storage_conf,
			registry_addr,
		}
	}

	fn inherit_coverage_env(command: &mut Command) {
		for key in [
			"LLVM_PROFILE_FILE",
			"LLVM_PROFILE_FILE_NAME",
			"CARGO_LLVM_COV_TARGET_DIR",
			"CARGO_LLVM_COV_BUILD_DIR",
			"LLVM_COV",
			"LLVM_PROFDATA",
		] {
			if let Some(value) = std::env::var_os(key) {
				command.env(key, value);
			}
		}
	}

	fn child_command(&self, program: impl Into<OsString>) -> Command {
		let mut command = Command::new(program.into());
		command.env_clear();
		if let Some(path) = std::env::var_os("PATH") {
			command.env("PATH", path);
		}
		if let Some(term) = std::env::var_os("TERM") {
			command.env("TERM", term);
		}
		Self::inherit_coverage_env(&mut command);
		command.env("HOME", &self.home_dir);
		command.env("XDG_CONFIG_HOME", self.home_dir.join(".config"));
		command.env("XDG_DATA_HOME", self.home_dir.join(".local/share"));
		command.env("XDG_RUNTIME_DIR", &self.runtime_dir);
		command.env("CONTAINERS_STORAGE_CONF", &self.storage_conf);
		command
	}

	fn run_command(&self, program: impl Into<OsString>, args: &[OsString]) -> std::process::Output {
		println!("{:?}", args);
		let mut command = self.child_command(program);
		command.args(args);
		command.output().expect("command should run")
	}
}

fn free_local_addr() -> SocketAddr {
	let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
	let addr = listener.local_addr().expect("listener addr should exist");
	drop(listener);
	addr
}

fn plugin_bin() -> PathBuf {
	PathBuf::from(env!("CARGO_BIN_EXE_nix-storage-plugin"))
}

fn nix_out_path(flake_ref: String) -> PathBuf {
	let output = Command::new("nix")
		.args(&[
			"build",
			"--no-link",
			"--print-out-paths",
			"--extra-experimental-features",
			"nix-command flakes",
			"--",
			&flake_ref,
		])
		.output()
		.expect("Finding podman failed");

	let stdout = String::from_utf8(output.stdout)
		.map_err(|e| format!("nix output was not valid UTF-8: {e}"))
		.unwrap();

	let paths = stdout
		.lines()
		.filter(|s| !s.trim().is_empty())
		.map(PathBuf::from)
		.collect::<Vec<_>>();

	paths[0].clone()
}

fn podman_bin() -> OsString {
	std::env::var_os("NSP_E2E_PODMAN").unwrap_or_else(|| {
		format!(
			"{}/bin/podman",
			nix_out_path(".#podman.out".to_string()).to_string_lossy()
		)
		.into()
	})
}

fn split_command_args(value: Option<String>) -> Vec<OsString> {
	value
		.unwrap_or_default()
		.split_whitespace()
		.map(OsString::from)
		.collect()
}

fn wait_for_registry(addr: SocketAddr) {
	let deadline = Instant::now() + Duration::from_secs(15);
	while Instant::now() < deadline {
		if let Ok(mut stream) = TcpStream::connect(addr) {
			let _ = stream
				.write_all(b"GET /v2/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
			let mut response = String::new();
			let _ = stream.read_to_string(&mut response);
			if response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.1 401") {
				return;
			}
		}
		thread::sleep(Duration::from_millis(200));
	}
	panic!("registry adapter did not become ready")
}

fn wait_for_mount(env: &E2eEnv) {
	let deadline = Instant::now() + Duration::from_secs(15);
	while Instant::now() < deadline {
		let output = env.run_command(
			"mountpoint",
			&[
				OsString::from("-q"),
				env.mount_path.clone().into_os_string(),
			],
		);
		if output.status.success() {
			return;
		}
		thread::sleep(Duration::from_millis(200));
	}
	panic!("layer store mount did not become ready")
}

fn start_mount_store(env: &E2eEnv) -> ChildGuard {
	let mut command = env.child_command(plugin_bin().into_os_string());
	command.args([
		OsString::from("mount-store"),
		OsString::from("--mount-path"),
		env.mount_path.clone().into_os_string(),
	]);
	let child = ChildGuard::spawn(command);
	wait_for_mount(env);
	println!("{}", env.mount_path.clone().display());
	child
}

fn start_registry(env: &E2eEnv) -> ChildGuard {
	let mut command = env.child_command(plugin_bin().into_os_string());
	command.args([
		OsString::from("serve-image"),
		OsString::from("--bind"),
		OsString::from(env.registry_addr.to_string()),
	]);
	let child = ChildGuard::spawn(command);
	wait_for_registry(env.registry_addr);
	child
}

fn assert_podman_success(output: std::process::Output) {
	assert!(
		output.status.success(),
		"podman failed\nstdout:\n{}\n\nstderr:\n{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr),
	)
}

#[test]
fn podman_runs_nix_image() {
	let env = E2eEnv::new();
	let _mount = start_mount_store(&env);
	let image = std::env::var("NSP_E2E_NIX_IMAGE_REMOTE")
		.unwrap_or_else(|_| "ghcr.io/pdtpartners/redis-shell:latest".to_owned());
	let pull_output = env.run_command(
		podman_bin(),
		&[OsString::from("pull"), OsString::from(image.clone())],
	);
	assert_podman_success(pull_output);
	let output = env.run_command(
		podman_bin(),
		&[
			OsString::from("run"),
			OsString::from("--rm"),
			OsString::from(image),
			OsString::from("-lc"),
			OsString::from("true"),
		],
	);
	assert_podman_success(output);
}

#[test]
fn podman_runs_nix_image_via_registry_alias() {
	let image_ref = std::env::var("NSP_E2E_NIX_IMAGE_NIX").unwrap_or_else(|_| {
		format!(
			"nix:0{}",
			nix_out_path("github:pdtpartners/nix-snapshotter#image-hello".to_string())
				.to_string_lossy()
		)
	});
	let extra_args = split_command_args(std::env::var("NSP_E2E_NIX_IMAGE_NIX_COMMAND").ok());
	let env = E2eEnv::new();
	let _mount = start_mount_store(&env);
	let _registry = start_registry(&env);
	let mut args = vec![
		OsString::from("run"),
		OsString::from("--rm"),
		OsString::from(image_ref),
	];
	args.extend(extra_args);
	let output = env.run_command(podman_bin(), &args);
	assert_podman_success(output);
}

#[test]
fn podman_runs_flake_image_via_registry_alias() {
	let flake_ref = std::env::var("NSP_E2E_FLAKE_REF")
		.unwrap_or_else(|_| "github:pdtpartners/nix-snapshotter#image-hello".to_owned());
	let encoded = encode_flake_ref(&flake_ref).expect("flake ref should encode");
	let suffix = encoded
		.strip_prefix("flake-github:0/")
		.expect("encoded github flake ref should have flake-github:0 prefix");
	let image_ref = format!("nix:0/flake-github/{suffix}");
	let extra_args = split_command_args(std::env::var("NSP_E2E_FLAKE_COMMAND").ok());
	let env = E2eEnv::new();
	let _mount = start_mount_store(&env);
	let _registry = start_registry(&env);
	let mut args = vec![
		OsString::from("run"),
		OsString::from("--rm"),
		OsString::from(image_ref),
	];
	args.extend(extra_args);
	let output = env.run_command(podman_bin(), &args);
	assert_podman_success(output);
}

fn assert_podman_failure_contains(output: std::process::Output, needle: &str) {
	assert!(
		!output.status.success(),
		"podman unexpectedly succeeded\nstdout:\n{}\n\nstderr:\n{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr),
	);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(
		stderr.contains(needle),
		"expected podman stderr to contain {needle:?}\nstdout:\n{}\n\nstderr:\n{}",
		String::from_utf8_lossy(&output.stdout),
		stderr,
	);
}

#[test]
fn podman_reports_not_found_for_invalid_flake_registry_alias() {
	let flake_ref = std::env::var("NSP_E2E_BAD_FLAKE_REF").unwrap_or_else(|_| {
		"github:pdtpartners/definitely-not-a-real-flake-repo#image-hello".to_owned()
	});
	let encoded = encode_flake_ref(&flake_ref).expect("flake ref should encode");
	let suffix = encoded
		.strip_prefix("flake-github:0/")
		.expect("encoded github flake ref should have flake-github:0 prefix");
	let image_ref = format!("nix:0/flake-github/{suffix}");
	let env = E2eEnv::new();
	let _mount = start_mount_store(&env);
	let _registry = start_registry(&env);
	let output = env.run_command(
		podman_bin(),
		&[
			OsString::from("run"),
			OsString::from("--rm"),
			OsString::from(image_ref),
		],
	);
	assert_podman_failure_contains(output, "not found");
}
