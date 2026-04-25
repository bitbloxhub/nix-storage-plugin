use smol::fs;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use async_io::Async;
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request as HttpRequest, Response, StatusCode};
use oci_spec::image::ImageManifest;
use smol_hyper::rt::FuturesIo;

use crate::common::{
	DEFAULT_REGISTRY_BIND_ADDR, NixStoragePluginError, data_response, host_command,
	sha256_blob_file_name, sha256_digest, simple_response,
};
use crate::flake_ref::{decode_flake_installable_from_repo, flake_registry_prefixes_log_value};
use crate::nix::try_realize_nix_archive_path;
use crate::skopeo::export_source_to_temp_dir;

#[derive(Debug, Clone)]
struct ServedArchiveImage {
	manifest_digest: String,
	manifest: Bytes,
	config_digest: String,
	config: Bytes,
	blobs: BTreeMap<String, Bytes>,
}

#[derive(Debug, Default)]
struct RegistryCache {
	images: RwLock<BTreeMap<String, Arc<ServedArchiveImage>>>,
}

impl RegistryCache {
	async fn get_or_load(
		&self,
		repo: &str,
	) -> Result<Arc<ServedArchiveImage>, NixStoragePluginError> {
		let archive_path = archive_path_for_repo(repo).await?;
		self.get_or_load_archive_path_with(archive_path, |archive_path| async move {
			load_archive_image(&archive_path).await
		})
		.await
	}

	async fn get_or_load_archive_path_with<F, Fut>(
		&self,
		archive_path: PathBuf,
		loader: F,
	) -> Result<Arc<ServedArchiveImage>, NixStoragePluginError>
	where
		F: FnOnce(PathBuf) -> Fut,
		Fut: Future<Output = Result<ServedArchiveImage, NixStoragePluginError>>,
	{
		let cache_key = archive_path.display().to_string();

		if let Some(image) = self
			.images
			.read()
			.expect("registry cache read lock")
			.get(&cache_key)
			.cloned()
		{
			return Ok(image);
		}

		let image = Arc::new(loader(archive_path).await?);
		self.images
			.write()
			.expect("registry cache write lock")
			.insert(cache_key, image.clone());
		Ok(image)
	}
}

#[derive(Clone, Default)]
struct RegistryState {
	cache: Arc<RegistryCache>,
}

impl RegistryState {
	async fn response(&self, req: HttpRequest<Incoming>) -> Response<Full<Bytes>> {
		self.response_for_request(req.method(), req.uri().path())
			.await
	}

	async fn response_for_request(&self, method: &Method, path: &str) -> Response<Full<Bytes>> {
		let empty = *method == Method::HEAD;

		if path == "/v2/" {
			return simple_response(StatusCode::OK, Bytes::new(), empty);
		}

		let Some(target) = RegistryTarget::parse(path) else {
			return simple_response(
				StatusCode::NOT_FOUND,
				Bytes::from_static(b"not found\n"),
				empty,
			);
		};
		let image = match self.cache.get_or_load(&target.repo).await {
			Ok(image) => image,
			Err(error) => {
				tracing::warn!(%error, repo = target.repo, "failed to load archive image");
				return simple_response(
					StatusCode::NOT_FOUND,
					Bytes::from_static(b"not found\n"),
					empty,
				);
			}
		};

		Self::response_for_loaded_target(&target, image, empty)
	}

	fn response_for_loaded_target(
		target: &RegistryTarget,
		image: Arc<ServedArchiveImage>,
		empty: bool,
	) -> Response<Full<Bytes>> {
		match &target.kind {
			RegistryTargetKind::Manifest(reference) => {
				if reference != &image.manifest_digest {
					tracing::debug!(repo = target.repo, %reference, manifest = image.manifest_digest, "serving manifest by non-digest reference");
				}
				data_response(
					StatusCode::OK,
					if empty {
						Bytes::new()
					} else {
						image.manifest.clone()
					},
					"application/vnd.oci.image.manifest.v1+json",
					Some(image.manifest_digest.as_str()),
				)
			}
			RegistryTargetKind::Blob(digest) => {
				if digest == &image.config_digest {
					return data_response(
						StatusCode::OK,
						if empty {
							Bytes::new()
						} else {
							image.config.clone()
						},
						"application/vnd.oci.image.config.v1+json",
						Some(image.config_digest.as_str()),
					);
				}
				if let Some(blob) = image.blobs.get(digest) {
					return data_response(
						StatusCode::OK,
						if empty { Bytes::new() } else { blob.clone() },
						"application/octet-stream",
						Some(digest.as_str()),
					);
				}
				simple_response(
					StatusCode::NOT_FOUND,
					Bytes::from_static(b"not found\n"),
					empty,
				)
			}
		}
	}
}

#[derive(Debug)]
struct RegistryTarget {
	repo: String,
	kind: RegistryTargetKind,
}

#[derive(Debug)]
enum RegistryTargetKind {
	Manifest(String),
	Blob(String),
}

impl RegistryTarget {
	fn parse(path: &str) -> Option<Self> {
		let suffix = path.strip_prefix("/v2/")?;
		if let Some((repo, reference)) = suffix.split_once("/manifests/") {
			return Some(Self {
				repo: repo.to_owned(),
				kind: RegistryTargetKind::Manifest(reference.to_owned()),
			});
		}
		if let Some((repo, digest)) = suffix.split_once("/blobs/") {
			return Some(Self {
				repo: repo.to_owned(),
				kind: RegistryTargetKind::Blob(digest.to_owned()),
			});
		}
		None
	}
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use http_body_util::BodyExt;

	use super::*;
	use crate::flake_ref::encode_flake_ref;
	use hegel::generators::{self, Generator};

	fn served_image_with(
		manifest_digest: String,
		manifest: Bytes,
		config_digest: String,
		config: Bytes,
		blobs: BTreeMap<String, Bytes>,
	) -> Arc<ServedArchiveImage> {
		Arc::new(ServedArchiveImage {
			manifest_digest,
			manifest,
			config_digest,
			config,
			blobs,
		})
	}

	fn sha256_digest_string(tc: &hegel::TestCase) -> String {
		tc.draw(generators::from_regex(r"sha256:[a-f0-9]{64}").fullmatch(true))
	}

	async fn response_body(response: Response<Full<Bytes>>) -> Bytes {
		response
			.into_body()
			.collect()
			.await
			.expect("full body should collect")
			.to_bytes()
	}

	#[hegel::test(derandomize = true)]
	fn registry_target_parse_manifests_roundtrips_repo_and_reference(tc: hegel::TestCase) {
		let repo = tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{1,32}").fullmatch(true));
		let reference = tc.draw(generators::from_regex(r"[A-Za-z0-9._:-]{1,32}").fullmatch(true));
		let path = format!("/v2/{repo}/manifests/{reference}");

		let target = RegistryTarget::parse(&path).expect("manifest path should parse");

		assert_eq!(target.repo, repo);
		assert!(matches!(target.kind, RegistryTargetKind::Manifest(value) if value == reference));
	}

	#[hegel::test(derandomize = true)]
	fn registry_target_parse_blobs_roundtrips_repo_and_digest(tc: hegel::TestCase) {
		let repo = tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{1,32}").fullmatch(true));
		let digest = tc.draw(generators::from_regex(r"sha256:[a-f0-9]{64}").fullmatch(true));
		let path = format!("/v2/{repo}/blobs/{digest}");

		let target = RegistryTarget::parse(&path).expect("blob path should parse");

		assert_eq!(target.repo, repo);
		assert!(matches!(target.kind, RegistryTargetKind::Blob(value) if value == digest));
	}

	#[hegel::test(derandomize = true)]
	fn registry_target_parse_rejects_non_registry_paths(tc: hegel::TestCase) {
		let path = tc.draw(generators::text().filter(|value| {
			!value.starts_with("/v2/")
				|| (!value.contains("/manifests/") && !value.contains("/blobs/"))
		}));

		assert!(RegistryTarget::parse(&path).is_none());
	}

	#[hegel::test(derandomize = true)]
	fn registry_state_response_for_request_handles_registry_root_and_missing_paths(
		tc: hegel::TestCase,
	) {
		smol::block_on(async {
			let state = RegistryState::default();
			let method = tc.draw(generators::sampled_from(vec![Method::GET, Method::HEAD]));

			let root = state.response_for_request(&method, "/v2/").await;
			assert_eq!(root.status(), StatusCode::OK);
			assert_eq!(
				response_body(root).await,
				if method == Method::HEAD {
					Bytes::new()
				} else {
					Bytes::new()
				}
			);

			let missing = state.response_for_request(&method, "/missing").await;
			assert_eq!(missing.status(), StatusCode::NOT_FOUND);
			assert_eq!(
				missing.headers()["content-length"],
				if method == Method::HEAD { "0" } else { "10" }
			);
			assert_eq!(
				response_body(missing).await,
				if method == Method::HEAD {
					Bytes::new()
				} else {
					Bytes::from_static(b"not found\n")
				}
			);
		})
	}

	#[hegel::test(derandomize = true)]
	fn registry_state_response_for_loaded_manifest_preserves_headers_and_head_semantics(
		tc: hegel::TestCase,
	) {
		smol::block_on(async {
			let repo = tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{1,32}").fullmatch(true));
			let reference =
				tc.draw(generators::from_regex(r"[A-Za-z0-9._:-]{1,32}").fullmatch(true));
			let manifest_digest = sha256_digest_string(&tc);
			let manifest = Bytes::from(tc.draw(generators::binary()));
			let config_digest = sha256_digest_string(&tc);
			let config = Bytes::from(tc.draw(generators::binary()));
			let image = served_image_with(
				manifest_digest.clone(),
				manifest.clone(),
				config_digest,
				config,
				BTreeMap::new(),
			);
			let target = RegistryTarget {
				repo,
				kind: RegistryTargetKind::Manifest(reference),
			};

			let get = RegistryState::response_for_loaded_target(&target, image.clone(), false);
			assert_eq!(get.status(), StatusCode::OK);
			assert_eq!(
				get.headers()["content-type"],
				"application/vnd.oci.image.manifest.v1+json"
			);
			assert_eq!(get.headers()["docker-content-digest"], manifest_digest);
			assert_eq!(response_body(get).await, manifest);

			let head = RegistryState::response_for_loaded_target(&target, image, true);
			assert_eq!(head.status(), StatusCode::OK);
			assert_eq!(
				head.headers()["content-type"],
				"application/vnd.oci.image.manifest.v1+json"
			);
			assert_eq!(head.headers()["docker-content-digest"], manifest_digest);
			assert_eq!(head.headers()["content-length"], "0");
			assert_eq!(response_body(head).await, Bytes::new());
		})
	}

	#[hegel::test(derandomize = true)]
	fn registry_state_response_for_loaded_blob_distinguishes_config_known_blob_and_missing(
		tc: hegel::TestCase,
	) {
		smol::block_on(async {
			let repo = tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{1,32}").fullmatch(true));
			let digests = tc.draw(
				generators::vecs(generators::from_regex(r"[a-f0-9]{64}").fullmatch(true))
					.min_size(3)
					.max_size(3)
					.unique(true),
			);
			let config_digest = format!("sha256:{}", digests[0]);
			let blob_digest = format!("sha256:{}", digests[1]);
			let missing_digest = format!("sha256:{}", digests[2]);
			let manifest_digest = sha256_digest_string(&tc);
			let manifest = Bytes::from(tc.draw(generators::binary()));
			let config = Bytes::from(tc.draw(generators::binary()));
			let blob = Bytes::from(tc.draw(generators::binary()));
			let image = served_image_with(
				manifest_digest,
				manifest,
				config_digest.clone(),
				config.clone(),
				BTreeMap::from([(blob_digest.clone(), blob.clone())]),
			);

			let config_target = RegistryTarget {
				repo: repo.clone(),
				kind: RegistryTargetKind::Blob(config_digest.clone()),
			};
			let config_get =
				RegistryState::response_for_loaded_target(&config_target, image.clone(), false);
			assert_eq!(config_get.status(), StatusCode::OK);
			assert_eq!(
				config_get.headers()["content-type"],
				"application/vnd.oci.image.config.v1+json"
			);
			assert_eq!(config_get.headers()["docker-content-digest"], config_digest);
			assert_eq!(response_body(config_get).await, config);

			let blob_target = RegistryTarget {
				repo: repo.clone(),
				kind: RegistryTargetKind::Blob(blob_digest.clone()),
			};
			let blob_get =
				RegistryState::response_for_loaded_target(&blob_target, image.clone(), false);
			assert_eq!(blob_get.status(), StatusCode::OK);
			assert_eq!(
				blob_get.headers()["content-type"],
				"application/octet-stream"
			);
			assert_eq!(blob_get.headers()["docker-content-digest"], blob_digest);
			assert_eq!(response_body(blob_get).await, blob);

			let blob_head =
				RegistryState::response_for_loaded_target(&blob_target, image.clone(), true);
			assert_eq!(blob_head.status(), StatusCode::OK);
			assert_eq!(blob_head.headers()["content-length"], "0");
			assert_eq!(blob_head.headers()["docker-content-digest"], blob_digest);
			assert_eq!(response_body(blob_head).await, Bytes::new());

			let missing_target = RegistryTarget {
				repo,
				kind: RegistryTargetKind::Blob(missing_digest),
			};
			let missing = RegistryState::response_for_loaded_target(&missing_target, image, false);
			assert_eq!(missing.status(), StatusCode::NOT_FOUND);
			assert_eq!(
				response_body(missing).await,
				Bytes::from_static(b"not found\n")
			);
		})
	}

	#[hegel::test(derandomize = true)]
	fn validate_archive_path_matches_documented_contract(tc: hegel::TestCase) {
		let in_store = tc.draw(generators::booleans());
		let is_tar = tc.draw(generators::booleans());
		let name = tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true));
		let prefix = if in_store { "/nix/store" } else { "/tmp" };
		let ext = if is_tar { "tar" } else { "txt" };
		let path = PathBuf::from(format!("{prefix}/{name}.{ext}"));
		let result = validate_archive_path(&path, "image-ref");

		match (in_store, is_tar) {
			(true, true) => assert!(result.is_ok()),
			(false, _) => assert!(matches!(
				result,
				Err(NixStoragePluginError::InvalidImageRef(message)) if message == "image-ref"
			)),
			(true, false) => assert!(matches!(
				result,
				Err(NixStoragePluginError::InvalidLocalStorageState(message)) if message.contains("not a .tar file")
			)),
		}
	}

	#[hegel::test(derandomize = true)]
	fn read_exported_blob_reads_sha256_named_files(tc: hegel::TestCase) {
		smol::block_on(async {
			let dir = tempfile::tempdir().expect("tempdir should exist");
			let digest = sha256_digest_string(&tc);
			let blob_bytes = tc.draw(generators::binary());
			let file_name =
				sha256_blob_file_name(&digest).expect("sha256 digest should have file name");
			smol::fs::write(dir.path().join(file_name), &blob_bytes)
				.await
				.expect("blob file should be written");

			let blob = read_exported_blob(dir.path(), &digest)
				.await
				.expect("blob should be read");

			assert_eq!(blob, Bytes::from(blob_bytes));
		})
	}

	#[hegel::test(derandomize = true)]
	fn read_exported_blob_rejects_non_sha256_digests(tc: hegel::TestCase) {
		smol::block_on(async {
			let dir = tempfile::tempdir().expect("tempdir should exist");
			let error = read_exported_blob(
				dir.path(),
				&tc.draw(generators::text().filter(|value| !value.starts_with("sha256:"))),
			)
			.await
			.expect_err("non-sha256 digest should fail");

			assert!(matches!(
				error,
				NixStoragePluginError::InvalidLocalStorageState(message)
					if message.contains("unsupported exported digest format")
			));
		})
	}

	#[hegel::test(derandomize = true)]
	fn parse_flake_build_output_accepts_single_trimmed_nix_store_tar_path(tc: hegel::TestCase) {
		let installable =
			tc.draw(generators::from_regex(r"[A-Za-z0-9._#:/?-]{1,32}").fullmatch(true));
		let name = tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true));
		let path = format!("/nix/store/{name}.tar");
		let output = format!("\n  {path}  \n\n");

		assert_eq!(
			parse_flake_build_output(&output, &installable).expect("single path should parse"),
			PathBuf::from(path),
		);
	}

	#[hegel::test(derandomize = true)]
	fn parse_flake_build_output_rejects_empty_multiple_and_invalid_paths(tc: hegel::TestCase) {
		let installable =
			tc.draw(generators::from_regex(r"[A-Za-z0-9._#:/?-]{1,32}").fullmatch(true));
		let name_a = tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true));
		let name_b = tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true));
		let mode = tc.draw(generators::sampled_from(vec![
			"empty",
			"multiple",
			"wrong-prefix",
			"wrong-ext",
		]));
		let result = match mode {
			"empty" => parse_flake_build_output(" \n\t\n ", &installable),
			"multiple" => parse_flake_build_output(
				&format!("/nix/store/{name_a}.tar\n/nix/store/{name_b}.tar\n"),
				&installable,
			),
			"wrong-prefix" => {
				parse_flake_build_output(&format!("/tmp/{name_a}.tar\n"), &installable)
			}
			"wrong-ext" => {
				parse_flake_build_output(&format!("/nix/store/{name_a}.txt\n"), &installable)
			}
			_ => unreachable!(),
		};

		match mode {
			"empty" => assert!(matches!(
				result,
				Err(NixStoragePluginError::InvalidLocalStorageState(message))
					if message.contains("returned no output path")
			)),
			"multiple" => assert!(matches!(
				result,
				Err(NixStoragePluginError::InvalidLocalStorageState(message))
					if message.contains("returned multiple output paths")
			)),
			"wrong-prefix" => assert!(matches!(
				result,
				Err(NixStoragePluginError::InvalidImageRef(message)) if message == installable
			)),
			"wrong-ext" => assert!(matches!(
				result,
				Err(NixStoragePluginError::InvalidLocalStorageState(message))
					if message.contains("not a .tar file")
			)),
			_ => unreachable!(),
		}
	}

	#[hegel::test(derandomize = true)]
	fn archive_path_from_local_repo_rejects_non_nix_store_repos(tc: hegel::TestCase) {
		smol::block_on(async {
			let repo = format!(
				"tmp/{}.tar",
				tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
			);
			let result = archive_path_from_local_repo(&repo).await;

			assert!(matches!(
				result,
				Err(NixStoragePluginError::InvalidImageRef(message)) if message == repo
			));
		})
	}

	#[hegel::test(derandomize = true)]
	fn archive_path_from_local_repo_rejects_non_tar_repos(tc: hegel::TestCase) {
		smol::block_on(async {
			let repo = format!(
				"nix/store/{}.txt",
				tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
			);
			let result = archive_path_from_local_repo(&repo).await;

			assert!(matches!(
				result,
				Err(NixStoragePluginError::InvalidLocalStorageState(message)) if message.contains("not a .tar file")
			));
		})
	}

	#[test]
	fn registry_state_response_for_request_returns_not_found_when_image_load_fails() {
		smol::block_on(async {
			let state = RegistryState::default();
			let response = state
				.response_for_request(&Method::GET, "/v2/tmp/bad.tar/manifests/latest")
				.await;

			assert_eq!(response.status(), StatusCode::NOT_FOUND);
			assert_eq!(
				response_body(response).await,
				Bytes::from_static(b"not found\n")
			);
		})
	}

	#[hegel::test(derandomize = true)]
	fn registry_cache_get_or_load_archive_path_with_caches_successful_loads(tc: hegel::TestCase) {
		smol::block_on(async {
			let cache = RegistryCache::default();
			let path = PathBuf::from(format!(
				"/nix/store/{}.tar",
				tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
			));
			let manifest = Bytes::from(tc.draw(generators::binary()));
			let config = Bytes::from(tc.draw(generators::binary()));
			let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
			let first = cache
				.get_or_load_archive_path_with(path.clone(), {
					let loads = loads.clone();
					let manifest = manifest.clone();
					let config = config.clone();
					move |_| async move {
						loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
						Ok(ServedArchiveImage {
							manifest_digest: sha256_digest(&manifest),
							manifest,
							config_digest: sha256_digest(&config),
							config,
							blobs: BTreeMap::new(),
						})
					}
				})
				.await
				.expect("first load should succeed");
			let second = cache
				.get_or_load_archive_path_with(path, |_| async move {
					panic!("cache hit should skip loader")
				})
				.await
				.expect("cached load should succeed");

			assert!(Arc::ptr_eq(&first, &second));
			assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 1);
		})
	}

	#[hegel::test(derandomize = true)]
	fn registry_cache_get_or_load_archive_path_with_does_not_cache_errors(tc: hegel::TestCase) {
		smol::block_on(async {
			let cache = RegistryCache::default();
			let path = PathBuf::from(format!(
				"/nix/store/{}.tar",
				tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
			));
			let message = tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,24}").fullmatch(true));
			let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));

			for _ in 0..2 {
				let result = cache
					.get_or_load_archive_path_with(path.clone(), {
						let loads = loads.clone();
						let message = message.clone();
						move |_| async move {
							loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
							Err(NixStoragePluginError::InvalidImageRef(message))
						}
					})
					.await;
				assert!(
					matches!(result, Err(NixStoragePluginError::InvalidImageRef(actual)) if actual == message)
				);
			}

			assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 2);
		})
	}

	#[hegel::test(derandomize = true)]
	fn load_exported_dir_reads_manifest_config_and_layer_blobs(tc: hegel::TestCase) {
		smol::block_on(async {
			let dir = tempfile::tempdir().expect("tempdir should exist");
			let config_bytes = tc.draw(generators::binary());
			let layer_bytes = tc.draw(generators::binary());
			let config_digest = sha256_digest(&config_bytes);
			let layer_digest = sha256_digest(&layer_bytes);
			let manifest_bytes = serde_json::to_vec(&serde_json::json!({
				"schemaVersion": 2,
				"config": {
					"mediaType": "application/vnd.oci.image.config.v1+json",
					"digest": config_digest,
					"size": config_bytes.len(),
				},
				"layers": [{
					"mediaType": "application/vnd.oci.image.layer.v1.tar",
					"digest": layer_digest,
					"size": layer_bytes.len(),
				}],
			}))
			.expect("manifest should serialize");
			smol::fs::write(dir.path().join("manifest.json"), &manifest_bytes)
				.await
				.expect("manifest should be written");
			smol::fs::write(
				dir.path()
					.join(sha256_blob_file_name(&config_digest).expect("config digest name")),
				&config_bytes,
			)
			.await
			.expect("config blob should be written");
			smol::fs::write(
				dir.path()
					.join(sha256_blob_file_name(&layer_digest).expect("layer digest name")),
				&layer_bytes,
			)
			.await
			.expect("layer blob should be written");

			let image = load_exported_dir(dir.path())
				.await
				.expect("exported dir should load");

			assert_eq!(image.manifest_digest, sha256_digest(&manifest_bytes));
			assert_eq!(image.manifest, Bytes::from(manifest_bytes));
			assert_eq!(image.config_digest, config_digest);
			assert_eq!(image.config, Bytes::from(config_bytes));
			assert_eq!(
				image.blobs.get(&layer_digest),
				Some(&Bytes::from(layer_bytes))
			);
		})
	}

	#[test]
	fn registry_target_parse_rejects_unknown_v2_suffix() {
		assert!(RegistryTarget::parse("/v2/repo/tags/list").is_none());
	}

	#[hegel::test(derandomize = true)]
	fn archive_path_from_local_repo_accepts_nix_store_tar_paths(tc: hegel::TestCase) {
		smol::block_on(async {
			let repo = format!(
				"nix/store/{}.tar",
				tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
			);
			let path = archive_path_from_local_repo(&repo)
				.await
				.expect("valid nix store tar repo should resolve");
			assert_eq!(path, PathBuf::from(format!("/{repo}")));
		});
	}

	#[hegel::test(derandomize = true)]
	fn archive_path_for_repo_uses_local_repo_path_for_non_flake_refs(tc: hegel::TestCase) {
		smol::block_on(async {
			let repo = format!(
				"nix/store/{}.tar",
				tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
			);
			let path = archive_path_for_repo(&repo)
				.await
				.expect("non-flake nix store repo should resolve locally");
			assert_eq!(path, PathBuf::from(format!("/{repo}")));
		});
	}

	#[hegel::test(derandomize = true)]
	fn registry_cache_get_or_load_returns_preseeded_entry_without_loader(tc: hegel::TestCase) {
		smol::block_on(async {
			let cache = RegistryCache::default();
			let repo = format!(
				"nix/store/{}.tar",
				tc.draw(generators::from_regex(r"[A-Za-z0-9._+-]{1,24}").fullmatch(true))
			);
			let archive_path = PathBuf::from(format!("/{repo}"));
			let cache_key = archive_path.display().to_string();
			let manifest = Bytes::from(tc.draw(generators::binary()));
			let config = Bytes::from(tc.draw(generators::binary()));
			let preseeded = served_image_with(
				sha256_digest(&manifest),
				manifest,
				sha256_digest(&config),
				config,
				BTreeMap::new(),
			);
			cache
				.images
				.write()
				.expect("registry cache write lock")
				.insert(cache_key, preseeded.clone());

			let loaded = cache
				.get_or_load(&repo)
				.await
				.expect("preseeded cache should satisfy lookup");
			assert!(Arc::ptr_eq(&loaded, &preseeded));
		});
	}

	#[hegel::test(derandomize = true)]
	fn load_exported_dir_reads_manifest_and_config_when_no_layers(tc: hegel::TestCase) {
		smol::block_on(async {
			let dir = tempfile::tempdir().expect("tempdir should exist");
			let config_bytes = tc.draw(generators::binary());
			let config_digest = sha256_digest(&config_bytes);
			let manifest_bytes = serde_json::to_vec(&serde_json::json!({
				"schemaVersion": 2,
				"config": {
					"mediaType": "application/vnd.oci.image.config.v1+json",
					"digest": config_digest,
					"size": config_bytes.len(),
				},
				"layers": [],
			}))
			.expect("manifest should serialize");
			smol::fs::write(dir.path().join("manifest.json"), &manifest_bytes)
				.await
				.expect("manifest should be written");
			smol::fs::write(
				dir.path()
					.join(sha256_blob_file_name(&config_digest).expect("config digest name")),
				&config_bytes,
			)
			.await
			.expect("config blob should be written");

			let image = load_exported_dir(dir.path())
				.await
				.expect("exported dir should load");
			assert_eq!(image.manifest_digest, sha256_digest(&manifest_bytes));
			assert_eq!(image.config_digest, config_digest);
			assert_eq!(image.blobs, BTreeMap::new());
		});
	}

	#[hegel::test(derandomize = true)]
	fn read_exported_blob_returns_io_error_when_blob_file_missing(tc: hegel::TestCase) {
		smol::block_on(async {
			let dir = tempfile::tempdir().expect("tempdir should exist");
			let digest = sha256_digest_string(&tc);
			let error = read_exported_blob(dir.path(), &digest)
				.await
				.expect_err("missing blob file should fail");
			assert!(matches!(error, NixStoragePluginError::Io(_)));
		});
	}

	fn flake_repo_from_ref(flake_ref: &str) -> String {
		encode_flake_ref(flake_ref)
			.expect("flake ref should encode")
			.replacen(":0/", "/", 1)
	}

	#[test]
	#[cfg_attr(no_test_network, ignore = "NO_TEST_NETWORK is set")]
	fn flake_archive_resolution_and_loading_paths_work_end_to_end() {
		smol::block_on(async {
			let flake_ref = std::env::var("NSP_E2E_FLAKE_REF")
				.unwrap_or_else(|_| "github:pdtpartners/nix-snapshotter#image-hello".to_owned());
			let repo = flake_repo_from_ref(&flake_ref);
			let archive_path = archive_path_for_repo(&repo)
				.await
				.expect("flake repo should build archive path");
			assert!(archive_path.starts_with("/nix/store/"));
			assert_eq!(
				archive_path.extension().and_then(|ext| ext.to_str()),
				Some("tar")
			);
			assert!(
				smol::fs::metadata(&archive_path).await.is_ok(),
				"built archive path should exist"
			);

			let loaded = load_archive_image(&archive_path)
				.await
				.expect("archive should load through skopeo export");
			assert!(!loaded.manifest.is_empty());
			assert!(!loaded.config.is_empty());
			assert!(
				loaded.config_digest.starts_with("sha256:"),
				"config digest should be sha256"
			);

			let local_repo = archive_path
				.strip_prefix("/")
				.expect("archive path should be absolute")
				.display()
				.to_string();
			let cache = RegistryCache::default();
			let first = cache
				.get_or_load(&local_repo)
				.await
				.expect("cache should load archive via local repo path");
			let second = cache
				.get_or_load(&local_repo)
				.await
				.expect("cache should hit loaded archive");
			assert!(Arc::ptr_eq(&first, &second));
		});
	}
}

pub async fn run_registry_server(bind_addr: SocketAddr) -> Result<(), NixStoragePluginError> {
	let state = RegistryState::default();
	let listener = Async::<TcpListener>::bind(bind_addr)?;
	tracing::info!(
		bind = %bind_addr,
		default_bind = DEFAULT_REGISTRY_BIND_ADDR,
		registry_prefixes = %format!("nix:0, {}", flake_registry_prefixes_log_value()),
		"starting local nix image registry adapter"
	);

	loop {
		let (stream, _) = listener.accept().await?;
		let state = state.clone();

		smol::spawn(async move {
			let service = service_fn(move |req| {
				let state = state.clone();
				async move { Ok::<_, Infallible>(state.response(req).await) }
			});

			if let Err(error) = http1::Builder::new()
				.serve_connection(FuturesIo::new(stream), service)
				.await
			{
				tracing::warn!(%error, "registry connection failed")
			}
		})
		.detach();
	}
}

async fn load_archive_image(
	archive_path: &Path,
) -> Result<ServedArchiveImage, NixStoragePluginError> {
	let archive_ref = format!("oci-archive:{}", archive_path.display());
	let export_dir =
		export_source_to_temp_dir(&archive_ref, "nix-storage-plugin-registry-", &[]).await?;
	load_exported_dir(export_dir.path()).await
}

async fn load_exported_dir(
	export_dir_path: &Path,
) -> Result<ServedArchiveImage, NixStoragePluginError> {
	let manifest = Bytes::from(fs::read(export_dir_path.join("manifest.json")).await?);
	let manifest_digest = sha256_digest(&manifest);
	let parsed_manifest: ImageManifest = serde_json::from_slice(&manifest)?;
	let config_digest = parsed_manifest.config().digest().to_string();
	let config = read_exported_blob(export_dir_path, &config_digest).await?;
	let mut blobs = BTreeMap::new();
	for layer in parsed_manifest.layers() {
		let digest = layer.digest().to_string();
		let blob = read_exported_blob(export_dir_path, &digest).await?;
		blobs.insert(digest, blob);
	}

	Ok(ServedArchiveImage {
		manifest_digest,
		manifest,
		config_digest,
		config,
		blobs,
	})
}

async fn archive_path_for_repo(repo: &str) -> Result<PathBuf, NixStoragePluginError> {
	if let Some(installable) = decode_flake_installable_from_repo(repo)? {
		return build_flake_archive_path(&installable).await;
	}

	archive_path_from_local_repo(repo).await
}

async fn archive_path_from_local_repo(repo: &str) -> Result<PathBuf, NixStoragePluginError> {
	let path = PathBuf::from(format!("/{repo}"));
	validate_archive_path(&path, repo)?;
	try_realize_nix_archive_path(&path).await;
	Ok(path)
}

async fn build_flake_archive_path(installable: &str) -> Result<PathBuf, NixStoragePluginError> {
	tracing::info!(%installable, "building flake image archive on demand");
	let output = host_command(&[
		"nix",
		"build",
		"--no-link",
		"--print-out-paths",
		"--extra-experimental-features",
		"nix-command flakes",
		"--",
		installable,
	])
	.await?;
	let path = parse_flake_build_output(&output, installable)?;
	if fs::metadata(&path).await.is_err() {
		return Err(NixStoragePluginError::InvalidLocalStorageState(format!(
			"flake build output path does not exist: {}",
			path.display(),
		)));
	}
	tracing::info!(%installable, archive = %path.display(), "built flake image archive");
	Ok(path)
}

fn parse_flake_build_output(
	output: &str,
	installable: &str,
) -> Result<PathBuf, NixStoragePluginError> {
	let out_paths = output
		.lines()
		.map(str::trim)
		.filter(|line| !line.is_empty())
		.collect::<Vec<_>>();
	let Some(path_str) = out_paths.first() else {
		return Err(NixStoragePluginError::InvalidLocalStorageState(format!(
			"flake build returned no output path for {installable}",
		)));
	};
	if out_paths.len() != 1 {
		return Err(NixStoragePluginError::InvalidLocalStorageState(format!(
			"flake build returned multiple output paths for {installable}: {}",
			out_paths.join(", "),
		)));
	}
	let path = PathBuf::from(path_str);
	validate_archive_path(&path, installable)?;
	Ok(path)
}

fn validate_archive_path(path: &Path, image_ref: &str) -> Result<(), NixStoragePluginError> {
	if !path.starts_with("/nix/store/") {
		return Err(NixStoragePluginError::InvalidImageRef(image_ref.to_owned()));
	}
	if path.extension().and_then(|ext| ext.to_str()) != Some("tar") {
		return Err(NixStoragePluginError::InvalidLocalStorageState(format!(
			"image archive path is not a .tar file: {}",
			path.display(),
		)));
	}

	Ok(())
}

async fn read_exported_blob(
	export_dir: &Path,
	digest: &str,
) -> Result<Bytes, NixStoragePluginError> {
	let file_name = sha256_blob_file_name(digest).ok_or_else(|| {
		NixStoragePluginError::InvalidLocalStorageState(format!(
			"unsupported exported digest format: {digest}",
		))
	})?;
	Ok(Bytes::from(fs::read(export_dir.join(file_name)).await?))
}
