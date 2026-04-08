use smol::fs;
use std::collections::BTreeMap;
use std::convert::Infallible;
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
	DEFAULT_REGISTRY_BIND_ADDR, NixStoragePluginError, data_response, sha256_blob_file_name,
	sha256_digest, simple_response,
};
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
		if let Some(image) = self
			.images
			.read()
			.expect("registry cache read lock")
			.get(repo)
			.cloned()
		{
			return Ok(image);
		}

		let image = Arc::new(load_archive_image(repo).await?);
		self.images
			.write()
			.expect("registry cache write lock")
			.insert(repo.to_owned(), image.clone());
		Ok(image)
	}
}

#[derive(Clone, Default)]
struct RegistryState {
	cache: Arc<RegistryCache>,
}

impl RegistryState {
	async fn response(&self, req: HttpRequest<Incoming>) -> Response<Full<Bytes>> {
		let path = req.uri().path();
		let empty = req.method() == Method::HEAD;

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

		match target.kind {
			RegistryTargetKind::Manifest(reference) => {
				if reference != image.manifest_digest {
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
				if digest == image.config_digest {
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
				if let Some(blob) = image.blobs.get(&digest) {
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

pub async fn run_registry_server(bind_addr: SocketAddr) -> Result<(), NixStoragePluginError> {
	let state = RegistryState::default();
	let listener = Async::<TcpListener>::bind(bind_addr)?;
	tracing::info!(
		bind = %bind_addr,
		default_bind = DEFAULT_REGISTRY_BIND_ADDR,
		registry_prefix = "nix:0",
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

async fn load_archive_image(repo: &str) -> Result<ServedArchiveImage, NixStoragePluginError> {
	let archive_path = archive_path_for_repo(repo).await?;
	let archive_ref = format!("oci-archive:{}", archive_path.display());
	let export_dir =
		export_source_to_temp_dir(&archive_ref, "nix-storage-plugin-registry-").await?;
	let export_dir_path = export_dir.path();

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
	let path = PathBuf::from(format!("/{repo}"));
	if !path.starts_with("/nix/store/") {
		return Err(NixStoragePluginError::InvalidImageRef(repo.to_owned()));
	}
	if path.extension().and_then(|ext| ext.to_str()) != Some("tar") {
		return Err(NixStoragePluginError::InvalidImageRef(repo.to_owned()));
	}
	if fs::metadata(&path).await.is_err() {
		return Err(NixStoragePluginError::InvalidLocalStorageState(format!(
			"archive path does not exist: {}",
			path.display(),
		)));
	}
	Ok(path)
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
