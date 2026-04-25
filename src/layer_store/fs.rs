use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use fuse3::FileType;
use fuse3::path::prelude::{
	DirectoryEntry, DirectoryEntryPlus, PathFilesystem, ReplyAttr, ReplyCreated, ReplyDirectory,
	ReplyDirectoryPlus, ReplyEntry, ReplyInit, Request,
};
use fuse3::raw::reply::{ReplyData, ReplyOpen, ReplyStatFs};
use futures_util::stream;

use crate::common::{TTL, dir_attr, file_attr};
use crate::metadata::{LayerDiffEntry, LayerDiffEntryKind};

use super::{LayerStoreDiff, LayerStoreImage, LayerStoreLayer, LayerStoreResolver};

#[derive(Debug, Clone)]
enum StorePath {
	Root,
	Image {
		encoded_ref: String,
	},
	Layer {
		encoded_ref: String,
		layer_key: String,
	},
	LayerEntry {
		encoded_ref: String,
		layer_key: String,
		entry: String,
	},
	Diff {
		encoded_ref: String,
		layer_key: String,
		relative: Vec<String>,
	},
}

#[derive(Clone)]
pub struct LayerStoreFS {
	backend: Arc<LayerStoreResolver>,
}

impl LayerStoreFS {
	pub fn new(backend: Arc<LayerStoreResolver>) -> Self {
		Self { backend }
	}

	fn root_attr() -> fuse3::path::reply::FileAttr {
		dir_attr(0o555)
	}

	fn symlink_attr(size: usize) -> fuse3::path::reply::FileAttr {
		let now = std::time::SystemTime::now();

		fuse3::path::reply::FileAttr {
			size: size as u64,
			blocks: 1,
			atime: now,
			mtime: now,
			ctime: now,
			kind: FileType::Symlink,
			perm: 0o777,
			nlink: 1,
			uid: 0,
			gid: 0,
			rdev: 0,
			blksize: 4096,
		}
	}

	fn path_components(path: &OsStr) -> Vec<String> {
		path.to_string_lossy()
			.split('/')
			.filter(|component| !component.is_empty())
			.map(str::to_owned)
			.collect()
	}

	fn starts_with(haystack: &[String], needle: &[String]) -> bool {
		haystack.len() >= needle.len()
			&& haystack
				.iter()
				.zip(needle)
				.all(|(left, right)| left == right)
	}

	fn parse_path(path: &OsStr) -> Option<StorePath> {
		let components = Self::path_components(path);

		match components.as_slice() {
			[] => Some(StorePath::Root),
			[encoded_ref] => Some(StorePath::Image {
				encoded_ref: encoded_ref.clone(),
			}),
			[encoded_ref, layer_key] => Some(StorePath::Layer {
				encoded_ref: encoded_ref.clone(),
				layer_key: layer_key.clone(),
			}),
			[encoded_ref, layer_key, entry] if entry != "diff" => Some(StorePath::LayerEntry {
				encoded_ref: encoded_ref.clone(),
				layer_key: layer_key.clone(),
				entry: entry.clone(),
			}),
			[encoded_ref, layer_key, diff, relative @ ..] if diff == "diff" => {
				Some(StorePath::Diff {
					encoded_ref: encoded_ref.clone(),
					layer_key: layer_key.clone(),
					relative: relative.to_vec(),
				})
			}
			_ => None,
		}
	}

	async fn lookup_image(&self, encoded_ref: &str) -> Option<LayerStoreImage> {
		self.backend.image_by_encoded_ref(encoded_ref).await
	}

	async fn lookup_layer(&self, encoded_ref: &str, layer_key: &str) -> Option<LayerStoreLayer> {
		self.lookup_image(encoded_ref)
			.await
			.and_then(|image| image.layer_by_key(layer_key).cloned())
	}

	fn layer_entry_attr(
		layer: &LayerStoreLayer,
		entry: &str,
	) -> Option<fuse3::path::reply::FileAttr> {
		match entry {
			"info" => Some(file_attr(layer.info.len(), 0o444)),
			"blob" => Some(file_attr(layer.blob.len(), 0o444)),
			_ => None,
		}
	}

	fn layer_entry_bytes(layer: LayerStoreLayer, entry: &str) -> Option<Bytes> {
		match entry {
			"info" => Some(layer.info),
			"blob" => Some(layer.blob),
			_ => None,
		}
	}

	fn layer_dir_entries() -> Vec<DirectoryEntry> {
		vec![
			DirectoryEntry {
				kind: FileType::Directory,
				name: OsString::from("diff"),
				offset: 1,
			},
			DirectoryEntry {
				kind: FileType::RegularFile,
				name: OsString::from("info"),
				offset: 2,
			},
			DirectoryEntry {
				kind: FileType::RegularFile,
				name: OsString::from("blob"),
				offset: 3,
			},
		]
	}

	fn path_components_from_path(path: &Path) -> Vec<String> {
		path.components()
			.filter_map(|component| match component {
				Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
				_ => None,
			})
			.collect()
	}

	fn virtual_root_components(path: &Path) -> Vec<String> {
		Self::path_components_from_path(path)
	}

	fn host_path_from_virtual(root: &Path, relative: &[String]) -> PathBuf {
		let root_components = Self::virtual_root_components(root);
		let suffix = relative.get(root_components.len()..).unwrap_or(&[]);
		suffix
			.iter()
			.fold(root.to_path_buf(), |path, component| path.join(component))
	}

	fn attr_from_virtual_entry(entry: &LayerDiffEntry) -> fuse3::path::reply::FileAttr {
		match &entry.kind {
			LayerDiffEntryKind::Directory => {
				dir_attr(if entry.perm == 0 { 0o555 } else { entry.perm })
			}
			LayerDiffEntryKind::Regular { contents } => file_attr(
				contents.len(),
				if entry.perm == 0 { 0o444 } else { entry.perm },
			),
			LayerDiffEntryKind::Symlink { target } => Self::symlink_attr(target.as_os_str().len()),
		}
	}

	fn file_type_from_virtual_entry(entry: &LayerDiffEntry) -> FileType {
		match entry.kind {
			LayerDiffEntryKind::Directory => FileType::Directory,
			LayerDiffEntryKind::Regular { .. } => FileType::RegularFile,
			LayerDiffEntryKind::Symlink { .. } => FileType::Symlink,
		}
	}

	fn dir_entries_from_map(entries: BTreeMap<String, FileType>) -> Vec<DirectoryEntry> {
		entries
			.into_iter()
			.enumerate()
			.map(|(index, (name, kind))| DirectoryEntry {
				kind,
				name: OsString::from(name),
				offset: (index + 1) as i64,
			})
			.collect()
	}

	fn attr_from_metadata(metadata: &fs::Metadata) -> fuse3::path::reply::FileAttr {
		let mode = (metadata.permissions().mode() & 0o777) as u16;
		if metadata.file_type().is_dir() {
			return dir_attr(if mode == 0 { 0o555 } else { mode });
		}
		if metadata.file_type().is_symlink() {
			return Self::symlink_attr(metadata.len() as usize);
		}

		file_attr(
			metadata.len() as usize,
			if mode == 0 { 0o444 } else { mode },
		)
	}

	fn file_type_from_metadata(metadata: &fs::Metadata) -> FileType {
		if metadata.file_type().is_dir() {
			FileType::Directory
		} else if metadata.file_type().is_symlink() {
			FileType::Symlink
		} else {
			FileType::RegularFile
		}
	}

	fn path_from_parent(parent: &OsStr, name: &OsStr) -> OsString {
		if parent == OsStr::new("/") {
			OsString::from(format!("/{}", name.to_string_lossy()))
		} else {
			OsString::from(format!(
				"{}/{}",
				parent.to_string_lossy(),
				name.to_string_lossy()
			))
		}
	}

	async fn file_count_hint(&self) -> u64 {
		let mut files = 1_u64;

		for image in self.backend.images().await {
			files += 1;
			for _layer in image.layers {
				files += 4;
			}
		}

		files
	}

	async fn entry_attr(&self, path: &OsStr) -> Option<fuse3::path::reply::FileAttr> {
		match Self::parse_path(path)? {
			StorePath::Root => Some(Self::root_attr()),
			StorePath::Image { encoded_ref } => self
				.lookup_image(&encoded_ref)
				.await
				.map(|_| dir_attr(0o555)),
			StorePath::Layer {
				encoded_ref,
				layer_key,
			} => self
				.lookup_layer(&encoded_ref, &layer_key)
				.await
				.map(|_| dir_attr(0o555)),
			StorePath::LayerEntry {
				encoded_ref,
				layer_key,
				entry,
			} => {
				let layer = self.lookup_layer(&encoded_ref, &layer_key).await?;
				Self::layer_entry_attr(&layer, &entry)
			}
			StorePath::Diff {
				encoded_ref,
				layer_key,
				relative,
			} => {
				let layer = self.lookup_layer(&encoded_ref, &layer_key).await?;
				self.diff_entry_attr(&layer.diff, &relative).await
			}
		}
	}

	async fn read_bytes(&self, path: &OsStr) -> Option<Bytes> {
		match Self::parse_path(path)? {
			StorePath::LayerEntry {
				encoded_ref,
				layer_key,
				entry,
			} => {
				let layer = self.lookup_layer(&encoded_ref, &layer_key).await?;
				Self::layer_entry_bytes(layer, &entry)
			}
			StorePath::Diff {
				encoded_ref,
				layer_key,
				relative,
			} => {
				let layer = self.lookup_layer(&encoded_ref, &layer_key).await?;
				self.diff_read_bytes(&layer.diff, &relative).await
			}
			_ => None,
		}
	}

	async fn read_link(&self, path: &OsStr) -> Option<Bytes> {
		match Self::parse_path(path)? {
			StorePath::Diff {
				encoded_ref,
				layer_key,
				relative,
			} => {
				let layer = self.lookup_layer(&encoded_ref, &layer_key).await?;
				self.diff_read_link(&layer.diff, &relative).await
			}
			_ => None,
		}
	}

	async fn dir_entries(&self, path: &OsStr) -> Option<Vec<DirectoryEntry>> {
		match Self::parse_path(path)? {
			StorePath::Root => Some(self.root_dir_entries().await),
			StorePath::Image { encoded_ref } => self.image_dir_entries(&encoded_ref).await,
			StorePath::Layer {
				encoded_ref,
				layer_key,
			} => {
				self.lookup_layer(&encoded_ref, &layer_key).await?;
				Some(Self::layer_dir_entries())
			}
			StorePath::Diff {
				encoded_ref,
				layer_key,
				relative,
			} => {
				let layer = self.lookup_layer(&encoded_ref, &layer_key).await?;
				self.diff_dir_entries(&layer.diff, &relative).await
			}
			_ => None,
		}
	}

	async fn root_dir_entries(&self) -> Vec<DirectoryEntry> {
		self.backend
			.images()
			.await
			.into_iter()
			.enumerate()
			.map(|(index, image)| DirectoryEntry {
				kind: FileType::Directory,
				name: OsString::from(image.encoded_ref),
				offset: (index + 1) as i64,
			})
			.collect()
	}

	async fn image_dir_entries(&self, encoded_ref: &str) -> Option<Vec<DirectoryEntry>> {
		let image = self.lookup_image(encoded_ref).await?;
		let mut seen = BTreeSet::new();
		Some(
			image
				.layers
				.into_iter()
				.flat_map(|layer| layer.keys.into_iter())
				.filter(|key| seen.insert(key.clone()))
				.enumerate()
				.map(|(index, key)| DirectoryEntry {
					kind: FileType::Directory,
					name: OsString::from(key),
					offset: (index + 1) as i64,
				})
				.collect(),
		)
	}

	async fn diff_entry_attr(
		&self,
		diff: &LayerStoreDiff,
		relative: &[String],
	) -> Option<fuse3::path::reply::FileAttr> {
		if diff.is_empty() {
			if relative.is_empty() {
				return Some(dir_attr(0o555));
			}
			return None;
		}

		if let Some(attr) = Self::virtual_diff_entry_attr(&diff.tar_entries, relative) {
			Some(attr)
		} else {
			Self::host_diff_entry_attr(&diff.host_projection_roots, relative).await
		}
	}

	async fn diff_read_bytes(&self, diff: &LayerStoreDiff, relative: &[String]) -> Option<Bytes> {
		if let Some(bytes) = Self::virtual_diff_read_bytes(&diff.tar_entries, relative) {
			Some(bytes)
		} else {
			Self::host_diff_read_bytes(&diff.host_projection_roots, relative).await
		}
	}

	async fn diff_read_link(&self, diff: &LayerStoreDiff, relative: &[String]) -> Option<Bytes> {
		if let Some(target) = Self::virtual_diff_read_link(&diff.tar_entries, relative) {
			Some(target)
		} else {
			Self::host_diff_read_link(&diff.host_projection_roots, relative).await
		}
	}

	async fn diff_dir_entries(
		&self,
		diff: &LayerStoreDiff,
		relative: &[String],
	) -> Option<Vec<DirectoryEntry>> {
		if diff.is_empty() {
			if relative.is_empty() {
				return Some(Vec::new());
			}
			return None;
		}

		Self::merge_dir_entries(
			Self::virtual_diff_dir_entries(&diff.tar_entries, relative),
			Self::host_diff_dir_entries(&diff.host_projection_roots, relative).await,
		)
	}

	fn virtual_diff_entry_attr(
		entries: &[LayerDiffEntry],
		relative: &[String],
	) -> Option<fuse3::path::reply::FileAttr> {
		if relative.is_empty() {
			return Some(dir_attr(0o555));
		}

		for entry in entries {
			let components = Self::path_components_from_path(&entry.path);
			if components == relative {
				return Some(Self::attr_from_virtual_entry(entry));
			}
			if Self::starts_with(&components, relative) {
				return Some(dir_attr(0o555));
			}
		}

		None
	}

	fn virtual_diff_read_bytes(entries: &[LayerDiffEntry], relative: &[String]) -> Option<Bytes> {
		entries.iter().find_map(|entry| {
			if Self::path_components_from_path(&entry.path) != relative {
				return None;
			}
			match &entry.kind {
				LayerDiffEntryKind::Regular { contents } => Some(contents.clone()),
				_ => None,
			}
		})
	}

	fn virtual_diff_read_link(entries: &[LayerDiffEntry], relative: &[String]) -> Option<Bytes> {
		entries.iter().find_map(|entry| {
			if Self::path_components_from_path(&entry.path) != relative {
				return None;
			}
			match &entry.kind {
				LayerDiffEntryKind::Symlink { target } => {
					Some(Bytes::from(target.to_string_lossy().as_bytes().to_vec()))
				}
				_ => None,
			}
		})
	}

	fn virtual_diff_dir_entries(
		entries: &[LayerDiffEntry],
		relative: &[String],
	) -> Option<Vec<DirectoryEntry>> {
		let mut dir_entries = BTreeMap::new();
		let mut found = relative.is_empty();

		for entry in entries {
			let components = Self::path_components_from_path(&entry.path);
			if components == relative {
				found = matches!(entry.kind, LayerDiffEntryKind::Directory);
				continue;
			}
			if !Self::starts_with(&components, relative) || components.len() <= relative.len() {
				continue;
			}
			found = true;
			let next = components[relative.len()].clone();
			let kind = if components.len() == relative.len() + 1 {
				Self::file_type_from_virtual_entry(entry)
			} else {
				FileType::Directory
			};
			dir_entries.entry(next).or_insert(kind);
		}

		if !found {
			return None;
		}

		Some(Self::dir_entries_from_map(dir_entries))
	}

	fn merge_dir_entries(
		left: Option<Vec<DirectoryEntry>>,
		right: Option<Vec<DirectoryEntry>>,
	) -> Option<Vec<DirectoryEntry>> {
		let mut merged = BTreeMap::new();
		let mut found = false;

		if let Some(entries) = left {
			found = true;
			for entry in entries {
				merged.insert(entry.name.to_string_lossy().into_owned(), entry.kind);
			}
		}
		if let Some(entries) = right {
			found = true;
			for entry in entries {
				merged
					.entry(entry.name.to_string_lossy().into_owned())
					.or_insert(entry.kind);
			}
		}

		if !found {
			return None;
		}

		Some(Self::dir_entries_from_map(merged))
	}

	async fn host_diff_entry_attr(
		roots: &[PathBuf],
		relative: &[String],
	) -> Option<fuse3::path::reply::FileAttr> {
		if relative.is_empty() {
			return Some(dir_attr(0o555));
		}

		for root in roots {
			let root_components = Self::virtual_root_components(root);
			if relative.len() < root_components.len()
				&& Self::starts_with(&root_components, relative)
			{
				return Some(dir_attr(0o555));
			}

			if !Self::starts_with(relative, &root_components) {
				continue;
			}

			let host_path = Self::host_path_from_virtual(root, relative);
			if let Ok(metadata) = smol::fs::symlink_metadata(&host_path).await {
				return Some(Self::attr_from_metadata(&metadata));
			}
		}

		None
	}

	async fn host_diff_read_bytes(roots: &[PathBuf], relative: &[String]) -> Option<Bytes> {
		for root in roots {
			let root_components = Self::virtual_root_components(root);
			if !Self::starts_with(relative, &root_components) {
				continue;
			}

			let host_path = Self::host_path_from_virtual(root, relative);
			let metadata = smol::fs::symlink_metadata(&host_path).await.ok()?;
			if metadata.file_type().is_file() {
				return smol::fs::read(host_path).await.ok().map(Bytes::from);
			}
		}

		None
	}

	async fn host_diff_read_link(roots: &[PathBuf], relative: &[String]) -> Option<Bytes> {
		for root in roots {
			let root_components = Self::virtual_root_components(root);
			if !Self::starts_with(relative, &root_components) {
				continue;
			}

			let host_path = Self::host_path_from_virtual(root, relative);
			let metadata = smol::fs::symlink_metadata(&host_path).await.ok()?;
			if metadata.file_type().is_symlink() {
				return smol::fs::read_link(host_path)
					.await
					.ok()
					.map(|path| Bytes::from(path.to_string_lossy().as_bytes().to_vec()));
			}
		}

		None
	}

	async fn host_diff_dir_entries(
		roots: &[PathBuf],
		relative: &[String],
	) -> Option<Vec<DirectoryEntry>> {
		let mut entries = BTreeMap::new();
		let mut found = relative.is_empty();

		for root in roots {
			let root_components = Self::virtual_root_components(root);
			if relative.len() < root_components.len()
				&& Self::starts_with(&root_components, relative)
			{
				found = true;
				let next = root_components[relative.len()].clone();
				let kind = if relative.len() + 1 == root_components.len() {
					match smol::fs::symlink_metadata(root).await {
						Ok(metadata) => Self::file_type_from_metadata(&metadata),
						Err(_) => FileType::Directory,
					}
				} else {
					FileType::Directory
				};
				entries.entry(next).or_insert(kind);
				continue;
			}

			if !Self::starts_with(relative, &root_components) {
				continue;
			}

			let host_path = Self::host_path_from_virtual(root, relative);
			let metadata = match smol::fs::symlink_metadata(&host_path).await {
				Ok(metadata) => metadata,
				Err(_) => continue,
			};
			if !metadata.file_type().is_dir() {
				continue;
			}

			found = true;
			let dir_entries = smol::unblock({
				let host_path = host_path.clone();
				move || {
					let mut dir_entries = Vec::new();
					for entry in fs::read_dir(host_path).ok()? {
						let entry = entry.ok()?;
						let metadata = fs::symlink_metadata(entry.path()).ok()?;
						dir_entries.push((
							entry.file_name().to_string_lossy().into_owned(),
							Self::file_type_from_metadata(&metadata),
						));
					}
					Some(dir_entries)
				}
			})
			.await?;
			for (name, kind) in dir_entries {
				entries.insert(name, kind);
			}
		}

		if !found {
			return None;
		}

		Some(
			entries
				.into_iter()
				.enumerate()
				.map(|(index, (name, kind))| DirectoryEntry {
					kind,
					name: OsString::from(name),
					offset: (index + 1) as i64,
				})
				.collect(),
		)
	}
}

impl PathFilesystem for LayerStoreFS {
	async fn init(&self, _req: Request) -> fuse3::Result<ReplyInit> {
		Ok(ReplyInit {
			max_write: NonZeroU32::new(64 * 1024).expect("non-zero max_write"),
		})
	}

	async fn destroy(&self, _req: Request) {}

	async fn lookup(
		&self,
		_req: Request,
		parent: &OsStr,
		name: &OsStr,
	) -> fuse3::Result<ReplyEntry> {
		let path = Self::path_from_parent(parent, name);

		self.entry_attr(&path)
			.await
			.map(|attr| ReplyEntry { ttl: TTL, attr })
			.ok_or_else(|| libc::ENOENT.into())
	}

	async fn getattr(
		&self,
		_req: Request,
		path: Option<&OsStr>,
		_fh: Option<u64>,
		_flags: u32,
	) -> fuse3::Result<ReplyAttr> {
		let path = path.ok_or_else(|| fuse3::Errno::from(libc::ENOENT))?;
		let attr = self
			.entry_attr(path)
			.await
			.ok_or_else(|| fuse3::Errno::from(libc::ENOENT))?;

		Ok(ReplyAttr { ttl: TTL, attr })
	}

	async fn readlink(&self, _req: Request, path: &OsStr) -> fuse3::Result<ReplyData> {
		self.read_link(path)
			.await
			.map(|data| ReplyData { data })
			.ok_or_else(|| fuse3::Errno::from(libc::ENOENT))
	}

	async fn open(&self, _req: Request, path: &OsStr, _flags: u32) -> fuse3::Result<ReplyOpen> {
		if self.read_bytes(path).await.is_none() {
			return Err(libc::EISDIR.into());
		}

		Ok(ReplyOpen { fh: 0, flags: 0 })
	}

	async fn read(
		&self,
		_req: Request,
		path: Option<&OsStr>,
		_fh: u64,
		offset: u64,
		size: u32,
	) -> fuse3::Result<ReplyData> {
		let path = path.ok_or_else(|| fuse3::Errno::from(libc::ENOENT))?;
		let bytes = self
			.read_bytes(path)
			.await
			.ok_or_else(|| fuse3::Errno::from(libc::ENOENT))?;
		let offset = offset as usize;
		let end = offset.saturating_add(size as usize).min(bytes.len());

		Ok(ReplyData {
			data: if offset >= bytes.len() {
				Bytes::new()
			} else {
				bytes.slice(offset..end)
			},
		})
	}

	async fn opendir(&self, _req: Request, path: &OsStr, _flags: u32) -> fuse3::Result<ReplyOpen> {
		if self.dir_entries(path).await.is_none() {
			return Err(libc::ENOTDIR.into());
		}

		Ok(ReplyOpen { fh: 0, flags: 0 })
	}

	async fn readdir<'a>(
		&'a self,
		_req: Request,
		path: &'a OsStr,
		_fh: u64,
		offset: i64,
	) -> fuse3::Result<
		ReplyDirectory<
			impl futures_util::stream::Stream<Item = fuse3::Result<DirectoryEntry>> + Send + 'a,
		>,
	> {
		let entries = self
			.dir_entries(path)
			.await
			.ok_or_else(|| fuse3::Errno::from(libc::ENOTDIR))?;
		let start = offset.max(0) as usize;
		let sliced = if start >= entries.len() {
			Vec::new()
		} else {
			entries[start..].to_vec()
		};

		Ok(ReplyDirectory {
			entries: stream::iter(sliced.into_iter().map(Ok)),
		})
	}

	async fn readdirplus<'a>(
		&'a self,
		_req: Request,
		path: &'a OsStr,
		_fh: u64,
		offset: u64,
		_lock_owner: u64,
	) -> fuse3::Result<
		ReplyDirectoryPlus<
			impl futures_util::stream::Stream<Item = fuse3::Result<DirectoryEntryPlus>> + Send + 'a,
		>,
	> {
		let entries = self
			.dir_entries(path)
			.await
			.ok_or_else(|| fuse3::Errno::from(libc::ENOTDIR))?;
		let start = offset as usize;
		let sliced = if start >= entries.len() {
			Vec::new()
		} else {
			entries[start..].to_vec()
		};
		let mut plus_entries = Vec::with_capacity(sliced.len());
		for entry in sliced {
			let entry_path = Self::path_from_parent(path, &entry.name);
			let attr = self
				.entry_attr(&entry_path)
				.await
				.ok_or_else(|| fuse3::Errno::from(libc::ENOENT))?;

			plus_entries.push(DirectoryEntryPlus {
				kind: entry.kind,
				name: entry.name,
				offset: entry.offset,
				attr,
				entry_ttl: TTL,
				attr_ttl: TTL,
			});
		}

		Ok(ReplyDirectoryPlus {
			entries: stream::iter(plus_entries.into_iter().map(Ok)),
		})
	}

	async fn create(
		&self,
		_req: Request,
		parent: &OsStr,
		name: &OsStr,
		_mode: u32,
		_flags: u32,
	) -> fuse3::Result<ReplyCreated> {
		if let Some(StorePath::Layer { .. }) = Self::parse_path(parent)
			&& name == OsStr::new("use")
		{
			return Err(libc::ENOENT.into());
		}

		Err(libc::EROFS.into())
	}

	async fn unlink(&self, _req: Request, _parent: &OsStr, _name: &OsStr) -> fuse3::Result<()> {
		Err(libc::ENOENT.into())
	}

	async fn rmdir(&self, _req: Request, _parent: &OsStr, _name: &OsStr) -> fuse3::Result<()> {
		Err(libc::ENOENT.into())
	}

	async fn access(&self, _req: Request, path: &OsStr, _mask: u32) -> fuse3::Result<()> {
		if self.entry_attr(path).await.is_some() {
			Ok(())
		} else {
			Err(libc::ENOENT.into())
		}
	}

	async fn statfs(&self, _req: Request, _path: &OsStr) -> fuse3::Result<ReplyStatFs> {
		Ok(ReplyStatFs {
			blocks: 1,
			bfree: 0,
			bavail: 0,
			files: self.file_count_hint().await,
			ffree: 0,
			bsize: 4096,
			namelen: 255,
			frsize: 4096,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ffi::{OsStr, OsString};
	use std::os::unix::fs as unix_fs;
	use std::path::PathBuf;
	use std::sync::Arc;

	use bytes::Bytes;
	use fuse3::FileType;
	use hegel::generators::{self};

	use super::*;
	use crate::metadata::LayerDiffEntryKind;

	fn dummy_fs() -> LayerStoreFS {
		LayerStoreFS::new(Arc::new(LayerStoreResolver::new()))
	}

	fn regular_entry(path: &str, perm: u16, contents: &[u8]) -> LayerDiffEntry {
		LayerDiffEntry {
			path: PathBuf::from(path),
			perm,
			kind: LayerDiffEntryKind::Regular {
				contents: Bytes::copy_from_slice(contents),
			},
		}
	}

	fn directory_entry(path: &str, perm: u16) -> LayerDiffEntry {
		LayerDiffEntry {
			path: PathBuf::from(path),
			perm,
			kind: LayerDiffEntryKind::Directory,
		}
	}

	fn symlink_entry(path: &str, target: &str) -> LayerDiffEntry {
		LayerDiffEntry {
			path: PathBuf::from(path),
			perm: 0o777,
			kind: LayerDiffEntryKind::Symlink {
				target: PathBuf::from(target),
			},
		}
	}

	fn sample_layer(info: &[u8], blob: &[u8]) -> LayerStoreLayer {
		LayerStoreLayer {
			keys: vec!["sha256:key".to_owned()],
			info: Bytes::copy_from_slice(info),
			blob: Bytes::copy_from_slice(blob),
			diff: LayerStoreDiff::default(),
		}
	}

	#[hegel::test(derandomize = true)]
	fn parse_path_classifies_store_paths(tc: hegel::TestCase) {
		let encoded_ref =
			tc.draw(generators::from_regex(r"[A-Za-z0-9._=+-]{1,24}").fullmatch(true));
		let layer_key = tc.draw(generators::from_regex(r"sha256:[a-f0-9]{8,16}").fullmatch(true));
		let entry = tc.draw(generators::sampled_from(vec!["info", "blob", "use"]));
		let relative = tc.draw(
			generators::vecs(generators::from_regex(r"[A-Za-z0-9._-]{1,12}").fullmatch(true))
				.max_size(4),
		);

		assert!(matches!(
			LayerStoreFS::parse_path(OsStr::new("/")),
			Some(StorePath::Root)
		));
		assert!(matches!(
			LayerStoreFS::parse_path(OsStr::new(&format!("/{encoded_ref}"))),
			Some(StorePath::Image { encoded_ref: actual }) if actual == encoded_ref
		));
		assert!(matches!(
			LayerStoreFS::parse_path(OsStr::new(&format!("/{encoded_ref}/{layer_key}"))),
			Some(StorePath::Layer { encoded_ref: actual_ref, layer_key: actual_key }) if actual_ref == encoded_ref && actual_key == layer_key
		));
		assert!(matches!(
			LayerStoreFS::parse_path(OsStr::new(&format!("/{encoded_ref}/{layer_key}/{entry}"))),
			Some(StorePath::LayerEntry { encoded_ref: actual_ref, layer_key: actual_key, entry: actual_entry }) if actual_ref == encoded_ref && actual_key == layer_key && actual_entry == entry
		));
		let diff_path = if relative.is_empty() {
			format!("/{encoded_ref}/{layer_key}/diff")
		} else {
			format!("/{encoded_ref}/{layer_key}/diff/{}", relative.join("/"))
		};
		assert!(matches!(
			LayerStoreFS::parse_path(OsStr::new(&diff_path)),
			Some(StorePath::Diff { encoded_ref: actual_ref, layer_key: actual_key, relative: actual_relative }) if actual_ref == encoded_ref && actual_key == layer_key && actual_relative == relative
		));
		assert!(LayerStoreFS::parse_path(OsStr::new("/a/b/c/d/e")).is_none());
	}

	#[hegel::test(derandomize = true)]
	fn layer_entry_helpers_expose_info_blob_and_diff_entries(tc: hegel::TestCase) {
		let info = tc.draw(generators::binary());
		let blob = tc.draw(generators::binary());
		let layer = sample_layer(&info, &blob);
		assert_eq!(
			LayerStoreFS::layer_entry_attr(&layer, "info")
				.expect("info attr")
				.size,
			info.len() as u64
		);
		assert_eq!(
			LayerStoreFS::layer_entry_attr(&layer, "blob")
				.expect("blob attr")
				.size,
			blob.len() as u64
		);
		assert!(LayerStoreFS::layer_entry_attr(&layer, "diff").is_none());
		assert_eq!(
			LayerStoreFS::layer_entry_bytes(layer.clone(), "info"),
			Some(Bytes::from(info.clone()))
		);
		assert_eq!(
			LayerStoreFS::layer_entry_bytes(layer, "blob"),
			Some(Bytes::from(blob.clone()))
		);
		assert_eq!(LayerStoreFS::layer_dir_entries().len(), 3);
	}

	#[hegel::test(derandomize = true)]
	fn path_and_virtual_helpers_map_components_correctly(tc: hegel::TestCase) {
		let segments = tc.draw(
			generators::vecs(generators::from_regex(r"[A-Za-z0-9._-]{1,8}").fullmatch(true))
				.min_size(1)
				.max_size(4),
		);
		let root_segments = tc.draw(
			generators::vecs(generators::from_regex(r"[A-Za-z0-9._-]{1,8}").fullmatch(true))
				.min_size(1)
				.max_size(4),
		);
		let child = tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,8}").fullmatch(true));
		let path = format!("/{}/", segments.join("//"));
		assert_eq!(LayerStoreFS::path_components(OsStr::new(&path)), segments);
		assert!(LayerStoreFS::starts_with(
			&["a".into(), "b".into()],
			&["a".into()]
		));
		let root = PathBuf::from(format!("/{}", root_segments.join("/")));
		let relative = root_segments
			.iter()
			.cloned()
			.chain([child.clone()])
			.collect::<Vec<_>>();
		assert_eq!(LayerStoreFS::virtual_root_components(&root), root_segments);
		assert_eq!(
			LayerStoreFS::host_path_from_virtual(&root, &relative),
			root.join(&child)
		);
		assert_eq!(
			LayerStoreFS::path_from_parent(OsStr::new("/"), OsStr::new(&child)),
			OsString::from(format!("/{child}"))
		);
		assert_eq!(
			LayerStoreFS::path_from_parent(OsStr::new("/a"), OsStr::new(&child)),
			OsString::from(format!("/a/{child}"))
		);
	}

	#[hegel::test(derandomize = true)]
	fn virtual_diff_helpers_cover_attr_bytes_links_and_directory_merging(tc: hegel::TestCase) {
		let file_name = tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,8}").fullmatch(true));
		let link_name = tc.draw(generators::from_regex(r"[A-Za-z0-9._-]{1,8}").fullmatch(true));
		let target = tc.draw(generators::from_regex(r"[A-Za-z0-9._/-]{1,16}").fullmatch(true));
		let contents = tc.draw(generators::binary());
		let perm = tc.draw(generators::integers::<u16>());
		let entries = vec![
			directory_entry("bin", 0o755),
			regular_entry(&format!("bin/{file_name}"), perm, &contents),
			symlink_entry(&link_name, &target),
		];
		assert_eq!(
			LayerStoreFS::virtual_diff_entry_attr(&entries, &["bin".into()])
				.expect("bin attr")
				.kind,
			FileType::Directory
		);
		assert_eq!(
			LayerStoreFS::virtual_diff_entry_attr(&entries, &["bin".into(), file_name.clone()])
				.expect("file attr")
				.perm,
			if perm == 0 { 0o444 } else { perm }
		);
		assert_eq!(
			LayerStoreFS::virtual_diff_read_bytes(&entries, &["bin".into(), file_name.clone()]),
			Some(Bytes::from(contents.clone()))
		);
		assert_eq!(
			LayerStoreFS::virtual_diff_read_link(&entries, std::slice::from_ref(&link_name)),
			Some(Bytes::from(target.clone()))
		);
		let dir_entries =
			LayerStoreFS::virtual_diff_dir_entries(&entries, &["bin".into()]).expect("dir entries");
		assert_eq!(dir_entries.len(), 1);
		assert!(LayerStoreFS::virtual_diff_dir_entries(&entries, &["missing".into()]).is_none());
		let merged = LayerStoreFS::merge_dir_entries(
			Some(vec![DirectoryEntry {
				kind: FileType::Directory,
				name: OsString::from("bin"),
				offset: 1,
			}]),
			Some(vec![DirectoryEntry {
				kind: FileType::RegularFile,
				name: OsString::from(file_name),
				offset: 1,
			}]),
		)
		.expect("merged entries");
		assert_eq!(merged.len(), 2);
	}

	#[hegel::test(derandomize = true)]
	fn metadata_helpers_report_types_and_permissions(tc: hegel::TestCase) {
		let file_bytes = tc.draw(generators::binary());
		smol::block_on(async {
			let dir = tempfile::tempdir().expect("tempdir should exist");
			let file = dir.path().join("file");
			let subdir = dir.path().join("dir");
			let link = dir.path().join("link");
			std::fs::write(&file, &file_bytes).expect("file should be written");
			std::fs::create_dir(&subdir).expect("dir should be created");
			unix_fs::symlink("file", &link).expect("symlink should be created");
			let file_meta = std::fs::symlink_metadata(&file).expect("file metadata");
			let dir_meta = std::fs::symlink_metadata(&subdir).expect("dir metadata");
			let link_meta = std::fs::symlink_metadata(&link).expect("link metadata");
			assert_eq!(
				LayerStoreFS::attr_from_metadata(&file_meta).kind,
				FileType::RegularFile
			);
			assert_eq!(
				LayerStoreFS::attr_from_metadata(&dir_meta).kind,
				FileType::Directory
			);
			assert_eq!(
				LayerStoreFS::attr_from_metadata(&link_meta).kind,
				FileType::Symlink
			);
			assert_eq!(
				LayerStoreFS::file_type_from_metadata(&file_meta),
				FileType::RegularFile
			);
			assert_eq!(
				LayerStoreFS::file_type_from_metadata(&dir_meta),
				FileType::Directory
			);
			assert_eq!(
				LayerStoreFS::file_type_from_metadata(&link_meta),
				FileType::Symlink
			);
		})
	}

	#[hegel::test(derandomize = true)]
	fn diff_helpers_use_empty_and_host_projection_roots(tc: hegel::TestCase) {
		let host_bytes = tc.draw(generators::binary());
		smol::block_on(async {
			let fs = dummy_fs();
			let empty = LayerStoreDiff::default();
			assert_eq!(
				fs.diff_entry_attr(&empty, &[])
					.await
					.expect("root attr")
					.kind,
				FileType::Directory
			);
			assert!(
				fs.diff_entry_attr(&empty, &["missing".into()])
					.await
					.is_none()
			);
			assert_eq!(fs.diff_dir_entries(&empty, &[]).await, Some(Vec::new()));
			assert!(
				fs.diff_dir_entries(&empty, &["missing".into()])
					.await
					.is_none()
			);

			let dir = tempfile::tempdir().expect("tempdir should exist");
			let root = dir.path().join("proj");
			std::fs::create_dir_all(root.join("nested")).expect("nested dir should exist");
			std::fs::write(root.join("nested/file"), &host_bytes).expect("host file should exist");
			unix_fs::symlink("file", root.join("nested/link")).expect("host symlink should exist");
			let diff = LayerStoreDiff {
				tar_entries: Vec::new(),
				host_projection_roots: vec![root.clone()],
			};
			let root_components = LayerStoreFS::virtual_root_components(&root);
			assert_eq!(
				LayerStoreFS::host_diff_entry_attr(&diff.host_projection_roots, &root_components)
					.await
					.expect("host root attr")
					.kind,
				FileType::Directory
			);
			let file_relative = root_components
				.iter()
				.cloned()
				.chain(["nested".to_owned(), "file".to_owned()])
				.collect::<Vec<_>>();
			assert_eq!(
				LayerStoreFS::host_diff_read_bytes(&diff.host_projection_roots, &file_relative)
					.await,
				Some(Bytes::from(host_bytes.clone()))
			);
			let link_relative = root_components
				.iter()
				.cloned()
				.chain(["nested".to_owned(), "link".to_owned()])
				.collect::<Vec<_>>();
			assert_eq!(
				LayerStoreFS::host_diff_read_link(&diff.host_projection_roots, &link_relative)
					.await,
				Some(Bytes::from_static(b"file"))
			);
			let nested_relative = root_components
				.iter()
				.cloned()
				.chain(["nested".to_owned()])
				.collect::<Vec<_>>();
			assert!(
				LayerStoreFS::host_diff_dir_entries(&diff.host_projection_roots, &nested_relative)
					.await
					.expect("host dir entries")
					.len() >= 2
			);
			assert_eq!(
				fs.diff_read_bytes(&diff, &file_relative).await,
				Some(Bytes::from(host_bytes))
			);
			assert_eq!(
				fs.diff_read_link(&diff, &link_relative).await,
				Some(Bytes::from_static(b"file"))
			);
			assert!(fs.diff_dir_entries(&diff, &nested_relative).await.is_some());
		})
	}

	#[hegel::test(derandomize = true)]
	fn diff_helpers_prefer_virtual_entries_over_host_projection(tc: hegel::TestCase) {
		let virtual_bytes = tc.draw(generators::binary());
		let host_bytes = tc.draw(generators::binary());
		let perm = tc.draw(generators::integers::<u16>());
		smol::block_on(async {
			let fs = dummy_fs();
			let dir = tempfile::tempdir().expect("tempdir should exist");
			let root = dir.path().join("root");
			std::fs::create_dir_all(root.join("bin")).expect("bin dir should exist");
			std::fs::write(root.join("bin/app"), &host_bytes).expect("host app should exist");
			let diff = LayerStoreDiff {
				tar_entries: vec![regular_entry("bin/app", perm, &virtual_bytes)],
				host_projection_roots: vec![root.clone()],
			};
			assert_eq!(
				fs.diff_read_bytes(&diff, &["bin".into(), "app".into()])
					.await,
				Some(Bytes::from(virtual_bytes.clone()))
			);
			assert_eq!(
				fs.diff_entry_attr(&diff, &["bin".into(), "app".into()])
					.await
					.expect("virtual attr")
					.perm,
				if perm == 0 { 0o444 } else { perm }
			);
		})
	}

	fn seeded_fs_with_image() -> (LayerStoreFS, String, String) {
		let encoded_ref = "encoded-ref".to_owned();
		let layer_key = "sha256:layer-a".to_owned();
		let duplicate_key = "sha256:dup".to_owned();
		let resolver = Arc::new(LayerStoreResolver::new());
		resolver.insert_image_for_test(LayerStoreImage {
			encoded_ref: encoded_ref.clone(),
			layers: vec![
				LayerStoreLayer {
					keys: vec![layer_key.clone(), duplicate_key.clone()],
					info: Bytes::from_static(b"info"),
					blob: Bytes::from_static(b"blob"),
					diff: LayerStoreDiff::default(),
				},
				LayerStoreLayer {
					keys: vec![duplicate_key],
					info: Bytes::from_static(b"info2"),
					blob: Bytes::from_static(b"blob2"),
					diff: LayerStoreDiff::default(),
				},
			],
		});
		(LayerStoreFS::new(resolver), encoded_ref, layer_key)
	}

	#[test]
	fn store_entry_and_directory_helpers_cover_root_image_layer_and_diff_paths() {
		smol::block_on(async {
			let (fs, encoded_ref, layer_key) = seeded_fs_with_image();
			let image_path = format!("/{encoded_ref}");
			let layer_path = format!("/{encoded_ref}/{layer_key}");
			let info_path = format!("/{encoded_ref}/{layer_key}/info");
			let use_path = format!("/{encoded_ref}/{layer_key}/use");
			let diff_path = format!("/{encoded_ref}/{layer_key}/diff");
			let diff_missing_path = format!("/{encoded_ref}/{layer_key}/diff/missing");

			assert_eq!(fs.file_count_hint().await, 10);
			assert_eq!(
				fs.entry_attr(OsStr::new("/"))
					.await
					.expect("root attr")
					.kind,
				FileType::Directory
			);
			assert_eq!(
				fs.entry_attr(OsStr::new(&image_path))
					.await
					.expect("image attr")
					.kind,
				FileType::Directory
			);
			assert_eq!(
				fs.entry_attr(OsStr::new(&layer_path))
					.await
					.expect("layer attr")
					.kind,
				FileType::Directory
			);
			assert_eq!(
				fs.entry_attr(OsStr::new(&info_path))
					.await
					.expect("info attr")
					.size,
				4
			);
			assert_eq!(
				fs.read_bytes(OsStr::new(&info_path)).await,
				Some(Bytes::from_static(b"info"))
			);
			assert_eq!(fs.read_bytes(OsStr::new(&use_path)).await, None);
			assert_eq!(fs.read_bytes(OsStr::new("/")).await, None);
			assert_eq!(fs.read_link(OsStr::new(&info_path)).await, None);
			assert_eq!(fs.read_link(OsStr::new(&image_path)).await, None);

			let root_entries = fs.dir_entries(OsStr::new("/")).await.expect("root entries");
			assert_eq!(root_entries.len(), 1);
			assert_eq!(root_entries[0].name, OsString::from(encoded_ref.clone()));
			let image_entries = fs
				.dir_entries(OsStr::new(&image_path))
				.await
				.expect("image entries");
			assert_eq!(image_entries.len(), 2);
			let layer_entries = fs
				.dir_entries(OsStr::new(&layer_path))
				.await
				.expect("layer entries");
			assert_eq!(layer_entries.len(), 3);
			assert_eq!(
				fs.dir_entries(OsStr::new(&diff_path)).await,
				Some(Vec::new())
			);
			assert_eq!(fs.dir_entries(OsStr::new(&diff_missing_path)).await, None);
			assert_eq!(fs.dir_entries(OsStr::new(&use_path)).await, None);
			assert_eq!(fs.dir_entries(OsStr::new("/missing")).await, None);
		});
	}

	#[test]
	fn virtual_and_host_diff_helpers_cover_missing_and_non_file_non_link_paths() {
		smol::block_on(async {
			let entries = vec![
				directory_entry("a/dir", 0o755),
				symlink_entry("a/link", "target"),
				regular_entry("a/deep/file", 0o644, b"v"),
			];
			assert_eq!(
				LayerStoreFS::virtual_diff_entry_attr(&entries, &["a".to_owned()])
					.expect("prefix should appear as directory")
					.kind,
				FileType::Directory
			);
			assert_eq!(
				LayerStoreFS::virtual_diff_read_bytes(
					&entries,
					&["a".to_owned(), "dir".to_owned()]
				),
				None
			);
			assert_eq!(
				LayerStoreFS::virtual_diff_read_link(&entries, &["a".to_owned(), "dir".to_owned()]),
				None
			);
			let virtual_dir = LayerStoreFS::virtual_diff_dir_entries(&entries, &["a".to_owned()])
				.expect("virtual dir entries");
			assert!(virtual_dir.iter().any(|entry| {
				entry.name == OsString::from("deep") && entry.kind == FileType::Directory
			}));
			assert_eq!(LayerStoreFS::merge_dir_entries(None, None), None);
			assert!(
				LayerStoreFS::merge_dir_entries(
					None,
					Some(vec![DirectoryEntry {
						kind: FileType::RegularFile,
						name: OsString::from("x"),
						offset: 1,
					}]),
				)
				.is_some()
			);

			let dir = tempfile::tempdir().expect("tempdir should exist");
			let root = dir.path().join("root");
			std::fs::create_dir_all(root.join("nested")).expect("nested dir should exist");
			std::fs::write(root.join("file"), b"bytes").expect("file should exist");
			let root_components = LayerStoreFS::virtual_root_components(&root);
			let nested_relative = root_components
				.iter()
				.cloned()
				.chain(["nested".to_owned()])
				.collect::<Vec<_>>();
			let file_relative = root_components
				.iter()
				.cloned()
				.chain(["file".to_owned()])
				.collect::<Vec<_>>();
			assert_eq!(
				LayerStoreFS::host_diff_read_bytes(&[root.clone()], &nested_relative).await,
				None
			);
			assert_eq!(
				LayerStoreFS::host_diff_read_link(&[root.clone()], &file_relative).await,
				None
			);

			let missing_root = dir.path().join("missing-root").join("leaf");
			let missing_components = LayerStoreFS::virtual_root_components(&missing_root);
			let missing_parent = missing_components[..missing_components.len() - 1].to_vec();
			let missing_dir_entries = LayerStoreFS::host_diff_dir_entries(
				std::slice::from_ref(&missing_root),
				&missing_parent,
			)
			.await
			.expect("missing root parent should still produce virtual directory entry");
			assert_eq!(missing_dir_entries.len(), 1);
			assert_eq!(missing_dir_entries[0].kind, FileType::Directory);
			assert_eq!(
				LayerStoreFS::host_diff_dir_entries(&[root], &["nope".to_owned()]).await,
				None
			);
		});
	}
}
