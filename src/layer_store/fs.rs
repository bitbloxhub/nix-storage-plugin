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
