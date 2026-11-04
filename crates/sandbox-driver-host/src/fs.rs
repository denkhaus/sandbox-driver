use std::fs::{FileType, Permissions};
use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sandbox_driver::{DirEntry, Error, FileKind, FileMetadata, Filesystem, ResourceKind, Result};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, BufReader, copy, copy_buf};

const TRANSFER_BUFFER_BYTES: usize = 64 * 1024;

/// Native filesystem access rooted at the sandbox workspace.
///
/// Relative paths resolve against the workspace; absolute paths are used
/// as-is (the host provider is not an isolation boundary, and does not
/// pretend to be one).
pub struct HostFs {
    workspace: PathBuf,
}

impl HostFs {
    pub fn new(workspace: PathBuf) -> Self {
        Self { workspace }
    }

    fn resolve(&self, path: &str) -> PathBuf {
        let candidate = Path::new(path);
        if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            self.workspace.join(candidate)
        }
    }
}

fn kind_of(file_type: FileType) -> FileKind {
    if file_type.is_file() {
        FileKind::File
    } else if file_type.is_dir() {
        FileKind::Directory
    } else if file_type.is_symlink() {
        FileKind::Symlink
    } else {
        FileKind::Other
    }
}

fn io_error(context: impl Into<String>) -> impl FnOnce(io::Error) -> Error {
    let context = context.into();
    move |error| Error::io(context, error)
}

/// A read of a path that is not there is `NotFound`, which callers branch
/// on; any other failure keeps its I/O cause.
fn read_error(path: &str, full: &Path) -> impl FnOnce(io::Error) -> Error {
    let path = path.to_owned();
    let context = format!("reading {}", full.display());
    move |error| {
        if error.kind() == io::ErrorKind::NotFound {
            Error::NotFound {
                resource: ResourceKind::File,
                id:       path,
            }
        } else {
            Error::io(context, error)
        }
    }
}

// Errors log at DEBUG: each one is returned to the caller, which decides
// whether it is a failure. A missing file is often the answer it asked for.
#[async_trait]
impl Filesystem for HostFs {
    #[tracing::instrument(skip_all, fields(provider_kind = "host"), err(level = "debug"))]
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        let full = self.resolve(path);
        let mut file = fs::File::open(&full)
            .await
            .map_err(read_error(path, &full))?;
        let mut bytes = sandbox_driver::BoundedBuffer::new(sandbox_driver::DEFAULT_BUFFER_BYTES);
        let outcome = copy(&mut file, &mut bytes)
            .await
            .map(|_| ())
            .map_err(read_error(path, &full));
        bytes.finish(outcome)
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host"), err(level = "debug"))]
    async fn read_to(
        &self,
        path: &str,
        output: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<()> {
        let full = self.resolve(path);
        let mut file = fs::File::open(&full)
            .await
            .map_err(read_error(path, &full))?;
        copy_buf(
            &mut BufReader::with_capacity(TRANSFER_BUFFER_BYTES, &mut file),
            output,
        )
        .await
        .map(|_| ())
        .map_err(io_error(format!("copying {} to output", full.display())))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host", offset), err(level = "debug"))]
    async fn read_range(&self, path: &str, offset: u64, length: Option<u64>) -> Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let full = self.resolve(path);
        let mut file = fs::File::open(&full)
            .await
            .map_err(read_error(path, &full))?;
        file.seek(io::SeekFrom::Start(offset))
            .await
            .map_err(read_error(path, &full))?;
        let mut bytes = sandbox_driver::BoundedBuffer::new(sandbox_driver::DEFAULT_BUFFER_BYTES);
        let outcome = copy(&mut file.take(length.unwrap_or(u64::MAX)), &mut bytes)
            .await
            .map(|_| ())
            .map_err(read_error(path, &full));
        bytes.finish(outcome)
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "host", byte_count = content.len()),
        err(level = "debug")
    )]
    async fn write(&self, path: &str, content: &[u8]) -> Result<()> {
        let full = self.resolve(path);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(io_error(format!("creating parent of {}", full.display())))?;
        }
        fs::write(&full, content)
            .await
            .map_err(io_error(format!("writing {}", full.display())))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host", byte_count = length), err(level = "debug"))]
    async fn write_from(
        &self,
        path: &str,
        input: &mut (dyn AsyncRead + Unpin + Send),
        length: u64,
    ) -> Result<()> {
        let full = self.resolve(path);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(io_error(format!("creating parent of {}", full.display())))?;
        }
        let outcome = async {
            let mut file = fs::File::create(&full).await?;
            let mut input = BufReader::with_capacity(TRANSFER_BUFFER_BYTES, input.take(length));
            let copied = copy_buf(&mut input, &mut file).await?;
            if copied != length {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "file input ended before its length",
                ));
            }
            Ok(())
        }
        .await;
        outcome.map_err(io_error(format!("writing {}", full.display())))
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "host", byte_count = content.len()),
        err(level = "debug")
    )]
    async fn write_append(&self, path: &str, content: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let full = self.resolve(path);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(io_error(format!("creating parent of {}", full.display())))?;
        }
        let context = io_error(format!("appending to {}", full.display()));
        let outcome = async {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&full)
                .await?;
            file.write_all(content).await?;
            file.flush().await?;
            Ok::<_, io::Error>(())
        }
        .await;
        outcome.map_err(context)
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "host", recursive),
        err(level = "debug")
    )]
    async fn delete(&self, path: &str, recursive: bool) -> Result<()> {
        let full = self.resolve(path);
        let metadata = match fs::symlink_metadata(&full).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(Error::io(format!("inspecting {}", full.display()), error)),
        };
        let outcome = if metadata.is_dir() {
            if recursive {
                fs::remove_dir_all(&full).await
            } else {
                fs::remove_dir(&full).await
            }
        } else {
            fs::remove_file(&full).await
        };
        outcome.map_err(io_error(format!("deleting {}", full.display())))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host"), err(level = "debug"))]
    async fn exists(&self, path: &str) -> Result<bool> {
        Ok(fs::try_exists(self.resolve(path)).await.unwrap_or(false))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host"), err(level = "debug"))]
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let full = self.resolve(path);
        let metadata = fs::metadata(&full)
            .await
            .map_err(io_error(format!("reading metadata of {}", full.display())))?;
        let mut info = FileMetadata::new(kind_of(metadata.file_type()), metadata.len());
        info.modified_at = metadata.modified().ok();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            info.mode = Some(metadata.permissions().mode() & 0o7777);
        }
        Ok(info)
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host", depth), err(level = "debug"))]
    async fn list_dir(&self, path: &str, depth: usize) -> Result<Vec<DirEntry>> {
        let root = self.resolve(path);
        let mut entries = Vec::new();
        let mut pending: Vec<(PathBuf, usize)> = vec![(root.clone(), 1)];
        while let Some((dir, level)) = pending.pop() {
            let mut reader = fs::read_dir(&dir)
                .await
                .map_err(io_error(format!("listing {}", dir.display())))?;
            while let Some(entry) = reader
                .next_entry()
                .await
                .map_err(io_error(format!("listing {}", dir.display())))?
            {
                let file_type = entry
                    .file_type()
                    .await
                    .map_err(io_error(format!("inspecting {}", entry.path().display())))?;
                let relative = entry
                    .path()
                    .strip_prefix(&root)
                    .map_or_else(|_| entry.path(), Path::to_path_buf);
                let kind = kind_of(file_type);
                let mut dir_entry = DirEntry::new(relative.to_string_lossy(), kind);
                if kind == FileKind::File {
                    dir_entry.size = entry.metadata().await.ok().map(|m| m.len());
                }
                if kind == FileKind::Directory && level < depth {
                    pending.push((entry.path(), level + 1));
                }
                entries.push(dir_entry);
            }
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(entries)
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host"), err(level = "debug"))]
    async fn create_dir(&self, path: &str) -> Result<()> {
        let full = self.resolve(path);
        fs::create_dir_all(&full)
            .await
            .map_err(io_error(format!("creating {}", full.display())))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host"), err(level = "debug"))]
    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from_full = self.resolve(from);
        let to_full = self.resolve(to);
        fs::rename(&from_full, &to_full)
            .await
            .map_err(io_error(format!(
                "renaming {} to {}",
                from_full.display(),
                to_full.display()
            )))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host", mode), err(level = "debug"))]
    async fn set_permissions(&self, path: &str, mode: u32) -> Result<()> {
        let full = self.resolve(path);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&full, Permissions::from_mode(mode))
                .await
                .map_err(io_error(format!(
                    "setting permissions on {}",
                    full.display()
                )))
        }
        #[cfg(not(unix))]
        {
            let _ = (full, mode);
            Err(Error::unsupported(
                sandbox_driver::Capability::FsPermissions,
            ))
        }
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host"), err(level = "debug"))]
    async fn upload(&self, local: &Path, remote: &str) -> Result<()> {
        let full = self.resolve(remote);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(io_error(format!("creating parent of {}", full.display())))?;
        }
        fs::copy(local, &full)
            .await
            .map(|_| ())
            .map_err(io_error(format!("uploading to {}", full.display())))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "host"), err(level = "debug"))]
    async fn download(&self, remote: &str, local: &Path) -> Result<()> {
        let full = self.resolve(remote);
        if let Some(parent) = local.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(io_error(format!("creating parent of {}", local.display())))?;
        }
        fs::copy(&full, local)
            .await
            .map(|_| ())
            .map_err(io_error(format!("downloading {}", full.display())))
    }
}
