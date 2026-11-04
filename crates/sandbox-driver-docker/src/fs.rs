//! Hybrid filesystem for Docker sandboxes.
//!
//! File **content** moves through the daemon's archive API — reads that
//! stream and stop at the requested range, writes that carry their missing
//! parent directories, an explicit file mode, and both working on a
//! stopped container — while metadata operations (exists, metadata, list,
//! mkdir, rename, delete, permissions) stay exec-derived. The content paths
//! run no command in the image except `readlink`, to follow a symlink, and
//! `id`, once, to own what they create as the container's user; so a
//! program that needs only these — Petri's step runner, say — runs on any
//! Linux image with a POSIX userland.

pub(crate) mod tar;

use std::io;
use std::path::Path;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bollard::container::{DownloadFromContainerOptions, UploadToContainerOptions};
use bollard::errors::Error as DockerApiError;
use futures_util::{StreamExt, stream};
use sandbox_driver::{
    DerivedFs, DirEntry, Error, Exec, ExecSpec, FileMetadata, Filesystem, ResourceKind, Result,
};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{OnceCell, mpsc};

use self::tar::{
    Archived, Owner, TRANSFER_CHUNK_BYTES, TarScanner, directory_archive, file_archive_header,
    send_file_archive,
};
use crate::container::ContainerRef;
use crate::daemon::{docker_error, is_not_found};

const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct DockerFs {
    container: ContainerRef,
    exec:      Arc<dyn Exec>,
    derived:   DerivedFs,
    /// The container user's numeric identity, for what the archive
    /// uploads create. Cached after a successful lookup.
    owner:     OnceCell<Owner>,
}

impl DockerFs {
    pub(crate) fn new(container: ContainerRef, exec: Arc<dyn Exec>) -> Self {
        let derived = DerivedFs::new(Arc::clone(&exec));
        Self {
            container,
            exec,
            derived,
            owner: OnceCell::new(),
        }
    }

    async fn upload_tar(&self, parent: &str, archive: Vec<u8>) -> StdResult<(), DockerApiError> {
        let options = UploadToContainerOptions {
            path:                     parent.to_owned(),
            no_overwrite_dir_non_dir: "false".to_owned(),
        };
        self.container
            .docker
            .upload_to_container(&self.container.id, Some(options), archive.into())
            .await
    }

    /// Who the container runs as. A stopped container answers nothing,
    /// and root is then the honest default: the daemon extracts as root.
    /// Failed lookups are retried on the next write.
    async fn owner(&self) -> Owner {
        self.owner
            .get_or_try_init(|| async {
                let lookup = |flag| async move {
                    let spec = ExecSpec::new("id").arg(flag).timeout(PROBE_TIMEOUT);
                    let result = self.exec.run(&spec).await.map_err(|_| ())?;
                    if !result.success() {
                        return Err(());
                    }
                    result.stdout_lossy().trim().parse::<u64>().map_err(|_| ())
                };
                let (uid, gid) = tokio::join!(lookup("-u"), lookup("-g"));
                Ok::<_, ()>(Owner {
                    uid: uid?,
                    gid: gid?,
                })
            })
            .await
            .copied()
            .unwrap_or_default()
    }

    /// The path a symlink at `container_path` finally points at, through
    /// `readlink -f`, which busybox and coreutils both provide.
    async fn resolve_link(&self, container_path: &str) -> Result<String> {
        let spec = ExecSpec::new("readlink")
            .args(["-f", container_path])
            .timeout(PROBE_TIMEOUT);
        let result = self.exec.run(&spec).await?;
        let target = result.stdout_lossy().trim().to_owned();
        if !result.success() || target.is_empty() {
            return Err(Error::NotFound {
                resource: ResourceKind::File,
                id:       container_path.to_owned(),
            });
        }
        Ok(target)
    }

    /// Streams the requested file bytes to `output`. A non-file archive
    /// is reported without writing, so the caller can follow a symlink.
    async fn read_archived_to(
        &self,
        container_path: &str,
        offset: u64,
        length: Option<u64>,
        output: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<Archived> {
        let options = DownloadFromContainerOptions {
            path: container_path.to_owned(),
        };
        let mut stream = self
            .container
            .docker
            .download_from_container(&self.container.id, Some(options));
        let mut scanner = TarScanner::new(offset, length);
        'archive: while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                if is_not_found(&error) {
                    Error::NotFound {
                        resource: ResourceKind::File,
                        id:       container_path.to_owned(),
                    }
                } else {
                    docker_error("downloading file", error)
                }
            })?;
            for chunk in chunk.chunks(TRANSFER_CHUNK_BYTES) {
                let done = scanner.feed(chunk)?;
                output
                    .write_all(&scanner.collected)
                    .await
                    .map_err(|error| Error::io("writing downloaded file", error))?;
                scanner.collected.clear();
                if done {
                    break 'archive;
                }
            }
        }
        scanner.finish()
    }

    async fn read_range_to(
        &self,
        path: &str,
        offset: u64,
        length: Option<u64>,
        output: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<()> {
        let container_path = self.container.resolve(path);
        if self
            .read_archived_to(&container_path, offset, length, output)
            .await?
            == Archived::File
        {
            return Ok(());
        }
        let target = self.resolve_link(&container_path).await?;
        match self
            .read_archived_to(&target, offset, length, output)
            .await?
        {
            Archived::File => Ok(()),
            Archived::NotAFile => Err(Error::invalid_spec(
                "path",
                format!("{path:?} is not a regular file"),
            )),
        }
    }

    /// Creates only missing directories before consuming the file input.
    /// Retrying a small directory archive never requires replaying input.
    async fn prepare_parent(&self, parent: &str, mode: u32, owner: Owner) -> Result<()> {
        for root in Path::new(parent).ancestors() {
            let root = root.to_string_lossy();
            let below = parent
                .strip_prefix(root.as_ref())
                .unwrap_or_default()
                .trim_matches('/');
            let mut dirs: Vec<String> = Vec::new();
            for component in below.split('/').filter(|part| !part.is_empty()) {
                let dir = match dirs.last() {
                    Some(previous) => format!("{previous}/{component}"),
                    None => component.to_owned(),
                };
                dirs.push(dir);
            }
            match self
                .upload_tar(&root, directory_archive(&dirs, mode, owner)?)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) if is_not_found(&error) => {}
                Err(error) => return Err(docker_error("creating upload directories", error)),
            }
        }
        Err(Error::io(
            format!("creating parent directory {parent}"),
            io::Error::other("no existing ancestor accepted the upload"),
        ))
    }
}

/// Splits a resolved container path into its parent directory and file
/// name for the archive upload.
fn split_container_path(container_path: &str) -> Result<(String, String)> {
    let path = Path::new(container_path);
    let file_name = path
        .file_name()
        .ok_or_else(|| Error::invalid_spec("path", format!("no file name in {container_path:?}")))?
        .to_string_lossy()
        .into_owned();
    let parent = path
        .parent()
        .map_or_else(|| "/".to_owned(), |p| p.to_string_lossy().into_owned());
    Ok((parent, file_name))
}

/// Whether `container_path` lies in the runtime directory tree.
fn is_runtime_path(container_path: &str) -> bool {
    container_path
        .strip_prefix(crate::RUNTIME_DIRECTORY)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

// Errors log at DEBUG: each one is returned to the caller, which decides
// whether it is a failure. A missing file is often the answer it asked for.
#[async_trait]
impl Filesystem for DockerFs {
    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id),
        err(level = "debug")
    )]
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.read_range(path, 0, None).await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id, offset),
        err(level = "debug")
    )]
    async fn read_range(&self, path: &str, offset: u64, length: Option<u64>) -> Result<Vec<u8>> {
        let mut bytes = sandbox_driver::BoundedBuffer::new(sandbox_driver::DEFAULT_BUFFER_BYTES);
        let outcome = self.read_range_to(path, offset, length, &mut bytes).await;
        bytes.finish(outcome)
    }

    async fn read_to(
        &self,
        path: &str,
        output: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<()> {
        self.read_range_to(path, 0, None, output).await?;
        output
            .flush()
            .await
            .map_err(|error| Error::io("flushing downloaded file", error))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            provider_kind = "docker",
            sandbox_id = %self.container.id,
            byte_count = content.len()
        ),
        err(level = "debug")
    )]
    async fn write(&self, path: &str, content: &[u8]) -> Result<()> {
        let mut input = content;
        self.write_from(path, &mut input, content.len() as u64)
            .await
    }

    async fn write_from(
        &self,
        path: &str,
        input: &mut (dyn AsyncRead + Unpin + Send),
        length: u64,
    ) -> Result<()> {
        let container_path = self.container.resolve(path);
        let (parent, file_name) = split_container_path(&container_path)?;
        let (file_mode, dir_mode) = if is_runtime_path(&container_path) {
            (0o600, 0o700)
        } else {
            (0o644, 0o755)
        };
        let owner = self.owner().await;
        self.prepare_parent(&parent, dir_mode, owner).await?;
        let prefix = file_archive_header(&file_name, length, file_mode, owner)?;
        let (sender, receiver) = mpsc::channel(2);
        let body = stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|bytes| (bytes, receiver))
        });
        let upload = async {
            self.container
                .docker
                .upload_to_container_streaming(
                    &self.container.id,
                    Some(UploadToContainerOptions {
                        path:                     parent,
                        no_overwrite_dir_non_dir: "false".to_owned(),
                    }),
                    body,
                )
                .await
                .map_err(|error| docker_error("uploading file", error))
        };
        // Poll upload first so a daemon rejection keeps its provider error
        // instead of being replaced by the producer's closed-channel error.
        tokio::try_join!(biased; upload, send_file_archive(sender, prefix, input, length))?;
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id, recursive),
        err(level = "debug")
    )]
    async fn delete(&self, path: &str, recursive: bool) -> Result<()> {
        self.derived.delete(path, recursive).await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.container.id), err(level = "debug"))]
    async fn exists(&self, path: &str) -> Result<bool> {
        self.derived.exists(path).await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.container.id), err(level = "debug"))]
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.derived.metadata(path).await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id, depth),
        err(level = "debug")
    )]
    async fn list_dir(&self, path: &str, depth: usize) -> Result<Vec<DirEntry>> {
        self.derived.list_dir(path, depth).await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.container.id), err(level = "debug"))]
    async fn create_dir(&self, path: &str) -> Result<()> {
        self.derived.create_dir(path).await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.container.id), err(level = "debug"))]
    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.derived.rename(from, to).await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id, mode),
        err(level = "debug")
    )]
    async fn set_permissions(&self, path: &str, mode: u32) -> Result<()> {
        self.derived.set_permissions(path, mode).await
    }

    #[tracing::instrument(
        skip_all,
        fields(
            provider_kind = "docker",
            sandbox_id = %self.container.id,
            byte_count = content.len()
        ),
        err(level = "debug")
    )]
    async fn write_append(&self, path: &str, content: &[u8]) -> Result<()> {
        self.derived.write_append(path, content).await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.container.id), err(level = "debug"))]
    async fn upload(&self, local: &Path, remote: &str) -> Result<()> {
        let mut input = fs::File::open(local)
            .await
            .map_err(|error| Error::io(format!("reading {}", local.display()), error))?;
        let length = input
            .metadata()
            .await
            .map_err(|error| Error::io(format!("reading metadata for {}", local.display()), error))?
            .len();
        self.write_from(remote, &mut input, length).await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.container.id), err(level = "debug"))]
    async fn download(&self, remote: &str, local: &Path) -> Result<()> {
        if let Some(parent) = local.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|error| Error::io(format!("creating {}", parent.display()), error))?;
        }
        let mut output = fs::File::create(local)
            .await
            .map_err(|error| Error::io(format!("writing {}", local.display()), error))?;
        self.read_to(remote, &mut output).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use bollard::Docker;
    use sandbox_driver::{ExecControls, ExecResult, ExecStreamingResult, Termination};

    use super::*;

    #[test]
    fn runtime_paths_are_detected_by_tree_membership() {
        assert!(is_runtime_path("/tmp/sandbox-driver/runtime"));
        assert!(is_runtime_path("/tmp/sandbox-driver/runtime/blob.json"));
        assert!(!is_runtime_path("/tmp/sandbox-driver/runtime-extra/x"));
        assert!(!is_runtime_path("/workspace/file.txt"));
    }

    struct UserLookup {
        ready:             AtomicBool,
        calls:             AtomicUsize,
        transport_failure: bool,
    }

    #[async_trait]
    impl Exec for UserLookup {
        async fn run(&self, spec: &ExecSpec) -> Result<ExecResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(spec.program, "id");
            let ready = self.ready.load(Ordering::Relaxed);
            if !ready && self.transport_failure {
                return Err(Error::io(
                    "looking up user",
                    io::Error::other("container stopped"),
                ));
            }
            let mut result =
                ExecResult::new(Termination::Exited, Some(i32::from(!ready)), Duration::ZERO);
            result.stdout = match spec.args[0].as_str() {
                "-u" => b"1000\n".to_vec(),
                "-g" => b"1001\n".to_vec(),
                other => panic!("unexpected id argument {other}"),
            };
            Ok(result)
        }

        async fn run_streaming(
            &self,
            _spec: &ExecSpec,
            _controls: ExecControls,
        ) -> Result<ExecStreamingResult> {
            panic!("user lookups use buffered exec")
        }
    }

    #[tokio::test]
    async fn failed_user_lookups_are_retried_and_successful_ones_are_cached() {
        for transport_failure in [false, true] {
            let exec = Arc::new(UserLookup {
                ready: AtomicBool::new(false),
                calls: AtomicUsize::new(0),
                transport_failure,
            });
            let container = ContainerRef::new(
                Docker::connect_with_http("http://127.0.0.1:1", 1, bollard::API_DEFAULT_VERSION)
                    .expect("docker client"),
                "test-container".to_owned(),
                "/workspace".to_owned(),
            );
            let fs = DockerFs::new(container, exec.clone());
            let fallback = fs.owner().await;
            assert_eq!((fallback.uid, fallback.gid), (0, 0));

            exec.ready.store(true, Ordering::Relaxed);
            let owner = fs.owner().await;
            assert_eq!((owner.uid, owner.gid), (1000, 1001));

            exec.ready.store(false, Ordering::Relaxed);
            let cached = fs.owner().await;
            assert_eq!((cached.uid, cached.gid), (1000, 1001));
            assert_eq!(exec.calls.load(Ordering::Relaxed), 4);
        }
    }
}
