//! Byte streams between the shared contract's `Files` and the container's
//! archive API. Reads stream through a bounded pipe; a write's length must be
//! known before its archive header, so it is spooled to an unlinked local
//! file first rather than held in memory.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::{env, io, process};

use sandbox_driver::contract::{ByteStream, Error as ContractError, MutationOutcome};
use sandbox_driver::{Error, Filesystem};
use tokio::fs::{self as tokio_fs, File, OpenOptions};
use tokio::io::{
    self as tokio_io, AsyncRead, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufWriter, DuplexStream,
    ReadBuf, duplex,
};
use tokio::sync::oneshot;

use super::error::to_contract;
use crate::sandbox::DockerSandbox;

const PIPE_BYTES: usize = 64 * 1024;
const SPOOL_WRITE_BYTES: usize = 1024 * 1024;

/// Streams `path` from the sandbox. Failures before the first byte (a
/// missing file, say) are returned here; later ones fail the stream, so a
/// truncated read is never a successful one.
pub(super) async fn read(
    sandbox: Arc<DockerSandbox>,
    path: String,
    offset: u64,
    length: Option<u64>,
) -> Result<ByteStream, ContractError> {
    let (writer, reader) = duplex(PIPE_BYTES);
    let (first, started) = oneshot::channel();
    // Set until the task finishes, so a task that panics mid-stream fails
    // the read instead of ending it like a clean end of file.
    let late = Arc::new(Mutex::new(Some("the file read stopped early".to_owned())));
    let late_failure = Arc::clone(&late);
    tokio::spawn(async move {
        let mut writer = FirstWrite {
            inner: writer,
            first: Some(first),
        };
        let result = sandbox
            .fs
            .read_range_to(&path, offset, length, &mut writer)
            .await;
        let failure = match (result, writer.first.take()) {
            (result, Some(first)) => {
                let _ = first.send(result);
                None
            }
            (Err(error), None) => Some(error.to_string()),
            (Ok(()), None) => None,
        };
        *late_failure.lock().expect("read failure lock") = failure;
        // Dropping the writer ends the stream after the failure is recorded.
    });
    match started.await {
        Ok(Ok(())) => Ok(Box::pin(FailingReader {
            inner: reader,
            late,
        })),
        Ok(Err(error)) => Err(to_contract(error)),
        Err(_) => Err(ContractError::Transport {
            message: "file read ended without a result".to_owned(),
            outcome: MutationOutcome::Unknown,
        }),
    }
}

/// Spools `bytes` to a private local file, then writes it into the sandbox
/// with its length known. Success confirms the whole stream was written.
pub(super) async fn write(
    fs: &dyn Filesystem,
    path: &str,
    mut bytes: ByteStream,
) -> Result<(), ContractError> {
    let (mut spool, location) = spool_file().await.map_err(|error| io_failure(&error))?;
    // Unlinked at once: the open handle keeps the data, and nothing is left
    // behind if this future is dropped.
    let removed = tokio_fs::remove_file(&location).await;
    let copied = async {
        removed?;
        // Each write to a tokio `File` is a blocking-pool round trip, so
        // batch them rather than writing `copy`'s 8 KiB chunks one by one.
        let mut buffered = BufWriter::with_capacity(SPOOL_WRITE_BYTES, &mut spool);
        let length = tokio_io::copy(&mut bytes, &mut buffered).await?;
        buffered.flush().await?;
        spool.rewind().await?;
        Ok::<_, io::Error>(length)
    }
    .await
    .map_err(|error| io_failure(&error))?;
    fs.write_from(path, &mut spool, copied)
        .await
        .map_err(to_contract)
}

async fn spool_file() -> io::Result<(File, PathBuf)> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let location = env::temp_dir().join(format!(
        ".sandbox-driver-docker-put-{}-{}",
        process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    Ok((options.open(&location).await?, location))
}

fn io_failure(error: &io::Error) -> ContractError {
    ContractError::Transport {
        message: format!("reading file input failed: {error}"),
        outcome: MutationOutcome::NotStarted,
    }
}

/// Reports the first successful write, so `read` returns once bytes flow.
struct FirstWrite {
    inner: DuplexStream,
    first: Option<oneshot::Sender<Result<(), Error>>>,
}

impl AsyncWrite for FirstWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write(cx, buf);
        if matches!(written, Poll::Ready(Ok(count)) if count > 0) {
            if let Some(first) = self.first.take() {
                let _ = first.send(Ok(()));
            }
        }
        written
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// The read end of the pipe, which turns a failure recorded after the first
/// byte into an error at end of stream instead of a clean EOF.
struct FailingReader {
    inner: DuplexStream,
    late:  Arc<Mutex<Option<String>>>,
}

impl AsyncRead for FailingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) if buf.filled().len() == before && buf.remaining() > 0 => {
                match self.late.lock().expect("read failure lock").take() {
                    Some(message) => Poll::Ready(Err(io::Error::other(message))),
                    None => Poll::Ready(Ok(())),
                }
            }
            other => other,
        }
    }
}
