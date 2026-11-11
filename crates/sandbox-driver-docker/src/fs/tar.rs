//! The tar codec behind the archive API: a streaming scanner for ranged
//! reads, hand-built headers for uploads, and the re-rooting a build
//! context needs.

use std::borrow::Cow;
use std::io;
use std::io::Cursor;

use sandbox_driver::{Error, Result};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;
use tokio_util::bytes::Bytes;

const TAR_BLOCK: usize = 512;
pub(super) const TRANSFER_CHUNK_BYTES: usize = 64 * 1024;

/// A numeric uid and gid.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Owner {
    pub(super) uid: u64,
    pub(super) gid: u64,
}

/// The zero bytes that pad an entry of `size` bytes to a block boundary.
fn padding_for(size: u64) -> u64 {
    (TAR_BLOCK as u64 - size % TAR_BLOCK as u64) % TAR_BLOCK as u64
}

/// An incremental reader over a tar stream that finds the first regular
/// file and keeps only the requested range of its content.
pub(super) struct TarScanner {
    offset:               u64,
    length:               Option<u64>,
    header:               [u8; TAR_BLOCK],
    header_len:           usize,
    state:                ScanState,
    pub(super) collected: Vec<u8>,
    found:                bool,
}

enum ScanState {
    /// Reading a 512-byte header block.
    Header,
    /// Skipping `remaining` bytes of an entry nobody wants, plus padding.
    Skip {
        remaining: u64,
    },
    /// Inside the wanted file: `position` bytes of it seen so far, `size`
    /// in total.
    Content {
        position: u64,
        size:     u64,
    },
    Done,
}

/// What one state did with the bytes it was given.
enum Step {
    /// The chunk ran out before the state was done; feed more.
    NeedMore,
    /// The state changed; run the next one on the rest of the chunk.
    Continue,
    /// Nothing further is wanted from the archive.
    Finished,
}

/// Whether the archive held the regular file a read asked for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Archived {
    /// The first entry was a regular file; its requested range was
    /// collected.
    File,
    /// The archived resource is a directory or a link, or the archive
    /// was empty; the caller follows or refuses it.
    NotAFile,
}

impl TarScanner {
    pub(super) fn new(offset: u64, length: Option<u64>) -> Self {
        Self {
            offset,
            length,
            header: [0; TAR_BLOCK],
            header_len: 0,
            state: ScanState::Header,
            collected: Vec::new(),
            found: false,
        }
    }

    /// The last byte of the file this read wants, exclusive.
    fn end(&self) -> u64 {
        self.length
            .map_or(u64::MAX, |length| self.offset.saturating_add(length))
    }

    fn padded(size: u64) -> Result<u64> {
        size.checked_add(padding_for(size)).ok_or_else(|| {
            Error::io(
                "reading download archive",
                io::Error::new(io::ErrorKind::InvalidData, "archive entry size overflows"),
            )
        })
    }

    /// Consumes `chunk`; `true` once nothing further is wanted.
    pub(super) fn feed(&mut self, mut chunk: &[u8]) -> Result<bool> {
        loop {
            let step = match self.state {
                ScanState::Done => return Ok(true),
                ScanState::Header => self.feed_header(&mut chunk)?,
                ScanState::Skip { remaining } => self.feed_skip(&mut chunk, remaining),
                ScanState::Content { position, size } => self.feed_content(chunk, position, size),
            };
            match step {
                Step::NeedMore => return Ok(false),
                Step::Continue => {}
                Step::Finished => return Ok(true),
            }
        }
    }

    /// Reads header bytes; once a whole block is in, decides what the
    /// entry is and where to go next.
    fn feed_header(&mut self, chunk: &mut &[u8]) -> Result<Step> {
        let take = (TAR_BLOCK - self.header_len).min(chunk.len());
        self.header[self.header_len..self.header_len + take].copy_from_slice(&chunk[..take]);
        self.header_len += take;
        *chunk = &chunk[take..];
        if self.header_len < TAR_BLOCK {
            return Ok(Step::NeedMore);
        }
        self.header_len = 0;
        if self.header.iter().all(|byte| *byte == 0) {
            // End-of-archive marker: no regular file came.
            self.state = ScanState::Done;
            return Ok(Step::Finished);
        }
        let header = tar::Header::from_byte_slice(&self.header);
        let size = header
            .entry_size()
            .map_err(|error| Error::io("reading download archive", error))?;
        let kind = header.entry_type();
        if kind.is_file() {
            self.found = true;
            self.state = ScanState::Content { position: 0, size };
        } else if kind.is_dir() || kind.is_symlink() || kind.is_hard_link() {
            // The archived resource itself is not a regular
            // file; the caller follows or refuses it.
            self.state = ScanState::Done;
            return Ok(Step::Finished);
        } else {
            // A pax or long-name header, or something
            // exotic: skip its data and read on.
            self.state = ScanState::Skip {
                remaining: Self::padded(size)?,
            };
        }
        Ok(Step::Continue)
    }

    /// Discards the data of an entry nobody wants.
    fn feed_skip(&mut self, chunk: &mut &[u8], remaining: u64) -> Step {
        let take = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(chunk.len());
        *chunk = &chunk[take..];
        let remaining = remaining - take as u64;
        if remaining > 0 {
            self.state = ScanState::Skip { remaining };
            return Step::NeedMore;
        }
        self.state = ScanState::Header;
        Step::Continue
    }

    /// Collects the part of this chunk that lies inside the requested
    /// range. Nothing after the wanted file is read, so the chunk is not
    /// advanced.
    fn feed_content(&mut self, chunk: &[u8], position: u64, size: u64) -> Step {
        let available = chunk.len() as u64;
        let file_left = size.saturating_sub(position);
        let take = available.min(file_left);
        let end = self.end();
        // Keep the slice of this chunk inside [offset, end).
        let keep_from = self.offset.saturating_sub(position).min(take);
        let keep_to = end.saturating_sub(position).min(take);
        if keep_to > keep_from {
            let from = usize::try_from(keep_from).unwrap_or(usize::MAX);
            let to = usize::try_from(keep_to).unwrap_or(usize::MAX);
            self.collected.extend_from_slice(&chunk[from..to]);
        }
        let position = position + take;
        if position >= size || position >= end {
            self.state = ScanState::Done;
            return Step::Finished;
        }
        self.state = ScanState::Content { position, size };
        Step::NeedMore
    }

    pub(super) fn finish(self) -> Result<Archived> {
        if !matches!(self.state, ScanState::Done) {
            return Err(Error::io(
                "reading download archive",
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "archive ended before the requested file data was complete",
                ),
            ));
        }
        Ok(if self.found {
            Archived::File
        } else {
            Archived::NotAFile
        })
    }
}

/// A small archive containing only directories, never file content.
pub(super) fn directory_archive(dirs: &[String], mode: u32, owner: Owner) -> Result<Vec<u8>> {
    let tar_io = |error| Error::io("building directory archive", error);
    let mut builder = tar::Builder::new(Vec::new());
    for dir in dirs {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_size(0);
        header.set_mode(mode);
        header.set_uid(owner.uid);
        header.set_gid(owner.gid);
        builder
            .append_data(&mut header, format!("{dir}/"), io::empty())
            .map_err(tar_io)?;
    }
    builder.into_inner().map_err(tar_io)
}

/// Encodes a file header, including tar's long-name extension when needed.
/// The builder writes an empty entry; its final header is updated with the
/// streamed length and its end-of-archive blocks are replaced by the stream.
pub(super) fn file_archive_header(
    file: &str,
    length: u64,
    mode: u32,
    owner: Owner,
) -> Result<Vec<u8>> {
    let tar_io = |error| Error::io("building upload header", error);
    let mut header = tar::Header::new_gnu();
    header.set_size(0);
    header.set_mode(mode);
    header.set_uid(owner.uid);
    header.set_gid(owner.gid);
    let mut builder = tar::Builder::new(Vec::new());
    builder
        .append_data(&mut header, file, io::empty())
        .map_err(tar_io)?;
    let mut prefix = builder.into_inner().map_err(tar_io)?;
    prefix.truncate(prefix.len() - 2 * TAR_BLOCK);
    header.set_size(length);
    header.set_cksum();
    let start = prefix.len() - TAR_BLOCK;
    prefix[start..].copy_from_slice(header.as_bytes());
    Ok(prefix)
}

/// Produces a tar body in bounded chunks. The source is borrowed by the
/// caller's operation; cancellation drops this future and the upload together.
pub(super) async fn send_file_archive(
    sender: mpsc::Sender<Bytes>,
    prefix: Vec<u8>,
    input: &mut (dyn AsyncRead + Unpin + Send),
    length: u64,
) -> Result<()> {
    let send = |bytes| async {
        sender.send(bytes).await.map_err(|_| {
            Error::io(
                "streaming upload archive",
                io::Error::new(io::ErrorKind::BrokenPipe, "upload stopped reading"),
            )
        })
    };
    send(prefix.into()).await?;
    let mut remaining = length;
    let mut buffer = vec![0; TRANSFER_CHUNK_BYTES];
    while remaining > 0 {
        let wanted = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = input
            .read(&mut buffer[..wanted])
            .await
            .map_err(|error| Error::io("reading upload source", error))?;
        if read == 0 {
            return Err(Error::io(
                "reading upload source",
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "source ended before the declared file length",
                ),
            ));
        }
        send(Bytes::copy_from_slice(&buffer[..read])).await?;
        remaining -= read as u64;
    }
    let padding = usize::try_from(padding_for(length)).expect("tar padding fits in one block");
    send(vec![0; padding + 2 * TAR_BLOCK].into()).await
}

/// Re-roots an archive of one directory so its contents sit at the top
/// level, as a build context expects: `dir/Dockerfile` becomes
/// `Dockerfile`.
pub(crate) fn reroot(archive: &[u8]) -> Result<Vec<u8>> {
    let tar_io = |error| Error::io("re-rooting build context", error);
    let mut source = tar::Archive::new(Cursor::new(archive));
    let mut builder = tar::Builder::new(Vec::new());
    for entry in source.entries().map_err(tar_io)? {
        let mut entry = entry.map_err(tar_io)?;
        let path = entry.path().map_err(tar_io)?.into_owned();
        let mut components = path.components();
        components.next();
        let rest = components.as_path().to_path_buf();
        if rest.as_os_str().is_empty() {
            continue;
        }
        let mut header = entry.header().clone();
        // A cloned header can carry the source archive's USTAR path
        // split (prefix + name; the daemon's archive API splits paths
        // over 100 bytes). append_data's set_path writes only the name
        // field when the re-rooted path fits in it, so a stale prefix
        // would ship the entry under old-prefix + new-name — a doubled
        // path no build-context reader finds, silently dropping the
        // file from the image (work#1253). Clear the split so the
        // builder owns the whole path.
        if let Some(ustar) = header.as_ustar_mut() {
            ustar.prefix = [0; 155];
        }
        if header.entry_type().is_dir() {
            builder
                .append_data(&mut header, rest, io::empty())
                .map_err(tar_io)?;
        } else if header.entry_type().is_symlink() || header.entry_type().is_hard_link() {
            let mut link = entry
                .link_name()
                .map_err(tar_io)?
                .map(Cow::into_owned)
                .unwrap_or_default();
            // Hard-link targets are archive-root paths; symlink targets
            // remain relative to the symlink itself.
            if header.entry_type().is_hard_link() {
                let mut components = link.components();
                components.next();
                link = components.as_path().to_path_buf();
            }
            builder
                .append_link(&mut header, rest, link)
                .map_err(tar_io)?;
        } else {
            builder
                .append_data(&mut header, rest, &mut entry)
                .map_err(tar_io)?;
        }
    }
    builder.into_inner().map_err(tar_io)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn single_file_tar(name: &str, bytes: &[u8]) -> Vec<u8> {
        let mut archive =
            file_archive_header(name, bytes.len() as u64, 0o644, Owner::default()).expect("header");
        archive.extend_from_slice(bytes);
        archive.resize(
            archive.len().div_ceil(TAR_BLOCK) * TAR_BLOCK + 2 * TAR_BLOCK,
            0,
        );
        archive
    }

    #[tokio::test]
    async fn upload_archive_streams_long_names_and_leaves_extra_input_unread() {
        let name = format!("{}.bin", "long".repeat(40));
        let content = vec![b'x'; TRANSFER_CHUNK_BYTES * 4 + 19];
        let mut input = content.as_slice();
        let length = content.len() as u64 - 3;
        let prefix = file_archive_header(&name, length, 0o600, Owner {
            uid: 1000,
            gid: 1001,
        })
        .expect("header");
        let (sender, mut receiver) = mpsc::channel(2);
        let collect = async {
            let mut archive = Vec::new();
            while let Some(bytes) = receiver.recv().await {
                let bytes: Bytes = bytes;
                assert!(bytes.len() <= TRANSFER_CHUNK_BYTES);
                archive.extend(bytes);
            }
            archive
        };
        let (outcome, archive) = tokio::join!(
            send_file_archive(sender, prefix, &mut input, length),
            collect
        );
        outcome.expect("streamed archive");
        assert_eq!(input, b"xxx");
        let mut archive = tar::Archive::new(archive.as_slice());
        let mut entries = archive.entries().expect("entries");
        let mut entry = entries.next().expect("file").expect("valid file");
        assert_eq!(entry.path().expect("path").to_string_lossy(), name);
        assert_eq!(entry.header().mode().expect("mode"), 0o600);
        assert_eq!(entry.header().uid().expect("uid"), 1000);
        assert_eq!(entry.header().gid().expect("gid"), 1001);
        let mut bytes = Vec::new();
        io::Read::read_to_end(&mut entry, &mut bytes).expect("content");
        assert_eq!(bytes, content[..content.len() - 3]);
        assert!(entries.next().is_none());
    }

    #[tokio::test]
    async fn upload_archive_rejects_short_input_without_completing_the_tar() {
        let prefix = file_archive_header("file", 6, 0o644, Owner::default()).expect("header");
        let (sender, mut receiver) = mpsc::channel(2);
        let drain = async { while receiver.recv().await.is_some() {} };
        let mut input = b"short".as_slice();
        let (outcome, ()) = tokio::join!(send_file_archive(sender, prefix, &mut input, 6), drain);
        let error = outcome.expect_err("short source");
        assert!(
            matches!(error, Error::Io { source, .. } if source.kind() == io::ErrorKind::UnexpectedEof)
        );
    }

    #[test]
    fn upload_archive_carries_modes_and_directories() {
        let owner = Owner {
            uid: 1000,
            gid: 1000,
        };
        let mut bytes = directory_archive(&["a".to_owned(), "a/b".to_owned()], 0o700, owner)
            .expect("directories");
        bytes.truncate(bytes.len() - 2 * TAR_BLOCK);
        bytes.extend(file_archive_header("a/b/blob.json", 2, 0o600, owner).expect("file header"));
        bytes.extend(b"{}");
        bytes.resize(
            bytes.len().div_ceil(TAR_BLOCK) * TAR_BLOCK + 2 * TAR_BLOCK,
            0,
        );
        let mut archive = tar::Archive::new(bytes.as_slice());
        let entries: Vec<_> = archive
            .entries()
            .expect("entries")
            .map(|entry| entry.expect("valid entry"))
            .map(|entry| {
                let header = entry.header();
                (
                    entry.path().expect("path").to_string_lossy().into_owned(),
                    header.entry_type(),
                    header.mode().expect("mode"),
                    header.uid().expect("uid"),
                )
            })
            .collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, "a/");
        assert!(entries[0].1.is_dir());
        assert_eq!(entries[0].2, 0o700);
        assert_eq!(entries[2].0, "a/b/blob.json");
        assert!(entries[2].1.is_file());
        assert_eq!(entries[2].2, 0o600);
        assert_eq!(entries[2].3, 1000);
    }

    #[test]
    fn the_scanner_slices_a_file_in_arbitrary_chunks() {
        let payload: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
        let archive = single_file_tar("file.bin", &payload);
        for (offset, length, expected) in [
            (0, None, payload.clone()),
            (2, Some(5), payload[2..7].to_vec()),
            (2990, None, payload[2990..].to_vec()),
            (5000, Some(4), Vec::new()),
            (0, Some(0), Vec::new()),
        ] {
            for chunk_size in [1usize, 7, 512, 1000, 100_000] {
                let mut scanner = TarScanner::new(offset, length);
                let mut done = false;
                for chunk in archive.chunks(chunk_size) {
                    if scanner.feed(chunk).expect("feed") {
                        done = true;
                        break;
                    }
                }
                assert!(
                    done || length.is_none(),
                    "chunk {chunk_size} never finished"
                );
                let collected = scanner.collected.clone();
                assert_eq!(
                    scanner.finish().expect("the requested data is complete"),
                    Archived::File
                );
                assert_eq!(
                    collected, expected,
                    "offset {offset} length {length:?} chunk {chunk_size}"
                );
            }
        }
    }

    #[test]
    fn the_scanner_reports_a_symlink_as_not_a_file() {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(0o777);
        builder
            .append_link(&mut header, "link", "target")
            .expect("link");
        let archive = builder.into_inner().expect("tar");
        let mut scanner = TarScanner::new(0, None);
        scanner.feed(&archive).expect("feed");
        assert_eq!(
            scanner.finish().expect("complete symlink header"),
            Archived::NotAFile
        );
    }

    #[test]
    fn the_scanner_rejects_incomplete_headers_and_requested_content() {
        let archive = single_file_tar("file.bin", b"0123456789");
        for end in [0, 1, TAR_BLOCK - 1, TAR_BLOCK, TAR_BLOCK + 6] {
            let mut scanner = TarScanner::new(2, Some(5));
            assert!(!scanner.feed(&archive[..end]).expect("valid prefix"));
            assert!(
                scanner.finish().is_err(),
                "accepted a prefix of {end} bytes"
            );
        }
    }

    #[test]
    fn the_scanner_finishes_as_soon_as_the_requested_content_arrives() {
        let archive = single_file_tar("file.bin", b"0123456789");
        for (offset, length, end, expected) in [
            (2, Some(5), TAR_BLOCK + 7, b"23456".as_slice()),
            (0, None, TAR_BLOCK + 10, b"0123456789".as_slice()),
            (0, Some(0), TAR_BLOCK, b"".as_slice()),
        ] {
            let mut scanner = TarScanner::new(offset, length);
            assert!(scanner.feed(&archive[..end]).expect("valid archive prefix"));
            let collected = scanner.collected.clone();
            assert_eq!(scanner.finish().expect("complete range"), Archived::File);
            assert_eq!(collected, expected);
        }
    }

    #[test]
    fn the_scanner_rejects_an_incomplete_extended_header() {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::GNULongName);
        header.set_size(4);
        header.set_mode(0o644);
        builder
            .append_data(&mut header, "././@LongLink", b"abc\0".as_slice())
            .expect("extended header");
        let archive = builder.into_inner().expect("archive");
        for end in [TAR_BLOCK + 2, TAR_BLOCK * 2] {
            let mut scanner = TarScanner::new(0, None);
            assert!(!scanner.feed(&archive[..end]).expect("valid prefix"));
            assert!(scanner.finish().is_err());
        }
        let mut scanner = TarScanner::new(0, None);
        assert!(scanner.feed(&archive).expect("complete archive"));
        assert_eq!(
            scanner.finish().expect("archive with no file"),
            Archived::NotAFile
        );
    }

    #[test]
    fn the_build_context_is_re_rooted() {
        let mut builder = tar::Builder::new(Vec::new());
        let mut dir = tar::Header::new_gnu();
        dir.set_entry_type(tar::EntryType::Directory);
        dir.set_size(0);
        dir.set_mode(0o755);
        builder
            .append_data(&mut dir, "action/", io::empty())
            .expect("dir");
        let mut file = tar::Header::new_gnu();
        file.set_size(4);
        file.set_mode(0o644);
        builder
            .append_data(&mut file, "action/Dockerfile", Cursor::new(b"FROM"))
            .expect("file");
        let archive = builder.into_inner().expect("tar");
        let rerooted = reroot(&archive).expect("re-root");
        let mut archive = tar::Archive::new(Cursor::new(rerooted));
        let paths: Vec<String> = archive
            .entries()
            .expect("entries")
            .map(|entry| {
                entry
                    .expect("entry")
                    .path()
                    .expect("path")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(paths, ["Dockerfile"]);
    }

    #[test]
    fn re_rooting_replaces_a_split_ustar_path_completely() {
        // The daemon's archive API splits an entry path longer than the
        // 100-byte name field across the USTAR prefix and name fields.
        // Re-rooting must replace that path wholesale: a cloned header
        // keeps the old prefix, and append_data's set_path writes only
        // the name field when the re-rooted path fits in it — the entry
        // would ship under old-prefix + new-name, a doubled path no
        // build-context reader finds. This is the work#1253 cook
        // failure: every context file with a holder path over 100 bytes
        // silently vanished from the built image.
        let holder_path = "petri-env-context/lib/foundation/fabro-config/migrations/2026050101_legacy_sandbox_to_environments.rs";
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_ustar();
        header.set_size(4);
        header.set_mode(0o644);
        builder
            .append_data(&mut header, holder_path, Cursor::new(b"FROM"))
            .expect("file");
        let archive = builder.into_inner().expect("tar");
        let rerooted = reroot(&archive).expect("re-root");
        let mut archive = tar::Archive::new(Cursor::new(rerooted));
        let paths: Vec<String> = archive
            .entries()
            .expect("entries")
            .map(|entry| {
                entry
                    .expect("entry")
                    .path()
                    .expect("path")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(paths, [
            "lib/foundation/fabro-config/migrations/2026050101_legacy_sandbox_to_environments.rs"
        ]);
    }

    #[test]
    fn build_context_links_keep_their_targets_after_re_rooting() {
        let mut builder = tar::Builder::new(Vec::new());
        let mut file = tar::Header::new_gnu();
        file.set_size(4);
        file.set_mode(0o644);
        builder
            .append_data(&mut file, "action/source", Cursor::new(b"data"))
            .expect("file");
        for (kind, path, target) in [
            (tar::EntryType::Link, "action/hard", "action/source"),
            (tar::EntryType::Symlink, "action/sub/soft", "../source"),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(kind);
            header.set_size(0);
            header.set_mode(0o777);
            builder
                .append_link(&mut header, path, target)
                .expect("link");
        }
        let original = builder.into_inner().expect("archive");
        let rewritten = reroot(&original).expect("re-root");
        let mut archive = tar::Archive::new(rewritten.as_slice());
        let links: Vec<_> = archive
            .entries()
            .expect("entries")
            .map(|entry| entry.expect("entry"))
            .filter_map(|entry| entry.link_name().expect("link name").map(Cow::into_owned))
            .collect();
        assert_eq!(links, [PathBuf::from("source"), PathBuf::from("../source")]);
    }
}
