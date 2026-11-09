use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use sandbox_driver::{
    DirEntry, Error, FileKind, FileMetadata, Filesystem, ResourceKind, Result, WalkOptions,
    WalkedFile,
};
use tokio::fs;

/// An in-memory [`Filesystem`].
///
/// Relative paths resolve against the sandbox working directory the
/// double was built with. Writes are applied and recorded, so a test can
/// read back what the code wrote or assert on the sequence of writes.
pub struct MemoryFs {
    root:         String,
    files:        Mutex<BTreeMap<String, Vec<u8>>>,
    dirs:         Mutex<BTreeSet<String>>,
    writes:       Mutex<Vec<(String, Vec<u8>)>>,
    deletes:      Mutex<Vec<String>>,
    exists_calls: Mutex<usize>,
}

impl MemoryFs {
    pub fn new(root: impl Into<String>) -> Self {
        let root = root.into();
        let fs = Self {
            root:         root.clone(),
            files:        Mutex::new(BTreeMap::new()),
            dirs:         Mutex::new(BTreeSet::new()),
            writes:       Mutex::new(Vec::new()),
            deletes:      Mutex::new(Vec::new()),
            exists_calls: Mutex::new(0),
        };
        fs.dirs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(fs.resolve(&root));
        fs
    }

    /// The absolute sandbox path for `path`.
    pub fn resolve(&self, path: &str) -> String {
        resolve(&self.root, path)
    }

    /// Seeds a file without recording a write.
    pub fn insert(&self, path: &str, content: impl AsRef<[u8]>) -> &Self {
        let path = self.resolve(path);
        self.ensure_parents(&path);
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path, content.as_ref().to_vec());
        self
    }

    /// The current content of a file, if present.
    pub fn contents(&self, path: &str) -> Option<Vec<u8>> {
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&self.resolve(path))
            .cloned()
    }

    /// The regular files below `base`, as paths relative to it with their
    /// sizes, pruning any directory `options.exclude_dirs` names.
    pub fn walk(&self, base: &str, options: &WalkOptions) -> Vec<WalkedFile> {
        let base = self.resolve(base);
        let prefix = format!("{}/", base.trim_end_matches('/'));
        let files = self.files.lock().unwrap_or_else(PoisonError::into_inner);
        let mut walked: Vec<WalkedFile> = files
            .iter()
            .filter_map(|(path, content)| {
                let relative = path.strip_prefix(&prefix)?;
                let mut segments = relative.split('/');
                let file_name = segments.next_back()?;
                let excluded = segments.any(|dir| options.exclude_dirs.iter().any(|x| x == dir));
                (!excluded && !file_name.is_empty()).then(|| {
                    WalkedFile::new(
                        relative.to_owned(),
                        Some(u64::try_from(content.len()).unwrap_or(u64::MAX)),
                    )
                })
            })
            .collect();
        walked.sort_by(|a, b| a.path.cmp(&b.path));
        walked
    }

    /// The files below `base` whose base-relative path matches `pattern`,
    /// as absolute paths. `*` matches within a segment and `**` across
    /// segments.
    pub fn glob(&self, pattern: &str, base: &str) -> Vec<String> {
        let base = self.resolve(base);
        self.walk(&base, &WalkOptions::default())
            .into_iter()
            .filter(|file| glob_matches(pattern, &file.path))
            .map(|file| format!("{}/{}", base.trim_end_matches('/'), file.path))
            .collect()
    }

    /// Every write so far as (absolute path, content), in order.
    pub fn writes(&self) -> Vec<(String, Vec<u8>)> {
        self.writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Every delete so far as an absolute path, in order.
    pub fn deletes(&self) -> Vec<String> {
        self.deletes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// How many times `exists` was asked, for tests that assert a probe
    /// is paid once.
    pub fn exists_calls(&self) -> usize {
        *self
            .exists_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn ensure_parents(&self, path: &str) {
        let mut dirs = self.dirs.lock().unwrap_or_else(PoisonError::into_inner);
        let mut current = path;
        while let Some((parent, _)) = current.rsplit_once('/') {
            if parent.is_empty() {
                break;
            }
            dirs.insert(parent.to_owned());
            current = parent;
        }
    }

    fn is_dir(&self, path: &str) -> bool {
        self.dirs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(path)
    }

    fn not_found(path: &str) -> Error {
        Error::NotFound {
            resource: ResourceKind::File,
            id:       path.to_owned(),
        }
    }
}

/// A small glob: `**` spans segments, `*` and `?` stay within one, and
/// everything else is literal. Enough for a test's `*/SKILL.md` or
/// `**/*.md`.
fn glob_matches(pattern: &str, path: &str) -> bool {
    fn segments(pattern: &[&str], path: &[&str]) -> bool {
        match pattern.split_first() {
            None => path.is_empty(),
            Some((&"**", rest)) => (0..=path.len()).any(|skip| segments(rest, &path[skip..])),
            Some((head, rest)) => match path.split_first() {
                Some((segment, path_rest)) => {
                    segment_matches(head, segment) && segments(rest, path_rest)
                }
                None => false,
            },
        }
    }
    fn segment_matches(pattern: &str, segment: &str) -> bool {
        fn go(pattern: &[char], segment: &[char]) -> bool {
            match pattern.split_first() {
                None => segment.is_empty(),
                Some(('*', rest)) => (0..=segment.len()).any(|skip| go(rest, &segment[skip..])),
                Some(('?', rest)) => segment.split_first().is_some_and(|(_, s)| go(rest, s)),
                Some((c, rest)) => segment
                    .split_first()
                    .is_some_and(|(s, tail)| s == c && go(rest, tail)),
            }
        }
        let pattern: Vec<char> = pattern.chars().collect();
        let segment: Vec<char> = segment.chars().collect();
        go(&pattern, &segment)
    }
    let pattern: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    let path: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    segments(&pattern, &path)
}

/// The absolute, dot-free path for `path` resolved against `root`.
pub(crate) fn resolve(root: &str, path: &str) -> String {
    let joined = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("{}/{path}", root.trim_end_matches('/'))
    };
    normalize(&joined)
}

fn normalize(path: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    format!("/{}", parts.join("/"))
}

#[async_trait]
impl Filesystem for MemoryFs {
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        let resolved = self.resolve(path);
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&resolved)
            .cloned()
            .ok_or_else(|| Self::not_found(path))
    }

    async fn write(&self, path: &str, content: &[u8]) -> Result<()> {
        let resolved = self.resolve(path);
        self.ensure_parents(&resolved);
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(resolved.clone(), content.to_vec());
        self.writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((resolved, content.to_vec()));
        Ok(())
    }

    async fn delete(&self, path: &str, recursive: bool) -> Result<()> {
        let resolved = self.resolve(path);
        self.deletes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(resolved.clone());
        let prefix = format!("{resolved}/");
        let mut files = self.files.lock().unwrap_or_else(PoisonError::into_inner);
        let mut dirs = self.dirs.lock().unwrap_or_else(PoisonError::into_inner);
        if dirs.contains(&resolved) {
            let has_children = files.keys().any(|key| key.starts_with(&prefix))
                || dirs.iter().any(|dir| dir.starts_with(&prefix));
            if has_children && !recursive {
                return Err(Error::invalid_spec(
                    "path",
                    format!("{path} is a directory that is not empty"),
                ));
            }
            files.retain(|key, _| !key.starts_with(&prefix));
            dirs.retain(|dir| !dir.starts_with(&prefix) && *dir != resolved);
        } else {
            files.remove(&resolved);
        }
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        *self
            .exists_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += 1;
        let resolved = self.resolve(path);
        Ok(self
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&resolved)
            || self.is_dir(&resolved))
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let resolved = self.resolve(path);
        if let Some(content) = self
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&resolved)
        {
            return Ok(FileMetadata::new(
                FileKind::File,
                u64::try_from(content.len()).unwrap_or(u64::MAX),
            ));
        }
        if self.is_dir(&resolved) {
            return Ok(FileMetadata::new(FileKind::Directory, 0));
        }
        Err(Self::not_found(path))
    }

    async fn list_dir(&self, path: &str, depth: usize) -> Result<Vec<DirEntry>> {
        let resolved = self.resolve(path);
        if !self.is_dir(&resolved) {
            return Err(Self::not_found(path));
        }
        let prefix = format!("{}/", resolved.trim_end_matches('/'));
        let within = |candidate: &str| -> Option<String> {
            let relative = candidate.strip_prefix(&prefix)?;
            (!relative.is_empty() && relative.split('/').count() <= depth.max(1))
                .then(|| relative.to_owned())
        };
        let mut entries: Vec<DirEntry> = Vec::new();
        for dir in self
            .dirs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            if let Some(relative) = within(dir) {
                entries.push(DirEntry::new(relative, FileKind::Directory));
            }
        }
        for (file, content) in self
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            if let Some(relative) = within(file) {
                let mut entry = DirEntry::new(relative, FileKind::File);
                entry.size = Some(u64::try_from(content.len()).unwrap_or(u64::MAX));
                entries.push(entry);
            }
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(entries)
    }

    async fn create_dir(&self, path: &str) -> Result<()> {
        let resolved = self.resolve(path);
        self.ensure_parents(&resolved);
        self.dirs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(resolved);
        Ok(())
    }

    /// Copies a local file into the memory filesystem, recorded as a write.
    async fn upload(&self, local: &Path, remote: &str) -> Result<()> {
        let content = fs::read(local)
            .await
            .map_err(|error| Error::io(format!("reading {}", local.display()), error))?;
        self.write(remote, &content).await
    }

    /// Copies a memory file to the local filesystem, creating its parents.
    async fn download(&self, remote: &str, local: &Path) -> Result<()> {
        let content = self.read(remote).await?;
        if let Some(parent) = local.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|error| Error::io(format!("creating {}", parent.display()), error))?;
        }
        fs::write(local, content)
            .await
            .map_err(|error| Error::io(format!("writing {}", local.display()), error))
    }

    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from_resolved = self.resolve(from);
        let to_resolved = self.resolve(to);
        self.ensure_parents(&to_resolved);
        let mut files = self.files.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(content) = files.remove(&from_resolved) {
            files.insert(to_resolved, content);
            return Ok(());
        }
        drop(files);
        let mut dirs = self.dirs.lock().unwrap_or_else(PoisonError::into_inner);
        if !dirs.remove(&from_resolved) {
            return Err(Self::not_found(from));
        }
        dirs.insert(to_resolved.clone());
        let from_prefix = format!("{from_resolved}/");
        let moved: Vec<String> = dirs
            .iter()
            .filter(|dir| dir.starts_with(&from_prefix))
            .cloned()
            .collect();
        for dir in moved {
            dirs.remove(&dir);
            dirs.insert(format!("{to_resolved}/{}", &dir[from_prefix.len()..]));
        }
        drop(dirs);
        let mut files = self.files.lock().unwrap_or_else(PoisonError::into_inner);
        let moved: Vec<(String, Vec<u8>)> = files
            .iter()
            .filter(|(key, _)| key.starts_with(&from_prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        for (key, value) in moved {
            files.remove(&key);
            files.insert(
                format!("{to_resolved}/{}", &key[from_prefix.len()..]),
                value,
            );
        }
        Ok(())
    }
}
