//! Credential-free discovery for the new shared contract. This is separate
//! from the legacy v2 loader: discovering schemas never initializes a runtime.
//!
//! Probes execute trusted code, not sandboxed code. Installations must protect
//! executable files and their parent directories against concurrent
//! replacement: the executable is hashed, then executed by path. The digest
//! covers the executable, not an interpreter or its dependencies.

use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(unix)]
use nix::sys::signal::{Signal, killpg};
#[cfg(unix)]
use nix::unistd::Pid;
use sandbox_driver::contract::{
    Error, MutationOutcome, PluginLocation, PluginTrust, ProviderName, ProviderSchemas,
    Sha256Digest,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::{OnceCell, Semaphore};
use tokio::{fs, time};

use crate::discovery::file_sha256;

/// Budgets shared by all probes using one discovery cache.
#[derive(Clone, Debug)]
pub struct DiscoveryLimits {
    /// One discovery call, including waiting for probe capacity, hashing,
    /// probe exit, and both output pipes.
    pub timeout:           Duration,
    pub stdout_bytes:      usize,
    pub stderr_bytes:      usize,
    /// Probes running at once. Further probes wait within their timeout;
    /// cache hits never wait.
    pub concurrent_probes: usize,
    pub cached_binaries:   usize,
}

impl Default for DiscoveryLimits {
    fn default() -> Self {
        Self {
            timeout:           Duration::from_secs(5),
            stdout_bytes:      1024 * 1024,
            stderr_bytes:      64 * 1024,
            concurrent_probes: 4,
            cached_binaries:   64,
        }
    }
}

type Probe = Arc<OnceCell<Arc<ProviderSchemas>>>;

/// Bounded in-memory metadata cache. Construction performs no lookup or launch.
/// Each lookup rehashes the executable and enforces that location's trust
/// policy, including cache hits. Concurrent lookups of one binary share one
/// probe. Failed probes are never cached.
pub struct SchemaDiscovery {
    limits: DiscoveryLimits,
    probes: Semaphore,
    cache:  Mutex<Cache>,
}

#[derive(Default)]
struct Cache {
    entries: BTreeMap<Sha256Digest, Probe>,
    order:   VecDeque<Sha256Digest>,
}

impl Default for SchemaDiscovery {
    fn default() -> Self {
        Self::try_new(DiscoveryLimits::default()).expect("default discovery limits are valid")
    }
}

impl SchemaDiscovery {
    pub fn try_new(limits: DiscoveryLimits) -> Result<Self, Error> {
        if limits.timeout.is_zero()
            || limits.stdout_bytes == 0
            || limits.stderr_bytes == 0
            || limits.concurrent_probes == 0
            || limits.concurrent_probes > Semaphore::MAX_PERMITS
            || limits.cached_binaries == 0
        {
            return Err(Error::InvalidSpec {
                path:    "discovery_limits".into(),
                message:
                    "discovery budgets must be positive and concurrency within semaphore limits"
                        .into(),
            });
        }
        Ok(Self {
            probes: Semaphore::new(limits.concurrent_probes),
            limits,
            cache: Mutex::new(Cache::default()),
        })
    }

    /// Resolves only the selected location. The probe receives exactly
    /// `--describe-schema`, null stdin, `/` as its working directory, a
    /// platform-only environment, and no entry config, arguments,
    /// explicit/inherited environment, or secrets. It runs in its own process
    /// group, which is killed when the probe ends, fails or is cancelled.
    pub async fn discover(&self, location: &PluginLocation) -> Result<DiscoveredPlugin, Error> {
        time::timeout(self.limits.timeout, self.discover_inner(location))
            .await
            .map_err(|_| unavailable(&location.entry, "metadata discovery timed out"))?
    }

    async fn discover_inner(&self, location: &PluginLocation) -> Result<DiscoveredPlugin, Error> {
        let entry = &location.entry;
        let trust = location.trust.as_ref().ok_or_else(|| {
            invalid_config(
                entry,
                "sha256",
                "pin the plugin checksum or explicitly set dev = true",
            )
        })?;
        let path = resolve(location).await?;
        let digest = digest(entry, &path).await?;
        if let PluginTrust::Sha256 { digest: expected } = trust {
            if expected != &digest {
                return Err(invalid_config(
                    entry,
                    "sha256",
                    &format!(
                        "plugin executable checksum mismatch for {}: pinned {expected}, found {digest}",
                        path.display()
                    ),
                ));
            }
        }
        let cell = self.cell(&digest);
        let schemas = cell
            .get_or_try_init(|| async {
                let _permit = self
                    .probes
                    .acquire()
                    .await
                    .expect("the probe semaphore is never closed");
                let schemas = probe(entry, &path, &self.limits).await?;
                // A replacement during the probe must not be cached under
                // the digest that was trusted.
                if self::digest(entry, &path).await? != digest {
                    return Err(invalid_config(
                        entry,
                        "path",
                        "plugin executable changed during discovery",
                    ));
                }
                Ok(Arc::new(schemas))
            })
            .await?
            .clone();
        if schemas.kind != location.kind {
            return Err(invalid_config(
                entry,
                "plugin",
                &format!(
                    "the executable serves kind {}, not the configured {}",
                    schemas.kind, location.kind
                ),
            ));
        }
        Ok(DiscoveredPlugin {
            path,
            digest,
            schemas,
            verified: matches!(trust, PluginTrust::Sha256 { .. }),
        })
    }

    /// The shared probe slot for one digest. The oldest digest is evicted
    /// beyond the cache budget; a caller already waiting keeps its slot.
    fn cell(&self, digest: &Sha256Digest) -> Probe {
        let mut cache = self.cache.lock().expect("metadata cache lock");
        if let Some(cell) = cache.entries.get(digest) {
            return Arc::clone(cell);
        }
        while cache.entries.len() >= self.limits.cached_binaries {
            let Some(oldest) = cache.order.pop_front() else {
                break;
            };
            cache.entries.remove(&oldest);
        }
        let cell = Probe::default();
        cache.entries.insert(digest.clone(), Arc::clone(&cell));
        cache.order.push_back(digest.clone());
        cell
    }
}

/// Verified metadata, not live namespace or capability facts. The private
/// fields prevent callers from substituting an unverified executable/schema.
#[derive(Clone, Debug)]
pub struct DiscoveredPlugin {
    path:     PathBuf,
    digest:   Sha256Digest,
    schemas:  Arc<ProviderSchemas>,
    verified: bool,
}

impl DiscoveredPlugin {
    /// The canonical executable path.
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn digest(&self) -> &Sha256Digest {
        &self.digest
    }
    pub fn schemas(&self) -> &ProviderSchemas {
        &self.schemas
    }
    /// False only for explicit development trust. Even development probes
    /// are cached by the actual digest, never by a path or kind.
    pub fn verified(&self) -> bool {
        self.verified
    }

    /// Recheck immediately before runtime launch. A replacement must go
    /// through discovery and trust verification again, even in development.
    pub async fn verify_unchanged(&self, entry: &ProviderName) -> Result<(), Error> {
        if digest(entry, &self.path).await? != self.digest {
            return Err(invalid_config(
                entry,
                "path",
                "plugin executable changed since metadata discovery",
            ));
        }
        Ok(())
    }
}

/// The configured executable, canonicalized. New-contract entries name an
/// absolute path; there is no search of the caller's ambient `PATH`.
async fn resolve(location: &PluginLocation) -> Result<PathBuf, Error> {
    let entry = &location.entry;
    let configured = location
        .path
        .as_deref()
        .ok_or_else(|| invalid_config(entry, "path", "a plugin entry must name its executable"))?;
    let configured = Path::new(configured);
    if !configured.is_absolute() {
        return Err(invalid_config(
            entry,
            "path",
            &format!(
                "plugin path {} must be absolute; expand ~ before configuring it",
                configured.display()
            ),
        ));
    }
    let missing = |reason: &str| {
        invalid_config(
            entry,
            "path",
            &format!("plugin executable {} {reason}", configured.display()),
        )
    };
    let path = fs::canonicalize(configured)
        .await
        .map_err(|_| missing("does not exist or is not accessible"))?;
    let metadata = fs::metadata(&path)
        .await
        .map_err(|_| missing("is not accessible"))?;
    if !metadata.is_file() {
        return Err(missing("is not a file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(missing("is not executable"));
        }
    }
    Ok(path)
}

async fn digest(entry: &ProviderName, path: &Path) -> Result<Sha256Digest, Error> {
    let hash = file_sha256(path)
        .await
        .map_err(|_| unavailable(entry, "cannot read plugin executable"))?;
    Ok(Sha256Digest::try_new(hash).expect("SHA-256 produces a valid digest"))
}

async fn probe(
    entry: &ProviderName,
    path: &Path,
    limits: &DiscoveryLimits,
) -> Result<ProviderSchemas, Error> {
    let mut command = Command::new(path);
    command.arg("--describe-schema").env_clear();
    // No HOME, user-configurable PATH, loader injection variables, or runtime
    // settings. A native executable needs no credential-bearing environment.
    #[cfg(unix)]
    command
        .env("PATH", "/usr/bin:/bin")
        .current_dir("/")
        .process_group(0);
    #[cfg(windows)]
    if let Some(root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", root);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_| unavailable(entry, "cannot start plugin metadata probe"))?;
    // Descendants may hold the pipes after the probe itself exits. Dropping
    // the guard (on return, timeout or cancellation) kills the whole group.
    let _group = child.id().map(ProcessGroup);
    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");
    // Poll pipes concurrently with wait: neither a noisy stderr nor a child
    // retaining stdout can deadlock discovery.
    let (stdout, stderr, status) = tokio::try_join!(
        read_bounded(entry, stdout, limits.stdout_bytes),
        read_bounded(entry, stderr, limits.stderr_bytes),
        async {
            child
                .wait()
                .await
                .map_err(|_| unavailable(entry, "cannot reap metadata probe"))
        },
    )?;
    if !status.success() {
        // The probe holds no credentials, so its diagnostics may be logged;
        // they never enter the error, which callers may show or persist.
        tracing::warn!(
            provider_entry = %entry,
            stderr = %String::from_utf8_lossy(&stderr),
            "plugin metadata probe failed"
        );
        return Err(unavailable(
            entry,
            "plugin metadata probe exited unsuccessfully",
        ));
    }
    // Do not echo plugin stdout or serde errors (which can quote input).
    serde_json::from_slice(&stdout).map_err(|_| {
        unavailable(
            entry,
            "plugin metadata probe did not return one ProviderSchemas object",
        )
    })
}

/// Kills a probe's process group, including descendants that outlive it.
struct ProcessGroup(u32);

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Ok(pid) = i32::try_from(self.0) {
            // ESRCH (already gone) is the common, successful case.
            let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
        }
    }
}

async fn read_bounded(
    entry: &ProviderName,
    mut reader: impl AsyncRead + Unpin,
    max: usize,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    let mut buffer = vec![0; 8192];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|_| unavailable(entry, "cannot read metadata probe output"))?;
        if count == 0 {
            return Ok(bytes);
        }
        if count > max.saturating_sub(bytes.len()) {
            return Err(unavailable(
                entry,
                "plugin metadata probe output exceeded its budget",
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

/// SDK helper for `--describe-schema`. Call before constructing a provider or
/// loading runtime configuration. The caller must exit after writing metadata.
pub fn write_schema(mut writer: impl Write, schemas: &ProviderSchemas) -> io::Result<()> {
    serde_json::to_writer(&mut writer, schemas)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn invalid_config(entry: &ProviderName, path: &str, message: &str) -> Error {
    Error::InvalidConfig {
        entry:   entry.clone(),
        path:    path.into(),
        message: message.into(),
    }
}

fn unavailable(entry: &ProviderName, message: &str) -> Error {
    Error::Unavailable {
        message: format!("provider entry {entry}: {message}"),
        outcome: MutationOutcome::NotStarted,
    }
}
