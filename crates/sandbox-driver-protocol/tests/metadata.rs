//! Exercises discovery through real executable boundaries, without a backend.
#![cfg(unix)]

use std::fmt::Debug;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, fs};

use nix::sys::signal::kill;
use nix::unistd::Pid;
use sandbox_driver::contract::{Error, PluginLocation, PluginTrust, ProviderName, ProviderSchemas};
use sandbox_driver_protocol::file_sha256;
use sandbox_driver_protocol::metadata::{DiscoveryLimits, SchemaDiscovery, write_schema};
use serde_json::json;
use tokio::time;

struct Fixture {
    root:   PathBuf,
    binary: PathBuf,
}

impl Fixture {
    fn new(body: &str) -> Self {
        let root = env::temp_dir().join(format!("schema-probe-{}", rand::random::<u64>()));
        fs::create_dir(&root).expect("fixture directory");
        let binary = root.join("plugin");
        let fixture = Self { root, binary };
        fixture.write(body);
        fixture
    }
    fn write(&self, body: &str) {
        fs::write(
            &self.binary,
            format!("#!/bin/sh\ncd '{}'\n{body}\n", self.root.display()),
        )
        .expect("fixture executable");
        fs::set_permissions(&self.binary, fs::Permissions::from_mode(0o700))
            .expect("executable permission");
    }
    /// What the fixture's script appended to its `calls` file.
    fn calls(&self) -> String {
        fs::read_to_string(self.root.join("calls")).expect("calls")
    }
    /// The process id the fixture's script wrote to its `pid` file.
    fn pid(&self) -> i32 {
        fs::read_to_string(self.root.join("pid"))
            .expect("pid")
            .trim()
            .parse()
            .expect("pid number")
    }
    fn entry(&self) -> PluginLocation {
        PluginLocation {
            entry: name(),
            kind:  "fixture".to_string().try_into().expect("kind"),
            path:  Some(self.binary.to_string_lossy().into()),
            trust: Some(PluginTrust::Development),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn name() -> ProviderName {
    ProviderName::try_new("test-entry").expect("name")
}
fn schemas() -> ProviderSchemas {
    serde_json::from_value(json!({
        "kind": "fixture",
        "version": "1.0",
        "config_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
        },
        "secrets": ["PRIVATE_TOKEN"],
    }))
    .expect("schemas")
}
fn emit() -> String {
    format!(
        "printf '%s\\n' '{}'",
        serde_json::to_string(&schemas()).expect("json")
    )
}

#[tokio::test]
async fn probe_is_credential_free_and_cache_is_shared_by_digest() {
    let fixture = Fixture::new(&format!(
        "[ \"$#\" = 1 ] && [ \"$1\" = '--describe-schema' ] || exit 8\n\
        [ -z \"${{HOME+x}}${{USER+x}}${{PRIVATE_TOKEN+x}}\" ] || exit 9\n\
        [ \"$PATH\" = /usr/bin:/bin ] || exit 10\n\
        if read line; then exit 11; fi\n\
        echo probe >> calls\n{}",
        emit()
    ));
    let cache = SchemaDiscovery::default();
    assert!(!fixture.root.join("calls").exists(), "construction is cold");
    let first = cache
        .discover(&fixture.entry())
        .await
        .expect("credential-free probe");
    assert_eq!(first.schemas(), &schemas());
    assert!(!first.verified());
    let mut renamed = fixture.entry();
    let copy = fixture.root.join("renamed");
    fs::copy(&fixture.binary, &copy).expect("same bytes at another path");
    renamed.path = Some(copy.to_string_lossy().into());
    let second = cache.discover(&renamed).await.expect("cached schema");
    assert_eq!(second.digest(), first.digest());
    assert_eq!(fixture.calls(), "probe\n");
}

#[tokio::test]
async fn every_cache_hit_enforces_trust_and_changed_bytes_invalidate_metadata() {
    let fixture = Fixture::new(&format!("echo probe >> calls\n{}", emit()));
    let cache = SchemaDiscovery::default();
    let original = cache.discover(&fixture.entry()).await.expect("first");
    let mut entry = fixture.entry();
    entry.trust = None;
    assert!(matches!(
        cache.discover(&entry).await,
        Err(Error::InvalidConfig { .. })
    ));
    let pin = file_sha256(&fixture.binary).await.expect("checksum");
    entry.trust = Some(PluginTrust::Sha256 {
        digest: pin.try_into().expect("digest"),
    });
    assert!(
        cache
            .discover(&entry)
            .await
            .expect("pinned cache hit")
            .verified()
    );
    fixture.write(&format!(
        "echo changed >> calls\n{}",
        emit().replace("1.0", "2.0")
    ));
    assert!(matches!(
        cache.discover(&entry).await,
        Err(Error::InvalidConfig { .. })
    ));
    original
        .verify_unchanged(&name())
        .await
        .expect_err("cannot launch changed binary");
    assert_eq!(fixture.calls(), "probe\n", "bad pin never executes");
    let changed = cache
        .discover(&fixture.entry())
        .await
        .expect("dev re-probes new bytes");
    assert_eq!(changed.schemas().version, "2.0");
    assert_ne!(changed.digest(), original.digest());
}

#[tokio::test]
async fn setup_errors_name_the_entry_and_do_not_poison_other_entries() {
    let fixture = Fixture::new(&emit());
    let cache = SchemaDiscovery::default();
    let absent = fixture.root.join("absent");
    let mut bad = fixture.entry();
    bad.path = Some(absent.to_string_lossy().into());
    let message = config_error(cache.discover(&bad).await, "path");
    assert!(message.contains(&absent.display().to_string()), "{message}");
    bad.path = Some("plugins/fixture".into());
    config_error(cache.discover(&bad).await, "path");
    bad.path = None;
    config_error(cache.discover(&bad).await, "path");
    bad = fixture.entry();
    bad.trust = Some(PluginTrust::Sha256 {
        digest: "0".repeat(64).try_into().expect("digest"),
    });
    let message = config_error(cache.discover(&bad).await, "sha256");
    let actual = file_sha256(&fixture.binary).await.expect("checksum");
    assert!(message.contains(&actual), "re-pinnable: {message}");
    bad = fixture.entry();
    bad.kind = "other".to_string().try_into().expect("kind");
    config_error(cache.discover(&bad).await, "plugin");
    cache
        .discover(&fixture.entry())
        .await
        .expect("unrelated entry still usable");
}

fn config_error(result: Result<impl Debug, Error>, field: &str) -> String {
    match result {
        Err(Error::InvalidConfig {
            entry,
            path,
            message,
        }) if entry == name() && path == field => message,
        other => panic!("expected an invalid {field} for the entry, got {other:?}"),
    }
}

#[tokio::test]
async fn invalid_failed_and_oversize_probes_are_never_cached_or_echoed() {
    for body in [
        "echo secret-value; echo secret-value >&2; exit 1".into(),
        "echo secret-value".into(),
        format!("{}\n{}", emit(), emit()),
        format!("{}\nexit 2", emit()),
        "while :; do printf secret-value; done".into(),
        "while :; do printf secret-value >&2; done".into(),
    ] {
        let fixture = Fixture::new(&body);
        let cache = SchemaDiscovery::try_new(DiscoveryLimits {
            stdout_bytes: 512,
            stderr_bytes: 64,
            ..DiscoveryLimits::default()
        })
        .expect("limits");
        let error = cache
            .discover(&fixture.entry())
            .await
            .expect_err("bad probe");
        assert!(!format!("{error:?}").contains("secret-value"));
        fixture.write(&emit());
        cache
            .discover(&fixture.entry())
            .await
            .expect("successful retry");
    }
}

async fn wait_for_file(path: &Path) {
    time::timeout(Duration::from_secs(3), async {
        while !path.exists() {
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("probe started");
}
async fn assert_dead(pid: i32) {
    time::timeout(Duration::from_secs(3), async {
        while kill(Pid::from_raw(pid), None).is_ok() {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("probe is killed and reaped");
}

#[tokio::test]
async fn timeout_and_caller_cancellation_kill_the_metadata_child() {
    let fixture = Fixture::new(&format!("{}\necho $$ > pid; exec /bin/sleep 30", emit()));
    let cache = SchemaDiscovery::try_new(DiscoveryLimits {
        timeout: Duration::from_secs(1),
        ..DiscoveryLimits::default()
    })
    .expect("limits");
    cache.discover(&fixture.entry()).await.expect_err("timeout");
    let pid = fixture.pid();
    assert_dead(pid).await;
    fs::remove_file(fixture.root.join("pid")).expect("remove pid");
    let entry = fixture.entry();
    let task = tokio::spawn(async move { SchemaDiscovery::default().discover(&entry).await });
    wait_for_file(&fixture.root.join("pid")).await;
    let pid = fixture.pid();
    task.abort();
    assert!(task.await.expect_err("cancelled").is_cancelled());
    assert_dead(pid).await;
}

#[tokio::test]
async fn concurrent_discoveries_wait_and_share_one_probe() {
    let fixture = Fixture::new(&format!("echo probe >> calls\n/bin/sleep 0.2\n{}", emit()));
    let other = Fixture::new(&format!("/bin/sleep 0.2\n{}", emit()));
    let cache = SchemaDiscovery::try_new(DiscoveryLimits {
        concurrent_probes: 1,
        ..DiscoveryLimits::default()
    })
    .expect("limits");
    let (entry, other_entry) = (fixture.entry(), other.entry());
    let results = tokio::join!(
        cache.discover(&entry),
        cache.discover(&entry),
        cache.discover(&entry),
        cache.discover(&other_entry),
        cache.discover(&other_entry),
    );
    for result in [results.0, results.1, results.2, results.3, results.4] {
        result.expect("capacity waits instead of refusing");
    }
    assert_eq!(fixture.calls(), "probe\n", "one probe per binary");
}

#[tokio::test]
async fn cache_storage_is_bounded() {
    let cache = SchemaDiscovery::try_new(DiscoveryLimits {
        cached_binaries: 1,
        ..DiscoveryLimits::default()
    })
    .expect("limits");
    let first = Fixture::new(&format!("echo probe >> calls\n{}", emit()));
    let second = Fixture::new(&emit());
    cache.discover(&first.entry()).await.expect("first");
    cache.discover(&second.entry()).await.expect("evicts first");
    cache.discover(&first.entry()).await.expect("probes again");
    assert_eq!(first.calls(), "probe\nprobe\n");
}

#[tokio::test]
async fn descendants_of_a_probe_are_killed_with_it() {
    for body in [
        // Fails after the timeout while a background child holds stdout.
        "/bin/sleep 30 &\necho $! > pid\nwait".to_owned(),
        // Succeeds, leaving a background child behind.
        format!(
            "/bin/sleep 30 </dev/null >/dev/null 2>&1 &\necho $! > pid\n{}",
            emit()
        ),
    ] {
        let fixture = Fixture::new(&body);
        let cache = SchemaDiscovery::try_new(DiscoveryLimits {
            timeout: Duration::from_millis(500),
            ..DiscoveryLimits::default()
        })
        .expect("limits");
        let _ = cache.discover(&fixture.entry()).await;
        let pid = fixture.pid();
        assert_dead(pid).await;
    }
}

#[tokio::test]
async fn sdk_output_round_trips_through_discovery() {
    let fixture = Fixture::new(&emit());
    let discovered = SchemaDiscovery::default()
        .discover(&fixture.entry())
        .await
        .expect("discovery");
    let mut bytes = Vec::new();
    write_schema(&mut bytes, &schemas()).expect("SDK output");
    assert_eq!(
        serde_json::from_slice::<ProviderSchemas>(&bytes).expect("one object"),
        schemas()
    );
    assert_eq!(discovered.schemas(), &schemas());
}
