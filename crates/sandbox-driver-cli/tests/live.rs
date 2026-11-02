//! Live black-box workflows for the `lithos-sandbox` binary.
//!
//! Docker runs only when its daemon is reachable. Daytona is ignored by
//! default; `mise run test:live` runs it with credentials from the process
//! environment or the workspace `.env` file. Every created sandbox has a
//! unique label and a cleanup guard.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{self, Command, Output, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs};

use serde_json::Value;

const DAYTONA_ENV: &[&str] = &[
    "DAYTONA_API_KEY",
    "DAYTONA_JWT_TOKEN",
    "DAYTONA_ORGANIZATION_ID",
    "DAYTONA_API_URL",
    "DAYTONA_SERVER_URL",
    "DAYTONA_TARGET",
];
const REQUIRE_LIVE_ENV: &str = "LITHOS_CLI_E2E_REQUIRE_LIVE";
const TEST_IMAGE: &str = "ghcr.io/lithoscomputer/ubuntu-24.04:slim-df708f910111";
const TEST_SNAPSHOT: &str = "daytona-medium";

#[test]
fn docker_cli_workflow() {
    let cli = CliRunner::new();
    if !docker_is_available(&cli) {
        assert!(
            !live_providers_required(),
            "Docker is required, but its daemon is unreachable"
        );
        return;
    }

    assert_provider_contract(&cli, "docker", "container");
    run_one_shot(&cli, "docker", &["--image", TEST_IMAGE]);
    run_failed_one_shot(&cli, "docker", &["--image", TEST_IMAGE]);
    run_persistent_workflow(&cli, "docker", &["--image", TEST_IMAGE]);
}

#[test]
#[ignore = "requires Daytona credentials; run with `mise run test:live`"]
fn daytona_cli_workflow() {
    let cli = CliRunner::new();
    assert!(
        cli.has_daytona_credentials(),
        "Daytona is required, but no API key or JWT token is available"
    );

    assert_provider_contract(&cli, "daytona", "vm");
    let source = ["--snapshot", TEST_SNAPSHOT, "--kind", "container"];
    run_one_shot(&cli, "daytona", &source);
    run_failed_one_shot(&cli, "daytona", &source);
    run_persistent_workflow(&cli, "daytona", &source);
}

fn assert_provider_contract(cli: &CliRunner, provider: &str, isolation: &str) {
    let mut health = cli.command();
    health.args([
        "--provider",
        provider,
        "--output",
        "json",
        "provider",
        "health",
    ]);
    let health = run(&mut health);
    let health_json = successful_json(&health, "provider health");
    assert_eq!(health_json["status"], "ok", "{provider} health");

    let mut capabilities = cli.command();
    capabilities.args([
        "--provider",
        provider,
        "--output",
        "json",
        "provider",
        "capabilities",
    ]);
    let capabilities = run(&mut capabilities);
    let capabilities_json = successful_json(&capabilities, "provider capabilities");
    assert_eq!(
        capabilities_json["isolation"], isolation,
        "{provider} isolation"
    );
    assert_eq!(
        capabilities_json["exec"]["live_streaming"], true,
        "{provider} streaming"
    );
}

fn run_one_shot(cli: &CliRunner, provider: &str, source: &[&str]) {
    let name = unique_name(&format!("lithos-cli-{provider}-run"));
    let label = format!("lithos-cli-e2e={name}");
    let workspace = format!("/tmp/{name}");
    let created_env = format!("CREATED_ENV={provider}");

    let mut command = cli.command();
    command
        .args(["--provider", provider, "--events", "json", "sandbox", "run"])
        .args(source)
        .args(["--name", &name, "--label", &label])
        .args(["--env", &created_env, "--workspace", &workspace]);
    add_provider_create_options(&mut command, provider, true);
    command
        .args(["--exec-env", "EXEC_ENV=one-shot", "--stdin"])
        .args(["--", "/bin/sh", "-c"])
        .arg("read input; printf '%s:%s:%s\\n' \"$CREATED_ENV\" \"$EXEC_ENV\" \"$input\"");
    let output = run_with_stdin(&mut command, b"payload\n");
    assert_success(&output, "one-shot sandbox run");
    assert_eq!(
        stdout_text(&output),
        format!("{provider}:one-shot:payload\n")
    );
    assert_json_events(&output, provider);

    assert_label_absent(cli, provider, &label, "one-shot sandbox");
}

fn run_failed_one_shot(cli: &CliRunner, provider: &str, source: &[&str]) {
    let name = unique_name(&format!("lithos-cli-{provider}-failed-run"));
    let label = format!("lithos-cli-e2e={name}");

    let mut command = cli.command();
    command
        .args(["--provider", provider, "--events", "off", "sandbox", "run"])
        .args(source)
        .args(["--name", &name, "--label", &label]);
    add_provider_create_options(&mut command, provider, true);
    command.args(["--", "/bin/sh", "-c", "exit 7"]);
    let output = run(&mut command);
    assert_eq!(output.status.code(), Some(7), "{}", diagnostic(&output));
    assert_eq!(stdout_text(&output), "");
    assert_eq!(stderr_text(&output), "");

    assert_label_absent(cli, provider, &label, "failed one-shot sandbox");
}

fn run_persistent_workflow(cli: &CliRunner, provider: &str, source: &[&str]) {
    let name = unique_name(&format!("lithos-cli-{provider}-persistent"));
    let label = format!("lithos-cli-e2e={name}");
    let workspace = format!("/tmp/{name}");
    let id = create_sandbox(cli, provider, source, &name, &label, &workspace);
    let mut sandbox = SandboxGuard::new(cli, provider, id.clone());

    assert_sandbox_listing(cli, provider, &id, &name, &label);
    assert_sandbox_status(cli, provider, &id, &name, "running");
    assert_exec(cli, provider, &id, &workspace);
    assert_file_transfer(cli, provider, &id, &workspace, &name);
    assert_raw_output_guards(cli, provider, &id);

    match provider {
        "docker" => {
            assert_lifecycle_state(cli, provider, &id, "pause", "paused");
            assert_lifecycle_state(cli, provider, &id, "resume", "running");
            assert_capability_failure(cli, provider, &id, "archive", "lifecycle.archive");
            assert_capability_failure(cli, provider, &id, "recover", "lifecycle.recover");
            assert_capability_failure(
                cli,
                provider,
                &id,
                "refresh-activity",
                "lifecycle.refresh_activity",
            );
            assert_capability_failure(cli, provider, &id, "undelete", "lifecycle.undelete");
        }
        "daytona" => {
            assert_capability_failure(cli, provider, &id, "pause", "lifecycle.pause");
            assert_capability_failure(cli, provider, &id, "resume", "lifecycle.pause");
            assert_capability_failure(cli, provider, &id, "recover", "lifecycle.recover");
            assert_action(
                cli,
                provider,
                &id,
                "refresh-activity",
                "refreshed activity for",
            );
            assert_undelete_requires_deleted(cli, provider, &id);
        }
        _ => panic!("unexpected persistent provider {provider}"),
    }

    assert_lifecycle_state(cli, provider, &id, "stop", "stopped");
    assert_lifecycle_state(cli, provider, &id, "start", "running");
    delete_sandbox(cli, provider, &id);
    sandbox.disarm();

    assert_label_absent(cli, provider, &label, "deleted sandbox");
}

fn create_sandbox(
    cli: &CliRunner,
    provider: &str,
    source: &[&str],
    name: &str,
    label: &str,
    workspace: &str,
) -> String {
    let created_env = format!("CREATED_ENV={provider}");
    let mut command = cli.command();
    command
        .args([
            "--provider",
            provider,
            "--events",
            "json",
            "--output",
            "id",
            "sandbox",
            "create",
        ])
        .args(source)
        .args(["--name", name, "--label", label])
        .args(["--env", &created_env, "--workspace", workspace]);
    add_provider_create_options(&mut command, provider, false);
    command.arg("--wait");
    let output = run(&mut command);
    assert_success(&output, "sandbox create");
    assert_json_events(&output, provider);

    let id = stdout_text(&output).trim().to_owned();
    assert!(!id.is_empty(), "sandbox create returned no ID");
    assert!(
        !id.chars().any(char::is_whitespace),
        "invalid sandbox ID {id:?}"
    );
    id
}

fn assert_sandbox_listing(cli: &CliRunner, provider: &str, id: &str, name: &str, label: &str) {
    let listed = list_by_label(cli, provider, label);
    let statuses = listed.as_array().expect("sandbox list is an array");
    let status = statuses
        .iter()
        .find(|status| status["id"] == id)
        .expect("created sandbox is listed");
    assert_eq!(status["name"], name);
    assert_eq!(status["labels"]["lithos-cli-e2e"], name);

    let mut command = cli.command();
    command
        .args(["--provider", provider, "--output", "id", "sandbox", "list"])
        .args(["--label", label]);
    let output = run(&mut command);
    assert_success(&output, "sandbox list --output id");
    assert_eq!(stdout_text(&output), format!("{id}\n"));
}

fn list_by_label(cli: &CliRunner, provider: &str, label: &str) -> Value {
    let mut command = cli.command();
    command
        .args([
            "--provider",
            provider,
            "--output",
            "json",
            "sandbox",
            "list",
        ])
        .args(["--label", label]);
    successful_json(&run(&mut command), "sandbox list")
}

fn assert_label_absent(cli: &CliRunner, provider: &str, label: &str, context: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let listed = list_by_label(cli, provider, label);
        if listed.as_array().is_some_and(Vec::is_empty) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{context} remains listed after deletion: {listed}"
        );
        sleep(Duration::from_millis(500));
    }
}

fn assert_sandbox_status(cli: &CliRunner, provider: &str, id: &str, name: &str, state: &str) {
    let mut command = cli.command();
    command.args([
        "--provider",
        provider,
        "--output",
        "json",
        "sandbox",
        "inspect",
        id,
    ]);
    let status = successful_json(&run(&mut command), "sandbox inspect");
    assert_eq!(status["id"], id);
    assert_eq!(status["name"], name);
    assert_eq!(status["state"], state);
    assert_eq!(status["sandbox_kind"], "container");
    // The status names what the sandbox runs from: Docker its image,
    // Daytona its snapshot.
    match provider {
        "docker" => {
            assert_eq!(status["image"], TEST_IMAGE);
            assert_eq!(status["snapshot"], Value::Null);
        }
        "daytona" => {
            assert_eq!(status["snapshot"], TEST_SNAPSHOT);
            assert_eq!(status["image"], Value::Null);
        }
        _ => panic!("unexpected provider {provider}"),
    }

    let mut table = cli.command();
    table.args(["--provider", provider, "sandbox", "inspect", id]);
    let output = run(&mut table);
    assert_success(&output, "sandbox inspect table");
    let table = stdout_text(&output);
    assert!(table.contains("ID\tNAME\tSTATE\tPROVIDER STATE\tREGION\n"));
    assert!(table.contains(id));
    assert!(table.contains(name));
}

fn assert_exec(cli: &CliRunner, provider: &str, id: &str, workspace: &str) {
    let mut command = cli.command();
    command
        .args(["--provider", provider, "--events", "off", "sandbox", "exec", id])
        .args(["--working-dir", workspace, "--exec-env", "EXEC_ENV=persistent"])
        .arg("--stdin")
        .args(["--", "/bin/sh", "-c"])
        .arg(
            "read input; printf '%s:%s:%s:%s\\n' \"$CREATED_ENV\" \"$EXEC_ENV\" \"$input\" \"$PWD\"; printf 'stderr-value\\n' >&2",
        );
    let output = run_with_stdin(&mut command, b"payload\n");
    assert_success(&output, "sandbox exec");
    assert_eq!(
        stdout_text(&output),
        format!("{provider}:persistent:payload:{workspace}\n")
    );
    assert_eq!(stderr_text(&output), "stderr-value\n");

    let mut timed_out = cli.command();
    timed_out.args([
        "--provider",
        provider,
        "--events",
        "off",
        "sandbox",
        "exec",
        id,
        "--timeout-seconds",
        "1",
        "--",
        "/bin/sh",
        "-c",
        "sleep 10",
    ]);
    let output = run(&mut timed_out);
    assert_eq!(output.status.code(), Some(124), "{}", diagnostic(&output));
    assert_eq!(stdout_text(&output), "");
    assert_eq!(stderr_text(&output), "sandbox command timed out\n");

    let mut unbounded = cli.command();
    unbounded.args([
        "--provider",
        provider,
        "--events",
        "off",
        "sandbox",
        "exec",
        id,
        "--no-timeout",
        "--",
        "/bin/true",
    ]);
    assert_success(&run(&mut unbounded), "sandbox exec --no-timeout");
}

fn add_provider_create_options(command: &mut Command, provider: &str, one_shot: bool) {
    match provider {
        "docker" => {
            command.args([
                "--kind",
                "container",
                "--cpu",
                "1",
                "--memory-mb",
                "128",
                "--network",
                "block",
                "--provider-config",
                r#"{"auto_pull":true}"#,
            ]);
        }
        "daytona" => {
            command.args(["--network", "allow-all", "--provider-config", "{}"]);
            if one_shot {
                command.arg("--ephemeral");
            }
        }
        _ => panic!("unexpected provider {provider}"),
    }
}

fn assert_file_transfer(cli: &CliRunner, provider: &str, id: &str, workspace: &str, name: &str) {
    let directory = TestDirectory::new(name);
    let source = directory.path.join("upload.txt");
    let destination = directory.path.join("download.txt");
    let contents = format!("{provider} file transfer\n");
    fs::write(&source, &contents).expect("write upload fixture");
    let remote = format!("{workspace}/uploaded.txt");

    let mut upload = cli.command();
    upload
        .args([
            "--provider",
            provider,
            "--output",
            "json",
            "sandbox",
            "fs",
            "upload",
            id,
        ])
        .arg(&source)
        .arg(&remote);
    assert_action_output(&run(&mut upload), provider, id, "uploaded file to");

    let mut download = cli.command();
    download
        .args([
            "--provider",
            provider,
            "--output",
            "json",
            "sandbox",
            "fs",
            "download",
            id,
        ])
        .arg(&remote)
        .arg(&destination);
    assert_action_output(&run(&mut download), provider, id, "downloaded file from");
    assert_eq!(
        fs::read_to_string(destination).expect("read downloaded fixture"),
        contents
    );
}

fn assert_raw_output_guards(cli: &CliRunner, provider: &str, id: &str) {
    let mut exec = cli.command();
    exec.args([
        "--provider",
        provider,
        "--output",
        "json",
        "sandbox",
        "exec",
        id,
        "--",
        "/bin/true",
    ]);
    assert_failure_contains(&run(&mut exec), "sandbox exec streams raw output");

    let mut shell = cli.command();
    shell.args([
        "--provider",
        provider,
        "--output",
        "json",
        "sandbox",
        "shell",
        id,
    ]);
    assert_failure_contains(&run(&mut shell), "sandbox shell streams raw output");
}

fn assert_lifecycle_state(
    cli: &CliRunner,
    provider: &str,
    id: &str,
    action: &str,
    expected_state: &str,
) {
    let mut command = cli.command();
    command.args([
        "--provider",
        provider,
        "--events",
        "off",
        "--output",
        "json",
        "sandbox",
        action,
        id,
        "--wait",
    ]);
    let status = successful_json(&run(&mut command), action);
    assert_eq!(status["id"], id);
    assert_eq!(status["state"], expected_state, "{provider} {action}");
}

fn assert_action(cli: &CliRunner, provider: &str, id: &str, action: &str, output: &str) {
    let mut command = cli.command();
    command.args([
        "--provider",
        provider,
        "--events",
        "off",
        "--output",
        "json",
        "sandbox",
        action,
        id,
    ]);
    assert_action_output(&run(&mut command), provider, id, output);
}

fn assert_action_output(output: &Output, provider: &str, id: &str, action: &str) {
    let value = successful_json(output, action);
    assert_eq!(value["provider"], provider);
    assert_eq!(value["sandbox_id"], id);
    assert_eq!(value["action"], action);
}

fn assert_capability_failure(
    cli: &CliRunner,
    provider: &str,
    id: &str,
    action: &str,
    capability: &str,
) {
    let mut command = cli.command();
    command.args([
        "--provider",
        provider,
        "--events",
        "off",
        "sandbox",
        action,
        id,
    ]);
    assert_failure_contains(&run(&mut command), capability);
}

fn delete_sandbox(cli: &CliRunner, provider: &str, id: &str) {
    let mut command = cli.command();
    command.args([
        "--provider",
        provider,
        "--events",
        "off",
        "--output",
        "json",
        "sandbox",
        "delete",
        id,
    ]);
    assert_action_output(&run(&mut command), provider, id, "deleted");
}

fn assert_undelete_requires_deleted(cli: &CliRunner, provider: &str, id: &str) {
    let mut command = cli.command();
    command.args([
        "--provider",
        provider,
        "--events",
        "off",
        "sandbox",
        "undelete",
        id,
    ]);
    assert_failure_contains(&run(&mut command), "undeleting sandbox");
}

fn docker_is_available(cli: &CliRunner) -> bool {
    let mut command = cli.command();
    command.args([
        "--provider",
        "docker",
        "--output",
        "json",
        "provider",
        "health",
    ]);
    let output = run(&mut command);
    let health: Value = match serde_json::from_slice(&output.stdout) {
        Ok(health) => health,
        Err(_)
            if !output.status.success()
                && stderr_text(&output).contains("connecting to the docker daemon") =>
        {
            return false;
        }
        Err(error) => {
            panic!(
                "Docker health was not JSON: {error}; {}",
                diagnostic(&output)
            );
        }
    };
    match health["status"].as_str() {
        Some("ok") => {
            assert_success(&output, "Docker health");
            true
        }
        Some("unreachable") => {
            assert!(!output.status.success(), "{}", diagnostic(&output));
            false
        }
        status => panic!("unexpected Docker health status {status:?}: {health}"),
    }
}

fn assert_json_events(output: &Output, provider: &str) {
    let stderr = stderr_text(output);
    let events: Vec<Value> = stderr
        .lines()
        .map(|line| serde_json::from_str(line).expect("event line is JSON"))
        .collect();
    assert!(
        events.len() >= 2,
        "expected lifecycle events, got {stderr:?}"
    );
    assert!(
        events.iter().all(|event| event["provider"] == provider),
        "event provider mismatch: {events:?}"
    );
}

fn successful_json(output: &Output, context: &str) -> Value {
    assert_success(output, context);
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{context} did not return JSON: {error}; {}",
            diagnostic(output)
        )
    })
}

fn assert_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed: {}",
        diagnostic(output)
    );
}

fn assert_failure_contains(output: &Output, expected: &str) {
    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = stderr_text(output);
    assert!(
        stderr.contains(expected),
        "stderr did not contain {expected:?}: {stderr:?}"
    );
}

fn diagnostic(output: &Output) -> String {
    format!(
        "status={:?}, stdout={:?}, stderr={:?}",
        output.status.code(),
        stdout_text(output),
        stderr_text(output)
    )
}

fn stdout_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn unique_name(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock follows the Unix epoch")
        .as_nanos();
    format!("{prefix}-{}-{timestamp}", process::id())
}

struct CliRunner {
    extra_env: BTreeMap<&'static str, OsString>,
}

impl CliRunner {
    fn new() -> Self {
        Self {
            extra_env: load_daytona_dotenv(),
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_lithos-sandbox"));
        command
            .current_dir(workspace_root())
            .env_remove("SANDBOX_DRIVER_CONFIG")
            .env("SANDBOX_DRIVER_PLUGIN_DEV", "1")
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "off")
            .env(
                "XDG_CONFIG_HOME",
                workspace_root().join("target/cli-e2e/no-config"),
            )
            .envs(&self.extra_env);
        command
    }

    fn has_daytona_credentials(&self) -> bool {
        has_nonempty_env("DAYTONA_API_KEY")
            || has_nonempty_env("DAYTONA_JWT_TOKEN")
            || self
                .extra_env
                .get("DAYTONA_API_KEY")
                .is_some_and(|value| !value.is_empty())
            || self
                .extra_env
                .get("DAYTONA_JWT_TOKEN")
                .is_some_and(|value| !value.is_empty())
    }
}

fn run(command: &mut Command) -> Output {
    command.output().expect("lithos-sandbox process starts")
}

fn run_with_stdin(command: &mut Command, input: &[u8]) -> Output {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("lithos-sandbox process starts");
    child
        .stdin
        .take()
        .expect("child stdin is piped")
        .write_all(input)
        .expect("write child stdin");
    child.wait_with_output().expect("wait for lithos-sandbox")
}

fn load_daytona_dotenv() -> BTreeMap<&'static str, OsString> {
    let Ok(contents) = fs::read_to_string(workspace_root().join(".env")) else {
        return BTreeMap::new();
    };
    let mut values = BTreeMap::new();
    for &name in DAYTONA_ENV {
        if has_nonempty_env(name) {
            continue;
        }
        if let Some(value) = dotenv_value(&contents, name) {
            values.insert(name, OsString::from(value));
        }
    }
    values
}

fn dotenv_value(contents: &str, name: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let line = line.trim().strip_prefix("export ").unwrap_or(line.trim());
        let (key, value) = line.split_once('=')?;
        if key.trim() != name {
            return None;
        }
        let value = value.trim();
        let unquoted = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .or_else(|| {
                value
                    .strip_prefix('\'')
                    .and_then(|value| value.strip_suffix('\''))
            })
            .unwrap_or(value);
        (!unquoted.is_empty()).then(|| unquoted.to_owned())
    })
}

fn has_nonempty_env(name: &str) -> bool {
    env::var_os(name).is_some_and(|value| !value.is_empty())
}

fn live_providers_required() -> bool {
    has_nonempty_env(REQUIRE_LIVE_ENV)
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

struct SandboxGuard<'a> {
    cli:      &'a CliRunner,
    provider: String,
    id:       String,
    armed:    bool,
}

impl<'a> SandboxGuard<'a> {
    fn new(cli: &'a CliRunner, provider: &str, id: String) -> Self {
        Self {
            cli,
            provider: provider.to_owned(),
            id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SandboxGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut command = self.cli.command();
        command.args([
            "--provider",
            &self.provider,
            "--events",
            "off",
            "sandbox",
            "delete",
            &self.id,
        ]);
        let _ = command.output();
    }
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let path = workspace_root().join("target/cli-e2e").join(name);
        fs::create_dir_all(&path).expect("create CLI test directory");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
