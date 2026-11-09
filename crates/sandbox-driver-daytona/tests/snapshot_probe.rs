//! Execute the actual remote snapshot probe against synthetic proc records.

#![cfg(unix)]

use std::process::{Command, Output};
use std::{env, fs};

const PROBE: &str = include_str!("support/snapshot_state.sh");
const PROCESS: &str = "sandbox-driver-cold-process";

fn probe(mode: &str, argv: &[&str], marker: &str) -> Output {
    let directory = env::temp_dir().join(format!("snapshot-probe-{:032x}", rand::random::<u128>()));
    let proc_root = directory.join("proc");
    fs::create_dir_all(proc_root.join("42")).expect("test proc directory");
    let marker_path = directory.join("marker");
    let pid_path = directory.join("pid");
    fs::write(&marker_path, marker).expect("marker file");
    fs::write(&pid_path, "42").expect("pid file");
    if !argv.is_empty() {
        let mut cmdline = argv.join("\0").into_bytes();
        cmdline.push(0);
        fs::write(proc_root.join("42/cmdline"), cmdline).expect("proc cmdline");
    }
    let output = Command::new("bash")
        .args(["-c", PROBE, "snapshot-probe"])
        .arg(marker_path)
        .arg(pid_path)
        .args([PROCESS, mode])
        .arg(proc_root)
        .output()
        .expect("snapshot probe runs locally");
    fs::remove_dir_all(directory).expect("remove probe fixture");
    output
}

#[test]
fn a_reused_pid_containing_the_name_is_not_the_restored_process() {
    let argv = ["bash", "-c", "grep -q sandbox-driver-cold-process"];
    assert!(probe("cold", &argv, "preserved").status.success());
    assert_eq!(probe("live", &argv, "preserved").status.code(), Some(24));
}

#[test]
fn the_original_process_is_detected_by_its_exact_first_argument() {
    let argv = [PROCESS, "3600"];
    assert_eq!(probe("cold", &argv, "preserved").status.code(), Some(23));
    let live = probe("live", &argv, "preserved");
    assert!(live.status.success());
    assert_eq!(live.stdout, b"42 preserved");
}

#[test]
fn an_absent_process_passes_only_the_filesystem_snapshot_check() {
    assert!(probe("cold", &[], "preserved").status.success());
    assert_eq!(probe("live", &[], "preserved").status.code(), Some(24));
}

#[test]
fn lost_files_fail_both_snapshot_modes() {
    for mode in ["cold", "live"] {
        assert_eq!(
            probe(mode, &[PROCESS, "3600"], "changed").status.code(),
            Some(21)
        );
    }
}
