use sandbox_driver::contract::ExecSpec;
use sandbox_driver_conformance::contract::{Commands, Conformance};
use sandbox_driver_testing::contract::{FakeExec, FakeProvider};

#[tokio::test]
async fn new_contract_fake_passes_common_suite() {
    let fake = FakeProvider::new();
    fake.script(["probe"], FakeExec {
        stdout: b"workload\n".to_vec(),
        ..FakeExec::default()
    })
    .await;
    fake.script(["long"], FakeExec {
        stdout: b"ready\n".to_vec(),
        wait_for_stop: true,
        ..FakeExec::default()
    })
    .await;
    let catalog = fake.sandboxes();
    let peer = fake.sandboxes();
    Conformance::new(catalog.clone(), "default", Commands {
        probe:        ExecSpec::argv(["probe"]),
        probe_output: b"workload\n".to_vec(),
        long_running: ExecSpec::argv(["long"]),
    })
    .with_peer(peer.clone())
    .run()
    .await
    .assert_pass();
    peer.shutdown().await.expect("peer shutdown");
    catalog.shutdown().await.expect("shutdown");
}
