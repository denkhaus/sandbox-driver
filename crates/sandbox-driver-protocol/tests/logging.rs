//! Expected outcomes stay out of ERROR logs. A clean shutdown closes the
//! transport, and a missing file is an answer the caller asked for; neither
//! is a failed operation, so an application that shows ERROR events to its
//! users must not see one.

use std::fmt::{self, Write as _};
use std::mem;
use std::sync::{Arc, Mutex};

use sandbox_driver::{Error, SandboxProvider, SandboxSource, SandboxSpec};
use sandbox_driver_host::HostProvider;
use sandbox_driver_protocol::{PluginProvider, serve};
use tokio::io::{duplex, split};
use tokio::task::{self, JoinHandle};
use tracing::field::{Field, Visit};
use tracing::subscriber::{self, DefaultGuard};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt as _};
use tracing_subscriber::{Layer, Registry};

/// Every ERROR event, as its message and fields.
#[derive(Clone, Default)]
struct Errors(Arc<Mutex<Vec<String>>>);

impl<S: Subscriber> Layer<S> for Errors {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() != Level::ERROR {
            return;
        }
        let mut text = String::new();
        event.record(&mut Fields(&mut text));
        self.0.lock().expect("errors lock").push(text);
    }
}

struct Fields<'a>(&'a mut String);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        write!(self.0, "{}={value:?} ", field.name()).expect("writing to a String");
    }
}

impl Errors {
    /// Captures ERROR events on this thread. The test runtime is
    /// single-threaded, so the transport and server tasks run here too.
    fn capture(&self) -> DefaultGuard {
        subscriber::set_default(Registry::default().with(self.clone()))
    }

    fn take(&self) -> Vec<String> {
        mem::take(&mut *self.0.lock().expect("errors lock"))
    }
}

/// A Host provider served over an in-memory pipe, and the server task.
async fn connect() -> (PluginProvider, JoinHandle<sandbox_driver::Result<()>>) {
    let (host_side, plugin_side) = duplex(1024 * 1024);
    let (host_read, host_write) = split(host_side);
    let (plugin_read, plugin_write) = split(plugin_side);
    let server = tokio::spawn(serve(
        Arc::new(HostProvider::new()),
        plugin_read,
        plugin_write,
    ));
    let provider = PluginProvider::connect(host_read, host_write)
        .await
        .expect("handshake succeeds");
    (provider, server)
}

#[tokio::test]
async fn a_clean_shutdown_logs_no_error() {
    let errors = Errors::default();
    let _guard = errors.capture();
    let (provider, _server) = connect().await;
    provider.shutdown().await.expect("shutdown");
    // The close is what used to log; wait until the client has seen it.
    for _ in 0..100 {
        if provider.is_closed() {
            break;
        }
        task::yield_now().await;
    }
    assert!(provider.is_closed(), "the transport closes after shutdown");
    assert_eq!(errors.take(), Vec::<String>::new());
}

#[tokio::test]
async fn reading_a_missing_file_logs_no_error() {
    let errors = Errors::default();
    let _guard = errors.capture();
    let (provider, _server) = connect().await;
    let sandbox = provider
        .create(&SandboxSpec::new(SandboxSource::HostDirectory), None)
        .await
        .expect("create");
    let error = sandbox
        .fs()
        .read("absent.json")
        .await
        .expect_err("the file is missing");
    assert!(matches!(error, Error::NotFound { .. }), "{error:?}");
    assert_eq!(errors.take(), Vec::<String>::new());
    sandbox.delete().await.expect("delete");
    provider.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_plugin_that_disappears_still_logs_an_error() {
    let errors = Errors::default();
    let _guard = errors.capture();
    let (provider, server) = connect().await;
    server.abort();
    let _ = server.await;
    for _ in 0..100 {
        if provider.is_closed() {
            break;
        }
        task::yield_now().await;
    }
    let logged = errors.take();
    assert!(
        logged.iter().any(|e| e.contains("plugin transport failed")),
        "{logged:?}"
    );
}
