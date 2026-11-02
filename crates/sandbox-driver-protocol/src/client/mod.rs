//! Host-side client: adapts a JSON-RPC plugin back into the
//! [`SandboxProvider`] / [`Sandbox`] traits.
//!
//! Control requests and responses cross the plugin's stdio; every byte
//! stream rides a data channel the plugin opens back to this host
//! ([`crate::channel`]). Each operation that moves bytes registers the
//! channel it expects before it sends the request, pumps it concurrently
//! with awaiting the response, and returns only after the channel ends —
//! so a result never arrives ahead of the output it describes.

use std::collections::HashMap;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use sandbox_driver::{
    Error, EventContext, EventSubject, ExecControls, Result, SandboxId, StopLevel, TransportError,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::runtime::Handle as RuntimeHandle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::field;

use self::exec::ExecProgress;
use crate::channel::{Channel, ChannelListener, ChannelReceiver};
use crate::wire::Message;
use crate::{TransportLimits, control, limits, methods as m};

mod exec;
mod fs;
mod provider;
mod pty;
mod sandbox;
mod streams;

pub use provider::PluginProvider;

/// How long the host waits, after a plugin has answered an operation,
/// for the data channel that operation must already have opened.
const LATE_CHANNEL_GRACE: Duration = Duration::from_secs(10);
/// The entire shutdown exchange, including acknowledgment and process exit.
#[cfg(test)]
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Request/response correlation plus notification routing.
///
/// The reader task never awaits consumer code: it only resolves pending
/// calls and forwards events. Every byte stream has a connection of its
/// own, pumped by the operation that asked for it.
struct Client {
    outbound: control::ControlSender,
    limits: TransportLimits,
    requests: Arc<Semaphore>,
    reserved: Arc<Semaphore>,
    events: mpsc::Sender<DeliveredEvent>,
    event_bytes: Arc<Semaphore>,
    event_failure: Mutex<Option<TransportError>>,
    listener: ChannelListener,
    next_id: AtomicU64,
    next_exec: AtomicU64,
    next_stream: AtomicU64,
    next_operation: AtomicU64,
    stop_requests: AtomicU64,
    stop_acknowledgments: AtomicU64,
    max_stop_acknowledgment_us: AtomicU64,
    /// Set when either transport task ends; every pending and future call
    /// fails fast instead of waiting on a dead pipe.
    closed: AtomicBool,
    /// Set once the plugin was asked to shut down, so the transport closing
    /// afterwards is the expected end of the session rather than a failure.
    shutdown_requested: AtomicBool,
    closed_signal: CancellationToken,
    cleanup: Mutex<JoinSet<()>>,
    failed_cleanup: Mutex<Vec<(Arc<OwnedSemaphorePermit>, TransportError)>>,
    closed_error: Mutex<Option<TransportError>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>,
    event_contexts: Mutex<HashMap<String, EventContext>>,
    /// Contexts for in-flight resource operations, keyed by a wire-only
    /// route id so events can arrive before a resource id exists.
    event_routes: Mutex<HashMap<String, EventContext>>,
    tasks: OnceLock<[AbortOnDropHandle<()>; 3]>,
}

struct DeliveredEvent {
    context: EventContext,
    event:   sandbox_driver::Event,
    _bytes:  OwnedSemaphorePermit,
}

struct PendingCall<'a> {
    pending: &'a Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>,
    id:      u64,
}

impl Drop for PendingCall<'_> {
    fn drop(&mut self) {
        self.pending.lock().expect("pending lock").remove(&self.id);
    }
}

impl Client {
    fn start(
        reader: impl AsyncRead + Unpin + Send + 'static,
        writer: impl AsyncWrite + Unpin + Send + 'static,
        listener: ChannelListener,
        limits: TransportLimits,
    ) -> Arc<Self> {
        let (outbound, mut outbound_rx) = control::queue(&limits);
        let (events, mut event_rx) = mpsc::channel::<DeliveredEvent>(limits.event_queue_messages);
        let progress_timeout = limits.output_progress_timeout;
        let max_message = limits.control_message_bytes;
        let listener_failed = listener.failure_signal();
        let client = Arc::new(Self {
            outbound,
            events,
            event_bytes: Arc::new(Semaphore::new(limits.event_queue_bytes)),
            event_failure: Mutex::new(None),
            requests: Arc::new(Semaphore::new(limits.provider_requests)),
            reserved: Arc::new(Semaphore::new(limits.reserved_requests)),
            limits,
            listener,
            next_id: AtomicU64::new(1),
            next_exec: AtomicU64::new(1),
            next_stream: AtomicU64::new(1),
            next_operation: AtomicU64::new(1),
            stop_requests: AtomicU64::new(0),
            stop_acknowledgments: AtomicU64::new(0),
            max_stop_acknowledgment_us: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            shutdown_requested: AtomicBool::new(false),
            closed_signal: CancellationToken::new(),
            cleanup: Mutex::new(JoinSet::new()),
            failed_cleanup: Mutex::new(Vec::new()),
            closed_error: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            event_contexts: Mutex::new(HashMap::new()),
            event_routes: Mutex::new(HashMap::new()),
            tasks: OnceLock::new(),
        });

        let writer_client = Arc::downgrade(&client);
        let writer_task = tokio::spawn(async move {
            let mut writer = writer;
            let outcome: Result<(), TransportError> = async {
                while let Some(message) = outbound_rx.recv().await {
                    time::timeout(progress_timeout, writer.write_all(&message.bytes))
                        .await
                        .map_err(|_| TransportError::new("plugin control writer made no progress"))?
                        .map_err(|error| {
                            TransportError::with_source("writing plugin request", error)
                        })?;
                }
                writer.shutdown().await.map_err(|error| {
                    TransportError::with_source("shutting down plugin writer", error)
                })?;
                Ok(())
            }
            .await;
            if let Some(client) = writer_client.upgrade() {
                client.mark_closed(
                    outcome
                        .err()
                        .unwrap_or_else(|| TransportError::new("plugin request transport closed")),
                );
            }
        });

        let reader_client = Arc::downgrade(&client);
        let reader_task = tokio::spawn(async move {
            let mut reader = BufReader::new(reader);
            let mut partial = Vec::new();
            let outcome: Result<(), TransportError> = async {
                loop {
                    let line = tokio::select! {
                        line = control::read_line(&mut reader, &mut partial, max_message) => line,
                        () = listener_failed.cancelled() => return Err(TransportError::new("data listener failed")),
                    };
                    let Some(line) = line.map_err(|error| {
                            TransportError::with_source("reading plugin response", error)
                        })?
                    else {
                        return Ok(());
                    };
                    if line.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    let message = serde_json::from_slice::<Message>(&line).map_err(|error| {
                        TransportError::with_source("decoding plugin response", error)
                    })?;
                    let Some(client) = reader_client.upgrade() else {
                        return Ok(());
                    };
                    client.route(message, line.len())?;
                }
            }
            .await;
            if let Some(client) = reader_client.upgrade() {
                client.mark_closed(
                    outcome
                        .err()
                        .unwrap_or_else(|| TransportError::new("plugin response transport closed")),
                );
            }
        });
        let event_client = Arc::downgrade(&client);
        let event_task = tokio::spawn(async move {
            while let Some(delivery) = event_rx.recv().await {
                let Some(client) = event_client.upgrade() else {
                    return;
                };
                if client
                    .event_failure
                    .lock()
                    .expect("event failure lock")
                    .is_some()
                {
                    return;
                }
                let timeout = client.limits.output_progress_timeout;
                drop(client);
                if time::timeout(timeout, delivery.context.forward(delivery.event))
                    .await
                    .is_err()
                {
                    if let Some(client) = event_client.upgrade() {
                        *client.event_failure.lock().expect("event failure lock") =
                            Some(TransportError::new(
                                "event observer timed out; event subscription is incomplete",
                            ));
                    }
                    return;
                }
            }
        });
        let _ = client.tasks.set([
            AbortOnDropHandle::new(writer_task),
            AbortOnDropHandle::new(reader_task),
            AbortOnDropHandle::new(event_task),
        ]);

        client
    }

    /// Marks the transport dead and fails everything pending. Called by
    /// both transport tasks; also re-checked by `call` after registering,
    /// closing the race where a call lands just after the drain.
    fn mark_closed(&self, error: TransportError) {
        self.closed_signal.cancel();
        let first_failure = !self.closed.swap(true, Ordering::SeqCst);
        let error = {
            let mut closed_error = self.closed_error.lock().expect("closed error lock");
            closed_error.get_or_insert(error).clone()
        };
        if first_failure {
            if self.shutdown_requested.load(Ordering::SeqCst) {
                tracing::debug!(error = ?error, "plugin transport closed after shutdown");
            } else {
                tracing::error!(error = ?error, "plugin transport failed");
            }
        }
        let pending: Vec<_> = {
            let mut pending = self.pending.lock().expect("pending lock");
            pending.drain().collect()
        };
        for (_, sender) in pending {
            let _ = sender.send(Err(Error::Transport(error.clone())));
        }
        self.event_contexts
            .lock()
            .expect("event contexts lock")
            .clear();
        self.event_routes.lock().expect("event routes lock").clear();
    }

    fn closed_error(&self) -> Error {
        let error = self
            .closed_error
            .lock()
            .expect("closed error lock")
            .clone()
            .unwrap_or_else(|| TransportError::new("plugin transport closed"));
        Error::Transport(error)
    }

    /// Sends `call` and pumps the data channel it names, together. The
    /// pump owns the channel from acceptance on, and acceptance is what
    /// proves the plugin opened the channel: a response that arrives
    /// first waits a bounded grace for it, so a result never lands ahead
    /// of the bytes it describes.
    async fn call_with_channel<R, T, Fut>(
        &self,
        method: &str,
        call: impl Future<Output = Result<R>>,
        receiver: ChannelReceiver,
        pump: impl FnOnce(Channel) -> Fut,
    ) -> Result<(R, T)>
    where
        Fut: Future<Output = Result<T>>,
    {
        let accepted = AtomicBool::new(false);
        let pump = async {
            let channel = receiver.accept().await?;
            accepted.store(true, Ordering::SeqCst);
            pump(channel).await
        };
        let responded = AtomicBool::new(false);
        let call = async {
            let result = call.await?;
            responded.store(true, Ordering::SeqCst);
            Ok(result)
        };
        let mut operation = pin!(call_with_pump(call, pump, &accepted, method));
        tokio::select! {
            biased;
            result = &mut operation => result,
            () = self.closed_signal.cancelled() => {
                // Graceful shutdown may close control after a response while
                // its final data frames still await scheduling on this side.
                if responded.load(Ordering::SeqCst) {
                    time::timeout(self.limits.hard_cancel_drain_timeout, operation).await
                        .unwrap_or_else(|_| Err(self.closed_error()))
                } else { Err(self.closed_error()) }
            }
        }
    }

    async fn cleanup_call<P: Serialize>(&self, method: &str, params: &P) -> Result<()> {
        loop {
            match self.call::<_, m::Empty>(method, params).await {
                Err(Error::Overloaded { .. }) => time::sleep(Duration::from_millis(1)).await,
                outcome => return outcome.map(|_| ()),
            }
        }
    }

    fn own_cleanup(
        self: &Arc<Self>,
        permit: Arc<OwnedSemaphorePermit>,
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        if RuntimeHandle::try_current().is_err() {
            self.failed_cleanup
                .lock()
                .expect("failed cleanup lock")
                .push((
                    permit,
                    TransportError::new("cleanup could not run without a runtime"),
                ));
            return;
        }
        let mut tasks = self.cleanup.lock().expect("cleanup tasks lock");
        while tasks.try_join_next().is_some() {}
        // Each task retains the original operation's I/O permit. The number
        // of running or failed cleanups therefore cannot exceed active_io.
        let weak = Arc::downgrade(self);
        let deadline = self.limits.shutdown_timeout;
        tasks.spawn(async move {
            let error = match time::timeout(deadline, future).await {
                Ok(Ok(())) => return,
                Ok(Err(_)) => TransportError::new("resource cleanup failed"),
                Err(_) => TransportError::new("resource cleanup timed out"),
            };
            if let Some(client) = weak.upgrade() {
                client
                    .failed_cleanup
                    .lock()
                    .expect("failed cleanup lock")
                    .push((permit, error));
            }
        });
    }

    fn next_stream_id(&self, prefix: &str) -> String {
        format!(
            "{prefix}-{}",
            self.next_stream.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn next_exec_id(&self, sandbox_id: &SandboxId) -> String {
        format!(
            "x{}-{}",
            sandbox_id.as_str(),
            self.next_exec.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Routes later events for sandbox `id` to `context`. The cache is
    /// bounded like the route cache: once it is full, an arbitrary older
    /// entry is evicted, and an event for an evicted sandbox fails the
    /// subscription explicitly rather than disappearing.
    fn remember_event_context(&self, id: &SandboxId, context: &EventContext) {
        let mut contexts = self.event_contexts.lock().expect("event contexts lock");
        if contexts.len() >= self.limits.cached_handles {
            if let Some(old) = contexts.keys().next().cloned() {
                contexts.remove(&old);
            }
        }
        contexts.insert(id.as_str().to_owned(), context.clone());
    }

    fn register_events(&self, events: Option<&EventContext>) -> Option<m::EventRequest> {
        events.map(|context| {
            let route_id = format!(
                "event-{}",
                self.next_operation.fetch_add(1, Ordering::Relaxed)
            );
            let mut routes = self.event_routes.lock().expect("event routes lock");
            // Responses can overtake asynchronous events. Keep completed routes
            // in a bounded cache; a late event for an evicted route explicitly
            // fails the event subscription instead of disappearing silently.
            if routes.len() >= self.limits.cached_handles {
                if let Some(old) = routes.keys().next().cloned() {
                    routes.remove(&old);
                }
            }
            routes.insert(route_id.clone(), context.clone());
            m::EventRequest {
                route_id,
                correlation_id: context.correlation_id_ref().cloned(),
            }
        })
    }

    fn route(&self, message: Message, bytes: usize) -> Result<(), TransportError> {
        if let Some(id) = message.id {
            let sender = self.pending.lock().expect("pending lock").remove(&id);
            if let Some(sender) = sender {
                let outcome = match (message.result, message.error) {
                    (_, Some(error)) => Err(error.into_error()),
                    (Some(result), None) => Ok(result),
                    (None, None) => Ok(Value::Null),
                };
                let _ = sender.send(outcome);
            }
            return Ok(());
        }
        let Some(method) = message.method.as_deref() else {
            return Ok(());
        };
        let params = message.params.unwrap_or(Value::Null);
        if method == "host/event_failed" {
            *self.event_failure.lock().expect("event failure lock") = Some(TransportError::new(
                "plugin event subscription exceeded its delivery capacity",
            ));
        }
        if method == m::HOST_EVENT {
            let notification =
                serde_json::from_value::<m::HostEventNotification>(params).map_err(|error| {
                    TransportError::with_source("decoding plugin host event", error)
                })?;
            // Route id first: a create event can arrive before a resource
            // id exists. Established sandbox handles fall back to the
            // resource id in the event subject.
            let resource_id = match &notification.event.subject {
                EventSubject::Sandbox { id: Some(id), .. } => Some(id.as_str()),
                _ => None,
            };
            let context = notification
                .route_id
                .as_ref()
                .and_then(|route_id| {
                    self.event_routes
                        .lock()
                        .expect("event routes lock")
                        .get(route_id)
                        .cloned()
                })
                .or_else(|| {
                    resource_id.and_then(|resource_id| {
                        self.event_contexts
                            .lock()
                            .expect("event contexts lock")
                            .get(resource_id)
                            .cloned()
                    })
                });
            if context.is_none() {
                *self.event_failure.lock().expect("event failure lock") = Some(
                    TransportError::new("event route expired; event subscription is incomplete"),
                );
            }
            if let Some(context) = context {
                let mut failure = self.event_failure.lock().expect("event failure lock");
                if failure.is_none() {
                    let permit = Arc::clone(&self.event_bytes)
                        .try_acquire_many_owned(u32::try_from(bytes).expect("bounded message"));
                    let delivered = permit.ok().is_some_and(|permit| {
                        self.events
                            .try_send(DeliveredEvent {
                                context,
                                event: notification.event,
                                _bytes: permit,
                            })
                            .is_ok()
                    });
                    if !delivered {
                        *failure = Some(TransportError::new(
                            "event delivery capacity exceeded; event subscription is incomplete",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    // The caller receives the error and decides whether it is a failure: a
    // missing file is often the answer it asked for.
    #[tracing::instrument(
        skip_all,
        fields(method = method, request_id = field::Empty),
        err(level = "debug")
    )]
    async fn call<P: Serialize, R: DeserializeOwned>(&self, method: &str, params: &P) -> Result<R> {
        let priority = m::traits(method).reserved;
        let _admission = limits::acquire(
            if priority {
                &self.reserved
            } else {
                &self.requests
            },
            if priority {
                "reserved_requests"
            } else {
                "provider_requests"
            },
        )?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        tracing::Span::current().record("request_id", id);
        let params = serde_json::to_value(params).map_err(|error| {
            Error::Transport(TransportError::with_source(
                "encoding plugin request parameters",
                error,
            ))
        })?;
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .expect("pending lock")
            .insert(id, sender);
        let _pending = PendingCall {
            pending: &self.pending,
            id,
        };
        if self.closed.load(Ordering::SeqCst) {
            // The transport may have died between the drain and our
            // registration. The guard removes this call on return.
            return Err(self.closed_error());
        }
        self.outbound
            .send(&Message::request(id, method, params), priority)?;
        let value = receiver.await.map_err(|error| {
            Error::Transport(TransportError::with_source(
                "receiving plugin response",
                error,
            ))
        })??;
        serde_json::from_value(value).map_err(|error| {
            Error::Transport(TransportError::with_source(
                "decoding plugin response result",
                error,
            ))
        })
    }

    /// Fires an `exec/stop` for each stop token as it fires: a term then
    /// a kill are two requests, in that order.
    fn forward_stops(
        self: &Arc<Self>,
        exec_id: &str,
        controls: &ExecControls,
        progress: Arc<ExecProgress>,
    ) -> Option<JoinHandle<()>> {
        (controls.term.is_some() || controls.kill.is_some()).then(|| {
            let client = Arc::clone(self);
            let exec_id = exec_id.to_owned();
            let term = controls.term.clone();
            let kill = controls.kill.clone();
            tokio::spawn(async move {
                let mut termed = std::pin::pin!(sandbox_driver::stop_signal(term.as_ref()));
                let mut killed = std::pin::pin!(sandbox_driver::stop_signal(kill.as_ref()));
                let mut term_sent = false;
                loop {
                    let level = tokio::select! {
                        () = &mut termed, if !term_sent => StopLevel::Term,
                        () = &mut killed => StopLevel::Kill,
                    };
                    if level == StopLevel::Kill {
                        progress.stop_acknowledged.store(client.send_stop(&exec_id, level).await, Ordering::SeqCst);
                        break;
                    }
                    tokio::select! {
                        _ = client.send_stop(&exec_id, level) => {},
                        () = &mut killed => {
                            progress.stop_acknowledged.store(client.send_stop(&exec_id, StopLevel::Kill).await, Ordering::SeqCst);
                            break;
                        }
                    }
                    term_sent = true;
                }
            })
        })
    }

    async fn send_stop(&self, exec_id: &str, level: StopLevel) -> bool {
        self.stop_requests.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        loop {
            let outcome: Result<m::Empty> = self
                .call(m::EXEC_STOP, &m::ExecStopParams {
                    exec_id: exec_id.to_owned(),
                    level,
                })
                .await;
            match outcome {
                Ok(_) => {
                    self.stop_acknowledgments.fetch_add(1, Ordering::Relaxed);
                    self.max_stop_acknowledgment_us.fetch_max(
                        u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    return true;
                }
                // Admitted operations already bound the number of stop tasks.
                // Retry delivery inside the local drain deadline; do not lose
                // an already-fired kill token when the reserve is briefly full.
                Err(Error::Overloaded { .. }) => time::sleep(Duration::from_millis(1)).await,
                Err(error) => {
                    tracing::warn!(error = %error, ?level, "plugin exec stop failed");
                    return false;
                }
            }
        }
    }
}

/// Polls the control call and data pump together. Both futures belong to
/// this operation, so cancellation drops the channel and its expectation.
async fn call_with_pump<R, T>(
    call: impl Future<Output = Result<R>>,
    pump: impl Future<Output = Result<T>>,
    accepted: &AtomicBool,
    method: &str,
) -> Result<(R, T)> {
    let mut call = pin!(call);
    let mut pump = pin!(pump);
    tokio::select! {
        pumped = &mut pump => {
            let pumped = pumped?;
            Ok((call.await?, pumped))
        },
        result = &mut call => {
            let result = result?;
            let pumped = if accepted.load(Ordering::SeqCst) {
                pump.await?
            } else {
                time::timeout(LATE_CHANNEL_GRACE, pump).await.map_err(|_| {
                    Error::Transport(TransportError::new(format!(
                        "the plugin answered {method} without opening its data channel"
                    )))
                })??
            };
            Ok((result, pumped))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::process::{self, Stdio};

    use sandbox_driver::{
        Capabilities, ExecResult, ExecStreamingResult, ProviderKind, SandboxFilter,
        SandboxProvider, Termination,
    };
    use tokio::io::{AsyncReadExt, duplex};
    use tokio::process::Command;
    use tokio::task::yield_now;
    use tokio::{fs as tokio_fs, time};

    use super::exec::run_channel_exec;
    use super::*;
    use crate::channel::{self, FrameKind, FrameWriter, TrustedPeer};

    #[tokio::test]
    async fn cancellation_before_open_releases_local_admission() {
        let limits = TransportLimits {
            active_io: 1,
            hard_cancel_drain_timeout: Duration::from_millis(50),
            ..TransportLimits::default()
        };
        let (reader, _peer_writer) = duplex(4096);
        let (writer, _peer_reader) = duplex(4096);
        let listener =
            ChannelListener::bind_with_limits(limits.clone(), TrustedPeer::Process(process::id()))
                .expect("listener");
        let client = Client::start(reader, writer, listener, limits);
        let (_, receiver) = client.listener.expect().expect("admission");
        let kill = CancellationToken::new();
        kill.cancel();
        let result = time::timeout(
            Duration::from_secs(1),
            run_channel_exec(
                &client,
                m::EXEC_STREAM,
                &m::Empty,
                "before-open",
                receiver,
                None,
                &ExecControls {
                    kill: Some(kill),
                    ..ExecControls::default()
                },
            ),
        )
        .await
        .expect("deadline");
        assert!(matches!(result, Err(Error::Incomplete(_))));
        assert_eq!(client.listener.diagnostics().active_io, 0);
        assert!(client.listener.expect().is_ok());
        assert!(!client.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancellation_after_result_bounds_output_and_partial_eof_delivery() {
        use tokio::io::split;
        use tokio::net::UnixStream;
        for partial_eof in [false, true] {
            let limits = TransportLimits {
                hard_cancel_drain_timeout: Duration::from_millis(100),
                ..TransportLimits::default()
            };
            let (host, peer_control) = duplex(4096);
            let (reader, writer) = split(host);
            let (peer_reader, mut peer_writer) = split(peer_control);
            let listener = ChannelListener::bind_with_limits(
                limits.clone(),
                TrustedPeer::Process(process::id()),
            )
            .expect("listener");
            let transport = listener.transport();
            let client = Client::start(reader, writer, listener, limits);
            let (request, receiver) = client.listener.expect().expect("admitted");
            let mut peer = UnixStream::connect(&transport.socket_path)
                .await
                .expect("socket");
            FrameWriter::new(&mut peer, transport.max_frame_bytes)
                .write(
                    FrameKind::Open,
                    &serde_json::to_vec(&request).expect("open"),
                )
                .await
                .expect("authenticate");
            if partial_eof {
                // A valid EOF kind with an incomplete length must never mean
                // complete output, even after a successful control response.
                peer.write_all(&[4, 0]).await.expect("partial EOF header");
            } else {
                FrameWriter::new(&mut peer, transport.max_frame_bytes)
                    .write(FrameKind::Stdout, b"pending output")
                    .await
                    .expect("output");
            }
            let kill = CancellationToken::new();
            let remote_kill = kill.clone();
            let response_task = AbortOnDropHandle::new(tokio::spawn(async move {
                let mut reader = BufReader::new(peer_reader);
                let mut partial = Vec::new();
                while let Some(line) = control::read_line(&mut reader, &mut partial, 4096)
                    .await
                    .expect("request")
                {
                    let request: Message = serde_json::from_slice(&line).expect("message");
                    let id = request.id.expect("id");
                    let response = if request.method.as_deref() == Some(m::EXEC_STREAM) {
                        let result = ExecStreamingResult::new(ExecResult::from_shell_status(
                            Termination::Exited,
                            Some(0),
                            Duration::ZERO,
                        ));
                        Message::response(
                            id,
                            serde_json::to_value(m::ExecStreamResult {
                                result:            m::ExecResultDto::from_result(&result.result),
                                live_streaming:    true,
                                streams_separated: true,
                                stdout_capture:    result.stdout_capture,
                                stderr_capture:    result.stderr_capture,
                                output_loss:       result.output_loss,
                            })
                            .expect("response"),
                        )
                    } else {
                        Message::response(id, Value::Null)
                    };
                    peer_writer
                        .write_all(
                            format!("{}\n", serde_json::to_string(&response).expect("encode"))
                                .as_bytes(),
                        )
                        .await
                        .expect("reply");
                    if request.method.as_deref() == Some(m::EXEC_STREAM) {
                        remote_kill.cancel();
                    }
                }
            }));
            let outcome = time::timeout(
                Duration::from_secs(2),
                run_channel_exec(
                    &client,
                    m::EXEC_STREAM,
                    &m::Empty,
                    "after-result",
                    receiver,
                    None,
                    &ExecControls {
                        kill: Some(kill),
                        sink: Some(Arc::new(|_, _| Box::pin(future::pending()))),
                        ..ExecControls::default()
                    },
                ),
            )
            .await
            .expect("local completion bounded");
            let Err(Error::Incomplete(outcome)) = outcome else {
                panic!("must report incomplete delivery");
            };
            assert!(outcome.output_abandoned);
            assert!(outcome.stop_acknowledged);
            assert!(outcome.termination_confirmed);
            assert!(!outcome.cleanup_confirmed);
            let _: m::Empty = client
                .call(m::PROVIDER_HEALTH, &m::Empty)
                .await
                .expect("unrelated response remains available");
            assert_eq!(client.listener.diagnostics().active_io, 0);
            drop(peer);
            drop(response_task);
        }
    }

    #[tokio::test]
    async fn cleanup_failure_retains_admission_without_closing_other_calls() {
        let limits = TransportLimits {
            active_io: 1,
            ..TransportLimits::default()
        };
        let (reader, _plugin_writer) = duplex(1024);
        let (writer, _plugin_reader) = duplex(1024);
        let listener =
            ChannelListener::bind_with_limits(limits.clone(), TrustedPeer::Process(process::id()))
                .expect("listener");
        let client = Client::start(reader, writer, listener, limits);
        let (_, receiver) = client.listener.expect().expect("admitted");
        client.own_cleanup(receiver.io_permit(), async {
            Err(Error::invalid_spec("cleanup", "failed"))
        });
        drop(receiver);
        time::timeout(Duration::from_secs(1), async {
            while client
                .failed_cleanup
                .lock()
                .expect("failed cleanup lock")
                .is_empty()
            {
                yield_now().await;
            }
        })
        .await
        .expect("cleanup failure recorded");
        assert!(!client.closed.load(Ordering::SeqCst));
        assert!(matches!(
            client.listener.expect(),
            Err(Error::Overloaded { .. })
        ));
        assert!(limits::acquire(&client.reserved, "reserved_requests").is_ok());
    }

    #[tokio::test]
    async fn hard_cancel_drain_does_not_wait_for_stop_acknowledgment() {
        let limits = TransportLimits {
            hard_cancel_drain_timeout: Duration::from_millis(50),
            ..TransportLimits::default()
        };
        let (reader, _plugin_writer) = duplex(1024);
        let (writer, _plugin_reader) = duplex(1024);
        let listener =
            ChannelListener::bind_with_limits(limits.clone(), TrustedPeer::Process(process::id()))
                .expect("listener");
        let transport = listener.transport();
        let client = Client::start(reader, writer, listener, limits);
        let (request, receiver) = client.listener.expect().expect("admitted");
        let peer = channel::open(&transport, &request)
            .await
            .expect("authenticated peer");
        let kill = CancellationToken::new();
        kill.cancel();
        let outcome = time::timeout(
            Duration::from_secs(1),
            run_channel_exec(
                &client,
                m::EXEC_STREAM,
                &m::Empty,
                "cancelled",
                receiver,
                None,
                &ExecControls {
                    kill: Some(kill),
                    ..ExecControls::default()
                },
            ),
        )
        .await
        .expect("local drain deadline");
        let Err(Error::Incomplete(outcome)) = outcome else {
            panic!("must report incomplete drain");
        };
        assert!(outcome.output_abandoned);
        assert!(!outcome.stop_acknowledged);
        assert!(!outcome.termination_confirmed);
        assert!(!outcome.cleanup_confirmed);
        assert!(!client.closed.load(Ordering::SeqCst));
        drop(peer);
    }

    #[tokio::test]
    async fn shutdown_kills_a_plugin_that_never_acknowledges() {
        let mut child = Command::new("sh")
            .args(["-c", "read request; exec sleep 300"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("unresponsive plugin");
        let reader = child.stdout.take().expect("stdout");
        let writer = child.stdin.take().expect("stdin");
        let provider = PluginProvider {
            client:       Client::start(
                reader,
                writer,
                ChannelListener::bind().expect("bind"),
                TransportLimits::default(),
            ),
            kind:         ProviderKind::try_new("host").expect("kind"),
            capabilities: Capabilities::minimal(sandbox_driver::Isolation::None),
            snapshots:    None,
            volumes:      None,
            child:        Some(Mutex::new(Some(child))),
        };
        let error = time::timeout(SHUTDOWN_GRACE + Duration::from_secs(1), provider.shutdown())
            .await
            .expect("the missing acknowledgment is bounded")
            .expect_err("the plugin never acknowledged shutdown");
        assert!(matches!(error, Error::Transport(_)), "{error:?}");
        let result = time::timeout(
            Duration::from_secs(1),
            provider.list(&SandboxFilter::default()),
        )
        .await
        .expect("the terminated plugin closes its transport");
        assert!(result.is_err());
        assert!(
            provider
                .client
                .pending
                .lock()
                .expect("pending lock")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_failed_pump_does_not_wait_for_the_control_response() {
        let accepted = AtomicBool::new(true);
        let outcome = time::timeout(
            Duration::from_secs(1),
            call_with_pump(
                future::pending::<Result<()>>(),
                async { Err::<(), _>(Error::invalid_spec("sink", "closed")) },
                &accepted,
                "test",
            ),
        )
        .await
        .expect("pump failure resolves promptly");
        assert!(matches!(outcome, Err(Error::InvalidSpec { .. })));
    }

    #[tokio::test]
    async fn cancelling_a_call_removes_its_pending_response() {
        let (reader, _plugin_writer) = duplex(1024);
        let (writer, _plugin_reader) = duplex(1024);
        let client = Client::start(
            reader,
            writer,
            ChannelListener::bind().expect("bind"),
            TransportLimits::default(),
        );
        let mut call = Box::pin(client.call::<_, m::Empty>("test", &m::Empty));
        tokio::select! {
            biased;
            result = &mut call => panic!("call must await a response: {result:?}"),
            () = yield_now() => {}
        }
        assert_eq!(client.pending.lock().expect("pending lock").len(), 1);
        drop(call);
        assert!(client.pending.lock().expect("pending lock").is_empty());
    }

    #[tokio::test]
    async fn dropping_the_client_releases_its_transport() {
        let (reader, _plugin_writer) = duplex(1024);
        let (writer, mut plugin_reader) = duplex(1024);
        let client = Client::start(
            reader,
            writer,
            ChannelListener::bind().expect("bind"),
            TransportLimits::default(),
        );
        let socket = client.listener.transport().socket_path;
        let weak = Arc::downgrade(&client);
        drop(client);
        assert!(
            weak.upgrade().is_none(),
            "transport tasks must not retain the client"
        );
        assert!(!tokio_fs::try_exists(socket).await.expect("socket lookup"));
        let mut byte = [0];
        assert_eq!(
            time::timeout(Duration::from_secs(1), plugin_reader.read(&mut byte))
                .await
                .expect("writer task closes")
                .expect("read"),
            0
        );
    }

    #[tokio::test]
    async fn abandoning_an_operation_drops_its_pump() {
        let (sender, mut receiver) = oneshot::channel::<()>();
        let accepted = AtomicBool::new(false);
        let pump = async move {
            let _sender = sender;
            future::pending::<Result<()>>().await
        };
        let mut operation = Box::pin(call_with_pump(
            future::pending::<Result<()>>(),
            pump,
            &accepted,
            "test",
        ));
        tokio::select! {
            biased;
            _ = &mut operation => panic!("operation must wait"),
            () = yield_now() => {}
        }
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(operation);
        assert!(
            receiver.await.is_err(),
            "the pump releases its owned resources"
        );
    }
}
