use crate::coordinator_metrics::OspreyCoordinatorMetrics;
use crate::priority_queue::ActionAcker;
use crate::priority_queue::{PriorityQueueReceiver, PriorityQueueSender};
use crate::proto;
use proto::action_request::ActionRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::{error::Error, io::ErrorKind};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{timeout, Duration, Instant};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

use crate::metrics::counters::StaticCounter;
use crate::metrics::histograms::StaticHistogram;

fn match_for_io_error(err_status: &tonic::Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}

#[derive(Debug)]
struct OutstandingActionState {
    action_acker: ActionAcker,
    send_time: Instant,
}

/// Per-connection state. A connection may hold up to `window` actions outstanding
/// (dispatched but not yet ack/nacked), keyed by `ack_id`. `window == 1` reproduces
/// the legacy single-flight behavior (one action out at a time).
struct ConnState {
    initialized: bool,
    window: usize,
    outstanding: HashMap<u64, OutstandingActionState>,
}

impl ConnState {
    fn new() -> ConnState {
        ConnState {
            initialized: false,
            window: 1,
            outstanding: HashMap::new(),
        }
    }

    fn has_capacity(&self) -> bool {
        self.initialized && self.outstanding.len() < self.window
    }
}

/// A `ClientDetails.max_outstanding_actions` of 0 (unset, e.g. legacy clients) or 1
/// both mean single-flight.
fn window_from_client_details(client_details: &proto::ClientDetails) -> usize {
    std::cmp::max(1, client_details.max_outstanding_actions as usize)
}

enum RequestOutcome {
    Continue,
    Disconnect,
    Error,
}

/// Apply an incoming client request to the connection state: set the window on the
/// initial request, ack/nack the referenced outstanding action by `ack_id`, or handle
/// a graceful disconnect. Sending new actions is handled by the dispatch loop, not here.
fn apply_request(
    request: proto::Request,
    state: &mut ConnState,
    metrics: &Arc<OspreyCoordinatorMetrics>,
) -> RequestOutcome {
    let inner = match request.request {
        Some(inner) => inner,
        None => {
            tracing::error!("request object missing from proto");
            return RequestOutcome::Error;
        }
    };

    match inner {
        proto::request::Request::ActionRequest(action_request) => {
            match action_request.action_request {
                Some(action_request) => apply_action_request(action_request, state, metrics),
                None => {
                    tracing::error!("no `action_request.action_request` in `ActionRequest` proto");
                    RequestOutcome::Error
                }
            }
        }
        proto::request::Request::Disconnect(disconnect) => {
            // Ack/nack the action carried in the disconnect (if any); the rest of the
            // outstanding actions get nacked when their ackers drop as this connection ends.
            if let Some(ack_or_nack) = disconnect.ack_or_nack {
                if let Some(outstanding) = state.outstanding.remove(&ack_or_nack.ack_id) {
                    if let Some(inner) = ack_or_nack.ack_or_nack {
                        outstanding.action_acker.ack_or_nack(inner);
                    }
                }
            }
            RequestOutcome::Disconnect
        }
    }
}

fn apply_action_request(
    action_request: ActionRequest,
    state: &mut ConnState,
    metrics: &Arc<OspreyCoordinatorMetrics>,
) -> RequestOutcome {
    match action_request {
        ActionRequest::Initial(client_details) => {
            if state.initialized {
                tracing::error!("got an initial action request while already initialized");
                return RequestOutcome::Error;
            }
            state.window = window_from_client_details(&client_details);
            state.initialized = true;
            RequestOutcome::Continue
        }
        ActionRequest::AckOrNack(ack_or_nack) => {
            if !state.initialized {
                tracing::error!("got an ack/nack before the initial request");
                return RequestOutcome::Error;
            }
            match state.outstanding.remove(&ack_or_nack.ack_id) {
                Some(outstanding) => {
                    metrics.bidi_acks_received.incr();
                    metrics
                        .action_outstanding_duration
                        .record(Instant::now().duration_since(outstanding.send_time));
                    match ack_or_nack.ack_or_nack {
                        Some(inner) => outstanding.action_acker.ack_or_nack(inner),
                        None => {
                            tracing::error!("no `ack_or_nack` in proto");
                            return RequestOutcome::Error;
                        }
                    }
                    RequestOutcome::Continue
                }
                None if state.outstanding.is_empty() => {
                    // Legacy invariant: an ack with nothing outstanding is a protocol error.
                    tracing::error!("got an {:?} with no outstanding actions", ack_or_nack);
                    RequestOutcome::Error
                }
                None => {
                    // Windowed only: an ack_id we don't have outstanding is most likely a
                    // late/duplicate ack for an action already redelivered on timeout. Ignore
                    // it rather than tearing down a healthy stream that still has work in flight.
                    tracing::warn!(
                        { ack_id = ack_or_nack.ack_id },
                        "got an ack/nack for an unknown ack_id, ignoring"
                    );
                    RequestOutcome::Continue
                }
            }
        }
    }
}

pub struct OspreyCoordinatorServer {
    priority_queue_receiver: PriorityQueueReceiver,
    #[allow(unused)]
    priority_queue_sender: PriorityQueueSender, // TODO: use this for retrying sync actions
    metrics: Arc<OspreyCoordinatorMetrics>,
}

impl OspreyCoordinatorServer {
    pub fn new(
        priority_queue_sender: PriorityQueueSender,
        priority_queue_receiver: PriorityQueueReceiver,
        metrics: Arc<OspreyCoordinatorMetrics>,
    ) -> OspreyCoordinatorServer {
        OspreyCoordinatorServer {
            priority_queue_sender,
            priority_queue_receiver,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl proto::osprey_coordinator_service_server::OspreyCoordinatorService
    for OspreyCoordinatorServer
{
    type OspreyBidirectionalStreamStream =
        ReceiverStream<Result<proto::OspreyCoordinatorAction, tonic::Status>>;

    async fn osprey_bidirectional_stream(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Request>>,
    ) -> Result<tonic::Response<Self::OspreyBidirectionalStreamStream>, tonic::Status> {
        tracing::debug!(
            { connection =? request.metadata() },
            "New Connection Received"
        );
        let mut in_stream = request.into_inner();
        self.metrics.new_connection_established.incr();
        let (tx, rx) = mpsc::channel(128);
        let action_receiver = self.priority_queue_receiver.clone();
        let metrics = self.metrics.clone();
        let max_pq_receive_await_time_ms = Duration::from_millis(
            std::env::var("MAX_PQ_RECEIVE_AWAIT_TIME_MS")
                .unwrap_or("5000".to_string())
                .parse::<u64>()
                .unwrap(),
        );
        tokio::spawn(async move {
            let mut state = ConnState::new();

            loop {
                tokio::select! {
                    biased;

                    // Handle acks/disconnects first so freed capacity is reflected before
                    // we decide whether to pull more work.
                    maybe_request = in_stream.next() => {
                        match maybe_request {
                            None => {
                                tracing::debug!("client closed request stream");
                                break;
                            }
                            Some(Ok(request)) => {
                                tracing::debug!({ request =? request }, "got request");
                                match apply_request(request, &mut state, &metrics) {
                                    RequestOutcome::Continue => {}
                                    RequestOutcome::Disconnect => {
                                        tracing::debug!("client requested a disconnect");
                                        metrics.client_disconnected_gracefully.incr();
                                        break;
                                    }
                                    RequestOutcome::Error => {
                                        metrics.client_disconnected_stream_error.incr();
                                        break;
                                    }
                                }
                            }
                            Some(Err(err)) => {
                                if let Some(io_err) = match_for_io_error(&err) {
                                    if io_err.kind() == ErrorKind::BrokenPipe {
                                        tracing::error!("client disconnected: broken pipe");
                                        metrics.client_disconnected_broken_pipe.incr();
                                        break;
                                    }
                                }
                                match tx.send(Err(err)).await {
                                    Ok(_) => (),
                                    Err(_err) => break, // response was dropped
                                }
                            }
                        }
                    }

                    // Pull and dispatch the next action whenever the window has room.
                    // Disabled (guard false) while at capacity, so backpressure is exact.
                    result = timeout(max_pq_receive_await_time_ms, action_receiver.recv(metrics.clone())), if state.has_capacity() => {
                        match result {
                            Ok(Ok(ackable_action)) => {
                                let (action, action_acker) = ackable_action.into_action();
                                let ack_id = action.ack_id;
                                if tx.send(Ok(action)).await.is_err() {
                                    break; // response stream dropped
                                }
                                metrics.bidi_actions_sent.incr();
                                state.outstanding.insert(
                                    ack_id,
                                    OutstandingActionState {
                                        action_acker,
                                        send_time: Instant::now(),
                                    },
                                );
                            }
                            Ok(Err(_)) => {
                                tracing::debug!("disconnecting client because receiver closed");
                                metrics.client_disconnected_receiver_closed.incr();
                                break;
                            }
                            Err(_timed_out) => {
                                if state.window <= 1 {
                                    // Legacy: a single-flight client idle-waiting past the
                                    // timeout is disconnected so the coordinator can rebalance it.
                                    tracing::debug!("disconnecting client because receiver timed out");
                                    metrics.client_disconnected_receiver_timeout.incr();
                                    break;
                                }
                                // Windowed: an empty queue is not a reason to tear down a stream
                                // that may still have actions in flight. Keep waiting.
                            }
                        }
                    }
                }
            }

            // Any actions still outstanding have their ackers dropped here, which closes
            // their oneshots and nacks the underlying messages for redelivery.
            tracing::debug!("stream ended");
        });

        let out_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(out_stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator_metrics::OspreyCoordinatorMetrics;
    use crate::metrics::emit_worker::SpawnEmitWorker;
    use crate::metrics::new_client;
    use crate::priority_queue::create_ackable_action_priority_queue;
    use crate::priority_queue::AckableAction;
    use crate::proto::osprey_coordinator_action::ActionData;
    use crate::proto::osprey_coordinator_service_client::OspreyCoordinatorServiceClient;
    use proto::osprey_coordinator_service_server::OspreyCoordinatorServiceServer;

    fn test_metrics() -> Arc<OspreyCoordinatorMetrics> {
        OspreyCoordinatorMetrics::new()
    }

    fn initial_request(max_outstanding_actions: u32) -> proto::Request {
        proto::Request {
            request: Some(proto::request::Request::ActionRequest(proto::ActionRequest {
                action_request: Some(proto::action_request::ActionRequest::Initial(
                    proto::ClientDetails {
                        id: "test".into(),
                        max_outstanding_actions,
                    },
                )),
            })),
        }
    }

    fn ack_request(ack_id: u64) -> proto::Request {
        proto::Request {
            request: Some(proto::request::Request::ActionRequest(proto::ActionRequest {
                action_request: Some(proto::action_request::ActionRequest::AckOrNack(
                    proto::AckOrNack {
                        ack_id,
                        ack_or_nack: Some(proto::ack_or_nack::AckOrNack::Ack(proto::Ack {
                            execution_result: None,
                            verdicts: None,
                        })),
                    },
                )),
            })),
        }
    }

    fn make_outstanding() -> (OutstandingActionState, tokio::sync::oneshot::Receiver<crate::priority_queue::AckOrNack>) {
        let action = proto::OspreyCoordinatorAction {
            ack_id: 0,
            action_id: 0,
            action_name: "test_action".into(),
            timestamp: None,
            action_data: Some(ActionData::JsonActionData("{}".into())),
            secret_data: None,
        };
        let (ackable, recv) = AckableAction::new(action);
        let (_action, acker) = ackable.into_action();
        (
            OutstandingActionState {
                action_acker: acker,
                send_time: Instant::now(),
            },
            recv,
        )
    }

    // ---- pure state-machine (apply_request) tests ----

    #[test]
    fn initial_sets_window_and_marks_initialized() {
        let metrics = test_metrics();
        let mut state = ConnState::new();
        assert!(!state.initialized);
        assert!(matches!(
            apply_request(initial_request(8), &mut state, &metrics),
            RequestOutcome::Continue
        ));
        assert!(state.initialized);
        assert_eq!(state.window, 8);
    }

    #[test]
    fn initial_window_zero_is_single_flight() {
        let metrics = test_metrics();
        let mut state = ConnState::new();
        apply_request(initial_request(0), &mut state, &metrics);
        assert_eq!(state.window, 1);
    }

    #[test]
    fn second_initial_is_an_error() {
        let metrics = test_metrics();
        let mut state = ConnState::new();
        apply_request(initial_request(4), &mut state, &metrics);
        assert!(matches!(
            apply_request(initial_request(4), &mut state, &metrics),
            RequestOutcome::Error
        ));
    }

    #[test]
    fn ack_removes_the_matching_outstanding_action_by_id() {
        let metrics = test_metrics();
        let mut state = ConnState::new();
        apply_request(initial_request(4), &mut state, &metrics);

        let (o1, r1) = make_outstanding();
        let (o2, r2) = make_outstanding();
        state.outstanding.insert(1, o1);
        state.outstanding.insert(2, o2);

        // Ack id 2 (out of insertion order); only id 2 should be acked, id 1 remains.
        assert!(matches!(
            apply_request(ack_request(2), &mut state, &metrics),
            RequestOutcome::Continue
        ));
        assert!(state.outstanding.contains_key(&1));
        assert!(!state.outstanding.contains_key(&2));
        // The acked action's oneshot resolved to an Ack; the other is still pending.
        assert!(r2.blocking_recv().is_ok());
        drop(r1);
    }

    #[test]
    fn ack_with_nothing_outstanding_is_an_error() {
        let metrics = test_metrics();
        let mut state = ConnState::new();
        apply_request(initial_request(4), &mut state, &metrics);
        assert!(matches!(
            apply_request(ack_request(1), &mut state, &metrics),
            RequestOutcome::Error
        ));
    }

    #[test]
    fn ack_before_initial_is_an_error() {
        let metrics = test_metrics();
        let mut state = ConnState::new();
        assert!(matches!(
            apply_request(ack_request(1), &mut state, &metrics),
            RequestOutcome::Error
        ));
    }

    #[test]
    fn unknown_ack_id_while_busy_is_ignored() {
        let metrics = test_metrics();
        let mut state = ConnState::new();
        apply_request(initial_request(4), &mut state, &metrics);
        let (o1, r1) = make_outstanding();
        state.outstanding.insert(1, o1);
        // Ack an id we don't hold, but we still have id 1 in flight: ignore, don't error.
        assert!(matches!(
            apply_request(ack_request(999), &mut state, &metrics),
            RequestOutcome::Continue
        ));
        assert!(state.outstanding.contains_key(&1));
        drop(r1);
    }

    // ---- integration: real server + client over an in-process stream ----

    async fn start_test_server(
        receiver: PriorityQueueReceiver,
        sender: PriorityQueueSender,
        metrics: Arc<OspreyCoordinatorMetrics>,
    ) -> (String, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = OspreyCoordinatorServer::new(sender, receiver, metrics);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(OspreyCoordinatorServiceServer::new(server))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .unwrap();
        });
        (format!("http://{}", addr), shutdown_tx)
    }

    async fn queue_actions(sender: &PriorityQueueSender, ack_ids: &[u64]) {
        for &ack_id in ack_ids {
            let action = proto::OspreyCoordinatorAction {
                ack_id,
                action_id: ack_id,
                action_name: "test_action".into(),
                timestamp: None,
                action_data: Some(ActionData::JsonActionData("{}".into())),
                secret_data: None,
            };
            let (ackable, _recv) = AckableAction::new(action);
            // Leak the ack receiver so the queue's is_closed() skip does not drop the action.
            std::mem::forget(_recv);
            sender.send_sync(ackable).await.unwrap();
        }
    }

    #[tokio::test]
    async fn windowed_stream_pushes_up_to_window_without_acks() {
        let (sender, receiver) = create_ackable_action_priority_queue();
        let metrics = test_metrics();
        let _guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator").unwrap());
        queue_actions(&sender, &[1, 2, 3, 4, 5]).await;

        let (addr, _shutdown) = start_test_server(receiver, sender.clone(), metrics).await;
        let mut client = OspreyCoordinatorServiceClient::connect(addr).await.unwrap();

        // Keep the request stream open: send only the Initial(window=3), never end it.
        let (req_tx, req_rx) = mpsc::channel::<proto::Request>(8);
        req_tx.send(initial_request(3)).await.unwrap();
        let response = client
            .osprey_bidirectional_stream(ReceiverStream::new(req_rx))
            .await
            .unwrap();
        let mut inbound = response.into_inner();

        // Exactly 3 actions should arrive proactively (window=3), without any ack.
        let mut received = Vec::new();
        for _ in 0..3 {
            let action = tokio::time::timeout(Duration::from_secs(5), inbound.next())
                .await
                .expect("timed out waiting for action")
                .expect("stream ended")
                .expect("stream error");
            received.push(action.ack_id);
        }
        received.sort();
        assert_eq!(received, vec![1, 2, 3]);

        // A 4th must NOT arrive until we free a slot.
        assert!(
            tokio::time::timeout(Duration::from_millis(300), inbound.next())
                .await
                .is_err(),
            "a 4th action arrived before any ack — window not enforced"
        );

        // Ack one -> exactly one more slot frees -> a 4th action arrives.
        req_tx.send(ack_request(received[0])).await.unwrap();
        let fourth = tokio::time::timeout(Duration::from_secs(5), inbound.next())
            .await
            .expect("timed out waiting for 4th action")
            .expect("stream ended")
            .expect("stream error");
        assert!([4u64, 5u64].contains(&fourth.ack_id));

        drop(req_tx);
    }

    #[tokio::test]
    async fn single_flight_client_gets_one_action_at_a_time() {
        let (sender, receiver) = create_ackable_action_priority_queue();
        let metrics = test_metrics();
        let _guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator").unwrap());
        queue_actions(&sender, &[10, 11]).await;

        let (addr, _shutdown) = start_test_server(receiver, sender.clone(), metrics).await;
        let mut client = OspreyCoordinatorServiceClient::connect(addr).await.unwrap();

        let (req_tx, req_rx) = mpsc::channel::<proto::Request>(8);
        req_tx.send(initial_request(1)).await.unwrap(); // window 1 = legacy
        let response = client
            .osprey_bidirectional_stream(ReceiverStream::new(req_rx))
            .await
            .unwrap();
        let mut inbound = response.into_inner();

        // First action arrives.
        let first = tokio::time::timeout(Duration::from_secs(5), inbound.next())
            .await
            .expect("timed out")
            .unwrap()
            .unwrap();
        assert_eq!(first.ack_id, 10);

        // No second action until the first is acked (single-flight).
        assert!(
            tokio::time::timeout(Duration::from_millis(300), inbound.next())
                .await
                .is_err(),
            "second action arrived before acking the first"
        );

        req_tx.send(ack_request(10)).await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), inbound.next())
            .await
            .expect("timed out")
            .unwrap()
            .unwrap();
        assert_eq!(second.ack_id, 11);

        drop(req_tx);
    }
}
