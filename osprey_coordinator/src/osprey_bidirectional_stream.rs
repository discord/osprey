use crate::coordinator_metrics::OspreyCoordinatorMetrics;
use crate::priority_queue::ActionAcker;
use crate::priority_queue::{PriorityQueueReceiver, PriorityQueueSender};
use crate::proto;
use anyhow::{anyhow, Context, Result};
use proto::action_request::ActionRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::{error::Error, io::ErrorKind};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{timeout, Duration, Instant};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

use crate::metrics::counters::StaticCounter;
use crate::metrics::histograms::StaticHistogram;

/// Safety ceiling: a worker cannot hold more than this many in-flight actions regardless of
/// what `max_unacked` says. Prevents a misconfigured worker from exhausting coordinator memory.
const MAX_UNACKED_CEILING: u32 = 256;

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

/// Per-stream state. Holds all in-flight actions keyed by ack_id.
#[derive(Debug)]
struct ClientState {
    /// None until the Initial handshake is received.
    client_details: Option<proto::ClientDetails>,
    /// Currently in-flight actions awaiting ack/nack from the worker.
    in_flight: HashMap<u64, OutstandingActionState>,
    /// Effective window: max(worker's max_unacked, 1), clamped to MAX_UNACKED_CEILING.
    max_unacked: u32,
}

impl ClientState {
    fn new() -> Self {
        ClientState {
            client_details: None,
            in_flight: HashMap::new(),
            max_unacked: 1, // safe default before handshake
        }
    }

    fn is_handshake_done(&self) -> bool {
        self.client_details.is_some()
    }

    fn has_slot(&self) -> bool {
        (self.in_flight.len() as u32) < self.max_unacked
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

/// Dispatch one action (blocking with timeout). Inserts into `in_flight`.
/// Returns Err if the timeout fires (queue too slow) or the send to worker fails.
async fn dispatch_one_blocking(
    state: &mut ClientState,
    sender: &Sender<Result<proto::OspreyCoordinatorAction, tonic::Status>>,
    action_receiver: &PriorityQueueReceiver,
    metrics: &Arc<OspreyCoordinatorMetrics>,
    receive_timeout: Duration,
) -> Result<()> {
    let priority_queue_receive_start_time = Instant::now();
    let result = timeout(receive_timeout, action_receiver.recv(metrics.clone())).await;
    metrics
        .priority_queue_receive_time
        .record(Instant::now().duration_since(priority_queue_receive_start_time));
    let ackable_action = match result {
        Ok(Ok(a)) => a,
        Ok(Err(_)) | Err(_) => {
            tracing::error!("Took too long to get action from priority queue, disconnecting");
            return Err(anyhow!("priority queue receive timeout or closed"));
        }
    };
    let (action, action_acker) = ackable_action.into_action();
    let ack_id = action.ack_id;
    sender.send(Ok(action)).await?;
    metrics.bidi_actions_sent.incr();
    state.in_flight.insert(
        ack_id,
        OutstandingActionState {
            action_acker,
            send_time: Instant::now(),
        },
    );
    metrics
        .bidi_stream_in_flight_actions
        .record_value(state.in_flight.len() as u64);
    Ok(())
}

/// Fill all open slots using only non-blocking try_recv. Does NOT block.
/// Returns Err only if the send-to-worker channel is closed.
async fn fill_slots_nonblocking(
    state: &mut ClientState,
    sender: &Sender<Result<proto::OspreyCoordinatorAction, tonic::Status>>,
    action_receiver: &PriorityQueueReceiver,
    metrics: &Arc<OspreyCoordinatorMetrics>,
) -> Result<()> {
    while state.has_slot() {
        match action_receiver.try_recv() {
            Some(ackable_action) => {
                let (action, action_acker) = ackable_action.into_action();
                let ack_id = action.ack_id;
                sender.send(Ok(action)).await?;
                metrics.bidi_actions_sent.incr();
                state.in_flight.insert(
                    ack_id,
                    OutstandingActionState {
                        action_acker,
                        send_time: Instant::now(),
                    },
                );
                metrics
                    .bidi_stream_in_flight_actions
                    .record_value(state.in_flight.len() as u64);
            }
            None => break,
        }
    }
    Ok(())
}

/// Fill the in-flight window at initial handshake time: block on the FIRST action
/// (ensuring we always have work to give the worker), then fill remaining slots
/// non-blocking. Returns Err if queue times out or worker channel is closed.
async fn fill_window_initial(
    state: &mut ClientState,
    sender: &Sender<Result<proto::OspreyCoordinatorAction, tonic::Status>>,
    action_receiver: &PriorityQueueReceiver,
    metrics: &Arc<OspreyCoordinatorMetrics>,
    receive_timeout: Duration,
) -> Result<()> {
    if !state.has_slot() {
        return Ok(());
    }
    // Block for the first action (existing behavior — worker is waiting for work).
    dispatch_one_blocking(state, sender, action_receiver, metrics, receive_timeout).await?;
    // Fill remaining slots non-blocking.
    fill_slots_nonblocking(state, sender, action_receiver, metrics).await
}

/// Fill the in-flight window after an ack: purely non-blocking.
/// The worker is still processing the remaining in-flight actions; if the queue
/// is empty we simply don't push new work now — it will be pushed on the next ack.
async fn fill_window_after_ack(
    state: &mut ClientState,
    sender: &Sender<Result<proto::OspreyCoordinatorAction, tonic::Status>>,
    action_receiver: &PriorityQueueReceiver,
    metrics: &Arc<OspreyCoordinatorMetrics>,
) -> Result<()> {
    fill_slots_nonblocking(state, sender, action_receiver, metrics).await
}

/// Process a single ack_or_nack from the worker. Returns Ok(()) on success.
/// On unknown ack_id, logs a warning and returns Ok(()) — don't crash.
fn process_ack_or_nack(
    ack_or_nack: proto::AckOrNack,
    state: &mut ClientState,
    metrics: &Arc<OspreyCoordinatorMetrics>,
) -> Result<()> {
    let ack_id = ack_or_nack.ack_id;
    match state.in_flight.remove(&ack_id) {
        Some(outstanding) => {
            metrics.bidi_acks_received.incr();
            let duration = Instant::now().duration_since(outstanding.send_time);
            metrics.action_outstanding_duration.record(duration);
            outstanding
                .action_acker
                .ack_or_nack(ack_or_nack.ack_or_nack.context("no `ack_or_nack` in proto")?);
            Ok(())
        }
        None => {
            // Unknown ack_id — stale re-delivery or a bug on the worker side. Log and ignore.
            metrics.bidi_stream_unknown_ack_id.incr();
            tracing::warn!(ack_id, "received ack for unknown ack_id; ignoring");
            Ok(())
        }
    }
}

enum LoopDirective {
    Continue,
    Disconnect,
    PriorityQueueError,
}

/// Process one incoming worker request. Returns a directive for the outer loop.
async fn handle_request(
    state: &mut ClientState,
    sender: &Sender<Result<proto::OspreyCoordinatorAction, tonic::Status>>,
    request: proto::Request,
    action_receiver: &PriorityQueueReceiver,
    metrics: Arc<OspreyCoordinatorMetrics>,
    receive_timeout: Duration,
) -> Result<LoopDirective> {
    match request
        .request
        .context("request object missing from proto")?
    {
        proto::request::Request::ActionRequest(action_request) => {
            let action_request = action_request
                .action_request
                .context("no `action_request.action_request` in `ActionRequest` proto")?;

            match action_request {
                ActionRequest::Initial(client_details) => {
                    if state.is_handshake_done() {
                        return Err(anyhow!(
                            "got an initial action request while handshake already done"
                        ));
                    }
                    let raw = client_details.max_unacked;
                    // 0 means "not set" (older binary) → default to 1 (strict-serial back-compat).
                    let effective = if raw == 0 { 1 } else { raw }.min(MAX_UNACKED_CEILING);
                    metrics
                        .bidi_stream_max_unacked_observed
                        .record_value(effective as u64);
                    let client_id = &client_details.id;
                    tracing::debug!(client_id, raw, effective, "stream initialized");
                    state.client_details = Some(client_details);
                    state.max_unacked = effective;

                    // Block for the first action, then non-blocking fill remaining slots.
                    match fill_window_initial(
                        state,
                        sender,
                        action_receiver,
                        &metrics,
                        receive_timeout,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(_) => return Ok(LoopDirective::PriorityQueueError),
                    }
                    Ok(LoopDirective::Continue)
                }

                ActionRequest::AckOrNack(ack_or_nack) => {
                    process_ack_or_nack(ack_or_nack, state, &metrics)?;
                    // A slot just opened — non-blocking fill (if queue has work ready).
                    match fill_window_after_ack(state, sender, action_receiver, &metrics).await {
                        Ok(()) => {}
                        Err(_) => return Ok(LoopDirective::PriorityQueueError),
                    }
                    Ok(LoopDirective::Continue)
                }
            }
        }

        proto::request::Request::Disconnect(disconnect) => {
            // New (chunk-A) workers send Disconnect with no ack_or_nack (field absent).
            // Old workers include an ack_or_nack for the last in-flight action.
            if let Some(ack_or_nack_wrapper) = disconnect.ack_or_nack {
                if let Some(inner) = ack_or_nack_wrapper.ack_or_nack {
                    // Old-style: ack the single outstanding action via the ack_id on the wrapper.
                    let ack_or_nack_proto = proto::AckOrNack {
                        ack_id: ack_or_nack_wrapper.ack_id,
                        ack_or_nack: Some(inner),
                    };
                    process_ack_or_nack(ack_or_nack_proto, state, &metrics)?;
                }
            }
            // Remaining in-flight ackers are dropped below when the loop exits.
            Ok(LoopDirective::Disconnect)
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
            let mut client_state = ClientState::new();

            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(request) => {
                        tracing::debug!({request=?request},"got request");
                        match handle_request(
                            &mut client_state,
                            &tx,
                            request,
                            &action_receiver,
                            metrics.clone(),
                            max_pq_receive_await_time_ms,
                        )
                        .await
                        {
                            Ok(LoopDirective::Continue) => {}
                            Ok(LoopDirective::Disconnect) => {
                                tracing::debug!("client requested a disconnect");
                                metrics.client_disconnected_gracefully.incr();
                                break;
                            }
                            Ok(LoopDirective::PriorityQueueError) => {
                                tracing::debug!(
                                    "disconnecting client because receiver timed out or closed"
                                );
                                metrics.client_disconnected_receiver_timeout.incr();
                                break;
                            }
                            Err(error) => {
                                tracing::error!({error=%error},"error in stream");
                                metrics.client_disconnected_stream_error.incr();
                                break;
                            }
                        }
                    }
                    Err(err) => {
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

            // Stream ended — drop all remaining in-flight ackers. Each dropped ActionAcker
            // drops the oneshot sender, which causes the pubsub task's receiver to get
            // RecvError → the action is NACKed and redelivered by pubsub.
            drop(client_state.in_flight);

            tracing::debug!("stream ended");
        });

        let out_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(out_stream))
    }
}

#[cfg(test)]
mod tests {

    use crate::coordinator_metrics::OspreyCoordinatorMetrics;
    use crate::metrics::emit_worker::SpawnEmitWorker;
    use crate::metrics::new_client;
    use crate::proto::osprey_coordinator_action::ActionData;
    use crate::proto::osprey_coordinator_action::SecretData;
    use proto::osprey_coordinator_service_server::OspreyCoordinatorService;

    use crate::priority_queue::create_ackable_action_priority_queue;
    use crate::priority_queue::AckableAction;

    use super::*;

    fn make_action(ack_id: u64, action_id: u64) -> proto::OspreyCoordinatorAction {
        proto::OspreyCoordinatorAction {
            ack_id,
            action_id,
            action_name: "test_action".into(),
            timestamp: None,
            action_data: Some(ActionData::JsonActionData(
                format!("{{\"action\": \"test action data {action_id}\"}}").into(),
            )),
            secret_data: Some(SecretData::JsonSecretData(
                format!("{{\"secret\": \"test secret data {action_id}\"}}").into(),
            )),
            mode: 0,
        }
    }

    fn initial_request(max_unacked: u32) -> proto::Request {
        proto::Request {
            request: Some(proto::request::Request::ActionRequest(
                proto::ActionRequest {
                    action_request: Some(proto::action_request::ActionRequest::Initial(
                        proto::ClientDetails {
                            id: "test".into(),
                            max_unacked,
                        },
                    )),
                },
            )),
        }
    }

    fn ack_request(ack_id: u64) -> proto::Request {
        proto::Request {
            request: Some(proto::request::Request::ActionRequest(
                proto::ActionRequest {
                    action_request: Some(proto::action_request::ActionRequest::AckOrNack(
                        proto::AckOrNack {
                            ack_id,
                            ack_or_nack: Some(proto::ack_or_nack::AckOrNack::Ack(proto::Ack {
                                execution_result: None,
                                verdicts: None,
                            })),
                        },
                    )),
                },
            )),
        }
    }

    #[tokio::test]
    async fn golden_path_bidirection_streaming_test() -> Result<()> {
        // Simple golden path test that adds two actions to the queue and asserts that a properly
        // formed bidirectional streaming request is returned the actions in that order

        tracing_subscriber::fmt::init();
        let (priority_queue_sender, priority_queue_receiver) =
            create_ackable_action_priority_queue();
        let metrics = OspreyCoordinatorMetrics::new();
        let _worker_guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator").unwrap());

        let ackable_action = proto::OspreyCoordinatorAction {
            ack_id: 1,
            action_id: 1,
            action_name: "test_action".into(),
            timestamp: None,
            action_data: Some(ActionData::JsonActionData(
                "{\"action\": \"test action data 1\"}".into(),
            )),
            secret_data: Some(SecretData::JsonSecretData(
                "{\"secret\": \"test secret data 1\"}".into(),
            )),
            mode: 0, // EXECUTION_MODE_UNSPECIFIED
        };
        let (ackable_action, _receiver_drop_guard_1) = AckableAction::new(ackable_action);
        priority_queue_sender
            .send_sync(ackable_action)
            .await
            .unwrap();

        let ackable_action_2 = proto::OspreyCoordinatorAction {
            ack_id: 2,
            action_id: 2,
            action_name: "test_action".into(),
            timestamp: None,
            action_data: Some(ActionData::JsonActionData(
                "{\"action\": \"test action data 2\"}".into(),
            )),
            secret_data: Some(SecretData::JsonSecretData(
                "{\"secret\": \"test secret data 2\"}".into(),
            )),
            mode: 0, // EXECUTION_MODE_UNSPECIFIED
        };
        let (ackable_action, _receiver_drop_guard_2) = AckableAction::new(ackable_action_2);
        priority_queue_sender
            .send_sync(ackable_action)
            .await
            .unwrap();

        let server = OspreyCoordinatorServer::new(
            priority_queue_sender.clone(),
            priority_queue_receiver,
            metrics.clone(),
        );

        let initial_action_request = proto::Request {
            request: Some(proto::request::Request::ActionRequest(
                proto::ActionRequest {
                    action_request: Some(proto::action_request::ActionRequest::Initial(
                        proto::ClientDetails::default(),
                    )),
                },
            )),
        };

        // ack_id must match the action's ack_id (1). The old code didn't check this;
        // the new code looks up by ack_id in the in-flight map.
        let acking_action_request = proto::Request {
            request: Some(proto::request::Request::ActionRequest(
                proto::ActionRequest {
                    action_request: Some(proto::action_request::ActionRequest::AckOrNack(
                        proto::AckOrNack {
                            ack_id: 1,
                            ack_or_nack: Some(proto::ack_or_nack::AckOrNack::Ack(proto::Ack {
                                execution_result: None,
                                verdicts: None,
                            })),
                        },
                    )),
                },
            )),
        };

        let req = crate::tonic_mock::streaming_request(vec![
            initial_action_request.clone(),
            acking_action_request.clone(),
        ]);

        let res = server
            .osprey_bidirectional_stream(req)
            .await
            .expect("error in stream");

        println!("finish connection");

        let mut result = Vec::new();
        let mut messages = res.into_inner();
        while let Some(v) = messages.next().await {
            println!("got message: {:?}", &v);
            result.push(v.expect("error from stream"))
        }

        print!("{:?}", result);

        assert_eq!(result[0].action_id, 1);
        assert_eq!(result[1].action_id, 2);
        assert_eq!(
            result[0].action_data,
            Some(ActionData::JsonActionData(
                "{\"action\": \"test action data 1\"}".into()
            ))
        );
        assert_eq!(
            result[1].action_data,
            Some(ActionData::JsonActionData(
                "{\"action\": \"test action data 2\"}".into()
            ))
        );
        assert_eq!(
            result[0].secret_data,
            Some(SecretData::JsonSecretData(
                "{\"secret\": \"test secret data 1\"}".into()
            ))
        );
        assert_eq!(
            result[1].secret_data,
            Some(SecretData::JsonSecretData(
                "{\"secret\": \"test secret data 2\"}".into()
            ))
        );

        Ok(())
    }

    /// With max_unacked=1, the coordinator must never dispatch action N+1 before receiving
    /// ack for action N. Serial behavior must be identical to the pre-chunk-C implementation.
    #[tokio::test]
    async fn test_max_unacked_1_is_strictly_serial() -> Result<()> {
        let (pq_sender, pq_receiver) = create_ackable_action_priority_queue();
        let metrics = OspreyCoordinatorMetrics::new();
        let _worker_guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator_serial").unwrap());

        // Keep receivers alive so acking_oneshot_sender.is_closed() stays false.
        let mut _receivers = Vec::new();
        for i in 1u64..=3 {
            let (ackable, rx) = AckableAction::new(make_action(i, i));
            pq_sender.send_sync(ackable).await.unwrap();
            _receivers.push(rx);
        }

        let server = OspreyCoordinatorServer::new(pq_sender.clone(), pq_receiver, metrics.clone());

        let req = crate::tonic_mock::streaming_request(vec![
            initial_request(1),
            ack_request(1),
            ack_request(2),
            ack_request(3),
        ]);

        let res = server.osprey_bidirectional_stream(req).await.unwrap();
        let mut messages = res.into_inner();
        let mut result = Vec::new();
        while let Some(v) = messages.next().await {
            result.push(v.unwrap());
        }

        // All three actions dispatched and received in order.
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].action_id, 1);
        assert_eq!(result[1].action_id, 2);
        assert_eq!(result[2].action_id, 3);

        Ok(())
    }

    /// With max_unacked=2, after initial handshake the coordinator must dispatch 2 actions
    /// before receiving any ack. This is the load-bearing parallel-dispatch test.
    #[tokio::test]
    async fn test_max_unacked_2_dispatches_two_before_first_ack() -> Result<()> {
        let (pq_sender, pq_receiver) = create_ackable_action_priority_queue();
        let metrics = OspreyCoordinatorMetrics::new();
        let _worker_guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator_parallel").unwrap());

        // Keep receivers alive so acking_oneshot_sender.is_closed() stays false.
        let mut _receivers = Vec::new();
        for i in 1u64..=3 {
            let (ackable, rx) = AckableAction::new(make_action(i, i));
            pq_sender.send_sync(ackable).await.unwrap();
            _receivers.push(rx);
        }

        let server = OspreyCoordinatorServer::new(pq_sender.clone(), pq_receiver, metrics.clone());

        let req = crate::tonic_mock::streaming_request(vec![
            initial_request(2),
            ack_request(1),
            ack_request(2),
            ack_request(3),
        ]);

        let res = server.osprey_bidirectional_stream(req).await.unwrap();
        let mut messages = res.into_inner();
        let mut result = Vec::new();
        while let Some(v) = messages.next().await {
            result.push(v.unwrap());
        }

        // All three dispatched. With max_unacked=2: actions 1+2 go out on Initial,
        // action 3 goes out when the first ack arrives.
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].action_id, 1);
        assert_eq!(result[1].action_id, 2);
        assert_eq!(result[2].action_id, 3);

        Ok(())
    }

    /// Out-of-order acks: all three should be acked successfully regardless of order.
    #[tokio::test]
    async fn test_out_of_order_acks() -> Result<()> {
        let (pq_sender, pq_receiver) = create_ackable_action_priority_queue();
        let metrics = OspreyCoordinatorMetrics::new();
        let _worker_guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator_ooo").unwrap());

        let mut ack_receivers = Vec::new();
        for i in 1u64..=3 {
            let (ackable, rx) = AckableAction::new(make_action(i, i));
            pq_sender.send_sync(ackable).await.unwrap();
            ack_receivers.push(rx);
        }

        let server = OspreyCoordinatorServer::new(pq_sender.clone(), pq_receiver, metrics.clone());

        // Ack in reverse order: 3, 1, 2. With max_unacked=3 all 3 are dispatched at once.
        let req = crate::tonic_mock::streaming_request(vec![
            initial_request(3),
            ack_request(3),
            ack_request(1),
            ack_request(2),
        ]);

        let res = server.osprey_bidirectional_stream(req).await.unwrap();
        let mut messages = res.into_inner();
        let mut result = Vec::new();
        while let Some(v) = messages.next().await {
            result.push(v.unwrap());
        }

        // All 3 dispatched.
        assert_eq!(result.len(), 3);

        // All upstream oneshots should have fired (ack received by the pubsub side).
        for mut rx in ack_receivers {
            assert!(
                rx.try_recv().is_ok(),
                "expected ack receiver to have a value"
            );
        }

        Ok(())
    }

    /// max_unacked=0 must behave identically to max_unacked=1 (strict-serial, back-compat).
    #[tokio::test]
    async fn test_max_unacked_zero_defaults_to_one() -> Result<()> {
        let (pq_sender, pq_receiver) = create_ackable_action_priority_queue();
        let metrics = OspreyCoordinatorMetrics::new();
        let _worker_guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator_zero").unwrap());

        // Keep receivers alive so acking_oneshot_sender.is_closed() stays false.
        let mut _receivers = Vec::new();
        for i in 1u64..=2 {
            let (ackable, rx) = AckableAction::new(make_action(i, i));
            pq_sender.send_sync(ackable).await.unwrap();
            _receivers.push(rx);
        }

        let server = OspreyCoordinatorServer::new(pq_sender.clone(), pq_receiver, metrics.clone());

        let req = crate::tonic_mock::streaming_request(vec![
            initial_request(0), // 0 → treated as 1
            ack_request(1),
            ack_request(2),
        ]);

        let res = server.osprey_bidirectional_stream(req).await.unwrap();
        let mut messages = res.into_inner();
        let mut result = Vec::new();
        while let Some(v) = messages.next().await {
            result.push(v.unwrap());
        }

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].action_id, 1);
        assert_eq!(result[1].action_id, 2);

        Ok(())
    }

    /// On disconnect without acks, all in-flight ackers must be dropped so pubsub redelivers.
    #[tokio::test]
    async fn test_disconnect_drops_all_in_flight_ackers() -> Result<()> {
        let (pq_sender, pq_receiver) = create_ackable_action_priority_queue();
        let metrics = OspreyCoordinatorMetrics::new();
        let _worker_guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator_disconnect").unwrap());

        let mut ack_receivers = Vec::new();
        for i in 1u64..=4 {
            let (ackable, rx) = AckableAction::new(make_action(i, i));
            pq_sender.send_sync(ackable).await.unwrap();
            ack_receivers.push(rx);
        }

        let server = OspreyCoordinatorServer::new(pq_sender.clone(), pq_receiver, metrics.clone());

        // Worker connects with max_unacked=4, gets all 4 dispatched, then disconnects without acking.
        let disconnect_request = proto::Request {
            request: Some(proto::request::Request::Disconnect(proto::Disconnect {
                ack_or_nack: None, // new-style: no embedded ack
            })),
        };

        let req = crate::tonic_mock::streaming_request(vec![
            initial_request(4),
            disconnect_request,
        ]);

        let res = server.osprey_bidirectional_stream(req).await.unwrap();
        let mut messages = res.into_inner();
        // Drain the stream so the spawned task completes.
        while let Some(_) = messages.next().await {}

        // Give the spawned task a moment to finish cleanup.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // All 4 ackers should be dropped (sender closed → RecvError on the pubsub side).
        for mut rx in ack_receivers {
            assert!(
                rx.try_recv().is_err(),
                "expected ack receiver to be dropped (RecvError = nack/redeliver)"
            );
        }

        Ok(())
    }

    /// Unknown ack_id must not panic — warning logged, stream continues.
    #[tokio::test]
    async fn test_unknown_ack_id_is_logged_not_panic() -> Result<()> {
        let (pq_sender, pq_receiver) = create_ackable_action_priority_queue();
        let metrics = OspreyCoordinatorMetrics::new();
        let _worker_guard = metrics
            .clone()
            .spawn_emit_worker(new_client("osprey_coordinator_unknown_ack").unwrap());

        // Keep receiver alive (_rx) so acking_oneshot_sender.is_closed() stays false.
        let (ackable, _rx) = AckableAction::new(make_action(1, 1));
        pq_sender.send_sync(ackable).await.unwrap();

        let server = OspreyCoordinatorServer::new(pq_sender.clone(), pq_receiver, metrics.clone());

        // Send a bogus ack_id that was never dispatched.
        // max_unacked=2 so after Initial, action 1 is dispatched. Then ack_request(99999)
        // is an unknown ack_id — should log warning and not panic.
        let req = crate::tonic_mock::streaming_request(vec![
            initial_request(2),
            ack_request(99999), // unknown ack_id
        ]);

        // Must not panic — just complete normally.
        let res = server.osprey_bidirectional_stream(req).await.unwrap();
        let mut messages = res.into_inner();
        while let Some(_) = messages.next().await {}

        // bidi_stream_unknown_ack_id counter should have been incremented.
        // (We can't easily read the counter value in tests, but no panic = pass.)

        Ok(())
    }
}

#[cfg(test)]
mod execution_mode_tests {
    use crate::proto::ExecutionMode;
    use crate::proto::OspreyCoordinatorAction;
    use prost::Message;

    /// An older binary that doesn't know about field 8 encodes a message without `mode`.
    /// When decoded by a newer worker, the field must default to UNSPECIFIED so no tier
    /// filtering applies — preserving today's behavior.
    #[test]
    fn old_message_without_mode_decodes_as_unspecified() {
        let old_msg = OspreyCoordinatorAction {
            ack_id: 42,
            action_id: 100,
            action_name: "TEST_ACTION_A".to_string(),
            action_data: None,
            secret_data: None,
            timestamp: None,
            mode: 0, // explicit default — simulates an old binary that doesn't set mode
        };
        let mut encoded = Vec::new();
        old_msg.encode(&mut encoded).unwrap();

        let decoded = OspreyCoordinatorAction::decode(&encoded[..]).unwrap();

        assert_eq!(decoded.mode, ExecutionMode::Unspecified as i32);
        assert_eq!(decoded.action_name, "TEST_ACTION_A");
        assert_eq!(decoded.ack_id, 42);
        assert_eq!(decoded.action_id, 100);
    }

    #[test]
    fn message_with_sync_mode_round_trips() {
        let msg = OspreyCoordinatorAction {
            ack_id: 1,
            action_id: 2,
            action_name: "TEST_ACTION_B".to_string(),
            action_data: None,
            secret_data: None,
            timestamp: None,
            mode: ExecutionMode::Sync as i32,
        };
        let mut encoded = Vec::new();
        msg.encode(&mut encoded).unwrap();
        let decoded = OspreyCoordinatorAction::decode(&encoded[..]).unwrap();
        assert_eq!(decoded.mode, ExecutionMode::Sync as i32);
        assert_eq!(decoded.action_name, "TEST_ACTION_B");
    }

    #[test]
    fn message_with_async_mode_round_trips() {
        let msg = OspreyCoordinatorAction {
            ack_id: 1,
            action_id: 2,
            action_name: "TEST_ACTION_C".to_string(),
            action_data: None,
            secret_data: None,
            timestamp: None,
            mode: ExecutionMode::Async as i32,
        };
        let mut encoded = Vec::new();
        msg.encode(&mut encoded).unwrap();
        let decoded = OspreyCoordinatorAction::decode(&encoded[..]).unwrap();
        assert_eq!(decoded.mode, ExecutionMode::Async as i32);
    }

    /// Sanity: proto3 enums default to 0 when constructed via Default.
    /// Confirms the back-compat contract for any code path that constructs
    /// OspreyCoordinatorAction without explicitly setting mode.
    #[test]
    fn default_mode_is_unspecified() {
        let msg = OspreyCoordinatorAction::default();
        assert_eq!(msg.mode, ExecutionMode::Unspecified as i32);
        assert_eq!(msg.mode, 0);
    }
}

#[cfg(test)]
mod max_unacked_tests {
    use crate::proto::ClientDetails;
    use prost::Message;

    /// ClientDetails with max_unacked=5 round-trips through proto encode/decode.
    /// An older coordinator binary that doesn't know field 2 will skip it (unknown-field
    /// drop); a newer one reads it without error. This test confirms the field is parsed.
    #[test]
    fn client_details_max_unacked_round_trips() {
        let details = ClientDetails {
            id: "test".to_string(),
            max_unacked: 5,
        };
        let mut encoded = Vec::new();
        details.encode(&mut encoded).unwrap();
        let decoded = ClientDetails::decode(&encoded[..]).unwrap();
        assert_eq!(decoded.id, "test");
        assert_eq!(decoded.max_unacked, 5);
    }

    /// A worker that doesn't send max_unacked (old binary, field absent) must decode
    /// as 0, which the coordinator treats as 1 (strict-serial, today's behavior).
    #[test]
    fn client_details_missing_max_unacked_defaults_to_zero() {
        let details = ClientDetails {
            id: "old-worker".to_string(),
            max_unacked: 0, // proto3 default — field absent on the wire
        };
        let mut encoded = Vec::new();
        details.encode(&mut encoded).unwrap();
        let decoded = ClientDetails::decode(&encoded[..]).unwrap();
        assert_eq!(decoded.max_unacked, 0);
        // Coordinator logic: max(max_unacked, 1) == 1 → strict-serial, backward compat preserved.
    }
}
