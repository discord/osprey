use crate::priority_queue::{PriorityQueueReceiver, PriorityQueueSender};
use crate::signals;

pub fn spawn_shutdown_handler(
    priority_queue_sender: PriorityQueueSender,
    priority_queue_receiver: PriorityQueueReceiver,
) {
    tokio::spawn(async move {
        tracing::info!("shutdown handler spawned - waiting on exit signal");
        signals::exit_signal().await;
        tracing::info!("got exit signal");
        // Drain everything queued-but-undispatched. Sync nacks bubble up to the
        // sync RPC handler as Status::aborted, which discord_api can retry on a
        // different coord pod. Async nacks trigger immediate pubsub redelivery
        // rather than waiting for the lease to expire.
        priority_queue_receiver.nack_all_sync();
        priority_queue_receiver.nack_all_async();
        tracing::info!("nacked all queued sync + async actions");
        // Hold the channel open while workers ack dispatched-but-not-yet-acked
        // actions over bidi. Bumped from 15s — at typical worker latencies of
        // ~150ms p95, 30s gives ~200x the processing window for in-flight
        // actions to drain naturally before the channel close tears down the
        // bidi streams.
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        priority_queue_sender.close();
        tracing::info!("closed priority queue");
    });
}
