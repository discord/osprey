use crate::engine_pool::EnginePool;
use crate::metrics::counters::StaticCounter;
use crate::metrics::define_metrics;
use crate::metrics::gauges::StaticGauge;
use crate::priority_queue::Priority;

define_metrics!(OspreyCoordinatorMetrics, [
    // How long an action has been processed by a worker for before it is acked/nacked
    action_outstanding_duration => StaticHistogram(),
    // How long an action has been held in the async queue for before it is sent to a worker
    action_time_in_async_queue => StaticHistogram("action_time_in_queue", ["async" => "true"]),
    // How many new connections have been established to the coordinator
    new_connection_established => StaticCounter(),

    // How many messages are currently buffered in the priority queue
    priority_queue_size_sync => StaticGauge("priority_queue_size",["type" => "sync"]),
    priority_queue_size_async => StaticGauge("priority_queue_size",["type" => "async"]),

    // How many receivers are open for the priority queue
    // can be used as a proxy for number of connections open from the osprey worker
    priority_queue_receiver_count_sync => StaticGauge("priority_queue_receiver_count",["type" => "sync"]),
    priority_queue_receiver_count_async => StaticGauge("priority_queue_receiver_count",["type" => "async"]),

    client_disconnected_gracefully => StaticCounter("client_disconnected", ["error" => "false"]),
    client_disconnected_broken_pipe => StaticCounter("client_disconnected", ["error" => "broken_pipe"]),
    client_disconnected_stream_error => StaticCounter("client_disconnected", ["error" => "stream_error"]),
    client_disconnected_receiver_closed => StaticCounter("client_disconnected", ["error" => "pq_closed"]),
    client_disconnected_receiver_timeout => StaticCounter("client_disconnected", ["error" => "pq_timeout"]),

    bidi_stream_ended_no_outstanding_action => StaticCounter("bidi_stream_ended", ["outstanding_action" => "false"]),
    bidi_stream_ended_yes_outstanding_action => StaticCounter("bidi_stream_ended", ["outstanding_action" => "true"]),

    // How many sync actions have been recieved
    sync_classification_action_received => StaticCounter(),

    // How many sync actions have failed due to deserialization issues
    sync_classification_failure_deserialization => StaticCounter("sync_classification_failure", ["error" => "deserialization"]),
    // How many sync actions have failed due to the acking oneshot being dropped
    sync_classification_failure_oneshot_dropped => StaticCounter("sync_classification_failure", ["error" => "oneshot_dropped"]),
    // How many sync actions have failed because we couldn't send to the priority queue
    sync_classification_failure_pq_send => StaticCounter("sync_classification_failure", ["error" => "pq_send"]),
    // How many sync actions were fast-rejected because this pod is shutting down
    sync_classification_failure_shutting_down => StaticCounter("sync_classification_failure", ["error" => "shutting_down"]),
    sync_classification_failure_label_service => StaticCounter("sync_classification_failure", ["error" => "label_service"]),

    // How many acks for sync actions
    sync_classification_result_ack => StaticCounter("sync_classification_result", ["ack" => "true"]),
    // How many nacks for sync actions
    sync_classification_result_nack => StaticCounter("sync_classification_result", ["ack" => "false"]),

    // How many messages we've added to the async queue
    async_classification_added_to_queue => StaticCounter("async_classification_added_to_queue"),
    // How many acks for async actions
    async_classification_result_ack => StaticCounter("async_classification_result", ["ack" => "true"]),
    // How many nacks for async actions
    async_classification_result_nack => StaticCounter("async_classification_result", ["ack" => "false"]),

    // Time it took to send a message to the Async-Channel priority queue
    priority_queue_send_time_async => StaticHistogram("priority_queue_send_time", ["type" => "async"]),
    // Time it took to send a message to the sync-Channel priority queue
    priority_queue_send_time_sync => StaticHistogram("priority_queue_send_time", ["type" => "sync"]),
    // Time it took to receive a message from the async/sync priority queue
    priority_queue_receive_time => StaticHistogram("priority_queue_receive_time"),
    // Time it took for the acking receiver in the MessageHandler to receive an ack/nack
    receiver_ack_time_async => StaticHistogram("receiver_ack_time", ["type" => "async"]),
    // Time it took for the acking receiver in the MessageHandler to receive an ack/nack
    receiver_ack_time_sync => StaticHistogram("receiver_ack_time", ["type" => "sync"]),

    // How many times action ID generation from snowflake is used (when pubsub_action.id is None)
    action_id_snowflake_generation_json => StaticCounter("action_id_snowflake_generation", ["proto"=>"false"]),
    action_id_snowflake_generation_proto => StaticCounter("action_id_snowflake_generation", ["proto"=>"true"]),

    // Bidi stream dispatch counters (for throughput investigation)
    bidi_actions_sent => StaticCounter("bidi_stream.actions_sent"),
    bidi_acks_received => StaticCounter("bidi_stream.acks_received"),

    // Engine pools: which pool the hash chose, which pool got the action, and fallbacks when the chosen
    // pool had no streams. `rescued` counts actions moved off a pool whose last stream went away.
    engine_pool_routed_python_sync => StaticCounter("engine_pool.routed", ["pool" => "python", "type" => "sync"]),
    engine_pool_routed_python_async => StaticCounter("engine_pool.routed", ["pool" => "python", "type" => "async"]),
    engine_pool_routed_rust_sync => StaticCounter("engine_pool.routed", ["pool" => "rust", "type" => "sync"]),
    engine_pool_routed_rust_async => StaticCounter("engine_pool.routed", ["pool" => "rust", "type" => "async"]),
    engine_pool_enqueued_python_sync => StaticCounter("engine_pool.enqueued", ["pool" => "python", "type" => "sync"]),
    engine_pool_enqueued_python_async => StaticCounter("engine_pool.enqueued", ["pool" => "python", "type" => "async"]),
    engine_pool_enqueued_rust_sync => StaticCounter("engine_pool.enqueued", ["pool" => "rust", "type" => "sync"]),
    engine_pool_enqueued_rust_async => StaticCounter("engine_pool.enqueued", ["pool" => "rust", "type" => "async"]),
    engine_pool_fallback_rust_sync => StaticCounter("engine_pool.fallback", ["pool" => "rust", "to" => "python", "type" => "sync"]),
    engine_pool_fallback_rust_async => StaticCounter("engine_pool.fallback", ["pool" => "rust", "to" => "python", "type" => "async"]),
    engine_pool_fallback_python_sync => StaticCounter("engine_pool.fallback", ["pool" => "python", "to" => "rust", "type" => "sync"]),
    engine_pool_fallback_python_async => StaticCounter("engine_pool.fallback", ["pool" => "python", "to" => "rust", "type" => "async"]),
    engine_pool_rescued_rust_sync => StaticCounter("engine_pool.rescued", ["pool" => "rust", "to" => "python", "type" => "sync"]),
    engine_pool_rescued_rust_async => StaticCounter("engine_pool.rescued", ["pool" => "rust", "to" => "python", "type" => "async"]),
    engine_pool_rescued_python_sync => StaticCounter("engine_pool.rescued", ["pool" => "python", "to" => "rust", "type" => "sync"]),
    engine_pool_rescued_python_async => StaticCounter("engine_pool.rescued", ["pool" => "python", "to" => "rust", "type" => "async"]),
    // Shadow copies of python-bound actions: sent, or dropped because the shadow queue was full or had no streams.
    engine_pool_shadow_sent_sync => StaticCounter("engine_pool.shadow_copy", ["result" => "sent", "type" => "sync"]),
    engine_pool_shadow_sent_async => StaticCounter("engine_pool.shadow_copy", ["result" => "sent", "type" => "async"]),
    engine_pool_shadow_dropped_full_sync => StaticCounter("engine_pool.shadow_copy", ["result" => "dropped_full", "type" => "sync"]),
    engine_pool_shadow_dropped_full_async => StaticCounter("engine_pool.shadow_copy", ["result" => "dropped_full", "type" => "async"]),
    engine_pool_shadow_dropped_no_receivers_sync => StaticCounter("engine_pool.shadow_copy", ["result" => "dropped_no_receivers", "type" => "sync"]),
    engine_pool_shadow_dropped_no_receivers_async => StaticCounter("engine_pool.shadow_copy", ["result" => "dropped_no_receivers", "type" => "async"]),
    // Streams whose `osprey-engine-pool` header names no pool; they join the python pool.
    engine_pool_unknown_header => StaticCounter("engine_pool.unknown_header"),
    engine_pool_shadow_answered_ack => StaticCounter("engine_pool.shadow_copy_answered", ["ack" => "true"]),
    engine_pool_shadow_answered_nack => StaticCounter("engine_pool.shadow_copy_answered", ["ack" => "false"]),
    engine_pool_queue_size_python_sync => StaticGauge("engine_pool.queue_size", ["pool" => "python", "type" => "sync"]),
    engine_pool_queue_size_python_async => StaticGauge("engine_pool.queue_size", ["pool" => "python", "type" => "async"]),
    engine_pool_queue_size_rust_sync => StaticGauge("engine_pool.queue_size", ["pool" => "rust", "type" => "sync"]),
    engine_pool_queue_size_rust_async => StaticGauge("engine_pool.queue_size", ["pool" => "rust", "type" => "async"]),
    engine_pool_queue_size_shadow_sync => StaticGauge("engine_pool.queue_size", ["pool" => "shadow", "type" => "sync"]),
    engine_pool_queue_size_shadow_async => StaticGauge("engine_pool.queue_size", ["pool" => "shadow", "type" => "async"]),
    engine_pool_connected_python => StaticGauge("engine_pool.connected_streams", ["pool" => "python"]),
    engine_pool_connected_rust => StaticGauge("engine_pool.connected_streams", ["pool" => "rust"]),
    engine_pool_connected_shadow => StaticGauge("engine_pool.connected_streams", ["pool" => "shadow"]),
]);

macro_rules! by_priority {
    ($self:ident, $priority:expr, $sync:ident, $async:ident, $op:ident($($arg:expr)?)) => {
        match $priority {
            Priority::Sync => $self.$sync.$op($($arg)?),
            Priority::Async => $self.$async.$op($($arg)?),
        }
    };
}

impl OspreyCoordinatorMetrics {
    pub fn incr_engine_pool_routed(&self, pool: EnginePool, priority: Priority) {
        match pool {
            EnginePool::Rust => by_priority!(
                self,
                priority,
                engine_pool_routed_rust_sync,
                engine_pool_routed_rust_async,
                incr()
            ),
            _ => by_priority!(
                self,
                priority,
                engine_pool_routed_python_sync,
                engine_pool_routed_python_async,
                incr()
            ),
        }
    }

    pub fn incr_engine_pool_enqueued(&self, pool: EnginePool, priority: Priority) {
        match pool {
            EnginePool::Rust => by_priority!(
                self,
                priority,
                engine_pool_enqueued_rust_sync,
                engine_pool_enqueued_rust_async,
                incr()
            ),
            _ => by_priority!(
                self,
                priority,
                engine_pool_enqueued_python_sync,
                engine_pool_enqueued_python_async,
                incr()
            ),
        }
    }

    /// Counts a fallback away from `from`, the pool the hash chose.
    pub fn incr_engine_pool_fallback(&self, from: EnginePool, priority: Priority) {
        match from {
            EnginePool::Rust => by_priority!(
                self,
                priority,
                engine_pool_fallback_rust_sync,
                engine_pool_fallback_rust_async,
                incr()
            ),
            _ => by_priority!(
                self,
                priority,
                engine_pool_fallback_python_sync,
                engine_pool_fallback_python_async,
                incr()
            ),
        }
    }

    pub fn incr_engine_pool_rescued(&self, from: EnginePool, priority: Priority) {
        match from {
            EnginePool::Rust => by_priority!(
                self,
                priority,
                engine_pool_rescued_rust_sync,
                engine_pool_rescued_rust_async,
                incr()
            ),
            _ => by_priority!(
                self,
                priority,
                engine_pool_rescued_python_sync,
                engine_pool_rescued_python_async,
                incr()
            ),
        }
    }

    pub fn incr_engine_pool_shadow_sent(&self, priority: Priority) {
        by_priority!(
            self,
            priority,
            engine_pool_shadow_sent_sync,
            engine_pool_shadow_sent_async,
            incr()
        )
    }

    pub fn incr_engine_pool_shadow_dropped_full(&self, priority: Priority) {
        by_priority!(
            self,
            priority,
            engine_pool_shadow_dropped_full_sync,
            engine_pool_shadow_dropped_full_async,
            incr()
        )
    }

    pub fn incr_engine_pool_shadow_dropped_no_receivers(&self, priority: Priority) {
        by_priority!(
            self,
            priority,
            engine_pool_shadow_dropped_no_receivers_sync,
            engine_pool_shadow_dropped_no_receivers_async,
            incr()
        )
    }

    pub fn set_engine_pool_queue_size(&self, pool: EnginePool, priority: Priority, size: u64) {
        match pool {
            EnginePool::Python => by_priority!(
                self,
                priority,
                engine_pool_queue_size_python_sync,
                engine_pool_queue_size_python_async,
                set(size)
            ),
            EnginePool::Rust => by_priority!(
                self,
                priority,
                engine_pool_queue_size_rust_sync,
                engine_pool_queue_size_rust_async,
                set(size)
            ),
            EnginePool::Shadow => by_priority!(
                self,
                priority,
                engine_pool_queue_size_shadow_sync,
                engine_pool_queue_size_shadow_async,
                set(size)
            ),
        }
    }

    pub fn set_engine_pool_connected(&self, pool: EnginePool, connected: u64) {
        match pool {
            EnginePool::Python => self.engine_pool_connected_python.set(connected),
            EnginePool::Rust => self.engine_pool_connected_rust.set(connected),
            EnginePool::Shadow => self.engine_pool_connected_shadow.set(connected),
        }
    }
}
