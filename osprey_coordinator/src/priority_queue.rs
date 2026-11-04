use crate::metrics::counters::StaticCounter;
use crate::metrics::gauges::StaticGauge;
use crate::metrics::histograms::StaticHistogram;
use tokio::{
    sync::oneshot,
    time::{interval, Duration, Instant, MissedTickBehavior},
};

use crate::engine_pool::{routing_bucket, shadow_bucket, EnginePool, EngineRoutingFlags};
use crate::{coordinator_metrics::OspreyCoordinatorMetrics, proto};

use crate::tokio_utils::AbortOnDrop;
use parking_lot::Mutex;
use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Debug)]
pub enum AckOrNack {
    Ack(Option<crate::proto::Verdicts>),
    Nack,
}

impl From<proto::ack_or_nack::AckOrNack> for AckOrNack {
    fn from(ack_or_nack: proto::ack_or_nack::AckOrNack) -> Self {
        match ack_or_nack {
            proto::ack_or_nack::AckOrNack::Ack(inner) => Self::Ack(inner.verdicts),
            proto::ack_or_nack::AckOrNack::Nack(_) => Self::Nack,
        }
    }
}

/// Where a worker's answer goes: back to the action's producer, or (for a shadow copy) only into a metric.
#[derive(Debug)]
enum Acking {
    Original(oneshot::Sender<AckOrNack>),
    ShadowCopy(Arc<OspreyCoordinatorMetrics>),
}

pub struct AckableAction {
    pub action: proto::OspreyCoordinatorAction,
    /// What routing hashes: the action id, or a key that is stable across redeliveries when the id was minted.
    routing_key: u64,
    acking: Acking,
    local_retry_count: Cell<u32>,
    pub created_at: Instant,
}

impl AckableAction {
    pub fn new(
        action: proto::OspreyCoordinatorAction,
    ) -> (
        AckableAction,
        oneshot::Receiver<crate::priority_queue::AckOrNack>,
    ) {
        let (acking_oneshot_sender, acking_oneshot_receiver) = oneshot::channel::<AckOrNack>();
        let ackable_action = AckableAction {
            routing_key: action.action_id,
            action,
            acking: Acking::Original(acking_oneshot_sender),
            local_retry_count: 0.into(),
            created_at: Instant::now(),
        };
        (ackable_action, acking_oneshot_receiver)
    }

    /// Routes on `key` instead of the action id.
    pub fn with_routing_key(mut self, key: u64) -> AckableAction {
        self.routing_key = key;
        self
    }

    fn shadow_copy(&self, metrics: Arc<OspreyCoordinatorMetrics>) -> AckableAction {
        AckableAction {
            routing_key: self.routing_key,
            action: self.action.clone(),
            acking: Acking::ShadowCopy(metrics),
            local_retry_count: 0.into(),
            created_at: Instant::now(),
        }
    }

    pub fn is_shadow_copy(&self) -> bool {
        matches!(self.acking, Acking::ShadowCopy(_))
    }

    /// True when nobody waits for the answer any more, e.g. a sync caller that timed out.
    fn is_abandoned(&self) -> bool {
        match &self.acking {
            Acking::Original(sender) => sender.is_closed(),
            Acking::ShadowCopy(_) => false,
        }
    }

    fn nack(self) {
        if let Acking::Original(sender) = self.acking {
            if sender.send(AckOrNack::Nack).is_err() {
                println!("{}", dropped_nack_message(&self.action));
            }
        }
    }

    pub fn into_action(self) -> (proto::OspreyCoordinatorAction, ActionAcker) {
        (
            self.action,
            ActionAcker {
                acking: self.acking,
            },
        )
    }

    fn increment_retry_count(&self) {
        let count = self.local_retry_count.get();
        self.local_retry_count.set(count + 1);
    }

    #[allow(unused)]
    pub fn retry_count(&self) -> u32 {
        self.local_retry_count.get()
    }
}

#[derive(Debug)]
pub struct ActionAcker {
    acking: Acking,
}

impl ActionAcker {
    pub fn ack_or_nack<T: Into<AckOrNack>>(self, ack_or_nack: T) {
        match self.acking {
            Acking::Original(sender) => {
                sender.send(ack_or_nack.into()).ok();
            }
            Acking::ShadowCopy(metrics) => match ack_or_nack.into() {
                AckOrNack::Ack(_) => metrics.engine_pool_shadow_answered_ack.incr(),
                AckOrNack::Nack => metrics.engine_pool_shadow_answered_nack.incr(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Priority {
    Sync,
    Async,
}

const PRIORITIES: [Priority; 2] = [Priority::Sync, Priority::Async];

/// One pool's sync and async queues, and how many bidi streams currently pull from them.
struct PoolQueue {
    sync_sender: async_channel::Sender<AckableAction>,
    async_sender: async_channel::Sender<AckableAction>,
    receiver: PriorityQueueReceiver,
    connected: AtomicUsize,
}

impl PoolQueue {
    fn new(capacity: Option<usize>) -> PoolQueue {
        let channel = || match capacity {
            Some(capacity) => async_channel::bounded(capacity.max(1)),
            None => async_channel::unbounded(),
        };
        let (sync_sender, sync_receiver) = channel();
        let (async_sender, async_receiver) = channel();
        PoolQueue {
            sync_sender,
            async_sender,
            receiver: PriorityQueueReceiver::new(sync_receiver, async_receiver),
            connected: AtomicUsize::new(0),
        }
    }

    fn sender(&self, priority: Priority) -> &async_channel::Sender<AckableAction> {
        match priority {
            Priority::Sync => &self.sync_sender,
            Priority::Async => &self.async_sender,
        }
    }

    fn receiver(&self, priority: Priority) -> &async_channel::Receiver<AckableAction> {
        match priority {
            Priority::Sync => &self.receiver.sync_receiver,
            Priority::Async => &self.receiver.async_receiver,
        }
    }

    fn connected(&self) -> usize {
        self.connected.load(Ordering::Relaxed)
    }

    fn close(&self) {
        self.sync_sender.close();
        self.async_sender.close();
    }
}

struct EngineQueues {
    python: PoolQueue,
    rust: PoolQueue,
    shadow: PoolQueue,
    flags: Arc<EngineRoutingFlags>,
    metrics: Arc<OspreyCoordinatorMetrics>,
    /// Set by the shutdown drain. Moves between pools hold `moving` and check `draining` under it, so the drain,
    /// which takes `moving` after setting `draining`, never misses an action a move put in a drained queue.
    draining: AtomicBool,
    moving: Mutex<()>,
}

/// Most actions one rescue moves on a runtime thread outside the sweep; the sweep moves the rest.
const RESCUE_LIMIT: usize = 1024;
const SWEEP_LIMIT: usize = 10_000;
const LEGACY_RECEIVER_OFFSET: usize = 2;

/// Where one enqueue goes: exactly one production pool, plus maybe a shadow copy.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Route {
    pub pool: EnginePool,
    pub shadow_copy: bool,
}

impl EngineQueues {
    fn pool(&self, pool: EnginePool) -> &PoolQueue {
        match pool {
            EnginePool::Python => &self.python,
            EnginePool::Rust => &self.rust,
            EnginePool::Shadow => &self.shadow,
        }
    }

    // With rust_bps at 0 this never hashes and always returns python: rust and shadow get nothing.
    fn route(&self, routing_key: u64, priority: Priority) -> Route {
        let rust_bps = self.flags.rust_bps(priority);
        let chosen = if rust_bps > 0 && routing_bucket(routing_key) < rust_bps {
            EnginePool::Rust
        } else {
            EnginePool::Python
        };
        self.metrics.incr_engine_pool_routed(chosen, priority);
        // Only rust falls back. Python is the safe default and never spills onto a pool sized for the ramp.
        let pool = if chosen == EnginePool::Rust
            && self.rust.connected() == 0
            && self.python.connected() > 0
        {
            self.metrics.incr_engine_pool_fallback(priority);
            EnginePool::Python
        } else {
            chosen
        };
        self.metrics.incr_engine_pool_enqueued(pool, priority);
        let shadow_bps = self.flags.shadow_bps(priority);
        let shadow_copy =
            pool == EnginePool::Python && shadow_bps > 0 && shadow_bucket(routing_key) < shadow_bps;
        Route { pool, shadow_copy }
    }

    // Never waits: a full or unwatched shadow queue drops the copy.
    fn send_shadow_copy(&self, original: &AckableAction, priority: Priority) {
        if self.shadow.connected() == 0 {
            self.metrics
                .incr_engine_pool_shadow_dropped_no_receivers(priority);
            return;
        }
        let copy = original.shadow_copy(self.metrics.clone());
        match self.shadow.sender(priority).try_send(copy) {
            Ok(()) => self.metrics.incr_engine_pool_shadow_sent(priority),
            Err(async_channel::TrySendError::Full(_)) => {
                self.metrics.incr_engine_pool_shadow_dropped_full(priority)
            }
            Err(async_channel::TrySendError::Closed(_)) => self
                .metrics
                .incr_engine_pool_shadow_dropped_no_receivers(priority),
        }
    }

    /// Moves up to `limit` actions per priority from rust, once it has no streams, to python.
    fn rescue_stranded(&self, limit: usize) {
        if self.rust.connected() != 0 || self.python.connected() == 0 {
            return;
        }
        let _moving = self.moving.lock();
        if self.draining.load(Ordering::Acquire) {
            return;
        }
        for priority in PRIORITIES {
            let (from, to) = (self.rust.receiver(priority), self.python.sender(priority));
            for ackable_action in std::iter::from_fn(|| from.try_recv().ok()).take(limit) {
                self.metrics.incr_engine_pool_rescued(priority);
                if let Err(err) = to.try_send(ackable_action) {
                    err.into_inner().nack();
                }
            }
        }
    }

    /// Drops the copies queued for a shadow pool that has lost its last stream, so none is served stale later.
    fn drop_stale_shadow_copies(&self) {
        for priority in PRIORITIES {
            while self.shadow.receiver(priority).try_recv().is_ok() {
                self.metrics
                    .incr_engine_pool_shadow_dropped_no_receivers(priority);
            }
        }
    }
}

#[derive(Clone)]
pub struct PriorityQueueSender {
    queues: Arc<EngineQueues>,
}

impl PriorityQueueSender {
    pub fn close(&self) {
        self.queues.python.close();
        self.queues.rust.close();
        self.queues.shadow.close();
    }
    pub async fn send_sync(
        &self,
        ackable_action: AckableAction,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        self.send(ackable_action, Priority::Sync).await
    }

    pub async fn send_async(
        &self,
        ackable_action: AckableAction,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        self.send(ackable_action, Priority::Async).await
    }

    async fn send(
        &self,
        ackable_action: AckableAction,
        priority: Priority,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        ackable_action.increment_retry_count();
        let route = self.queues.route(ackable_action.routing_key, priority);
        if route.shadow_copy {
            self.queues.send_shadow_copy(&ackable_action, priority);
        }
        let queue = self.queues.pool(route.pool);
        queue.sender(priority).send(ackable_action).await?;
        // Rust's last stream may have left after route() looked; move the action now, not at the next sweep.
        if route.pool == EnginePool::Rust && queue.connected() == 0 {
            self.queues.rescue_stranded(RESCUE_LIMIT);
        }
        Ok(())
    }

    pub fn len_sync(&self) -> usize {
        self.queues.python.sync_sender.len()
    }

    pub fn len_async(&self) -> usize {
        self.queues.python.async_sender.len()
    }

    // Before pools, `main`, the bidi server and the shutdown handler each held a receiver; one pool queue holds
    // one now. Adding the difference keeps `priority_queue_receiver_count` as six dashboards read it (N streams + 3).
    pub fn receiver_count_sync(&self) -> usize {
        self.queues.python.sync_sender.receiver_count() + LEGACY_RECEIVER_OFFSET
    }

    pub fn receiver_count_async(&self) -> usize {
        self.queues.python.async_sender.receiver_count() + LEGACY_RECEIVER_OFFSET
    }
}

#[derive(Clone)]
pub struct PriorityQueueReceiver {
    sync_receiver: async_channel::Receiver<AckableAction>,
    async_receiver: async_channel::Receiver<AckableAction>,
}

fn dropped_nack_message(action: &proto::OspreyCoordinatorAction) -> String {
    format!(
        "tried to nack action_id={} ack_id={} and the nacking receiver was dropped",
        action.action_id, action.ack_id
    )
}

impl PriorityQueueReceiver {
    fn new(
        sync_receiver: async_channel::Receiver<AckableAction>,
        async_receiver: async_channel::Receiver<AckableAction>,
    ) -> PriorityQueueReceiver {
        PriorityQueueReceiver {
            sync_receiver,
            async_receiver,
        }
    }
    pub async fn recv(
        &self,
        metrics: Arc<OspreyCoordinatorMetrics>,
    ) -> Result<AckableAction, async_channel::RecvError> {
        loop {
            let result = tokio::select! {
                biased;
                result = self.sync_receiver.recv() => result,
                result = self.async_receiver.recv() => match result {
                    Ok(ackable_action) => {
                        if !ackable_action.is_shadow_copy() {
                            metrics.action_time_in_async_queue.record(Instant::now().duration_since(ackable_action.created_at));
                        }
                        Ok(ackable_action)
                    }
                    Err(_) => self.sync_receiver.recv().await
                },
            };
            match result {
                Ok(ackable_action) => {
                    // If the acking oneshot receiver is closed then there is no reason to process this action
                    // This can happen if the client sending a sync classification request times out
                    if ackable_action.is_abandoned() {
                        continue;
                    } else {
                        return Ok(ackable_action);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Drain the sync queue and nack each pending action. Surfaces to the sync
    /// RPC handler as `AckOrNack::Nack`, which it maps to
    /// `tonic::Status::aborted("action nacked")` — retryable by the client on a
    /// different coordinator pod. Called on shutdown so queued-but-undispatched
    /// sync requests don't hang to the per-request timeout and then return
    /// `internal("acking onshot dropped")` when the oneshot is finally torn
    /// down. Shadow copies are dropped.
    fn nack_all(receiver: &async_channel::Receiver<AckableAction>) {
        while let Ok(action) = receiver.try_recv() {
            action.nack();
        }
    }
}

/// The receiving side of every pool. A bidi stream takes one pool's receiver with [PoolReceivers::connect].
#[derive(Clone)]
pub struct PoolReceivers {
    queues: Arc<EngineQueues>,
}

/// A stream's hold on its pool: counted as connected until dropped.
pub struct PoolConnection {
    pub receiver: PriorityQueueReceiver,
    pool: EnginePool,
    queues: Arc<EngineQueues>,
}

impl Drop for PoolConnection {
    fn drop(&mut self) {
        let before = self
            .queues
            .pool(self.pool)
            .connected
            .fetch_sub(1, Ordering::Relaxed);
        if before == 1 {
            match self.pool {
                EnginePool::Rust => self.queues.rescue_stranded(RESCUE_LIMIT),
                EnginePool::Shadow => self.queues.drop_stale_shadow_copies(),
                EnginePool::Python => {}
            }
        }
    }
}

impl PoolReceivers {
    pub fn connect(&self, pool: EnginePool) -> PoolConnection {
        let queue = self.queues.pool(pool);
        queue.connected.fetch_add(1, Ordering::Relaxed);
        PoolConnection {
            receiver: queue.receiver.clone(),
            pool,
            queues: self.queues.clone(),
        }
    }

    pub fn nack_all_sync(&self) {
        self.nack_all(Priority::Sync);
    }

    pub fn nack_all_async(&self) {
        self.nack_all(Priority::Async);
    }

    fn nack_all(&self, priority: Priority) {
        self.queues.draining.store(true, Ordering::Release);
        let _moving = self.queues.moving.lock();
        for pool in [EnginePool::Python, EnginePool::Rust, EnginePool::Shadow] {
            PriorityQueueReceiver::nack_all(self.queues.pool(pool).receiver(priority));
        }
    }
}

/// Builds the python and rust pools (unbounded, like the single queue before pools) and the shadow pool,
/// bounded at `shadow_capacity` per priority.
pub fn create_engine_queues(
    flags: Arc<EngineRoutingFlags>,
    metrics: Arc<OspreyCoordinatorMetrics>,
    shadow_capacity: usize,
) -> (PriorityQueueSender, PoolReceivers) {
    let queues = Arc::new(EngineQueues {
        python: PoolQueue::new(None),
        rust: PoolQueue::new(None),
        shadow: PoolQueue::new(Some(shadow_capacity)),
        flags,
        metrics,
        draining: AtomicBool::new(false),
        moving: Mutex::new(()),
    });
    (
        PriorityQueueSender {
            queues: queues.clone(),
        },
        PoolReceivers { queues },
    )
}

#[cfg(test)]
pub fn create_ackable_action_priority_queue() -> (PriorityQueueSender, PoolReceivers) {
    create_engine_queues(
        Arc::new(EngineRoutingFlags::default()),
        OspreyCoordinatorMetrics::new(),
        16,
    )
}

pub fn spawn_priority_queue_metrics_worker(
    queue_sender: PriorityQueueSender,
    metrics: Arc<OspreyCoordinatorMetrics>,
) -> AbortOnDrop<()> {
    let mut interval = interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let join_handle = tokio::task::spawn(async move {
        loop {
            interval.tick().await;
            metrics
                .priority_queue_size_sync
                .set(queue_sender.len_sync() as u64);
            metrics
                .priority_queue_size_async
                .set(queue_sender.len_async() as u64);
            metrics
                .priority_queue_receiver_count_async
                .set(queue_sender.receiver_count_async() as u64);
            metrics
                .priority_queue_receiver_count_sync
                .set(queue_sender.receiver_count_sync() as u64);
            for pool in [EnginePool::Python, EnginePool::Rust, EnginePool::Shadow] {
                let queue = queue_sender.queues.pool(pool);
                for priority in PRIORITIES {
                    metrics.set_engine_pool_queue_size(
                        pool,
                        priority,
                        queue.sender(priority).len() as u64,
                    );
                }
                metrics.set_engine_pool_connected(pool, queue.connected() as u64);
            }
        }
    });

    AbortOnDrop::new(join_handle)
}

/// Every 100ms, moves actions queued on rust, once it has no streams, to python.
pub fn spawn_stranded_action_rescuer(queue_sender: PriorityQueueSender) -> AbortOnDrop<()> {
    let mut interval = interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    AbortOnDrop::new(tokio::task::spawn(async move {
        loop {
            interval.tick().await;
            queue_sender.queues.rescue_stranded(SWEEP_LIMIT);
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_pool::BPS_SCALE;
    use crate::metrics::counters::StaticCounter;
    use std::collections::HashSet;

    fn action(action_id: u64) -> proto::OspreyCoordinatorAction {
        proto::OspreyCoordinatorAction {
            ack_id: action_id,
            action_id,
            ..Default::default()
        }
    }

    struct Fixture {
        flags: Arc<EngineRoutingFlags>,
        metrics: Arc<OspreyCoordinatorMetrics>,
        sender: PriorityQueueSender,
        receivers: PoolReceivers,
    }

    fn fixture(shadow_capacity: usize) -> Fixture {
        let flags = Arc::new(EngineRoutingFlags::default());
        let metrics = OspreyCoordinatorMetrics::new();
        let (sender, receivers) =
            create_engine_queues(flags.clone(), metrics.clone(), shadow_capacity);
        Fixture {
            flags,
            metrics,
            sender,
            receivers,
        }
    }

    impl Fixture {
        fn set(&self, name: &str, bps: u32) {
            assert!(self.flags.set(name, Some(&bps.to_string())));
        }

        fn queued(&self, pool: EnginePool, priority: Priority) -> Vec<AckableAction> {
            let receiver = self.sender.queues.pool(pool).receiver(priority);
            std::iter::from_fn(|| receiver.try_recv().ok()).collect()
        }

        fn queued_ids(&self, pool: EnginePool, priority: Priority) -> Vec<u64> {
            self.queued(pool, priority)
                .into_iter()
                .map(|a| a.action.action_id)
                .collect()
        }
    }

    fn count(counter: &impl StaticCounter) -> i64 {
        counter.value().load(Ordering::Relaxed)
    }

    async fn send(f: &Fixture, ids: impl Iterator<Item = u64>, priority: Priority) {
        for id in ids {
            let (ackable_action, receiver) = AckableAction::new(action(id));
            std::mem::forget(receiver);
            match priority {
                Priority::Sync => f.sender.send_sync(ackable_action).await.unwrap(),
                Priority::Async => f.sender.send_async(ackable_action).await.unwrap(),
            }
        }
    }

    #[tokio::test]
    async fn each_action_reaches_exactly_one_of_python_and_rust() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let _rust = f.receivers.connect(EnginePool::Rust);
        f.set("rust_bps_async", 5_000);
        f.set("rust_bps_sync", 5_000);
        for priority in PRIORITIES {
            send(&f, 0..2_000, priority).await;
            let python = f.queued_ids(EnginePool::Python, priority);
            let rust = f.queued_ids(EnginePool::Rust, priority);
            let python_set: HashSet<u64> = python.iter().copied().collect();
            let rust_set: HashSet<u64> = rust.iter().copied().collect();
            assert_eq!(python.len() + rust.len(), 2_000);
            assert!(python_set.is_disjoint(&rust_set));
            assert_eq!(python_set.len() + rust_set.len(), 2_000);
            assert!((800..1_200).contains(&rust.len()), "{}", rust.len());
            for id in &rust {
                assert!(routing_bucket(*id) < 5_000);
            }
        }
        assert_eq!(
            count(&*f.metrics.engine_pool_enqueued_rust_async)
                + count(&*f.metrics.engine_pool_enqueued_python_async),
            2_000
        );
    }

    #[tokio::test]
    async fn the_same_action_id_always_routes_the_same_way() {
        let first = fixture(16);
        let second = fixture(16);
        // Held until the assertions: a pool losing its last stream would hand its queue to python.
        let mut connections = Vec::new();
        for f in [&first, &second] {
            connections.push(f.receivers.connect(EnginePool::Python));
            connections.push(f.receivers.connect(EnginePool::Rust));
            f.set("rust_bps_async", 3_000);
            for _ in 0..3 {
                send(f, 1_000..1_500, Priority::Async).await;
            }
        }
        let rust_first = first.queued_ids(EnginePool::Rust, Priority::Async);
        let rust_second = second.queued_ids(EnginePool::Rust, Priority::Async);
        assert!(
            (100..200).contains(&(rust_first.len() / 3)),
            "{}",
            rust_first.len()
        );
        assert_eq!(rust_first, rust_second);
        let distinct: HashSet<u64> = rust_first.iter().copied().collect();
        assert_eq!(rust_first.len(), distinct.len() * 3);
        assert!(distinct.iter().all(|id| routing_bucket(*id) < 3_000));
        drop(connections);
    }

    #[tokio::test]
    async fn zero_and_full_rates_route_everything_to_one_side() {
        for (bps, expected) in [(0, EnginePool::Python), (BPS_SCALE, EnginePool::Rust)] {
            let f = fixture(16);
            let _python = f.receivers.connect(EnginePool::Python);
            let _rust = f.receivers.connect(EnginePool::Rust);
            f.set("rust_bps_async", bps);
            f.set("rust_bps_sync", bps);
            for priority in PRIORITIES {
                send(&f, 0..1_000, priority).await;
                let other = match expected {
                    EnginePool::Python => EnginePool::Rust,
                    _ => EnginePool::Python,
                };
                assert_eq!(f.queued(expected, priority).len(), 1_000);
                assert!(f.queued(other, priority).is_empty());
            }
        }
    }

    #[tokio::test]
    async fn flags_at_zero_send_nothing_to_rust_or_shadow_even_with_their_workers_connected() {
        let f = fixture(16);
        // No python stream: still python, because a zero rust rate never falls back onto rust.
        let _rust = f.receivers.connect(EnginePool::Rust);
        let _shadow = f.receivers.connect(EnginePool::Shadow);
        for priority in PRIORITIES {
            send(&f, 0..500, priority).await;
            assert_eq!(f.queued(EnginePool::Python, priority).len(), 500);
            assert!(f.queued(EnginePool::Rust, priority).is_empty());
            assert!(f.queued(EnginePool::Shadow, priority).is_empty());
        }
        f.sender.queues.rescue_stranded(SWEEP_LIMIT);
        assert!(f.queued(EnginePool::Rust, Priority::Async).is_empty());
        assert_eq!(count(&*f.metrics.engine_pool_enqueued_rust_async), 0);
        assert_eq!(count(&*f.metrics.engine_pool_enqueued_rust_sync), 0);
        assert_eq!(count(&*f.metrics.engine_pool_shadow_sent_async), 0);
        assert_eq!(
            count(&*f.metrics.engine_pool_shadow_dropped_no_receivers_async),
            0
        );
    }

    #[tokio::test]
    async fn falls_back_when_the_chosen_pool_has_no_streams() {
        let f = fixture(16);
        let python = f.receivers.connect(EnginePool::Python);
        f.set("rust_bps_async", BPS_SCALE);
        send(&f, 0..100, Priority::Async).await;
        assert_eq!(f.queued(EnginePool::Python, Priority::Async).len(), 100);
        assert_eq!(count(&*f.metrics.engine_pool_fallback_rust_async), 100);
        assert_eq!(count(&*f.metrics.engine_pool_routed_rust_async), 100);
        drop(python);
    }

    #[tokio::test]
    async fn a_python_blip_never_moves_python_traffic_or_backlog_to_rust() {
        let f = fixture(16);
        let python = f.receivers.connect(EnginePool::Python);
        let _rust = f.receivers.connect(EnginePool::Rust);
        f.set("rust_bps_async", 100);
        send(&f, 0..1_000, Priority::Async).await;
        // Python's backlog stays put when its last stream drops, and new python-bound actions queue for it.
        drop(python);
        send(&f, 1_000..2_000, Priority::Async).await;
        f.sender.queues.rescue_stranded(SWEEP_LIMIT);
        let rust = f.queued_ids(EnginePool::Rust, Priority::Async);
        let expected: Vec<u64> = (0..2_000).filter(|id| routing_bucket(*id) < 100).collect();
        assert_eq!(rust, expected);
        assert!(rust.len() < 50, "{}", rust.len());
        assert_eq!(
            f.queued(EnginePool::Python, Priority::Async).len(),
            2_000 - expected.len()
        );
    }

    #[tokio::test]
    async fn a_zero_rate_sends_nothing_new_to_rust_while_a_rescue_runs() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let rust = f.receivers.connect(EnginePool::Rust);
        f.set("rust_bps_async", BPS_SCALE);
        send(&f, 0..3_000, Priority::Async).await;
        // The last rust stream leaving moves RESCUE_LIMIT at once and leaves the rest for the sweep.
        drop(rust);
        assert_eq!(
            f.sender.queues.rust.async_sender.len(),
            3_000 - RESCUE_LIMIT
        );
        f.set("rust_bps_async", 0);
        let _rust_again = f.receivers.connect(EnginePool::Rust);
        send(&f, 3_000..3_500, Priority::Async).await;
        let rust: HashSet<u64> = f
            .queued_ids(EnginePool::Rust, Priority::Async)
            .into_iter()
            .collect();
        assert!(rust.iter().all(|id| *id < 3_000));
        assert_eq!(count(&*f.metrics.engine_pool_enqueued_rust_async), 3_000);
    }

    #[tokio::test]
    async fn actions_stranded_on_a_pool_whose_streams_left_move_to_the_other_pool() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let rust = f.receivers.connect(EnginePool::Rust);
        f.set("rust_bps_sync", BPS_SCALE);
        send(&f, 0..10, Priority::Sync).await;
        f.sender.queues.rescue_stranded(SWEEP_LIMIT);
        assert_eq!(f.sender.queues.rust.sync_sender.len(), 10);
        // The last stream leaving moves them at once, without waiting for the sweep.
        drop(rust);
        assert_eq!(
            f.queued_ids(EnginePool::Python, Priority::Sync),
            (0..10).collect::<Vec<_>>()
        );
        assert_eq!(count(&*f.metrics.engine_pool_rescued_rust_sync), 10);
    }

    #[tokio::test]
    async fn the_shutdown_drain_stops_moving_actions_between_pools() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let rust = f.receivers.connect(EnginePool::Rust);
        f.set("rust_bps_sync", BPS_SCALE);
        f.receivers.nack_all_sync();
        let (ackable_action, answer) = AckableAction::new(action(1));
        f.sender.send_sync(ackable_action).await.unwrap();
        drop(rust);
        assert_eq!(f.sender.queues.rust.sync_sender.len(), 1);
        f.receivers.nack_all_sync();
        assert!(matches!(answer.await, Ok(AckOrNack::Nack)));
    }

    #[test]
    fn the_shutdown_drain_waits_for_a_move_in_progress_and_nacks_what_it_moved() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        // Stand in for a rescue paused mid-move: it holds `moving` and is about to forward one action.
        let moving = f.sender.queues.moving.lock();
        let receivers = f.receivers.clone();
        let drain = std::thread::spawn(move || receivers.nack_all_sync());
        while !f.sender.queues.draining.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        let (ackable_action, mut answer) = AckableAction::new(action(1));
        f.sender
            .queues
            .python
            .sync_sender
            .try_send(ackable_action)
            .unwrap();
        drop(moving);
        drop(drain.join());
        assert!(matches!(answer.try_recv(), Ok(AckOrNack::Nack)));
    }

    #[tokio::test]
    async fn copies_queued_for_a_shadow_pool_that_lost_its_streams_are_dropped() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let shadow = f.receivers.connect(EnginePool::Shadow);
        f.set("shadow_bps_async", BPS_SCALE);
        send(&f, 0..5, Priority::Async).await;
        assert_eq!(f.sender.queues.shadow.async_sender.len(), 5);
        drop(shadow);
        assert_eq!(f.sender.queues.shadow.async_sender.len(), 0);
        assert_eq!(
            count(&*f.metrics.engine_pool_shadow_dropped_no_receivers_async),
            5
        );
    }

    #[tokio::test]
    async fn the_routing_key_not_the_minted_id_picks_the_pool() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let _rust = f.receivers.connect(EnginePool::Rust);
        f.set("rust_bps_async", 5_000);
        let key = (0..).find(|k| routing_bucket(*k) < 5_000).unwrap();
        // Two deliveries of one message, each with a freshly minted id that alone would route to python.
        let ids: Vec<u64> = (0..)
            .filter(|id| routing_bucket(*id) >= 5_000)
            .take(2)
            .collect();
        for id in &ids {
            let (ackable_action, receiver) = AckableAction::new(action(*id));
            std::mem::forget(receiver);
            f.sender
                .send_async(ackable_action.with_routing_key(key))
                .await
                .unwrap();
        }
        assert_eq!(f.queued_ids(EnginePool::Rust, Priority::Async), ids);
    }

    #[tokio::test]
    async fn two_deliveries_of_one_id_less_message_route_the_same_way() {
        use crate::consumer::message_decoder::DecodedAction;
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let _rust = f.receivers.connect(EnginePool::Rust);
        f.set("rust_bps_async", 5_000);
        // Message ids routing to rust, each redelivered with a freshly minted id that alone would route to python.
        let message_ids: Vec<String> = (0..)
            .map(|i| format!("{i}"))
            .filter(|m| routing_bucket(crate::engine_pool::message_routing_key(m)) < 5_000)
            .take(3)
            .collect();
        let mut minted = (0..).filter(|id| routing_bucket(*id) >= 5_000);
        for message_id in message_ids.iter().chain(message_ids.iter()) {
            let decoded = DecodedAction {
                action: action(minted.next().unwrap()),
                id_minted: true,
            };
            let (ackable_action, receiver) = decoded.into_ackable(message_id);
            std::mem::forget(receiver);
            f.sender.send_async(ackable_action).await.unwrap();
        }
        assert_eq!(f.queued(EnginePool::Rust, Priority::Async).len(), 6);
        // An id the message carried is what routes, not the message id.
        let carried = (0..).find(|id| routing_bucket(*id) >= 5_000).unwrap();
        let decoded = DecodedAction {
            action: action(carried),
            id_minted: false,
        };
        let (ackable_action, receiver) = decoded.into_ackable(&message_ids[0]);
        std::mem::forget(receiver);
        f.sender.send_async(ackable_action).await.unwrap();
        assert_eq!(
            f.queued_ids(EnginePool::Python, Priority::Async),
            vec![carried]
        );
    }

    #[tokio::test]
    async fn the_receiver_count_gauge_reads_as_before_pools() {
        let f = fixture(16);
        // Before pools: three long-lived receivers plus one per stream.
        assert_eq!(f.sender.receiver_count_sync(), 3);
        let _streams = [
            f.receivers.connect(EnginePool::Python),
            f.receivers.connect(EnginePool::Python),
        ];
        assert_eq!(f.sender.receiver_count_async(), 5);
    }

    #[tokio::test]
    async fn a_full_shadow_pool_never_blocks_the_production_path() {
        let f = fixture(1);
        let _python = f.receivers.connect(EnginePool::Python);
        let _shadow = f.receivers.connect(EnginePool::Shadow);
        f.set("shadow_bps_async", BPS_SCALE);
        tokio::time::timeout(Duration::from_secs(1), send(&f, 0..100, Priority::Async))
            .await
            .expect("send blocked on the shadow pool");
        assert_eq!(f.queued(EnginePool::Python, Priority::Async).len(), 100);
        assert_eq!(f.queued(EnginePool::Shadow, Priority::Async).len(), 1);
        assert_eq!(count(&*f.metrics.engine_pool_shadow_sent_async), 1);
        assert_eq!(count(&*f.metrics.engine_pool_shadow_dropped_full_async), 99);
    }

    #[tokio::test]
    async fn no_shadow_copy_without_a_shadow_stream_or_for_rust_bound_actions() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let _rust = f.receivers.connect(EnginePool::Rust);
        f.set("shadow_bps_async", BPS_SCALE);
        send(&f, 0..10, Priority::Async).await;
        assert_eq!(
            count(&*f.metrics.engine_pool_shadow_dropped_no_receivers_async),
            10
        );
        let _shadow = f.receivers.connect(EnginePool::Shadow);
        f.set("rust_bps_async", BPS_SCALE);
        send(&f, 0..10, Priority::Async).await;
        assert!(f.queued(EnginePool::Shadow, Priority::Async).is_empty());
    }

    #[tokio::test]
    async fn a_shadow_answer_never_acks_or_nacks_the_original() {
        let f = fixture(16);
        let python = f.receivers.connect(EnginePool::Python);
        let shadow = f.receivers.connect(EnginePool::Shadow);
        f.set("shadow_bps_async", BPS_SCALE);
        let (ackable_action, mut original) = AckableAction::new(action(7));
        f.sender.send_async(ackable_action).await.unwrap();

        // recv must not skip the copy although nothing waits on it.
        let copy = shadow.receiver.recv(f.metrics.clone()).await.unwrap();
        assert!(copy.is_shadow_copy());
        let (copy_action, copy_acker) = copy.into_action();
        assert_eq!(copy_action.action_id, 7);
        copy_acker.ack_or_nack(AckOrNack::Nack);
        assert!(matches!(
            original.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(count(&*f.metrics.engine_pool_shadow_answered_nack), 1);

        let (_, acker) = python
            .receiver
            .recv(f.metrics.clone())
            .await
            .unwrap()
            .into_action();
        acker.ack_or_nack(AckOrNack::Ack(None));
        assert!(matches!(original.await, Ok(AckOrNack::Ack(None))));
    }

    #[tokio::test]
    async fn shutdown_nacks_every_production_pool_and_drops_shadow_copies() {
        let f = fixture(16);
        let _python = f.receivers.connect(EnginePool::Python);
        let _rust = f.receivers.connect(EnginePool::Rust);
        let _shadow = f.receivers.connect(EnginePool::Shadow);
        f.set("rust_bps_sync", 5_000);
        f.set("shadow_bps_sync", BPS_SCALE);
        let mut answers = Vec::new();
        for id in 0..50 {
            let (ackable_action, receiver) = AckableAction::new(action(id));
            f.sender.send_sync(ackable_action).await.unwrap();
            answers.push(receiver);
        }
        f.receivers.nack_all_sync();
        for answer in answers {
            assert!(matches!(answer.await, Ok(AckOrNack::Nack)));
        }
        assert!(f.queued(EnginePool::Shadow, Priority::Sync).is_empty());
    }

    #[test]
    fn dropped_nack_message_excludes_action_payloads() {
        let action = proto::OspreyCoordinatorAction {
            ack_id: 456,
            action_id: 123,
            action_data: Some(
                proto::osprey_coordinator_action::ActionData::JsonActionData(
                    b"public-data-sentinel".to_vec(),
                ),
            ),
            secret_data: Some(
                proto::osprey_coordinator_action::SecretData::JsonSecretData(
                    b"private-secret-sentinel".to_vec(),
                ),
            ),
            ..Default::default()
        };

        let message = dropped_nack_message(&action);

        assert_eq!(
            message,
            "tried to nack action_id=123 ack_id=456 and the nacking receiver was dropped"
        );
        assert!(!message.contains("public-data-sentinel"));
        assert!(!message.contains("private-secret-sentinel"));
    }
}
