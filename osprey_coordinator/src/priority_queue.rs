use crate::metrics::gauges::StaticGauge;
use crate::metrics::histograms::StaticHistogram;
use tokio::{
    sync::oneshot,
    time::{interval, Duration, Instant, MissedTickBehavior},
};

use crate::{coordinator_metrics::OspreyCoordinatorMetrics, proto};

use crate::tokio_utils::AbortOnDrop;
use std::{cell::Cell, sync::Arc};

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

pub struct AckableAction {
    pub action: proto::OspreyCoordinatorAction,
    acking_oneshot_sender: oneshot::Sender<AckOrNack>,
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
            action,
            acking_oneshot_sender,
            local_retry_count: 0.into(),
            created_at: Instant::now(),
        };
        (ackable_action, acking_oneshot_receiver)
    }

    pub fn into_action(self) -> (proto::OspreyCoordinatorAction, ActionAcker) {
        (
            self.action,
            ActionAcker {
                acking_oneshot_sender: self.acking_oneshot_sender,
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
    acking_oneshot_sender: oneshot::Sender<AckOrNack>,
}

impl ActionAcker {
    pub fn ack_or_nack<T: Into<AckOrNack>>(self, ack_or_nack: T) {
        self.acking_oneshot_sender.send(ack_or_nack.into()).ok();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    Sync,
    Async,
    NotifSteady,
    NotifBatch,
}

/// Which channel class a worker connection serves, advertised in
/// `ClientDetails.served_queue`. `Fast` (the default / legacy value) serves the
/// existing `[sync, async]` biased path; the notif classes serve exactly one channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServedQueue {
    Fast,
    NotifSteady,
    NotifBatch,
}

impl ServedQueue {
    pub fn from_str(s: &str) -> ServedQueue {
        match s {
            "notif_steady" => ServedQueue::NotifSteady,
            "notif_batch" => ServedQueue::NotifBatch,
            // "", "fast", or any unknown value => legacy fast path (fail safe).
            _ => ServedQueue::Fast,
        }
    }
}

#[derive(Clone)]
pub struct PriorityQueueSender {
    sync_sender: async_channel::Sender<AckableAction>,
    async_sender: async_channel::Sender<AckableAction>,
    notif_steady_sender: async_channel::Sender<AckableAction>,
    notif_batch_sender: async_channel::Sender<AckableAction>,
}

impl PriorityQueueSender {
    fn new(
        sync_sender: async_channel::Sender<AckableAction>,
        async_sender: async_channel::Sender<AckableAction>,
        notif_steady_sender: async_channel::Sender<AckableAction>,
        notif_batch_sender: async_channel::Sender<AckableAction>,
    ) -> PriorityQueueSender {
        PriorityQueueSender {
            sync_sender,
            async_sender,
            notif_steady_sender,
            notif_batch_sender,
        }
    }

    pub fn close(&self) {
        self.sync_sender.close();
        self.async_sender.close();
        self.notif_steady_sender.close();
        self.notif_batch_sender.close();
    }
    pub async fn send_sync(
        &self,
        ackable_action: AckableAction,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        self.send(ackable_action, Channel::Sync).await
    }

    pub async fn send_async(
        &self,
        ackable_action: AckableAction,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        self.send(ackable_action, Channel::Async).await
    }

    pub async fn send_notif_steady(
        &self,
        ackable_action: AckableAction,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        self.send(ackable_action, Channel::NotifSteady).await
    }

    pub async fn send_notif_batch(
        &self,
        ackable_action: AckableAction,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        self.send(ackable_action, Channel::NotifBatch).await
    }

    pub async fn send(
        &self,
        ackable_action: AckableAction,
        channel: Channel,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        ackable_action.increment_retry_count();
        match channel {
            Channel::Sync => self.sync_sender.send(ackable_action).await,
            Channel::Async => self.async_sender.send(ackable_action).await,
            Channel::NotifSteady => self.notif_steady_sender.send(ackable_action).await,
            Channel::NotifBatch => self.notif_batch_sender.send(ackable_action).await,
        }
    }

    pub fn len(&self, channel: Channel) -> usize {
        match channel {
            Channel::Sync => self.sync_sender.len(),
            Channel::Async => self.async_sender.len(),
            Channel::NotifSteady => self.notif_steady_sender.len(),
            Channel::NotifBatch => self.notif_batch_sender.len(),
        }
    }

    pub fn len_sync(&self) -> usize {
        self.sync_sender.len()
    }

    pub fn len_async(&self) -> usize {
        self.async_sender.len()
    }

    pub fn receiver_count_sync(&self) -> usize {
        self.sync_sender.receiver_count()
    }

    pub fn receiver_count_async(&self) -> usize {
        self.async_sender.receiver_count()
    }
}

#[derive(Clone)]
pub struct PriorityQueueReceiver {
    sync_receiver: async_channel::Receiver<AckableAction>,
    async_receiver: async_channel::Receiver<AckableAction>,
    notif_steady_receiver: async_channel::Receiver<AckableAction>,
    notif_batch_receiver: async_channel::Receiver<AckableAction>,
}

impl PriorityQueueReceiver {
    fn new(
        sync_receiver: async_channel::Receiver<AckableAction>,
        async_receiver: async_channel::Receiver<AckableAction>,
        notif_steady_receiver: async_channel::Receiver<AckableAction>,
        notif_batch_receiver: async_channel::Receiver<AckableAction>,
    ) -> PriorityQueueReceiver {
        PriorityQueueReceiver {
            sync_receiver,
            async_receiver,
            notif_steady_receiver,
            notif_batch_receiver,
        }
    }

    pub async fn recv(
        &self,
        metrics: Arc<OspreyCoordinatorMetrics>,
    ) -> Result<AckableAction, async_channel::RecvError> {
        self.recv_for(ServedQueue::Fast, metrics).await
    }

    /// Affinity-aware receive: a connection only pulls from the channels its pool
    /// serves. `Fast` preserves the existing biased sync>async behavior.
    pub async fn recv_for(
        &self,
        served: ServedQueue,
        metrics: Arc<OspreyCoordinatorMetrics>,
    ) -> Result<AckableAction, async_channel::RecvError> {
        loop {
            let result = match served {
                ServedQueue::Fast => tokio::select! {
                    biased;
                    result = self.sync_receiver.recv() => result,
                    result = self.async_receiver.recv() => match result {
                        Ok(ackable_action) => {
                            metrics.action_time_in_async_queue.record(Instant::now().duration_since(ackable_action.created_at));
                            Ok(ackable_action)
                        }
                        Err(_) => self.sync_receiver.recv().await
                    },
                },
                ServedQueue::NotifSteady => self.notif_steady_receiver.recv().await,
                ServedQueue::NotifBatch => self.notif_batch_receiver.recv().await,
            };
            match result {
                Ok(ackable_action) => {
                    // If the acking oneshot receiver is closed then there is no reason to process this action
                    // This can happen if the client sending a sync classification request times out
                    if ackable_action.acking_oneshot_sender.is_closed() {
                        continue;
                    } else {
                        return Ok(ackable_action);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn nack_all_async(&self) {
        Self::nack_all(&self.async_receiver);
    }

    /// Drain the sync queue and nack each pending action. Surfaces to the sync
    /// RPC handler as `AckOrNack::Nack`, which it maps to
    /// `tonic::Status::aborted("action nacked")` — retryable by the client on a
    /// different coordinator pod. Called on shutdown so queued-but-undispatched
    /// sync requests don't hang to the per-request timeout and then return
    /// `internal("acking onshot dropped")` when the oneshot is finally torn
    /// down.
    pub fn nack_all_sync(&self) {
        Self::nack_all(&self.sync_receiver);
    }

    fn nack_all(receiver: &async_channel::Receiver<AckableAction>) {
        loop {
            match receiver.try_recv() {
                Ok(action) => match action.acking_oneshot_sender.send(AckOrNack::Nack) {
                    Ok(_) => (),
                    Err(_) => println!(
                        "tried to nack {:?} and the nacking receiver was dropped",
                        action.action
                    ),
                },
                Err(_) => return,
            }
        }
    }
}

pub fn create_ackable_action_priority_queue() -> (PriorityQueueSender, PriorityQueueReceiver) {
    let (sync_sender, sync_receiver) = async_channel::unbounded();
    let (async_sender, async_receiver) = async_channel::unbounded();
    let (notif_steady_sender, notif_steady_receiver) = async_channel::unbounded();
    let (notif_batch_sender, notif_batch_receiver) = async_channel::unbounded();
    (
        PriorityQueueSender::new(
            sync_sender,
            async_sender,
            notif_steady_sender,
            notif_batch_sender,
        ),
        PriorityQueueReceiver::new(
            sync_receiver,
            async_receiver,
            notif_steady_receiver,
            notif_batch_receiver,
        ),
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
        }
    });

    AbortOnDrop::new(join_handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator_metrics::OspreyCoordinatorMetrics;

    fn ackable() -> (AckableAction, oneshot::Receiver<AckOrNack>) {
        AckableAction::new(proto::OspreyCoordinatorAction::default())
    }

    #[tokio::test]
    async fn served_queue_from_str_defaults_to_fast() {
        assert_eq!(
            ServedQueue::from_str("notif_steady"),
            ServedQueue::NotifSteady
        );
        assert_eq!(
            ServedQueue::from_str("notif_batch"),
            ServedQueue::NotifBatch
        );
        assert_eq!(ServedQueue::from_str(""), ServedQueue::Fast);
        assert_eq!(ServedQueue::from_str("fast"), ServedQueue::Fast);
        assert_eq!(ServedQueue::from_str("garbage"), ServedQueue::Fast);
    }

    #[tokio::test]
    async fn notif_steady_pool_only_sees_notif_steady() {
        let metrics = OspreyCoordinatorMetrics::new();
        let (tx, rx) = create_ackable_action_priority_queue();

        // Put one action on async and one on notif-steady, with distinct names so
        // we can tell which one the notif-steady pool actually received.
        let (a_async, _r1) = AckableAction::new(proto::OspreyCoordinatorAction {
            action_name: "async_action".into(),
            ..Default::default()
        });
        let (a_steady, _r2) = AckableAction::new(proto::OspreyCoordinatorAction {
            action_name: "notif_steady_action".into(),
            ..Default::default()
        });
        tx.send(a_async, Channel::Async).await.unwrap();
        tx.send(a_steady, Channel::NotifSteady).await.unwrap();

        // A NotifSteady stream must receive ONLY the notif-steady action, never the async one.
        let got = rx
            .recv_for(ServedQueue::NotifSteady, metrics.clone())
            .await
            .unwrap();
        assert_eq!(got.action.action_name, "notif_steady_action");
        // async action is still queued (not drained by the notif-steady pool)
        assert_eq!(tx.len(Channel::Async), 1);
        assert_eq!(tx.len(Channel::NotifSteady), 0);
    }

    #[tokio::test]
    async fn fast_pool_prefers_sync_then_async() {
        let metrics = OspreyCoordinatorMetrics::new();
        let (tx, rx) = create_ackable_action_priority_queue();
        let (a_async, _r1) = ackable();
        let (a_sync, _r2) = ackable();
        tx.send(a_async, Channel::Async).await.unwrap();
        tx.send(a_sync, Channel::Sync).await.unwrap();
        // biased: sync drains first
        rx.recv_for(ServedQueue::Fast, metrics.clone())
            .await
            .unwrap();
        assert_eq!(tx.len(Channel::Sync), 0);
        assert_eq!(tx.len(Channel::Async), 1);
    }
}
