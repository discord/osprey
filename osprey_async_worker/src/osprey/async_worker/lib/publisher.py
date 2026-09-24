"""Async Pub/Sub publisher with batching for the async worker.

Uses asyncio.Queue for buffering and a background task for periodic
flushing. No threading locks, no gevent, no background threads.

Each flush is sent as one Pub/Sub request through the GAPIC client. The
`google.cloud.pubsub_v1.PublisherClient` wrapper is not used: it adds a second
batching layer that starts a commit thread per batch, so configuring it not to
interfere (`max_messages=1`) cost one thread and one RPC per message.
"""

import asyncio
import logging
from typing import List, Optional

from google.api_core.exceptions import (
    Aborted,
    Cancelled,
    DeadlineExceeded,
    InternalServerError,
    ResourceExhausted,
    RetryError,
    ServiceUnavailable,
    TooManyRequests,
    Unknown,
)
from google.api_core.retry import Retry, if_exception_type
from google.pubsub_v1.services.publisher import PublisherClient
from google.pubsub_v1.types import PubsubMessage
from osprey.worker.lib.instruments import metrics
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Keep a flush under the 10MB server limit on one publish request.
_MAX_BATCH_BYTES = 9_000_000

# Give up on the shutdown backlog after this long, so a Pub/Sub outage cannot
# hold a terminating pod open indefinitely.
_DRAIN_DEADLINE_SECONDS = 30.0

_TRANSIENT_PUBLISH_ERRORS = (
    TimeoutError,
    Aborted,
    Cancelled,
    DeadlineExceeded,
    InternalServerError,
    ResourceExhausted,
    ServiceUnavailable,
    TooManyRequests,
    Unknown,
)
_is_retryable_publish_error = if_exception_type(*_TRANSIENT_PUBLISH_ERRORS)


def _is_transient_publish_error(error: Exception) -> bool:
    if isinstance(error, RetryError):
        error = error.cause
    return _is_retryable_publish_error(error)


# Retry policy passed to the publish RPC. The explicit predicate includes the
# TimeoutError failures seen in production, and otherwise matches the set the
# library itself retries publish on (services/publisher/transports/base.py).
# The 30-second deadline gives the internal retry loop enough headroom for a
# few backoff attempts before we give up.  The publish timeout below is set
# slightly above deadline so the Retry loop, not the wall-clock cap, decides
# when to stop.
_PUBLISH_RETRY = Retry(
    predicate=_is_transient_publish_error,
    initial=0.5,
    maximum=10.0,
    multiplier=2.0,
    deadline=30.0,
)
_PUBLISH_TIMEOUT = 35.0


class AsyncPubSubPublisher:
    """Publishes Pydantic models to a Pub/Sub topic with async batching.

    Messages are buffered in an asyncio.Queue and flushed as one request when
    the batch reaches max_messages or when the queue runs dry, so batch size
    follows arrival rate rather than an added delay. max_latency_seconds bounds
    how long the flush task waits for the first message.
    """

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        max_messages: int = 250,
        max_latency_seconds: float = 1.0,
    ):
        self._topic_path = f'projects/{project_id}/topics/{topic_id}'
        self._client = PublisherClient()
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._max_messages = max_messages
        self._max_latency = max_latency_seconds
        self._flush_task: Optional[asyncio.Task[None]] = None
        self._started = False
        self._stopping = False
        self._metric_tags = [f'project:{project_id}', f'topic:{topic_id}']

    def _ensure_started(self) -> None:
        """Start the background flush task on first publish."""
        if not self._started:
            self._started = True
            try:
                loop = asyncio.get_running_loop()
                self._flush_task = loop.create_task(self._flush_loop())
            except RuntimeError:
                # No event loop. Messages will be enqueued but never flushed —
                # surface that explicitly so dashboards can catch it.
                metrics.increment('async_pubsub_publisher.no_event_loop', tags=self._metric_tags)

    async def _flush_loop(self) -> None:
        """Background task that flushes the buffer periodically.

        `_stopping`, not cancellation, is what ends this loop: on a cancel
        landing after `queue.get()` has produced a message, `wait_for` returns
        the message and consumes the CancelledError, so a loop watching only
        for cancellation would run on with `stop()` awaiting it forever.
        """
        while not self._stopping:
            try:
                # Wait for first message or timeout. asyncio.TimeoutError is
                # not a builtin TimeoutError subclass on <=3.10.
                try:
                    msg = await asyncio.wait_for(self._queue.get(), timeout=self._max_latency)
                except asyncio.TimeoutError:
                    continue
                await self._flush_batch(self._take_batch(msg))
            except Exception:
                logger.exception('Error in flush loop')

    def _take_batch(self, first: bytes) -> List[bytes]:
        """Drain up to max_messages, from an already-dequeued first message."""
        batch = [first]
        size = len(first)
        while len(batch) < self._max_messages and size < _MAX_BATCH_BYTES:
            try:
                data = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            batch.append(data)
            size += len(data)
        metrics.gauge('async_pubsub_publisher.queue_depth', self._queue.qsize(), tags=self._metric_tags)
        return batch

    async def _drain(self) -> None:
        """Publish what is still queued once the flush loop has stopped.

        Deadline-bounded rather than driven by a queue-size snapshot, because
        `_flush_batch` requeues transient failures and those have to be
        re-attempted rather than abandoned.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _DRAIN_DEADLINE_SECONDS
        while not self._queue.empty() and loop.time() < deadline:
            try:
                await self._flush_batch(self._take_batch(self._queue.get_nowait()))
            except Exception:
                logger.exception('Error draining publisher on shutdown')
                break
        residual = self._queue.qsize()
        if residual:
            logger.error('Dropping %d queued messages that could not be published before shutdown', residual)
            metrics.increment('async_pubsub_publisher.shutdown_dropped', value=residual, tags=self._metric_tags)

    async def _flush_batch(self, batch: List[bytes]) -> None:
        """Publish a batch of messages. Runs sync publishes in executor."""
        loop = asyncio.get_running_loop()
        flush_future = loop.run_in_executor(None, self._sync_flush, batch)
        try:
            retry_messages = await asyncio.shield(flush_future)
        except asyncio.CancelledError:
            retry_messages = await flush_future
            self._requeue(retry_messages)
            raise
        self._requeue(retry_messages)

    def _requeue(self, messages: List[bytes]) -> None:
        """Put transient publish failures back on the process-local queue."""
        for data in messages:
            self._queue.put_nowait(data)
            metrics.increment('async_pubsub_publisher.publish.retry_queued', tags=self._metric_tags)

    def _sync_flush(self, batch: List[bytes]) -> List[bytes]:
        """Synchronous batch publish. One request covers the whole batch, so
        its outcome is counted, logged and retried once per message it carried.
        """
        metrics.increment('async_pubsub_publisher.publish.attempt', value=len(batch), tags=self._metric_tags)
        # This histogram's own .count is the request rate.
        metrics.histogram('async_pubsub_publisher.messages_per_request', len(batch), tags=self._metric_tags)
        try:
            response = self._client.publish(
                topic=self._topic_path,
                messages=[PubsubMessage(data=data) for data in batch],
                retry=_PUBLISH_RETRY,
                timeout=_PUBLISH_TIMEOUT,
            )
        except Exception as e:
            metrics.increment(
                'async_pubsub_publisher.publish.failure',
                value=len(batch),
                tags=self._metric_tags + [f'error:{e.__class__.__name__}'],
            )
            if _is_transient_publish_error(e):
                logger.warning('Transient publish failure; requeuing %d messages', len(batch), exc_info=True)
                return batch
            logger.exception('Failed to publish %d messages', len(batch))
            return []

        # IDs come back in request order, so a short response means the tail
        # was never accepted and has to go back on the queue.
        published = len(response.message_ids)
        if published:
            metrics.increment('async_pubsub_publisher.publish.success', value=published, tags=self._metric_tags)
        if published == len(batch):
            return []
        metrics.increment(
            'async_pubsub_publisher.publish.failure',
            value=len(batch) - published,
            tags=self._metric_tags + ['error:PartialPublish'],
        )
        logger.warning('Pub/Sub accepted %d of %d messages; requeuing the rest', published, len(batch))
        return batch[published:]

    def publish(self, data: BaseModel) -> None:
        """Queue a Pydantic model for async batched publishing."""
        self.publish_bytes(data.json(exclude_none=True).encode())

    def publish_bytes(self, data: bytes) -> None:
        """Queue raw bytes for async batched publishing."""
        self._ensure_started()
        try:
            self._queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.warning('Publisher queue full, dropping message')
            metrics.increment('async_pubsub_publisher.queue_full', tags=self._metric_tags)

    async def stop(self) -> None:
        """Flush remaining messages and stop."""
        self._stopping = True
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self._drain()
