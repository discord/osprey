"""Async Pub/Sub publisher with batching for the async worker.

Uses asyncio.Queue for buffering and a background task for periodic
flushing. Only the process-level client count uses a threading lock;
message buffering uses no locks, gevent, or custom threads.
"""

import asyncio
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from google.api_core.exceptions import (
    Aborted,
    DeadlineExceeded,
    InternalServerError,
    ResourceExhausted,
    RetryError,
    ServiceUnavailable,
    TooManyRequests,
)
from google.api_core.retry import Retry, if_exception_type
from google.cloud import pubsub_v1
from osprey.worker.lib.instruments import metrics
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_live_client_count = 0
_live_client_count_lock = threading.Lock()


def _change_live_client_count(delta: int) -> None:
    global _live_client_count
    with _live_client_count_lock:
        next_count = _live_client_count + delta
        if next_count < 0:
            raise RuntimeError('live Pub/Sub client count became negative')
        _live_client_count = next_count
        metrics.gauge(
            'async_pubsub_publisher.live_clients',
            _live_client_count,
        )


_TRANSIENT_PUBLISH_ERRORS = (
    TimeoutError,
    Aborted,
    DeadlineExceeded,
    InternalServerError,
    ResourceExhausted,
    ServiceUnavailable,
    TooManyRequests,
)
_is_retryable_publish_error = if_exception_type(*_TRANSIENT_PUBLISH_ERRORS)


def _is_transient_publish_error(error: Exception) -> bool:
    if isinstance(error, RetryError):
        error = error.cause
    return _is_retryable_publish_error(error)


# Retry policy passed to PublisherClient.publish(). The explicit predicate
# includes the TimeoutError and DeadlineExceeded failures seen in production.
# The 30-second deadline gives the internal retry loop enough headroom for a
# few backoff attempts before we give up.  The future.result() timeout below
# is set slightly above deadline so the Retry loop, not the wall-clock cap,
# decides when to stop.
_PUBLISH_RETRY = Retry(
    predicate=_is_transient_publish_error,
    initial=0.5,
    maximum=10.0,
    multiplier=2.0,
    deadline=30.0,
)


@dataclass(frozen=True)
class PublisherBatchSettings:
    max_bytes: int = 2_000_000
    max_messages: int = 250
    transport_max_latency_seconds: float = 0.01

    def google_settings(self) -> pubsub_v1.types.BatchSettings:
        return pubsub_v1.types.BatchSettings(
            max_bytes=self.max_bytes,
            max_messages=self.max_messages,
            max_latency=self.transport_max_latency_seconds,
        )


def _topic_metric_tags(topic_path: str) -> list[str]:
    parts = topic_path.split('/')
    if len(parts) != 4 or parts[0] != 'projects' or parts[2] != 'topics':
        return [f'topic_path:{topic_path}']
    return [f'project:{parts[1]}', f'topic:{parts[3]}']


# google-cloud-pubsub 2.15.2 does not expose typed PublisherClient metadata.
class _InstrumentedPublisherClient(pubsub_v1.PublisherClient):  # type: ignore[misc]
    """Meters logical GAPIC batches, not internal physical retry attempts."""

    def _gapic_publish(self, *args: Any, **kwargs: Any) -> Any:
        topic = kwargs['topic']
        messages = kwargs['messages']
        tags = _topic_metric_tags(topic)
        metrics.increment('async_pubsub_publisher.transport_batch', tags=tags)
        metrics.histogram(
            'async_pubsub_publisher.messages_per_transport_batch',
            len(messages),
            tags=tags,
        )
        return super()._gapic_publish(*args, **kwargs)


PublisherClientFactory = Callable[[PublisherBatchSettings], pubsub_v1.PublisherClient]


def _create_client(settings: PublisherBatchSettings) -> pubsub_v1.PublisherClient:
    return _InstrumentedPublisherClient(batch_settings=settings.google_settings())


class _PublisherState:
    def __init__(
        self,
        client: pubsub_v1.PublisherClient,
        project_id: str,
        topic_id: str,
        max_messages: int,
        max_latency: float,
    ) -> None:
        self._client = client
        self._topic_path = f'projects/{project_id}/topics/{topic_id}'
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._max_messages = max_messages
        self._max_latency = max_latency
        self._metric_tags = [f'project:{project_id}', f'topic:{topic_id}']
        self._flush_task: Optional[asyncio.Task[None]] = None
        self._stop_task: Optional[asyncio.Task[None]] = None
        self._started = False
        self._stopped = False

    def _ensure_started(self) -> None:
        if self._started:
            return
        self._started = True
        try:
            loop = asyncio.get_running_loop()
            self._flush_task = loop.create_task(self._flush_loop())
        except RuntimeError:
            metrics.increment(
                'async_pubsub_publisher.no_event_loop',
                tags=self._metric_tags,
            )

    async def _flush_loop(self) -> None:
        while True:
            try:
                batch: List[bytes] = []
                try:
                    batch.append(
                        await asyncio.wait_for(
                            self._queue.get(),
                            timeout=self._max_latency,
                        )
                    )
                except asyncio.TimeoutError:
                    continue
                while len(batch) < self._max_messages:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                await self._flush_batch(batch)
            except asyncio.CancelledError:
                remaining: List[bytes] = []
                while not self._queue.empty():
                    remaining.append(self._queue.get_nowait())
                if remaining:
                    await self._flush_batch(remaining)
                return
            except Exception:
                logger.exception('Error in flush loop')

    async def _flush_batch(self, batch: List[bytes]) -> None:
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
        for data in messages:
            self._queue.put_nowait(data)
            metrics.increment(
                'async_pubsub_publisher.publish.retry_queued',
                tags=self._metric_tags,
            )

    def _sync_flush(self, batch: List[bytes]) -> List[bytes]:
        futures = []
        for data in batch:
            metrics.increment(
                'async_pubsub_publisher.publish.attempt',
                tags=self._metric_tags,
            )
            future = self._client.publish(
                self._topic_path,
                data,
                retry=_PUBLISH_RETRY,
            )
            futures.append((data, future))

        retry_messages = []
        for data, future in futures:
            try:
                future.result(timeout=35)
                metrics.increment(
                    'async_pubsub_publisher.publish.success',
                    tags=self._metric_tags,
                )
            except Exception as error:
                metrics.increment(
                    'async_pubsub_publisher.publish.failure',
                    tags=self._metric_tags + [f'error:{error.__class__.__name__}'],
                )
                if _is_transient_publish_error(error):
                    logger.warning('Transient publish failure; requeuing', exc_info=True)
                    retry_messages.append(data)
                else:
                    logger.exception('Failed to publish message')
        return retry_messages

    def publish_bytes(self, data: bytes) -> None:
        if self._stopped:
            raise RuntimeError('publisher is stopped')
        self._ensure_started()
        try:
            self._queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.warning('Publisher queue full, dropping message')
            metrics.increment(
                'async_pubsub_publisher.queue_full',
                tags=self._metric_tags,
            )

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass


class AsyncPubSubPublisherPool:
    def __init__(
        self,
        settings: PublisherBatchSettings = PublisherBatchSettings(),
        outer_max_latency_seconds: float = 1.0,
        client_factory: Optional[PublisherClientFactory] = None,
    ) -> None:
        self._settings = settings
        self._outer_max_latency_seconds = outer_max_latency_seconds
        # Resolve at construction time so tests can patch `_create_client`.
        self._client_factory = client_factory or _create_client
        self._client: Optional[pubsub_v1.PublisherClient] = None
        self._states: dict[tuple[str, str], _PublisherState] = {}
        self._lease_counts: dict[tuple[str, str], int] = {}
        self._stopped = False
        self._client_stopped = False

    def _acquire_state(
        self,
        project_id: str,
        topic_id: str,
    ) -> tuple[tuple[str, str], _PublisherState]:
        if self._stopped:
            raise RuntimeError('publisher pool is stopped')
        if self._client is None:
            self._client = self._client_factory(self._settings)
            _change_live_client_count(1)
        key = (project_id, topic_id)
        if key not in self._states:
            self._states[key] = _PublisherState(
                self._client,
                project_id,
                topic_id,
                self._settings.max_messages,
                self._outer_max_latency_seconds,
            )
            self._lease_counts[key] = 0
        self._lease_counts[key] += 1
        return key, self._states[key]

    def acquire(self, project_id: str, topic_id: str) -> 'AsyncPubSubPublisher':
        key, state = self._acquire_state(project_id, topic_id)
        return AsyncPubSubPublisher._from_pool(self, key, state)

    async def _release(self, key: tuple[str, str], state: _PublisherState) -> None:
        if key not in self._lease_counts:
            return
        self._lease_counts[key] -= 1
        if self._lease_counts[key] != 0:
            return
        self._states.pop(key)
        self._lease_counts.pop(key)
        await state.stop()

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        states = list(self._states.values())
        self._states.clear()
        self._lease_counts.clear()
        try:
            await asyncio.gather(*(state.stop() for state in states))
        finally:
            await self._stop_client()

    async def _stop_client(self) -> None:
        if self._client is None or self._client_stopped:
            return
        self._client_stopped = True
        await asyncio.to_thread(self._client.stop)
        _change_live_client_count(-1)


class AsyncPubSubPublisher:
    """Publishes Pydantic models to a Pub/Sub topic with async batching."""

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        max_messages: int = 250,
        max_latency_seconds: float = 1.0,
    ) -> None:
        owned_pool = AsyncPubSubPublisherPool(
            settings=PublisherBatchSettings(max_messages=max_messages),
            outer_max_latency_seconds=max_latency_seconds,
        )
        key, state = owned_pool._acquire_state(project_id, topic_id)
        self._initialize_lease(owned_pool, key, state, owned_pool)

    @classmethod
    def _from_pool(
        cls,
        pool: AsyncPubSubPublisherPool,
        key: tuple[str, str],
        state: _PublisherState,
    ) -> 'AsyncPubSubPublisher':
        lease = cls.__new__(cls)
        lease._initialize_lease(pool, key, state, None)
        return lease

    def _initialize_lease(
        self,
        pool: AsyncPubSubPublisherPool,
        key: tuple[str, str],
        state: _PublisherState,
        owned_pool: Optional[AsyncPubSubPublisherPool],
    ) -> None:
        self._pool = pool
        self._key = key
        self._state = state
        self._owned_pool = owned_pool
        self._stopped = False
        self._stop_task: Optional[asyncio.Task[None]] = None

    def _assert_running(self) -> None:
        if self._stopped or self._state._stopped or self._pool._stopped:
            raise RuntimeError('publisher is stopped')

    def publish(self, data: BaseModel) -> None:
        self.publish_bytes(data.json(exclude_none=True).encode())

    def publish_bytes(self, data: bytes) -> None:
        self._assert_running()
        self._state.publish_bytes(data)

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._owned_pool is None:
            await self._pool._release(self._key, self._state)
            return
        try:
            await self._pool._release(self._key, self._state)
        finally:
            await self._owned_pool.stop()
