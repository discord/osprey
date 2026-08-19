"""Tests for AsyncPubSubPublisher."""

import asyncio
import threading
from unittest.mock import MagicMock, call, patch

import osprey.async_worker.lib.publisher as publisher_module
import pytest
from google.api_core.exceptions import DeadlineExceeded, NotFound, RetryError, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.cloud import pubsub_v1
from osprey.async_worker.lib.publisher import (
    _PUBLISH_RETRY,
    AsyncPubSubPublisher,
    AsyncPubSubPublisherPool,
    PublisherBatchSettings,
    _create_client,
    _InstrumentedPublisherClient,
)


def _make_publisher():
    """Return a publisher whose native client is always a mock."""
    client = MagicMock()
    with patch('osprey.async_worker.lib.publisher._create_client', return_value=client):
        publisher = AsyncPubSubPublisher(project_id='proj', topic_id='topic')
    assert publisher._state._client is client
    return publisher


@pytest.fixture(autouse=True)
def assert_live_client_count_unchanged():
    live_client_count = publisher_module._live_client_count
    yield
    assert publisher_module._live_client_count == live_client_count


@patch('osprey.async_worker.lib.publisher._InstrumentedPublisherClient', autospec=True)
def test_create_client_enables_bounded_native_batching(mock_client_class):
    _create_client(PublisherBatchSettings())

    mock_client_class.assert_called_once_with(
        batch_settings=pubsub_v1.types.BatchSettings(
            max_bytes=2_000_000,
            max_messages=250,
            max_latency=0.01,
        )
    )


@patch('osprey.async_worker.lib.publisher.metrics')
def test_gapic_publish_records_one_logical_transport_batch(mock_metrics):
    client = object.__new__(_InstrumentedPublisherClient)
    messages = [
        pubsub_v1.types.PubsubMessage(data=b'one'),
        pubsub_v1.types.PubsubMessage(data=b'two'),
    ]

    with patch.object(pubsub_v1.PublisherClient, '_gapic_publish', autospec=True) as base_publish:
        client._gapic_publish(
            topic='projects/proj/topics/topic',
            messages=messages,
            retry=_PUBLISH_RETRY,
            timeout=35,
        )

    base_publish.assert_called_once_with(
        client,
        topic='projects/proj/topics/topic',
        messages=messages,
        retry=_PUBLISH_RETRY,
        timeout=35,
    )
    tags = ['project:proj', 'topic:topic']
    assert mock_metrics.method_calls == [
        call.increment('async_pubsub_publisher.transport_batch', tags=tags),
        call.histogram('async_pubsub_publisher.messages_per_transport_batch', 2, tags=tags),
    ]


@patch('osprey.async_worker.lib.publisher.metrics')
def test_two_native_publish_calls_commit_as_one_logical_batch(mock_metrics):
    response = pubsub_v1.types.PublishResponse(message_ids=['id-one', 'id-two'])
    with patch.object(
        pubsub_v1.PublisherClient,
        '_gapic_publish',
        autospec=True,
        return_value=response,
    ) as base_publish:
        client = _InstrumentedPublisherClient(
            batch_settings=pubsub_v1.types.BatchSettings(
                max_bytes=2_000_000,
                # 2.15.2 treats reaching max_messages as overflow before
                # appending, so use room for both messages and stop to commit.
                max_messages=3,
                max_latency=60.0,
            ),
            credentials=AnonymousCredentials(),
        )
        first = client.publish('projects/proj/topics/topic', b'one', retry=_PUBLISH_RETRY)
        second = client.publish('projects/proj/topics/topic', b'two', retry=_PUBLISH_RETRY)

        client.stop()
        assert first.result(timeout=1) == 'id-one'
        assert second.result(timeout=1) == 'id-two'
        client.transport.close()

    base_publish.assert_called_once()
    assert base_publish.call_args.kwargs['topic'] == 'projects/proj/topics/topic'
    assert [message.data for message in base_publish.call_args.kwargs['messages']] == [b'one', b'two']
    mock_metrics.increment.assert_called_once_with(
        'async_pubsub_publisher.transport_batch',
        tags=['project:proj', 'topic:topic'],
    )
    mock_metrics.histogram.assert_called_once_with(
        'async_pubsub_publisher.messages_per_transport_batch',
        2,
        tags=['project:proj', 'topic:topic'],
    )


def _make_future(result=None, exc=None):
    future = MagicMock()
    if exc is not None:
        future.result.side_effect = exc
    else:
        future.result.return_value = result
    return future


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_immediate_publish_then_stop_drains_message_exactly_once(mock_metrics):
    client = MagicMock()
    client.publish.return_value = _make_future(result='message-id')
    pool = AsyncPubSubPublisherPool(client_factory=lambda _settings: client)
    publisher = pool.acquire('proj', 'topic')

    publisher.publish_bytes(b'immediate')
    await asyncio.gather(publisher.stop(), publisher.stop())
    await asyncio.gather(pool.stop(), pool.stop())

    client.publish.assert_called_once_with(
        'projects/proj/topics/topic',
        b'immediate',
        retry=_PUBLISH_RETRY,
    )
    client.stop.assert_called_once_with()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_pool_waits_for_detached_blocked_drain_before_client_stop(mock_metrics):
    client = MagicMock()
    pool = AsyncPubSubPublisherPool(client_factory=lambda _settings: client)
    publisher = pool.acquire('proj', 'topic')
    started = threading.Event()
    release = threading.Event()
    drain_finished = threading.Event()

    def stop_client():
        assert drain_finished.is_set()

    client.stop.side_effect = stop_client

    def blocked_sync_flush(batch):
        assert batch == [b'blocked']
        started.set()
        release.wait()
        drain_finished.set()
        return []

    publisher._state._sync_flush = blocked_sync_flush
    publisher.publish_bytes(b'blocked')
    lease_stop = asyncio.create_task(publisher.stop())
    assert await asyncio.to_thread(started.wait, 1.0)
    lease_stop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await lease_stop
    pool_stop = asyncio.create_task(pool.stop())
    await asyncio.sleep(0)

    client.stop.assert_not_called()
    release.set()
    await pool_stop
    await publisher.stop()
    client.stop.assert_called_once_with()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_cancelled_stop_waiter_does_not_cancel_shared_drain(mock_metrics):
    client = MagicMock()
    pool = AsyncPubSubPublisherPool(client_factory=lambda _settings: client)
    publisher = pool.acquire('proj', 'topic')
    started = threading.Event()
    release = threading.Event()

    def blocked_sync_flush(batch):
        started.set()
        release.wait()
        return []

    publisher._state._sync_flush = blocked_sync_flush
    publisher.publish_bytes(b'blocked')
    first_waiter = asyncio.create_task(publisher.stop())
    assert await asyncio.to_thread(started.wait, 1.0)
    first_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_waiter

    release.set()
    await publisher.stop()
    await pool.stop()
    client.stop.assert_called_once_with()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_pool_first_stop_rejects_new_work_and_later_lease_stop_is_safe(mock_metrics):
    client = MagicMock()
    pool = AsyncPubSubPublisherPool(client_factory=lambda _settings: client)
    publisher = pool.acquire('proj', 'topic')

    await pool.stop()

    with pytest.raises(RuntimeError, match='^publisher pool is stopped$'):
        pool.acquire('proj', 'other')
    with pytest.raises(RuntimeError, match='^publisher is stopped$'):
        publisher.publish_bytes(b'too late')
    await publisher.stop()
    client.stop.assert_called_once_with()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_queue_depth_is_not_recorded_per_enqueue(mock_metrics):
    publisher = _make_publisher()

    publisher.publish_bytes(b'queued')

    queue_calls = [
        call for call in mock_metrics.gauge.call_args_list if call.args[0] == 'async_pubsub_publisher.queue_depth'
    ]
    assert queue_calls == []
    await publisher.stop()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_queue_depth_is_recorded_once_per_outer_batch_and_requeue(mock_metrics):
    publisher = _make_publisher()
    state = publisher._state
    state._queue.put_nowait(b'two')

    assert state._take_outer_batch(b'one') == [b'one', b'two']
    state._requeue([b'retry-one', b'retry-two'])

    queue_calls = [
        call for call in mock_metrics.gauge.call_args_list if call.args[0] == 'async_pubsub_publisher.queue_depth'
    ]
    assert queue_calls == [
        call('async_pubsub_publisher.queue_depth', 0, tags=state._metric_tags),
        call('async_pubsub_publisher.queue_depth', 2, tags=state._metric_tags),
    ]
    while not state._queue.empty():
        state._queue.get_nowait()
    await publisher.stop()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_pool_shares_equal_topic_state_and_isolates_other_topics(mock_metrics):
    client = MagicMock()
    pool = AsyncPubSubPublisherPool(client_factory=lambda _settings: client)

    first = pool.acquire('proj', 'topic-a')
    second = pool.acquire('proj', 'topic-a')
    other = pool.acquire('proj', 'topic-b')

    assert first._state is second._state
    assert first._state is not other._state
    assert first._state._client is other._state._client is client
    await pool.stop()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_live_clients_is_a_process_running_count(mock_metrics, monkeypatch):
    monkeypatch.setattr(publisher_module, '_live_client_count', 0)
    first_client = MagicMock()
    second_client = MagicMock()
    first_pool = AsyncPubSubPublisherPool(client_factory=lambda _settings: first_client)
    second_pool = AsyncPubSubPublisherPool(client_factory=lambda _settings: second_client)

    first = first_pool.acquire('proj', 'first')
    second = second_pool.acquire('proj', 'second')
    await first.stop()
    await second.stop()
    await first_pool.stop()
    await second_pool.stop()

    live_calls = [
        call for call in mock_metrics.gauge.call_args_list if call.args[0] == 'async_pubsub_publisher.live_clients'
    ]
    assert live_calls == [
        call('async_pubsub_publisher.live_clients', 1),
        call('async_pubsub_publisher.live_clients', 2),
        call('async_pubsub_publisher.live_clients', 1),
        call('async_pubsub_publisher.live_clients', 0),
    ]


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_transient_batch_failure_requeues_every_affected_message(mock_metrics):
    publisher = _make_publisher()
    state = publisher._state
    transient = ServiceUnavailable('retry this logical batch')
    state._client.publish.side_effect = [
        _make_future(exc=transient),
        _make_future(exc=transient),
    ]

    retry_messages = state._sync_flush([b'one', b'two'])
    state._requeue(retry_messages)

    assert retry_messages == [b'one', b'two']
    assert state._queue.get_nowait() == b'one'
    assert state._queue.get_nowait() == b'two'
    assert (
        mock_metrics.increment.call_args_list.count(
            call('async_pubsub_publisher.publish.retry_queued', tags=state._metric_tags)
        )
        == 2
    )
    await publisher.stop()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_single_attempt_success(mock_metrics):
    publisher = _make_publisher()
    publisher._state._client.publish.return_value = _make_future(result='msg-id-1')

    publisher._state._sync_flush([b'hello'])

    publisher._state._client.publish.assert_called_once_with(
        publisher._state._topic_path,
        b'hello',
        retry=_PUBLISH_RETRY,
    )
    mock_metrics.increment.assert_any_call(
        'async_pubsub_publisher.publish.success',
        tags=publisher._state._metric_tags,
    )
    failure_calls = [c for c in mock_metrics.increment.call_args_list if 'failure' in c[0][0]]
    assert failure_calls == []
    await publisher.stop()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_permanent_failure_metric_fires(mock_metrics):
    publisher = _make_publisher()
    exc = NotFound('topic not found')
    publisher._state._client.publish.return_value = _make_future(exc=exc)

    retry_messages = publisher._state._sync_flush([b'data'])

    assert retry_messages == []
    failure_calls = [c for c in mock_metrics.increment.call_args_list if 'failure' in c[0][0]]
    assert len(failure_calls) == 1
    assert failure_calls[0][0][0] == 'async_pubsub_publisher.publish.failure'
    assert f'error:{exc.__class__.__name__}' in failure_calls[0][1]['tags']
    await publisher.stop()


def test_retry_policy_includes_observed_timeout_errors():
    assert _PUBLISH_RETRY._predicate(TimeoutError())
    assert _PUBLISH_RETRY._predicate(DeadlineExceeded('deadline exceeded'))


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_sync_flush_requeues_exhausted_transient_retry(mock_metrics):
    publisher = _make_publisher()
    publisher._state._client.publish.return_value = _make_future(
        exc=RetryError('deadline exceeded', DeadlineExceeded('retry me')),
    )

    retry_messages = publisher._state._sync_flush([b'retry'])

    assert retry_messages == [b'retry']
    await publisher.stop()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
@patch('osprey.async_worker.lib.publisher.logger')
async def test_sync_flush_returns_only_transient_failures(mock_logger, mock_metrics):
    publisher = _make_publisher()
    publisher._state._client.publish.side_effect = [
        _make_future(result='msg-id-1'),
        _make_future(exc=DeadlineExceeded('retry me')),
        _make_future(exc=NotFound('drop me')),
    ]

    retry_messages = publisher._state._sync_flush([b'success', b'retry', b'permanent'])

    assert retry_messages == [b'retry']
    mock_logger.warning.assert_called_once_with('Transient publish failure; requeuing', exc_info=True)
    await publisher.stop()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_flush_batch_requeues_transient_failures(mock_metrics):
    publisher = _make_publisher()
    publisher._state._client.publish.side_effect = [
        _make_future(result='msg-id-1'),
        _make_future(exc=DeadlineExceeded('retry me')),
    ]

    await publisher._state._flush_batch([b'success', b'retry'])

    assert publisher._state._queue.get_nowait() == b'retry'
    with pytest.raises(asyncio.QueueEmpty):
        publisher._state._queue.get_nowait()
    await publisher.stop()


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_flush_batch_finishes_in_flight_publish_before_cancellation(mock_metrics):
    publisher = _make_publisher()
    started = threading.Event()
    release = threading.Event()

    def sync_flush(batch):
        started.set()
        release.wait()
        return batch

    publisher._state._sync_flush = sync_flush
    flush_task = asyncio.create_task(publisher._state._flush_batch([b'retry']))
    await asyncio.to_thread(started.wait)

    flush_task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await flush_task
    assert publisher._state._queue.get_nowait() == b'retry'
    await publisher.stop()
