"""Tests for AsyncPubSubPublisher."""

import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import Cancelled, DeadlineExceeded, NotFound, RetryError, ServiceUnavailable, Unknown
from google.auth.credentials import AnonymousCredentials
from google.pubsub_v1.services.publisher import PublisherClient
from google.pubsub_v1.types import PublishResponse
from osprey.async_worker.lib.publisher import (
    _MAX_BATCH_BYTES,
    _PUBLISH_RETRY,
    AsyncPubSubPublisher,
    _is_transient_publish_error,
)


def _make_publisher(**kwargs):
    """Return an AsyncPubSubPublisher with a mocked PublisherClient."""
    with patch('osprey.async_worker.lib.publisher.PublisherClient'):
        publisher = AsyncPubSubPublisher(project_id='proj', topic_id='topic', **kwargs)
    publisher._client = MagicMock()
    publisher._client.publish.return_value = PublishResponse(message_ids=[])
    return publisher


def _response(count):
    """A response acknowledging `count` messages."""
    return PublishResponse(message_ids=[f'msg-id-{i}' for i in range(count)])


def _published(publisher, call_index=0):
    """The message payloads of one publish call."""
    messages = publisher._client.publish.call_args_list[call_index].kwargs['messages']
    return [message.data for message in messages]


@patch('osprey.async_worker.lib.publisher.metrics')
def test_single_attempt_success(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.return_value = _response(1)

    publisher._sync_flush([b'hello'])

    assert publisher._client.publish.call_args.kwargs['topic'] == publisher._topic_path
    assert publisher._client.publish.call_args.kwargs['retry'] is _PUBLISH_RETRY
    assert _published(publisher) == [b'hello']
    mock_metrics.increment.assert_any_call(
        'async_pubsub_publisher.publish.success', value=1, tags=publisher._metric_tags
    )
    failure_calls = [c for c in mock_metrics.increment.call_args_list if 'failure' in c[0][0]]
    assert failure_calls == []


@patch('osprey.async_worker.lib.publisher.metrics')
def test_permanent_failure_metric_fires(mock_metrics):
    publisher = _make_publisher()
    exc = NotFound('topic not found')
    publisher._client.publish.side_effect = exc

    retry_messages = publisher._sync_flush([b'data'])

    assert retry_messages == []
    failure_calls = [c for c in mock_metrics.increment.call_args_list if 'failure' in c[0][0]]
    assert len(failure_calls) == 1
    assert failure_calls[0][0][0] == 'async_pubsub_publisher.publish.failure'
    assert f'error:{exc.__class__.__name__}' in failure_calls[0][1]['tags']


def test_retry_policy_includes_observed_timeout_errors():
    assert _PUBLISH_RETRY._predicate(TimeoutError())
    assert _PUBLISH_RETRY._predicate(DeadlineExceeded('deadline exceeded'))


@patch('osprey.async_worker.lib.publisher.metrics')
def test_sync_flush_requeues_exhausted_transient_retry(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.side_effect = RetryError('deadline exceeded', DeadlineExceeded('retry me'))

    retry_messages = publisher._sync_flush([b'retry'])

    assert retry_messages == [b'retry']


@patch('osprey.async_worker.lib.publisher.metrics')
@patch('osprey.async_worker.lib.publisher.logger')
def test_sync_flush_requeues_every_message_a_failed_request_carried(mock_logger, mock_metrics):
    """One request covers the whole batch, so its outcome applies to all of it."""
    publisher = _make_publisher()
    publisher._client.publish.side_effect = ServiceUnavailable('pub/sub blip')

    retry_messages = publisher._sync_flush([b'one', b'two'])

    assert retry_messages == [b'one', b'two']
    mock_metrics.increment.assert_any_call(
        'async_pubsub_publisher.publish.failure',
        value=2,
        tags=publisher._metric_tags + ['error:ServiceUnavailable'],
    )
    mock_logger.warning.assert_called_once()


@patch('osprey.async_worker.lib.publisher.metrics')
async def test_flush_batch_requeues_transient_failures(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.side_effect = DeadlineExceeded('retry me')

    await publisher._flush_batch([b'retry'])

    assert publisher._queue.get_nowait() == b'retry'
    with pytest.raises(asyncio.QueueEmpty):
        publisher._queue.get_nowait()


@patch('osprey.async_worker.lib.publisher.metrics')
async def test_flush_batch_finishes_in_flight_publish_before_cancellation(mock_metrics):
    publisher = _make_publisher()
    started = threading.Event()
    release = threading.Event()

    def sync_flush(batch):
        started.set()
        release.wait()
        return batch

    publisher._sync_flush = sync_flush
    flush_task = asyncio.create_task(publisher._flush_batch([b'retry']))
    await asyncio.to_thread(started.wait)

    flush_task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await flush_task
    assert publisher._queue.get_nowait() == b'retry'


@patch('osprey.async_worker.lib.publisher.metrics')
def test_whole_batch_is_published_as_one_request(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.return_value = _response(3)

    assert publisher._sync_flush([b'one', b'two', b'three']) == []

    publisher._client.publish.assert_called_once()
    assert _published(publisher) == [b'one', b'two', b'three']
    # Counters stay per message so the pre-change baseline stays comparable.
    mock_metrics.increment.assert_any_call(
        'async_pubsub_publisher.publish.attempt', value=3, tags=publisher._metric_tags
    )
    mock_metrics.histogram.assert_called_once_with(
        'async_pubsub_publisher.messages_per_request', 3, tags=publisher._metric_tags
    )


@patch('osprey.async_worker.lib.publisher.metrics')
def test_publishing_spawns_no_helper_threads(mock_metrics):
    """The point of the change: no per-message commit thread, one RPC per flush."""
    client = PublisherClient(credentials=AnonymousCredentials())
    with patch('osprey.async_worker.lib.publisher.PublisherClient', return_value=client):
        publisher = AsyncPubSubPublisher(project_id='proj', topic_id='topic')

    started = []
    real_start = threading.Thread.start
    with patch.object(PublisherClient, 'publish', autospec=True, return_value=_response(250)) as publish:
        with patch.object(threading.Thread, 'start', lambda self: started.append(self) or real_start(self)):
            try:
                assert publisher._sync_flush([b'payload'] * 250) == []
            finally:
                client.transport.close()

    assert len(publish.call_args.kwargs['messages']) == 250
    assert started == []


@patch('osprey.async_worker.lib.publisher.metrics')
def test_take_batch_stops_at_the_request_byte_limit(mock_metrics):
    """Pub/Sub rejects a request over 10MB, so a batch cannot grow past it."""
    publisher = _make_publisher()
    for _ in range(3):
        publisher._queue.put_nowait(b'x' * (_MAX_BATCH_BYTES // 2))

    batch = publisher._take_batch(publisher._queue.get_nowait())

    assert len(batch) == 2
    assert publisher._queue.qsize() == 1


@patch('osprey.async_worker.lib.publisher.metrics')
def test_partial_publish_requeues_the_unacknowledged_tail(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.return_value = _response(2)

    assert publisher._sync_flush([b'one', b'two', b'three']) == [b'three']

    mock_metrics.increment.assert_any_call(
        'async_pubsub_publisher.publish.failure',
        value=1,
        tags=publisher._metric_tags + ['error:PartialPublish'],
    )


def test_transient_set_matches_the_library_default_publish_retry_set():
    assert _is_transient_publish_error(Cancelled('cancelled'))
    assert _is_transient_publish_error(Unknown('unknown'))
    assert not _is_transient_publish_error(NotFound('topic not found'))


@patch('osprey.async_worker.lib.publisher.metrics')
async def test_stop_republishes_messages_requeued_during_the_drain(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.side_effect = [ServiceUnavailable('pub/sub blip'), _response(1)]
    publisher._queue.put_nowait(b'survive-shutdown')

    await publisher.stop()

    assert publisher._client.publish.call_count == 2
    assert publisher._queue.empty()


@patch('osprey.async_worker.lib.publisher.metrics')
async def test_stop_stops_retrying_at_the_drain_deadline_and_counts_the_loss(mock_metrics, monkeypatch):
    monkeypatch.setattr('osprey.async_worker.lib.publisher._DRAIN_DEADLINE_SECONDS', 0.0)
    publisher = _make_publisher()
    publisher._queue.put_nowait(b'never-published')

    await publisher.stop()

    publisher._client.publish.assert_not_called()
    mock_metrics.increment.assert_any_call(
        'async_pubsub_publisher.shutdown_dropped', value=1, tags=publisher._metric_tags
    )


@patch('osprey.async_worker.lib.publisher.metrics')
async def test_flush_loop_exits_on_the_stop_flag_alone(mock_metrics):
    """`wait_for` can swallow the cancel, so the flag has to end the loop."""
    publisher = _make_publisher(max_latency_seconds=0.01)
    publisher._ensure_started()
    assert publisher._flush_task is not None

    publisher._stopping = True

    await asyncio.wait_for(publisher._flush_task, timeout=5)


@patch('osprey.async_worker.lib.publisher.metrics')
async def test_stop_drains_a_queue_the_flush_loop_never_ran_for(mock_metrics):
    """The task is cancelled before its first step, so stop() must drain."""
    publisher = _make_publisher(max_latency_seconds=5.0)
    publisher._client.publish.return_value = _response(1)
    publisher.publish_bytes(b'queued')

    await publisher.stop()

    publisher._client.publish.assert_called_once()
    assert _published(publisher) == [b'queued']
    assert publisher._queue.empty()
