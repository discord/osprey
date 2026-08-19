"""Tests for AsyncPubSubPublisher."""

import asyncio
import threading
from unittest.mock import MagicMock, call, patch

import pytest
from google.api_core.exceptions import DeadlineExceeded, NotFound, RetryError
from google.auth.credentials import AnonymousCredentials
from google.cloud import pubsub_v1
from osprey.async_worker.lib.publisher import (
    _PUBLISH_RETRY,
    AsyncPubSubPublisher,
    PublisherBatchSettings,
    _create_client,
    _InstrumentedPublisherClient,
)


def _make_publisher():
    """Return a publisher whose native client is always a mock."""
    client = MagicMock()
    with patch('osprey.async_worker.lib.publisher._create_client', return_value=client):
        publisher = AsyncPubSubPublisher(project_id='proj', topic_id='topic')
    assert publisher._client is client
    return publisher


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


@patch('osprey.async_worker.lib.publisher.metrics')
def test_single_attempt_success(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.return_value = _make_future(result='msg-id-1')

    publisher._sync_flush([b'hello'])

    publisher._client.publish.assert_called_once_with(publisher._topic_path, b'hello', retry=_PUBLISH_RETRY)
    mock_metrics.increment.assert_any_call('async_pubsub_publisher.publish.success', tags=publisher._metric_tags)
    failure_calls = [c for c in mock_metrics.increment.call_args_list if 'failure' in c[0][0]]
    assert failure_calls == []


@patch('osprey.async_worker.lib.publisher.metrics')
def test_permanent_failure_metric_fires(mock_metrics):
    publisher = _make_publisher()
    exc = NotFound('topic not found')
    publisher._client.publish.return_value = _make_future(exc=exc)

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
    publisher._client.publish.return_value = _make_future(
        exc=RetryError('deadline exceeded', DeadlineExceeded('retry me')),
    )

    retry_messages = publisher._sync_flush([b'retry'])

    assert retry_messages == [b'retry']


@patch('osprey.async_worker.lib.publisher.metrics')
@patch('osprey.async_worker.lib.publisher.logger')
def test_sync_flush_returns_only_transient_failures(mock_logger, mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.side_effect = [
        _make_future(result='msg-id-1'),
        _make_future(exc=DeadlineExceeded('retry me')),
        _make_future(exc=NotFound('drop me')),
    ]

    retry_messages = publisher._sync_flush([b'success', b'retry', b'permanent'])

    assert retry_messages == [b'retry']
    mock_logger.warning.assert_called_once_with('Transient publish failure; requeuing', exc_info=True)


@pytest.mark.asyncio
@patch('osprey.async_worker.lib.publisher.metrics')
async def test_flush_batch_requeues_transient_failures(mock_metrics):
    publisher = _make_publisher()
    publisher._client.publish.side_effect = [
        _make_future(result='msg-id-1'),
        _make_future(exc=DeadlineExceeded('retry me')),
    ]

    await publisher._flush_batch([b'success', b'retry'])

    assert publisher._queue.get_nowait() == b'retry'
    with pytest.raises(asyncio.QueueEmpty):
        publisher._queue.get_nowait()


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

    publisher._sync_flush = sync_flush
    flush_task = asyncio.create_task(publisher._flush_batch([b'retry']))
    await asyncio.to_thread(started.wait)

    flush_task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await flush_task
    assert publisher._queue.get_nowait() == b'retry'
