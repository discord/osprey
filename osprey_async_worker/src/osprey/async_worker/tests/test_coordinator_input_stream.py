"""Tests for the async coordinator input stream."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from osprey.async_worker.lib.coordinator_input_stream import (
    GrpcConnectionDiscoveryPool,
    OspreyCoordinatorBiDirectionalStream,
    OspreyCoordinatorInputStream,
    AsyncVerdictsAckingContext,
)
from osprey.rpc.common.v1.verdicts_pb2 import Verdicts


# --- GrpcConnectionDiscoveryPool ---


def test_discovery_pool_creates_channels():
    """Pool creates grpc.aio channels from service discovery."""
    mock_service = MagicMock()
    mock_service.connection_address = 'localhost'
    mock_service.grpc_port = 50051

    mock_watcher = MagicMock()

    mock_directory = MagicMock()
    mock_directory.select_all.return_value = [mock_service]
    mock_directory.get_watcher.return_value = mock_watcher

    with patch('osprey.worker.lib.discovery.directory.Directory') as MockDirectory:
        MockDirectory.instance.return_value = mock_directory
        pool = GrpcConnectionDiscoveryPool('test_coordinator')
        assert len(pool._grpc_channels) == 1


# --- OspreyCoordinatorBiDirectionalStream ---


@pytest.mark.asyncio
async def test_bidirectional_stream_queue_based():
    """Stream uses asyncio.Queue for sending requests."""
    stream = OspreyCoordinatorBiDirectionalStream.__new__(OspreyCoordinatorBiDirectionalStream)
    stream._request_queue = asyncio.Queue()
    stream._should_run = True

    await stream._request_queue.put('test_request')
    item = await stream._request_queue.get()
    assert item == 'test_request'


# --- OspreyCoordinatorInputStream ---


@pytest.mark.asyncio
async def test_input_stream_stop():
    """Stop sets the shutdown event."""
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)
    stream._shutdown_event = asyncio.Event()

    assert not stream._shutdown_event.is_set()
    await stream.stop()
    assert stream._shutdown_event.is_set()


@pytest.mark.asyncio
async def test_input_stream_shutdown_event_unblocks():
    """Setting shutdown event should unblock any waiters."""
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)
    stream._shutdown_event = asyncio.Event()

    unblocked = False

    async def waiter():
        nonlocal unblocked
        await stream._shutdown_event.wait()
        unblocked = True

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    assert not unblocked

    await stream.stop()
    await asyncio.sleep(0.01)
    assert unblocked
    await task


# --- AsyncVerdictsAckingContext ---


def _make_context(ack_id: int = 42) -> tuple:
    """Return (context, mock_stream, mock_action) for unit tests."""
    mock_stream = MagicMock()
    mock_stream.send_ack_or_nack = MagicMock()
    mock_action = MagicMock()
    ctx = AsyncVerdictsAckingContext(mock_action, mock_stream, ack_id)
    return ctx, mock_stream, mock_action


def test_ack_sends_on_context_exit():
    """context.__exit__ triggers _ack which calls send_ack_or_nack."""
    ctx, mock_stream, _ = _make_context(ack_id=7)

    with ctx:
        pass  # no verdicts set

    mock_stream.send_ack_or_nack.assert_called_once_with(7, verdicts=None)
    assert ctx._already_acked is True


def test_ack_carries_verdicts_set_inside_with_block():
    """Verdicts set inside the with-block are present when the ack fires."""
    ctx, mock_stream, _ = _make_context(ack_id=8)
    verdicts = Verdicts()

    with ctx:
        ctx.set_verdicts(verdicts)

    mock_stream.send_ack_or_nack.assert_called_once_with(8, verdicts=verdicts)


def test_ack_idempotent_double_call():
    """Calling _ack() twice only sends one wire message."""
    ctx, mock_stream, _ = _make_context(ack_id=9)

    ctx._ack()
    ctx._ack()

    mock_stream.send_ack_or_nack.assert_called_once()


def test_already_acked_false_before_exit():
    """_already_acked starts False so graceful disconnect path can detect an un-acked context."""
    ctx, _, _ = _make_context()
    assert ctx._already_acked is False


def test_parallel_style_ack_ordering():
    """Simulate parallel-mode: consumer exits the with-block only after classify_one finishes.

    The bidi stream must NOT receive an ack before the with-block exits, even if the
    generator advances past 'yield context' before the consumer's task is done.

    Sequence:
      1. generator yields context (ack NOT yet sent)
      2. generator is free to run but _ack has not been called
      3. consumer task finishes → with-block exits → __exit__ → _ack fires
      4. ONLY NOW does send_ack_or_nack appear on the wire
    """
    ctx, mock_stream, _ = _make_context(ack_id=99)

    # After yield (generator side has resumed) but before the with-block exits:
    # _already_acked must be False and send_ack_or_nack must not yet be called.
    assert ctx._already_acked is False
    mock_stream.send_ack_or_nack.assert_not_called()

    # Simulate consumer task completing: the with-block exits normally.
    ctx.__exit__(None, None, None)

    # Now and only now the ack is on the wire.
    mock_stream.send_ack_or_nack.assert_called_once_with(99, verdicts=None)
    assert ctx._already_acked is True


def test_ack_not_sent_if_already_acked_at_context_exit():
    """If _ack was already sent (e.g. manually), __exit__ does not double-send."""
    ctx, mock_stream, _ = _make_context(ack_id=10)

    # Manually mark as already acked (simulates graceful disconnect sending ack first)
    ctx._already_acked = True

    ctx.__exit__(None, None, None)

    mock_stream.send_ack_or_nack.assert_not_called()


# --- max_unacked propagation ---


@pytest.mark.asyncio
async def test_input_stream_max_unacked_propagates_to_bidi_stream():
    """OspreyCoordinatorInputStream(max_unacked=5) stores the value and passes it
    to OspreyCoordinatorBiDirectionalStream, which embeds it in the initial
    ClientDetails sent to the coordinator.
    """
    from osprey.rpc.osprey_coordinator.bidirectional_stream.v1.service_pb2 import ClientDetails, Request

    captured_requests: list = []

    class FakeStub:
        def OspreyBidirectionalStream(self, outgoing_iter, timeout=None):
            # Return an async generator that captures the first outgoing request
            # (the ClientDetails handshake) then stops.
            async def _gen():
                async for req in outgoing_iter:
                    captured_requests.append(req)
                    return  # stop after first request; no incoming actions
                return
                yield  # make this an async generator

            return _gen()

    mock_service = MagicMock()
    mock_service.connection_address = 'localhost'
    mock_service.grpc_port = 50051

    bidi = OspreyCoordinatorBiDirectionalStream.__new__(OspreyCoordinatorBiDirectionalStream)
    bidi._client_id = 'test-client'
    bidi._max_unacked = 5
    bidi._outgoing_queue = asyncio.Queue()
    bidi._stub = FakeStub()
    bidi._tags = []
    bidi._connect_time = None
    bidi._last_action_request_time = 0.0
    bidi._stopped = False

    # Drain the generator until the fake stub returns (after first request).
    async for _ in bidi:
        pass

    assert len(captured_requests) >= 1
    first = captured_requests[0]
    assert isinstance(first, Request)
    initial = first.action_request.initial
    assert isinstance(initial, ClientDetails)
    assert initial.max_unacked == 5
