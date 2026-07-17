"""Tests for windowed (concurrent) action consumption in the async worker.

Covers two layers:
* the coordinator bidi stream advertising its window via ClientDetails.max_outstanding_actions
* AsyncRulesSink's windowed processing loop, driven against a fake bidi stream that
  records send_ack_or_nack() calls (so acking can be asserted by ack_id, independent
  of processing order).
"""

import asyncio
from datetime import datetime
from typing import List, Tuple
from unittest.mock import MagicMock, patch

import pytest
from osprey.engine.executor.execution_context import Action, ExecutionResult

from osprey.async_worker.lib.coordinator_input_stream import (
    AsyncVerdictsAckingContext,
    OspreyCoordinatorBiDirectionalStream,
)
from osprey.async_worker.sinks.sink.input_stream import AsyncStaticInputStream
from osprey.async_worker.sinks.sink.rules_sink import AsyncRulesSink


def _make_action(action_id: int, action_name: str) -> Action:
    return Action(action_id=action_id, action_name=action_name, data={}, timestamp=datetime.utcnow())


def _make_result(action: Action) -> ExecutionResult:
    return ExecutionResult(
        extracted_features={},
        action=action,
        effects={},
        error_infos=[],
        validator_results=None,
        sample_rate=100,
    )


class RecordingBidiStream:
    """Fake bidi stream: records every send_ack_or_nack() call by ack_id."""

    def __init__(self) -> None:
        self.acks: List[Tuple[int, bool, object]] = []

    def send_ack_or_nack(self, ack_id: int, ack: bool = True, verdicts=None) -> None:
        self.acks.append((ack_id, ack, verdicts))


def _make_sink(input_stream: AsyncStaticInputStream, window: int) -> AsyncRulesSink:
    sink = AsyncRulesSink(
        engine=MagicMock(),
        input_stream=input_stream,
        output_sink=MagicMock(),
        udf_helpers=MagicMock(),
        window=window,
    )
    return sink


# --- OspreyCoordinatorBiDirectionalStream: Initial carries window ---


class _EmptyStub:
    """Fake gRPC stub whose bidi call yields no incoming actions, so _gen() drains
    cleanly after sending the Initial request."""

    def __init__(self, channel) -> None:
        pass

    def OspreyBidirectionalStream(self, request_iterator, timeout=None):
        async def _empty():
            return
            yield  # pragma: no cover - makes this an async generator

        return _empty()


@pytest.mark.asyncio
async def test_initial_request_carries_window() -> None:
    """ClientDetails.max_outstanding_actions must reflect the advertised window."""
    service = MagicMock()
    service.connection_address = 'localhost'
    service.grpc_port = 1234

    with patch('osprey.async_worker.lib.coordinator_input_stream.OspreyCoordinatorServiceStub', _EmptyStub):
        stream = OspreyCoordinatorBiDirectionalStream(
            client_id='client-1', channel=MagicMock(), service=service, window=5
        )
        async for _ in stream._gen():
            pass

    initial_request = stream._outgoing_queue.get_nowait()
    assert initial_request.action_request.initial.max_outstanding_actions == 5


@pytest.mark.asyncio
async def test_initial_request_defaults_to_single_flight() -> None:
    """window defaults to 1, matching legacy single-outstanding-action behavior."""
    service = MagicMock()
    service.connection_address = 'localhost'
    service.grpc_port = 1234

    with patch('osprey.async_worker.lib.coordinator_input_stream.OspreyCoordinatorServiceStub', _EmptyStub):
        stream = OspreyCoordinatorBiDirectionalStream(client_id='client-1', channel=MagicMock(), service=service)
        async for _ in stream._gen():
            pass

    initial_request = stream._outgoing_queue.get_nowait()
    assert initial_request.action_request.initial.max_outstanding_actions == 1


# --- AsyncRulesSink: windowed processing ---


@pytest.mark.asyncio
async def test_windowed_fast_actions_ack_without_waiting_for_slow_action() -> None:
    """A slow action must not block faster ones behind it from finishing and acking."""
    slow_event = asyncio.Event()
    bidi = RecordingBidiStream()

    actions = [_make_action(1, 'slow')] + [_make_action(i, f'fast{i}') for i in range(2, 6)]
    contexts = [AsyncVerdictsAckingContext(a, bidi, ack_id=a.action_id) for a in actions]

    sink = _make_sink(AsyncStaticInputStream(contexts), window=5)

    async def fake_classify_one(action, tag, parent_tracer_span=None):
        if action.action_name == 'slow':
            await slow_event.wait()
        return _make_result(action)

    sink._rules_runner.classify_one = fake_classify_one

    run_task = asyncio.create_task(sink.run())
    await asyncio.sleep(0.05)

    acked_ids = {ack_id for ack_id, _, _ in bidi.acks}
    assert acked_ids == {2, 3, 4, 5}, 'fast actions should ack while slow action is still in flight'

    slow_event.set()
    await run_task

    acked_ids = {ack_id for ack_id, _, _ in bidi.acks}
    assert acked_ids == {1, 2, 3, 4, 5}


@pytest.mark.asyncio
async def test_windowed_bounds_concurrency_to_window() -> None:
    """With window=2 and 5 blocking actions, at most 2 may be inside classify_one at once."""
    window = 2
    release_events = {i: asyncio.Event() for i in range(1, 6)}
    concurrent = 0
    max_concurrent = 0

    async def fake_classify_one(action, tag, parent_tracer_span=None):
        nonlocal concurrent, max_concurrent
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        await release_events[action.action_id].wait()
        concurrent -= 1
        return _make_result(action)

    bidi = RecordingBidiStream()
    actions = [_make_action(i, f'a{i}') for i in range(1, 6)]
    contexts = [AsyncVerdictsAckingContext(a, bidi, ack_id=a.action_id) for a in actions]

    sink = _make_sink(AsyncStaticInputStream(contexts), window=window)
    sink._rules_runner.classify_one = fake_classify_one

    run_task = asyncio.create_task(sink.run())
    await asyncio.sleep(0.05)
    assert max_concurrent == window, 'concurrency should reach the window bound, and no further'

    for event in release_events.values():
        event.set()
    await run_task

    assert max_concurrent == window


@pytest.mark.asyncio
async def test_windowed_acks_carry_each_actions_own_id_and_may_complete_out_of_order() -> None:
    """Each action acks exactly once, by its own ack_id, regardless of completion order."""
    bidi = RecordingBidiStream()
    # action 1 finishes last, 2 and 3 finish immediately - out-of-order completion.
    delays = {1: 0.05, 2: 0.0, 3: 0.0}
    actions = [_make_action(i, f'a{i}') for i in (1, 2, 3)]
    contexts = [AsyncVerdictsAckingContext(a, bidi, ack_id=a.action_id) for a in actions]

    sink = _make_sink(AsyncStaticInputStream(contexts), window=3)

    async def fake_classify_one(action, tag, parent_tracer_span=None):
        await asyncio.sleep(delays[action.action_id])
        return _make_result(action)

    sink._rules_runner.classify_one = fake_classify_one

    await sink.run()

    ack_ids_in_order = [ack_id for ack_id, _, _ in bidi.acks]
    assert sorted(ack_ids_in_order) == [1, 2, 3], 'each action ack exactly once'
    assert ack_ids_in_order[:2] == [2, 3], 'faster actions ack before the slower one, out of enqueue order'
    assert all(ack for _, ack, _ in bidi.acks), 'all actions succeeded, so all acks (not nacks)'


@pytest.mark.asyncio
async def test_windowed_nacks_on_classify_error() -> None:
    """An action that raises during processing is nacked, not acked."""
    bidi = RecordingBidiStream()
    action = _make_action(1, 'boom')
    context = AsyncVerdictsAckingContext(action, bidi, ack_id=1)

    sink = _make_sink(AsyncStaticInputStream([context]), window=2)

    async def fake_classify_one(action, tag, parent_tracer_span=None):
        raise RuntimeError('boom')

    sink._rules_runner.classify_one = fake_classify_one

    with patch('osprey.async_worker.sinks.sink.rules_sink.sentry_sdk', MagicMock()):
        await sink.run()

    assert bidi.acks == [(1, False, None)]


# --- AsyncRulesSink: window<=1 preserves the legacy serial path ---


@pytest.mark.asyncio
async def test_serial_path_processes_and_acks_in_order() -> None:
    """window=1 (the default) must still process/ack serially, one at a time."""
    bidi = RecordingBidiStream()
    processed_order: List[int] = []
    actions = [_make_action(i, f'a{i}') for i in (1, 2, 3)]
    contexts = [AsyncVerdictsAckingContext(a, bidi, ack_id=a.action_id) for a in actions]

    sink = _make_sink(AsyncStaticInputStream(contexts), window=1)

    async def fake_classify_one(action, tag, parent_tracer_span=None):
        processed_order.append(action.action_id)
        return _make_result(action)

    sink._rules_runner.classify_one = fake_classify_one

    await sink.run()

    assert processed_order == [1, 2, 3]
    # The serial path acks via the context manager's __exit__ (a no-op for
    # AsyncVerdictsAckingContext), NOT via the fake bidi stream - acking the
    # real wire request is the input stream's job, not the sink's, in serial mode.
    assert bidi.acks == []
