"""Tests for AsyncRulesSink parallel-in-flight mode.

Validates the architectural fix for slow async UDFs: instead of running
classify_one strictly sequentially per stream, the sink optionally dispatches
classify_one as tasks gated by a semaphore. This lifts throughput when the
input stream permits multi-in-flight (e.g. file/static streams, or a future
bidi protocol revision).

The tests deliberately use real wall-clock time (asyncio.sleep) so the
throughput claim is verified end-to-end, not just by inspecting code paths.
"""

import asyncio
import time
from datetime import datetime
from typing import Optional

import pytest
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.worker.sinks.utils.acking_contexts_base import NoopAckingContext

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.async_worker.sinks.sink.input_stream import AsyncStaticInputStream
from osprey.async_worker.sinks.sink.rules_sink import AsyncRulesRunner, AsyncRulesSink


class _RecordingSink(AsyncBaseOutputSink):
    def __init__(self) -> None:
        self.results: list = []

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.results.append(result)

    async def stop(self) -> None:
        pass


class _SlowClassifier(AsyncRulesRunner):
    """Stand-in for AsyncRulesRunner.classify_one that sleeps for `latency_s`.

    Bypasses the full engine machinery so we can test sink-level concurrency
    in isolation. The real classify_one's behavior is exercised in other tests.
    """

    def __init__(self, latency_s: float) -> None:
        self._latency_s = latency_s
        self._call_count = 0

    async def classify_one(self, action, tag, parent_tracer_span=None) -> Optional[ExecutionResult]:
        self._call_count += 1
        await asyncio.sleep(self._latency_s)
        return ExecutionResult(
            extracted_features={},
            action=action,
            effects={},
            error_infos=[],
            validator_results=None,
            sample_rate=100,
        )


def _make_actions(n: int) -> list:
    actions = []
    for i in range(n):
        actions.append(
            NoopAckingContext(
                Action(
                    action_id=i + 1,
                    action_name='test_action',
                    data={},
                    timestamp=datetime.utcnow(),
                )
            )
        )
    return actions


def _build_sink(items, max_in_flight: int, latency_s: float) -> AsyncRulesSink:
    sink = AsyncRulesSink.__new__(AsyncRulesSink)
    sink._input_stream = AsyncStaticInputStream(items)
    sink._rules_runner = _SlowClassifier(latency_s)
    sink._max_in_flight = max_in_flight
    sink._in_flight_count = 0
    sink._peak_in_flight = 0
    return sink


@pytest.mark.asyncio
async def test_sequential_mode_processes_actions_serially():
    """Default max_in_flight=1 — wall time ~= N * latency, peak in-flight = 1."""
    n, latency_s = 5, 0.05
    sink = _build_sink(_make_actions(n), max_in_flight=1, latency_s=latency_s)

    start = time.perf_counter()
    await sink.run()
    wall = time.perf_counter() - start

    assert sink.peak_in_flight <= 1, 'sequential mode must not parallelize'
    # Walltime should be at least N * latency. Allow some slack but require it's
    # substantially closer to serial than parallel.
    assert wall >= 0.9 * n * latency_s, f'sequential wall {wall} < expected {n * latency_s}'
    assert sink._rules_runner._call_count == n


@pytest.mark.asyncio
async def test_parallel_mode_lifts_throughput():
    """max_in_flight=N — N actions, latency L, wall ~= L (not N*L)."""
    n, latency_s = 10, 0.1
    sink = _build_sink(_make_actions(n), max_in_flight=n, latency_s=latency_s)

    start = time.perf_counter()
    await sink.run()
    wall = time.perf_counter() - start

    # Wall should be substantially less than N * latency. Use a 3× tolerance to
    # avoid flakiness on slow CI.
    sequential_wall = n * latency_s
    assert wall < sequential_wall / 3, (
        f'parallel wall {wall:.3f}s is not meaningfully faster than sequential {sequential_wall:.3f}s'
    )
    assert sink._rules_runner._call_count == n
    assert sink.peak_in_flight >= 2, f'expected peak in-flight >= 2, got {sink.peak_in_flight}'


@pytest.mark.asyncio
async def test_parallel_mode_respects_semaphore_cap():
    """max_in_flight=3 with 12 actions — peak in-flight should never exceed 3."""
    n, latency_s, cap = 12, 0.05, 3
    sink = _build_sink(_make_actions(n), max_in_flight=cap, latency_s=latency_s)

    await sink.run()

    assert sink.peak_in_flight <= cap, (
        f'peak in-flight {sink.peak_in_flight} exceeded cap {cap}'
    )
    assert sink._rules_runner._call_count == n


@pytest.mark.asyncio
async def test_parallel_mode_drains_pending_on_completion():
    """When the stream exhausts, all dispatched tasks must complete before run() returns."""
    n, latency_s = 10, 0.05
    sink = _build_sink(_make_actions(n), max_in_flight=n, latency_s=latency_s)

    await sink.run()

    # All actions classified, no tasks left orphaned.
    assert sink._rules_runner._call_count == n
    assert sink.in_flight == 0


@pytest.mark.asyncio
async def test_parallel_mode_isolates_task_errors():
    """A classify_one that raises must not kill the sink — other actions still process."""
    n = 5

    class _PartiallyFailingClassifier(_SlowClassifier):
        def __init__(self):
            super().__init__(0.01)
            self.fail_id = 3

        async def classify_one(self, action, tag, parent_tracer_span=None):
            self._call_count += 1
            await asyncio.sleep(0.01)
            if action.action_id == self.fail_id:
                raise RuntimeError(f'simulated failure for action {action.action_id}')
            return ExecutionResult(
                extracted_features={}, action=action, effects={},
                error_infos=[], validator_results=None, sample_rate=100,
            )

    sink = _build_sink(_make_actions(n), max_in_flight=n, latency_s=0.01)
    sink._rules_runner = _PartiallyFailingClassifier()

    await sink.run()

    # All five tasks should have been attempted (one raised, four succeeded).
    assert sink._rules_runner._call_count == n


@pytest.mark.asyncio
async def test_invalid_max_in_flight_rejected():
    """max_in_flight < 1 must raise — would silently break the run loop otherwise."""
    with pytest.raises(ValueError):
        AsyncRulesSink.__new__(AsyncRulesSink)._max_in_flight = 0  # set directly to bypass
        # The validation happens in __init__; we want to ensure that path is exercised.
        # Construct via the normal path using a minimal stub.
        # Since constructing AsyncRulesSink requires real engine/sinks, just instantiate
        # by calling __init__ with stubs to trigger the validation.
        from unittest.mock import MagicMock
        AsyncRulesSink(
            engine=MagicMock(),
            input_stream=AsyncStaticInputStream([]),
            output_sink=MagicMock(),
            udf_helpers=MagicMock(),
            max_in_flight=0,
        )
