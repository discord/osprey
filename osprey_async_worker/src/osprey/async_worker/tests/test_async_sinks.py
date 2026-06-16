"""Tests for async sink infrastructure."""

import asyncio
from datetime import datetime
from typing import List

import pytest
from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.async_worker.sinks.sink.input_stream import AsyncStaticInputStream
from osprey.async_worker.sinks.sink.output_sink import AsyncMultiOutputSink, AsyncStdoutOutputSink
from osprey.engine.executor.execution_context import Action, ExecutionResult


def _make_result(action_id: int = 1, action_name: str = 'test') -> ExecutionResult:
    return ExecutionResult(
        extracted_features={},
        action=Action(
            action_id=action_id,
            action_name=action_name,
            data={},
            timestamp=datetime.utcnow(),
        ),
        effects={},
        error_infos=[],
        validator_results=None,
        sample_rate=100,
    )


# --- AsyncStaticInputStream ---


@pytest.mark.asyncio
async def test_static_input_stream_yields_all_items():
    items = ['a', 'b', 'c']
    stream = AsyncStaticInputStream(items)
    collected = []
    async for item in stream:
        collected.append(item)
    assert collected == items


@pytest.mark.asyncio
async def test_static_input_stream_empty():
    stream = AsyncStaticInputStream([])
    collected = []
    async for item in stream:
        collected.append(item)
    assert collected == []


# --- AsyncMultiOutputSink ---


class RecordingSink(AsyncBaseOutputSink):
    """Test sink that records all pushed results."""

    def __init__(self):
        self.results: List[ExecutionResult] = []
        self.stopped = False

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.results.append(result)

    async def stop(self) -> None:
        self.stopped = True


class SelectiveSink(AsyncBaseOutputSink):
    """Test sink that only processes specific action names."""

    def __init__(self, allowed: str):
        self._allowed = allowed
        self.results: List[ExecutionResult] = []

    def will_do_work(self, result: ExecutionResult) -> bool:
        return result.action.action_name == self._allowed

    async def push(self, result: ExecutionResult) -> None:
        self.results.append(result)

    async def stop(self) -> None:
        pass


class FailingSink(AsyncBaseOutputSink):
    """Test sink that always raises."""

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        raise RuntimeError('sink failure')

    async def stop(self) -> None:
        pass


class SlowSink(AsyncBaseOutputSink):
    """Test sink that takes too long."""

    timeout = 0.05

    def __init__(self):
        self.attempted = False

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.attempted = True
        await asyncio.sleep(1.0)  # way longer than timeout

    async def stop(self) -> None:
        pass


@pytest.mark.asyncio
async def test_multi_sink_pushes_to_all():
    sink_a = RecordingSink()
    sink_b = RecordingSink()
    multi = AsyncMultiOutputSink([sink_a, sink_b])

    result = _make_result()
    await multi.push(result)

    assert len(sink_a.results) == 1
    assert len(sink_b.results) == 1


@pytest.mark.asyncio
async def test_multi_sink_respects_will_do_work():
    sink_a = SelectiveSink('action_a')
    sink_b = SelectiveSink('action_b')
    multi = AsyncMultiOutputSink([sink_a, sink_b])

    await multi.push(_make_result(action_name='action_a'))
    await multi.push(_make_result(action_name='action_b'))
    await multi.push(_make_result(action_name='action_c'))

    assert len(sink_a.results) == 1
    assert len(sink_b.results) == 1


@pytest.mark.asyncio
async def test_multi_sink_continues_after_failure():
    """A failing sink doesn't prevent other sinks from receiving results."""
    failing = FailingSink()
    recording = RecordingSink()
    multi = AsyncMultiOutputSink([failing, recording])

    await multi.push(_make_result())

    # Recording sink still got the result despite failing sink
    assert len(recording.results) == 1


@pytest.mark.asyncio
async def test_multi_sink_handles_timeout():
    """A slow sink times out without blocking other sinks."""
    slow = SlowSink()
    recording = RecordingSink()
    multi = AsyncMultiOutputSink([slow, recording])

    await multi.push(_make_result())

    assert slow.attempted is True
    assert len(recording.results) == 1


@pytest.mark.asyncio
async def test_multi_sink_stop():
    sink_a = RecordingSink()
    sink_b = RecordingSink()
    multi = AsyncMultiOutputSink([sink_a, sink_b])

    await multi.stop()

    assert sink_a.stopped is True
    assert sink_b.stopped is True


@pytest.mark.asyncio
async def test_stdout_sink_will_do_work():
    sink = AsyncStdoutOutputSink()
    assert sink.will_do_work(_make_result()) is True


# --- circuit breaker / bounded retry ---


class CountingFailingSink(AsyncBaseOutputSink):
    """Always fails; counts how many times push() is actually attempted."""

    circuit_breaker_threshold = 3
    circuit_breaker_cooldown_seconds = 30.0  # long: stays open for the test

    def __init__(self):
        self.push_count = 0

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.push_count += 1
        raise RuntimeError('downstream down')

    async def stop(self) -> None:
        pass


@pytest.mark.asyncio
async def test_circuit_breaker_opens_and_sheds():
    """After threshold consecutive failures the circuit opens and pushes are shed."""
    sink = CountingFailingSink()
    multi = AsyncMultiOutputSink([sink])

    for _ in range(10):
        await multi.push(_make_result())

    # Attempted exactly `threshold` times, then shed (not attempted) while open.
    assert sink.push_count == 3


@pytest.mark.asyncio
async def test_circuit_breaker_half_opens_after_cooldown():
    """Once the cooldown elapses the next push is attempted again (half-open)."""

    class FastCooldownSink(CountingFailingSink):
        circuit_breaker_threshold = 2
        circuit_breaker_cooldown_seconds = 0.05

    sink = FastCooldownSink()
    multi = AsyncMultiOutputSink([sink])

    await multi.push(_make_result())
    await multi.push(_make_result())  # opens after 2 failures
    await multi.push(_make_result())  # circuit open -> shed
    assert sink.push_count == 2

    await asyncio.sleep(0.06)  # cooldown elapses
    await multi.push(_make_result())  # half-open -> attempted again
    assert sink.push_count == 3


@pytest.mark.asyncio
async def test_circuit_breaker_resets_on_success():
    """A single success resets the consecutive-failure counter, so the circuit stays closed."""

    class ToggleSink(AsyncBaseOutputSink):
        circuit_breaker_threshold = 3

        def __init__(self):
            self.push_count = 0
            self.fail = True

        def will_do_work(self, result: ExecutionResult) -> bool:
            return True

        async def push(self, result: ExecutionResult) -> None:
            self.push_count += 1
            if self.fail:
                raise RuntimeError('boom')

        async def stop(self) -> None:
            pass

    sink = ToggleSink()
    multi = AsyncMultiOutputSink([sink])

    await multi.push(_make_result())  # fail 1
    await multi.push(_make_result())  # fail 2
    sink.fail = False
    await multi.push(_make_result())  # success -> counter resets to 0
    sink.fail = True
    await multi.push(_make_result())  # fail 1
    await multi.push(_make_result())  # fail 2 (threshold 3 never reached)

    # Every push was attempted; the circuit never opened.
    assert sink.push_count == 5


@pytest.mark.asyncio
async def test_total_push_budget_bounds_retry_time():
    """max_total_push_seconds caps cumulative retry time regardless of max_retries."""

    class BudgetedFailingSink(AsyncBaseOutputSink):
        max_retries = 200
        max_backoff_seconds = 0.01
        max_total_push_seconds = 0.05
        circuit_breaker_threshold = 0  # disable breaker to isolate the budget

        def will_do_work(self, result: ExecutionResult) -> bool:
            return True

        async def push(self, result: ExecutionResult) -> None:
            raise RuntimeError('boom')

        async def stop(self) -> None:
            pass

    sink = BudgetedFailingSink()
    multi = AsyncMultiOutputSink([sink])

    start = asyncio.get_running_loop().time()
    await multi.push(_make_result())  # would be ~2s (200 * 0.01) if unbounded
    elapsed = asyncio.get_running_loop().time() - start

    assert elapsed < 1.0  # budget (0.05s) stopped it well short of the uncapped time


@pytest.mark.asyncio
async def test_circuit_opens_mid_push_stops_retrying():
    """A retry-enabled sink stops retrying the instant the breaker opens, mid-push."""

    class RetryingFailingSink(CountingFailingSink):
        max_retries = 5  # would be 6 attempts if the breaker didn't cut it short
        circuit_breaker_threshold = 3

    sink = RetryingFailingSink()
    multi = AsyncMultiOutputSink([sink])

    await multi.push(_make_result())

    # The 3rd consecutive failure opens the circuit; remaining retries are shed
    # instead of holding the stream for the rest of max_total_push_seconds.
    assert sink.push_count == 3


@pytest.mark.asyncio
async def test_success_clears_circuit_open_state():
    """A successful push clears the open-until entry, fully closing the circuit."""

    class FlakySink(AsyncBaseOutputSink):
        circuit_breaker_threshold = 2
        circuit_breaker_cooldown_seconds = 0.02

        def __init__(self):
            self.push_count = 0
            self.fail = True

        def will_do_work(self, result: ExecutionResult) -> bool:
            return True

        async def push(self, result: ExecutionResult) -> None:
            self.push_count += 1
            if self.fail:
                raise RuntimeError('down')

        async def stop(self) -> None:
            pass

    sink = FlakySink()
    multi = AsyncMultiOutputSink([sink])
    key = id(sink)

    await multi.push(_make_result())  # fail 1
    await multi.push(_make_result())  # fail 2 -> opens
    assert key in multi._circuit_open_until

    await asyncio.sleep(0.03)  # cooldown elapses
    sink.fail = False
    await multi.push(_make_result())  # half-open probe succeeds

    # Success must fully close the circuit, not just reset the failure count.
    assert key not in multi._circuit_open_until
    assert multi._consecutive_failures[key] == 0
