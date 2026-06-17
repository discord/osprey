"""Tests for async sink infrastructure."""

import asyncio
import time
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


# --- Bounded retry + circuit breaker (Mechanism 3: output-sink amplification) ---
#
# A down downstream (e.g. labels gRPC UNAVAILABLE during a node drain) must not
# be able to stall the stream, which processes one message at a time. These
# tests pin the three bounds: an overall per-push deadline, capped backoff, and
# a per-sink circuit breaker that degrades a sustained outage to fast-skip.


class _FastMulti(AsyncMultiOutputSink):
    """Same behavior, small time/threshold knobs so tests stay fast and deterministic."""

    max_total_push_seconds = 0.3
    max_backoff_seconds = 0.02
    circuit_failure_threshold = 3
    circuit_cooldown_seconds = 0.2


class CountingHangSink(AsyncBaseOutputSink):
    """Always hangs past its timeout; counts how many times push() is entered."""

    def __init__(self, timeout: float = 0.05, max_retries: int = 20):
        self.timeout = timeout
        self.max_retries = max_retries
        self.attempts = 0

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.attempts += 1
        await asyncio.sleep(10.0)

    async def stop(self) -> None:
        pass


class CountingFailSink(AsyncBaseOutputSink):
    """Always raises immediately; counts how many times push() is entered."""

    def __init__(self, max_retries: int = 0):
        self.max_retries = max_retries
        self.timeout = 1.0
        self.attempts = 0

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.attempts += 1
        raise RuntimeError('unavailable')

    async def stop(self) -> None:
        pass


class ProgrammableSink(AsyncBaseOutputSink):
    """Push fails while ``fail`` is True; records attempts and successes."""

    max_retries = 0
    timeout = 1.0

    def __init__(self) -> None:
        self.fail = True
        self.attempts = 0
        self.successes = 0

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        self.attempts += 1
        if self.fail:
            raise RuntimeError('unavailable')
        self.successes += 1

    async def stop(self) -> None:
        pass


@pytest.mark.asyncio
async def test_push_respects_total_deadline():
    """A hanging sink is bounded by max_total_push_seconds, not max_retries+1 timeouts."""
    sink = CountingHangSink(timeout=0.05, max_retries=50)
    multi = _FastMulti([sink])

    start = time.perf_counter()
    await multi.push(_make_result())
    elapsed = time.perf_counter() - start

    # Unbounded: 51 attempts * 0.05s = 2.55s. Bounded: ~max_total_push_seconds.
    assert elapsed < _FastMulti.max_total_push_seconds + sink.timeout + 0.2
    assert sink.attempts < sink.max_retries + 1  # deadline cut retries short


@pytest.mark.asyncio
async def test_backoff_is_capped():
    """Capped backoff keeps a raising sink's full retry budget small."""
    sink = CountingFailSink(max_retries=5)
    multi = _FastMulti([sink])

    start = time.perf_counter()
    await multi.push(_make_result())
    elapsed = time.perf_counter() - start

    # Unbounded backoff would be 0.5+1.0+1.5+2.0+2.5 = 7.5s. Capped: 5 * 0.02s.
    # Loose bound (still 7x+ below unbounded) so a slow CI box doesn't flake.
    assert elapsed < 1.0
    assert sink.attempts == sink.max_retries + 1


@pytest.mark.asyncio
async def test_circuit_opens_after_threshold_and_skips():
    """After N consecutive failures the breaker opens and stops calling the sink."""
    sink = CountingFailSink(max_retries=0)
    multi = _FastMulti([sink])

    for _ in range(_FastMulti.circuit_failure_threshold):
        await multi.push(_make_result())
    assert sink.attempts == _FastMulti.circuit_failure_threshold

    # Circuit is now open: the next push is skipped fast, sink.push not called.
    start = time.perf_counter()
    await multi.push(_make_result())
    elapsed = time.perf_counter() - start

    assert sink.attempts == _FastMulti.circuit_failure_threshold  # no new attempt (the real signal)
    assert elapsed < 0.1  # near-instant skip — the stall is broken


@pytest.mark.asyncio
async def test_circuit_half_opens_after_cooldown():
    """Once the cooldown elapses the breaker probes the sink again."""
    sink = CountingFailSink(max_retries=0)
    multi = _FastMulti([sink])

    for _ in range(_FastMulti.circuit_failure_threshold):
        await multi.push(_make_result())
    tripped = sink.attempts

    await multi.push(_make_result())  # skipped while open
    assert sink.attempts == tripped

    await asyncio.sleep(_FastMulti.circuit_cooldown_seconds + 0.05)
    await multi.push(_make_result())  # half-open probe
    assert sink.attempts == tripped + 1


@pytest.mark.asyncio
async def test_success_resets_failure_counter():
    """A success between failures prevents the breaker from opening prematurely."""
    sink = ProgrammableSink()
    multi = _FastMulti([sink])  # threshold = 3

    sink.fail = True
    await multi.push(_make_result())
    await multi.push(_make_result())  # 2 consecutive failures (< threshold)

    sink.fail = False
    await multi.push(_make_result())  # success resets the counter

    sink.fail = True
    await multi.push(_make_result())
    await multi.push(_make_result())  # 2 more consecutive failures (< threshold again)

    # Counter never reached 3 consecutive, so the circuit must still be closed:
    # the next push is actually attempted rather than skipped.
    attempts_before = sink.attempts
    await multi.push(_make_result())
    assert sink.attempts == attempts_before + 1


@pytest.mark.asyncio
async def test_circuit_breaker_is_per_sink():
    """An open circuit on one sink never short-circuits a healthy sibling."""
    dead = CountingFailSink(max_retries=0)
    healthy = RecordingSink()
    multi = _FastMulti([dead, healthy])  # threshold = 3

    for _ in range(_FastMulti.circuit_failure_threshold):
        await multi.push(_make_result())
    # dead has tripped; healthy got every message.
    assert healthy.results and len(healthy.results) == _FastMulti.circuit_failure_threshold

    # With dead's circuit open, healthy keeps receiving and dead is not called.
    await multi.push(_make_result())
    assert len(healthy.results) == _FastMulti.circuit_failure_threshold + 1
    assert dead.attempts == _FastMulti.circuit_failure_threshold


@pytest.mark.asyncio
async def test_healthy_sink_never_trips():
    """Repeated successful pushes don't open the breaker."""
    sink = RecordingSink()
    multi = _FastMulti([sink])

    for _ in range(_FastMulti.circuit_failure_threshold * 3):
        await multi.push(_make_result())

    assert len(sink.results) == _FastMulti.circuit_failure_threshold * 3


@pytest.mark.asyncio
async def test_circuit_closes_after_recovery():
    """A recovered sink's half-open probe succeeds and the circuit closes again."""
    sink = ProgrammableSink()
    multi = _FastMulti([sink])  # threshold = 3, cooldown = 0.2

    sink.fail = True
    for _ in range(_FastMulti.circuit_failure_threshold):
        await multi.push(_make_result())
    tripped = sink.attempts

    await multi.push(_make_result())  # skipped while open
    assert sink.attempts == tripped

    # Downstream recovers; after the cooldown the half-open probe should succeed.
    sink.fail = False
    await asyncio.sleep(_FastMulti.circuit_cooldown_seconds + 0.05)
    await multi.push(_make_result())  # half-open probe -> success -> close
    assert sink.successes == 1
    probe_attempts = sink.attempts

    # Circuit is closed again: subsequent pushes flow normally (not skipped).
    for _ in range(3):
        await multi.push(_make_result())
    assert sink.attempts == probe_attempts + 3
    assert sink.successes == 4


@pytest.mark.asyncio
async def test_push_never_raises_when_all_sinks_fail():
    """A push where every sink fails is swallowed, never breaking the stream loop."""
    multi = _FastMulti([CountingFailSink(max_retries=1), CountingHangSink(timeout=0.02, max_retries=1)])
    await multi.push(_make_result())  # must return without raising
