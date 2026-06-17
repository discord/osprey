"""Async output sink with timeout, bounded retry, and per-sink circuit breaking."""

import asyncio
import logging
from typing import Sequence

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.instruments import metrics

logger = logging.getLogger(__name__)


class _CircuitBreaker:
    """Per-sink consecutive-failure tracker.

    Opens after ``failure_threshold`` consecutive failed pushes and stays open
    for ``cooldown_seconds``, during which pushes to that sink are skipped. Any
    success — including the half-open probe taken once the cooldown elapses —
    closes it again. The breaker is the thing that stops one unhealthy downstream
    from stalling every shared stream: without it, each message keeps paying the
    full retry budget.
    """

    def __init__(self, failure_threshold: int, cooldown_seconds: float) -> None:
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._consecutive_failures = 0
        self._open_until = 0.0

    def is_open(self, now: float) -> bool:
        return now < self._open_until

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._open_until = 0.0

    def record_failure(self, now: float) -> bool:
        """Record a failed push; return True iff this failure opened the circuit."""
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._failure_threshold and now >= self._open_until:
            self._open_until = now + self._cooldown_seconds
            return True
        return False


class AsyncMultiOutputSink(AsyncBaseOutputSink):
    """Tees execution results to multiple async output sinks.

    The stream that drives this sink processes one message at a time, so a single
    push that blocks blocks ingestion. Each per-sink push is therefore bounded
    three independent ways:

    - a per-attempt timeout (``sink.timeout``) with capped exponential backoff,
    - an overall per-push deadline (``max_total_push_seconds``) across all
      attempts and backoff, and
    - a per-sink circuit breaker that skips a sink for a cooldown after repeated
      failures, so a sustained outage degrades to fast-skip instead of stalling.

    A fully-failed push never propagates: it is logged, counted, and trips the
    breaker, but the other sinks and the stream keep moving. While a sink's
    circuit is open its pushes are skipped outright — the effect is dropped, not
    queued — trading durability for keeping the stream alive during an outage.
    The deadline relies on ``sink.push`` honoring cancellation (grpc.aio does).
    """

    # Push-orchestration policy. Class attributes so a sink instance or subclass
    # can override them (e.g. in tests) without a constructor change.
    max_backoff_seconds: float = 1.0
    max_total_push_seconds: float = 10.0
    circuit_failure_threshold: int = 5
    circuit_cooldown_seconds: float = 5.0

    def __init__(self, sinks: Sequence[AsyncBaseOutputSink]):
        self._sinks = sinks
        self._breakers = [_CircuitBreaker(self.circuit_failure_threshold, self.circuit_cooldown_seconds) for _ in sinks]

    def will_do_work(self, result: ExecutionResult) -> bool:
        return any(sink.will_do_work(result) for sink in self._sinks)

    async def push(self, result: ExecutionResult) -> None:
        tasks = []
        for sink, breaker in zip(self._sinks, self._breakers):
            if sink.will_do_work(result):
                tasks.append(self._push_one(sink, breaker, result))
        if tasks:
            await asyncio.gather(*tasks)

    async def _push_one(self, sink: AsyncBaseOutputSink, breaker: _CircuitBreaker, result: ExecutionResult) -> None:
        """Push to a single sink within a bounded retry budget. Runs concurrently via gather()."""
        sink_name = sink.__class__.__name__
        loop = asyncio.get_running_loop()

        if breaker.is_open(loop.time()):
            metrics.increment('output_sink.circuit_open', tags=[f'sink:{sink_name}'])
            return

        attempts = sink.max_retries + 1
        deadline = loop.time() + self.max_total_push_seconds
        succeeded = False

        for attempt in range(1, attempts + 1):
            remaining = deadline - loop.time()
            if remaining <= 0:
                metrics.increment('output_sink.deadline_exhausted', tags=[f'sink:{sink_name}'])
                break

            try:
                start = loop.time()
                async with asyncio.timeout(min(sink.timeout, remaining)):
                    await sink.push(result)
                metrics.timing('handled_message_output', (loop.time() - start) * 1000, tags=[f'sink:{sink_name}'])
                succeeded = True
                break
            except TimeoutError:
                logger.warning(f'Timeout pushing to {sink_name} (attempt {attempt}/{attempts})')
                metrics.increment('output_sink.timeout', tags=[f'sink:{sink_name}'])
                if attempt == attempts:
                    metrics.increment('output_sink.timeout_exhausted', tags=[f'sink:{sink_name}'])
            except Exception as exc:
                logger.exception(f'Error pushing to {sink_name}: {exc}')
                metrics.increment('output_sink.error', tags=[f'sink:{sink_name}', f'error:{exc.__class__.__name__}'])
                if attempt == attempts:
                    break
                backoff = min(self.max_backoff_seconds, 0.5 * attempt, deadline - loop.time())
                if backoff <= 0:
                    break
                await asyncio.sleep(backoff)

        if succeeded:
            breaker.record_success()
        elif breaker.record_failure(loop.time()):
            metrics.increment('output_sink.circuit_opened', tags=[f'sink:{sink_name}'])
            logger.error(
                f'Circuit opened for {sink_name} after {self.circuit_failure_threshold} consecutive '
                f'failures; skipping pushes for {self.circuit_cooldown_seconds}s'
            )

    async def stop(self) -> None:
        for sink in self._sinks:
            await sink.stop()


class AsyncStdoutOutputSink(AsyncBaseOutputSink):
    """Debug output sink that prints to stdout."""

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        logger.info(f'result: {result.extracted_features_json} {result.verdicts}')

    async def stop(self) -> None:
        pass
