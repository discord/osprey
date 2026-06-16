"""Async output sink with timeout, bounded retry, and a per-sink circuit breaker.

The retry path is deliberately bounded. A push runs on the (sequential) rules-sink
stream, so time spent retrying a single sink directly delays reading the next action
from the coordinator. An unbounded retry+backoff against a service-wide-down
downstream therefore backpressures the coordinator and stalls ingestion fleet-wide.
Two guards prevent that: a total per-push time budget, and a circuit breaker that
sheds pushes after repeated consecutive failures instead of making every message pay
the full retry cost.
"""

import asyncio
import logging
from typing import Dict, Sequence

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.instruments import metrics

logger = logging.getLogger(__name__)


class AsyncMultiOutputSink(AsyncBaseOutputSink):
    """Tees execution results to multiple async output sinks with timeout and retry."""

    def __init__(self, sinks: Sequence[AsyncBaseOutputSink]):
        self._sinks = sinks
        # Per-sink circuit-breaker state, keyed by id(sink). This instance is shared
        # across every AsyncRulesSink (concurrency stream) in the process, so a
        # service-wide downstream outage trips the breaker for all streams at once.
        self._consecutive_failures: Dict[int, int] = {}
        self._circuit_open_until: Dict[int, float] = {}

    def will_do_work(self, result: ExecutionResult) -> bool:
        return any(sink.will_do_work(result) for sink in self._sinks)

    async def push(self, result: ExecutionResult) -> None:
        tasks = []
        for sink in self._sinks:
            if sink.will_do_work(result):
                tasks.append(self._push_one(sink, result))
        if tasks:
            await asyncio.gather(*tasks)

    def _record_failure(self, sink: AsyncBaseOutputSink, sink_name: str) -> bool:
        """Bump the consecutive-failure count; open the circuit if it crosses the threshold.

        Returns True if the circuit is now open, so the caller can stop retrying
        immediately rather than holding the stream for the rest of the budget.
        """
        key = id(sink)
        failures = self._consecutive_failures.get(key, 0) + 1
        self._consecutive_failures[key] = failures
        threshold = sink.circuit_breaker_threshold
        # >= (not ==) so a failed half-open probe (failures already past the
        # threshold) re-opens the circuit instead of reverting to full retries.
        if threshold and failures >= threshold:
            self._circuit_open_until[key] = asyncio.get_running_loop().time() + sink.circuit_breaker_cooldown_seconds
            logger.warning(
                f'Circuit opened for {sink_name} after {failures} consecutive failures; '
                f'shedding pushes for {sink.circuit_breaker_cooldown_seconds}s'
            )
            metrics.increment('output_sink.circuit_opened', tags=[f'sink:{sink_name}'])
            return True
        return False

    async def _push_one(self, sink: AsyncBaseOutputSink, result: ExecutionResult) -> None:
        """Push to a single sink with timeout, bounded retry, and circuit breaking.

        Runs concurrently with the other sinks via gather().
        """
        sink_name = sink.__class__.__name__
        loop = asyncio.get_running_loop()
        key = id(sink)

        # Circuit open => fail fast (shed) so a down downstream can't hold the stream.
        if sink.circuit_breaker_threshold and loop.time() < self._circuit_open_until.get(key, 0.0):
            metrics.increment('output_sink.circuit_open', tags=[f'sink:{sink_name}'])
            return

        attempts = sink.max_retries + 1
        deadline = loop.time() + sink.max_total_push_seconds

        for attempt in range(1, attempts + 1):
            # Stop if the total per-push budget is exhausted (bounds even the
            # all-timeouts case, where no backoff sleep ever runs).
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                start = loop.time()
                async with asyncio.timeout(min(sink.timeout, remaining)):
                    await sink.push(result)
                metrics.timing('handled_message_output', (loop.time() - start) * 1000, tags=[f'sink:{sink_name}'])
                # Success fully closes the circuit — clear both the failure count
                # and any open-until set by an earlier attempt or a concurrent push.
                self._consecutive_failures[key] = 0
                self._circuit_open_until.pop(key, None)
                return
            except TimeoutError:
                logger.warning(f'Timeout pushing to {sink_name} (attempt {attempt}/{attempts})')
                metrics.increment('output_sink.timeout', tags=[f'sink:{sink_name}'])
                opened = self._record_failure(sink, sink_name)
                if attempt == attempts:
                    metrics.increment('output_sink.timeout_exhausted', tags=[f'sink:{sink_name}'])
                if opened:
                    break  # circuit just opened — shed rather than hold the stream
            except Exception as exc:
                logger.exception(f'Error pushing to {sink_name}: {exc}')
                metrics.increment('output_sink.error', tags=[f'sink:{sink_name}', f'error:{exc.__class__.__name__}'])
                opened = self._record_failure(sink, sink_name)
                if opened or attempt == attempts:
                    break  # circuit just opened (shed) or retries exhausted
                # Bounded backoff that never sleeps past the total push budget.
                backoff = min(0.5 * attempt, sink.max_backoff_seconds)
                if loop.time() + backoff >= deadline:
                    break
                await asyncio.sleep(backoff)

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
