"""Async rules sink — the main processing loop for the async worker."""

import asyncio
import logging
from dataclasses import dataclass
from random import randint
from typing import Optional

import sentry_sdk
from ddtrace import tracer
from ddtrace.span import Span as TracerSpan
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import info_log_osprey_action
from osprey.worker.lib.snowflake import generate_snowflake
from osprey.worker.lib.sources_config.subkeys.action_config import ActionConfigs
from osprey.worker.sinks.utils.acking_contexts_base import BaseAckingContext, VerdictsAckingContext

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.async_worker.engine import AsyncOspreyEngine
from osprey.async_worker.executor import execute as async_execute
from osprey.async_worker.sinks.sink.input_stream import AsyncBaseInputStream

logger = logging.getLogger(__name__)


@dataclass
class SampleDecision:
    sample_rate: int
    drop: bool


_SAMPLE_NEVER = SampleDecision(sample_rate=100, drop=False)
_SAMPLE_ALWAYS = SampleDecision(sample_rate=0, drop=True)


class ActionSampler:
    """Checks whether an action should be sampled. No gevent dependency."""

    def __init__(self, engine: AsyncOspreyEngine):
        self._engine = engine

    def sample(self, action: Action) -> SampleDecision:
        action_configs = self._engine.get_config_subkey(ActionConfigs)
        action_config = action_configs.get_action_config(action.action_name)

        if not action_config or action_config.sample_rate == 100:
            return _SAMPLE_NEVER
        if action_config.sample_rate == 0:
            return _SAMPLE_ALWAYS

        p = randint(0, 99)
        should_drop = p < action_config.sample_rate
        return SampleDecision(sample_rate=action_config.sample_rate, drop=should_drop)


class AsyncRulesRunner:
    """Async version of RulesRunner — classifies one action and pushes to output sink."""

    def __init__(
        self,
        engine: AsyncOspreyEngine,
        output_sink: AsyncBaseOutputSink,
        udf_helpers: UDFHelpers,
        max_concurrent_udfs: int = 12,
    ) -> None:
        self._engine = engine
        self._sampler = ActionSampler(engine)
        self._output_sink = output_sink
        self._udf_helpers = udf_helpers
        self._max_concurrent_udfs = max_concurrent_udfs

    async def classify_one(
        self,
        action: Action,
        tag: str,
        parent_tracer_span: Optional[TracerSpan] = None,
    ) -> Optional[ExecutionResult]:
        sample_config = self._sampler.sample(action)
        tags = [
            tag,
            f'action:{action.action_name}',
            f'sample_rate:{sample_config.sample_rate}',
            f'rules_hash:{self._engine.execution_graph.validated_sources.sources.hash()}',
        ]

        if sample_config.drop:
            metrics.increment('dropped_message', tags=tags)
            return None

        result: Optional[ExecutionResult] = None
        try:
            with metrics.timed('handled_message', tags=tags, use_ms=True):
                result = await async_execute(
                    self._engine.execution_graph,
                    self._udf_helpers,
                    action,
                    max_concurrent=self._max_concurrent_udfs,
                    sample_rate=sample_config.sample_rate,
                    parent_tracer_span=parent_tracer_span,
                )
            with metrics.timed('handled_output', tags=tags, use_ms=True):
                await self._output_sink.push(result)
                info_log_osprey_action(action.action_id, action.action_name, 'pushed to output sink')
                return result
        except Exception:
            logging.exception('Error in classify_one for action %s', action.action_name)
            metrics.increment('rules_runner.classify_error', tags=tags)
            sentry_sdk.capture_exception()
            return result


class AsyncRulesSink:
    """Async rules sink — iterates an async input stream, executes rules, pushes to output sinks.

    The sink supports two operating modes:

    * ``max_in_flight=1`` (default): strict sequential — each action's ``classify_one``
      completes (including the ack inside ``message_context.__exit__``) before the next
      stream item is pulled. Preserves the protocol semantics of the
      coordinator bidi stream (one un-acked action per stream).

    * ``max_in_flight > 1``: classify_one runs as a Task, gated by a semaphore.
      The stream-read loop awaits the semaphore before pulling the next action, so
      input backpressure is preserved. Useful for input streams that do *not*
      enforce one-in-flight-per-stream upstream (e.g. file/static streams, or a
      future bidi protocol that permits multiple un-acked actions). For the
      coordinator bidi stream specifically, raising ``max_in_flight`` is a no-op
      until the coordinator side is extended — its protocol still sends one
      action per ack.
    """

    def __init__(
        self,
        engine: AsyncOspreyEngine,
        input_stream: AsyncBaseInputStream[BaseAckingContext[Action]],
        output_sink: AsyncBaseOutputSink,
        udf_helpers: UDFHelpers,
        max_concurrent_udfs: int = 12,
        max_in_flight: int = 1,
    ):
        if max_in_flight < 1:
            raise ValueError(f'max_in_flight must be >= 1, got {max_in_flight}')
        self._input_stream = input_stream
        self._rules_runner = AsyncRulesRunner(engine, output_sink, udf_helpers, max_concurrent_udfs)
        self._max_in_flight = max_in_flight
        self._in_flight_count = 0
        self._peak_in_flight = 0

    @property
    def in_flight(self) -> int:
        """Current number of actions being classified concurrently in this sink."""
        return self._in_flight_count

    @property
    def peak_in_flight(self) -> int:
        """High-water mark of concurrent in-flight actions since sink start."""
        return self._peak_in_flight

    async def run(self) -> None:
        if self._max_in_flight == 1:
            await self._run_sequential()
        else:
            await self._run_parallel()

    async def _run_sequential(self) -> None:
        async for message_context in self._input_stream:
            try:
                await self._process_one(message_context)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logging.exception('Unexpected error in async rules sink')
                metrics.increment('rules_sink.unexpected_error', tags=[f'err:{e.__class__.__name__}'])
                sentry_sdk.capture_exception(e)

    async def _run_parallel(self) -> None:
        semaphore = asyncio.Semaphore(self._max_in_flight)
        tasks: set = set()

        async def _wrapper(ctx: BaseAckingContext[Action]) -> None:
            try:
                self._in_flight_count += 1
                if self._in_flight_count > self._peak_in_flight:
                    self._peak_in_flight = self._in_flight_count
                metrics.gauge('rules_sink.in_flight_actions', self._in_flight_count)
                await self._process_one(ctx)
            except Exception as e:
                logging.exception('Unexpected error in parallel sink task')
                metrics.increment('rules_sink.unexpected_error', tags=[f'err:{e.__class__.__name__}'])
                sentry_sdk.capture_exception(e)
            finally:
                self._in_flight_count -= 1
                metrics.gauge('rules_sink.in_flight_actions', self._in_flight_count)
                semaphore.release()

        try:
            async for message_context in self._input_stream:
                # Block here when at capacity — provides upstream backpressure.
                await semaphore.acquire()
                task = asyncio.create_task(_wrapper(message_context))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        except asyncio.CancelledError:
            pass
        finally:
            # Drain in-flight tasks before returning so acks complete and we don't
            # cancel mid-classify_one (which would leave actions un-acked).
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_one(self, message_context: BaseAckingContext[Action]) -> None:
        with message_context as action:
            action_tags = [f'action:{action.action_name}']
            metrics.increment('rules_sink.input_action_received', tags=action_tags)

            if action.data.get('osprey_skip_async', False):
                metrics.increment('rules_sink.skipped', tags=action_tags)
                return

            with tracer.start_span('osprey.async.classify_one', child_of=None) as span:
                tracer.context_provider.activate(span.context)

                if not action.action_id and action.action_id != 0:
                    action.action_id = generate_snowflake(retries=3).to_int()

                info_log_osprey_action(action.action_id, action.action_name, 'beginning async classify_one')
                result = await self._rules_runner.classify_one(
                    action,
                    tag='sink:async-rules-sink',
                    parent_tracer_span=span,
                )

                if isinstance(message_context, VerdictsAckingContext):
                    if result is None:
                        metrics.increment('rules_sink.missing_result')
                    else:
                        message_context.set_verdicts(result.get_verdicts_pb2_proto())
                        metrics.increment('rules_sink.captured_verdicts')

                info_log_osprey_action(action.action_id, action.action_name, 'async classify_one complete')

    async def stop(self) -> None:
        await self._input_stream.stop()
