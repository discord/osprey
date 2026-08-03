"""Minimal async worker CLI for Phase 0 validation.

No monkey patching. No gevent. Uses asyncio event loop.
Supports static rules files for testing without etcd.
"""

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import click
from osprey.engine.executor.execution_context import Action
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.lib.config import Config
from osprey.engine.ast.sources import Sources
from osprey.worker.lib.instruments import set_worker_type_tag
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.sources_provider_base import StaticSourcesProvider

from osprey.async_worker.engine import AsyncOspreyEngine
from osprey.worker.sinks.utils.acking_contexts_base import NoopAckingContext

from osprey.async_worker.lib.coordinator_input_stream import OspreyCoordinatorInputStream
from osprey.async_worker.sinks.sink.input_stream import AsyncBaseInputStream, AsyncStaticInputStream
from osprey.async_worker.sinks.sink.output_sink import AsyncStdoutOutputSink
from osprey.async_worker.sinks.sink.rules_sink import AsyncRulesSink

logger = get_logger(__name__)


def init_config() -> Config:
    config = CONFIG.instance()
    config.configure_from_env()
    set_worker_type_tag('async')
    return config


def bootstrap_stdlib_engine(rules_path: str) -> Tuple[AsyncOspreyEngine, UDFHelpers]:
    """Bootstrap engine with stdlib + first-party async-plugin UDFs.

    Uses bootstrap_async_udfs so async-native UDFs (MXLookup, SleepUdf) registered
    through the first-party async stdlib plugin shadow their sync counterparts.
    The sync stdlib register path did not expose async plugin UDFs, so a rule
    calling e.g. SleepUdf failed validation with an unknown-UDF error.
    """
    from osprey.async_worker.adaptor.plugin_manager import bootstrap_async_ast_validators, bootstrap_async_udfs

    udf_registry, udf_helpers = bootstrap_async_udfs()
    bootstrap_async_ast_validators()

    sources_provider = StaticSourcesProvider(sources=Sources.from_path(Path(rules_path)))

    engine = AsyncOspreyEngine(
        sources_provider=sources_provider,
        udf_registry=udf_registry,
    )

    return engine, udf_helpers


class AsyncFileInputStream(AsyncBaseInputStream):
    """Read actions from a JSON file. Each line is a JSON action object."""

    def __init__(self, path: str):
        self._path = path

    async def _gen(self):
        with open(self._path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                action = Action(
                    action_id=data.get('id', 0),
                    action_name=data.get('name', 'unknown'),
                    data=data.get('data', {}),
                    timestamp=datetime.now(timezone.utc),
                )
                yield NoopAckingContext(action)


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.option('--rules-path', type=click.Path(exists=True), required=True, help='Path to rules directory')
@click.option('--input-file', type=click.Path(exists=True), default=None, help='Path to JSONL input file')
@click.option('--max-concurrent', type=int, default=12, help='Max concurrent async UDF executions')
@click.option('--with-plugins', is_flag=True, default=False, help='Load all plugins (requires external services)')
def run(rules_path: str, input_file: Optional[str], max_concurrent: int, with_plugins: bool) -> None:
    """Run the async rules worker with a static rules file and optional input file."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    logger.info('Starting async osprey worker (Phase 0)')
    logger.info(f'Rules path: {rules_path}')
    logger.info(f'Max concurrent UDFs: {max_concurrent}')

    config = init_config()

    if with_plugins:
        from osprey.worker.lib.osprey_engine import bootstrap_engine_with_helpers

        sources_provider = StaticSourcesProvider(sources=Sources.from_path(Path(rules_path)))
        engine, udf_helpers = bootstrap_engine_with_helpers(sources_provider=sources_provider)
    else:
        engine, udf_helpers = bootstrap_stdlib_engine(rules_path)

    # Input stream
    if input_file:
        input_stream = AsyncFileInputStream(input_file)
    else:
        # No input — just validate the worker boots correctly
        input_stream = AsyncStaticInputStream([])

    # Output sink
    output_sink = AsyncStdoutOutputSink()

    # Build and run the async rules sink
    rules_sink = AsyncRulesSink(
        engine=engine,
        input_stream=input_stream,
        output_sink=output_sink,
        udf_helpers=udf_helpers,
        max_concurrent_udfs=max_concurrent,
    )

    async def _run():
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def _signal_handler():
            logger.info('Received shutdown signal')
            stop_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _signal_handler)

        sink_task = asyncio.create_task(rules_sink.run())

        # Wait for either the sink to finish or a shutdown signal
        done = asyncio.create_task(stop_event.wait())
        await asyncio.wait([sink_task, done], return_when=asyncio.FIRST_COMPLETED)

        if not sink_task.done():
            sink_task.cancel()
            try:
                await sink_task
            except asyncio.CancelledError:
                pass

        await rules_sink.stop()
        logger.info('Async worker shutdown complete')

    asyncio.run(_run())


@cli.command()
@click.option('--rules-path', type=click.Path(exists=True), required=True, help='Path to rules directory')
@click.option('--input-file', type=click.Path(exists=True), required=True, help='Path to JSONL input file')
@click.option('--max-concurrent', type=int, default=12, help='Max concurrent async UDF executions')
@click.option('--iterations', type=int, default=1000, help='Number of iterations to run')
@click.option('--warmup', type=int, default=50, help='Warmup iterations (not counted)')
def benchmark(rules_path: str, input_file: str, max_concurrent: int, iterations: int, warmup: int) -> None:
    """Benchmark the async executor vs the gevent executor.

    Runs both executors against the same rules and input data, then compares
    throughput and latency.
    """
    import time

    from osprey.async_worker.executor import execute as async_execute

    logging.basicConfig(level=logging.WARNING)
    config = init_config()
    engine, udf_helpers = bootstrap_stdlib_engine(rules_path)

    # Load actions
    actions = []
    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            actions.append(
                Action(
                    action_id=data.get('id', 0),
                    action_name=data.get('name', 'unknown'),
                    data=data.get('data', {}),
                    timestamp=datetime.now(timezone.utc),
                )
            )

    if not actions:
        click.echo('No actions found in input file')
        return

    click.echo(f'Loaded {len(actions)} actions, {iterations} iterations (+ {warmup} warmup)')
    click.echo(f'Rules: {rules_path}')
    click.echo()

    # --- Gevent executor (optional, for comparison) ---
    try:
        import gevent.pool
        from osprey.engine.executor.executor import execute as gevent_execute

        pool = gevent.pool.Pool(max_concurrent)

        for i in range(warmup):
            action = actions[i % len(actions)]
            gevent_execute(engine.execution_graph, udf_helpers, action, pool)

        start = time.perf_counter()
        for i in range(iterations):
            action = actions[i % len(actions)]
            gevent_execute(engine.execution_graph, udf_helpers, action, pool)
        gevent_elapsed = time.perf_counter() - start

        gevent_throughput = iterations / gevent_elapsed
        gevent_latency_ms = (gevent_elapsed / iterations) * 1000

        click.echo(f'Gevent Executor:')
        click.echo(f'  Total time:  {gevent_elapsed:.3f}s')
        click.echo(f'  Throughput:  {gevent_throughput:.1f} actions/sec')
        click.echo(f'  Avg latency: {gevent_latency_ms:.3f}ms')
        click.echo()
    except ImportError:
        click.echo('Gevent not available, skipping gevent benchmark')
        click.echo()
        gevent_throughput = None

    # --- Async executor ---
    async def run_async():
        for i in range(warmup):
            action = actions[i % len(actions)]
            await async_execute(engine.execution_graph, udf_helpers, action, max_concurrent=max_concurrent)

        start = time.perf_counter()
        for i in range(iterations):
            action = actions[i % len(actions)]
            await async_execute(engine.execution_graph, udf_helpers, action, max_concurrent=max_concurrent)
        return time.perf_counter() - start

    async_elapsed = asyncio.run(run_async())
    async_throughput = iterations / async_elapsed
    async_latency_ms = (async_elapsed / iterations) * 1000

    click.echo(f'Async Executor:')
    click.echo(f'  Total time:  {async_elapsed:.3f}s')
    click.echo(f'  Throughput:  {async_throughput:.1f} actions/sec')
    click.echo(f'  Avg latency: {async_latency_ms:.3f}ms')
    click.echo()

    # --- Comparison ---
    if gevent_throughput:
        ratio = async_throughput / gevent_throughput
        click.echo(f'Comparison:')
        click.echo(f'  Async/Gevent ratio: {ratio:.2f}x')
        if ratio > 1:
            click.echo(f'  Async is {((ratio - 1) * 100):.1f}% faster')
        elif ratio < 1:
            click.echo(f'  Async is {((1 - ratio) * 100):.1f}% slower')
    else:
        click.echo(f'  Same performance')


@cli.command(name='run-coordinator')
@click.option('--rules-path', type=click.Path(exists=True), required=True, help='Path to rules directory')
@click.option('--coordinator-address', default='127.0.0.1:19950', help='host:port of the osprey coordinator bidi gRPC')
@click.option('--window', type=int, default=1, help='Max actions outstanding per stream (>1 = windowed/concurrent)')
@click.option('--max-concurrent', type=int, default=18, help='Max concurrent async UDF executions per action')
@click.option('--client-id', default='local-test', help='Client id advertised to the coordinator')
def run_coordinator(rules_path: str, coordinator_address: str, window: int, max_concurrent: int, client_id: str) -> None:
    """Run the async worker against a live coordinator over the bidirectional gRPC stream.

    With --window > 1 the worker advertises a per-stream window and processes that many
    actions concurrently, so a long-running UDF parks its action without stalling the others.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    logger.info('Starting async osprey worker (coordinator mode)')
    logger.info(f'Rules path: {rules_path} | coordinator: {coordinator_address} | window: {window}')

    # grpc.aio channels bind to the event loop; the deployed worker runs under uvloop
    # (set here to match). Optional: only this coordinator-mode dev command needs it.
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        logger.warning('uvloop unavailable; grpc.aio may misbehave under stock asyncio')

    init_config()
    engine, udf_helpers = bootstrap_stdlib_engine(rules_path)

    async def _run() -> None:
        # Build the input stream inside the running loop: grpc.aio channels bind to the
        # event loop that is current when they are created, so constructing this eagerly
        # outside asyncio.run() attaches the channel to the wrong loop.
        input_stream = OspreyCoordinatorInputStream.from_direct_address(
            client_id=client_id,
            address=coordinator_address,
            window=window,
        )
        rules_sink = AsyncRulesSink(
            engine=engine,
            input_stream=input_stream,
            output_sink=AsyncStdoutOutputSink(),
            udf_helpers=udf_helpers,
            max_concurrent_udfs=max_concurrent,
            window=window,
        )

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def _signal_handler() -> None:
            logger.info('Received shutdown signal')
            stop_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _signal_handler)

        sink_task = asyncio.create_task(rules_sink.run())
        done = asyncio.create_task(stop_event.wait())
        await asyncio.wait([sink_task, done], return_when=asyncio.FIRST_COMPLETED)

        if not sink_task.done():
            sink_task.cancel()
            try:
                await sink_task
            except asyncio.CancelledError:
                pass

        await rules_sink.stop()
        logger.info('Async worker shutdown complete')

    asyncio.run(_run())


if __name__ == '__main__':
    cli()
