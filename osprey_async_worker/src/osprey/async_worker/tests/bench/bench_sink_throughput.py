"""Benchmark async sink throughput under slow UDFs.

Question:
    Does the current sequential `async for ... await classify_one` pattern in
    `AsyncRulesSink.run()` cap a single stream at `1 / udf_latency` actions/sec,
    regardless of asyncio cooperative-multitasking gains?

Compares four sink shapes:
    1. sequential   — current pattern (one in-flight at a time)
    2. parallel-N   — tasks gated by a semaphore of size N (configurable)
    3. unbounded    — every action becomes a task with no cap
    4. asyncio.to_thread — sequential but the "slow work" is sync code wrapped
       in `asyncio.to_thread`, which exercises the default thread pool

Each variant runs the same workload: `num_actions` actions, each "classified"
by a stub that awaits `asyncio.sleep(udf_latency_s)` (or `time.sleep` for the
to_thread variant). Reports wall-clock duration, p50/p95 per-action latency,
peak in-flight count.

Run:
    cd /home/discord/osprey
    python -m osprey.async_worker.tests.bench.bench_sink_throughput \\
        --num-actions 50 --udf-latency 0.5
"""

import argparse
import asyncio
import statistics
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, List


@dataclass
class Run:
    name: str
    wall_clock_s: float
    per_action_latencies_s: List[float] = field(default_factory=list)
    peak_in_flight: int = 0
    completed: int = 0

    @property
    def throughput(self) -> float:
        return self.completed / self.wall_clock_s if self.wall_clock_s > 0 else 0.0

    @property
    def p50(self) -> float:
        return statistics.median(self.per_action_latencies_s) if self.per_action_latencies_s else 0.0

    @property
    def p95(self) -> float:
        if not self.per_action_latencies_s:
            return 0.0
        sorted_lats = sorted(self.per_action_latencies_s)
        idx = max(0, int(len(sorted_lats) * 0.95) - 1)
        return sorted_lats[idx]


class InFlightTracker:
    """Tracks concurrent in-flight count via inc/dec, records peak."""

    def __init__(self) -> None:
        self.current = 0
        self.peak = 0

    def enter(self) -> None:
        self.current += 1
        if self.current > self.peak:
            self.peak = self.current

    def exit(self) -> None:
        self.current -= 1


async def _classify_async_sleep(udf_latency_s: float) -> None:
    await asyncio.sleep(udf_latency_s)


def _classify_sync_sleep(udf_latency_s: float) -> None:
    time.sleep(udf_latency_s)


# --- Sink shapes ---


async def run_sequential(
    num_actions: int,
    udf_latency_s: float,
    classify: Callable[[float], Awaitable[None]],
) -> Run:
    """Mirrors AsyncRulesSink.run() — async for + sequential await."""
    tracker = InFlightTracker()
    latencies: List[float] = []
    completed = 0
    start = time.perf_counter()

    async def stream():
        for _ in range(num_actions):
            yield None

    async for _ in stream():
        action_start = time.perf_counter()
        tracker.enter()
        try:
            await classify(udf_latency_s)
        finally:
            tracker.exit()
        latencies.append(time.perf_counter() - action_start)
        completed += 1

    wall = time.perf_counter() - start
    return Run(
        name='sequential',
        wall_clock_s=wall,
        per_action_latencies_s=latencies,
        peak_in_flight=tracker.peak,
        completed=completed,
    )


async def run_parallel_semaphore(
    num_actions: int,
    udf_latency_s: float,
    classify: Callable[[float], Awaitable[None]],
    max_in_flight: int,
) -> Run:
    """Candidate fix: tasks + bounded semaphore.

    Every yielded action becomes a Task. The semaphore caps in-flight count.
    The stream loop is allowed to read-ahead up to `max_in_flight` actions.
    """
    tracker = InFlightTracker()
    latencies: List[float] = []
    semaphore = asyncio.Semaphore(max_in_flight)
    start = time.perf_counter()

    async def stream():
        for _ in range(num_actions):
            yield None

    async def handle_one():
        action_start = time.perf_counter()
        async with semaphore:
            tracker.enter()
            try:
                await classify(udf_latency_s)
            finally:
                tracker.exit()
        latencies.append(time.perf_counter() - action_start)

    tasks: List[asyncio.Task] = []
    async for _ in stream():
        # NOTE: the create_task pattern means stream-reads aren't backpressured
        # by classify completion. The semaphore inside `handle_one` is what
        # actually bounds concurrency, but the queue depth between stream-read
        # and semaphore-acquire is unbounded. For a finite test stream this
        # is fine; in prod the bidi-stream protocol provides its own backpressure.
        tasks.append(asyncio.create_task(handle_one()))

    await asyncio.gather(*tasks)
    wall = time.perf_counter() - start
    return Run(
        name=f'parallel-{max_in_flight}',
        wall_clock_s=wall,
        per_action_latencies_s=latencies,
        peak_in_flight=tracker.peak,
        completed=len(latencies),
    )


async def run_unbounded(
    num_actions: int,
    udf_latency_s: float,
    classify: Callable[[float], Awaitable[None]],
) -> Run:
    """Worst-case baseline: no cap. Every action becomes a task immediately."""
    tracker = InFlightTracker()
    latencies: List[float] = []
    start = time.perf_counter()

    async def handle_one():
        action_start = time.perf_counter()
        tracker.enter()
        try:
            await classify(udf_latency_s)
        finally:
            tracker.exit()
        latencies.append(time.perf_counter() - action_start)

    tasks = [asyncio.create_task(handle_one()) for _ in range(num_actions)]
    await asyncio.gather(*tasks)
    wall = time.perf_counter() - start
    return Run(
        name='unbounded',
        wall_clock_s=wall,
        per_action_latencies_s=latencies,
        peak_in_flight=tracker.peak,
        completed=len(latencies),
    )


async def run_sequential_to_thread(
    num_actions: int,
    udf_latency_s: float,
) -> Run:
    """Sequential sink whose 'slow work' is sync code wrapped in asyncio.to_thread.

    Mirrors what UDFs like SendIpFraudRequest do — sync gRPC wrapped in a thread.
    Sequential pattern means thread-pool size shouldn't matter; this is the
    control to compare against parallel-to-thread.
    """
    tracker = InFlightTracker()
    latencies: List[float] = []
    start = time.perf_counter()

    for _ in range(num_actions):
        action_start = time.perf_counter()
        tracker.enter()
        try:
            await asyncio.to_thread(_classify_sync_sleep, udf_latency_s)
        finally:
            tracker.exit()
        latencies.append(time.perf_counter() - action_start)

    wall = time.perf_counter() - start
    return Run(
        name='seq-to-thread',
        wall_clock_s=wall,
        per_action_latencies_s=latencies,
        peak_in_flight=tracker.peak,
        completed=len(latencies),
    )


async def run_parallel_to_thread(
    num_actions: int,
    udf_latency_s: float,
    max_in_flight: int,
) -> Run:
    """Parallel sink with sync slow work wrapped in asyncio.to_thread.

    This exercises the ThreadPoolExecutor. Default size is min(32, cpu+4).
    Past that size, in-flight calls queue.
    """
    tracker = InFlightTracker()
    latencies: List[float] = []
    semaphore = asyncio.Semaphore(max_in_flight)
    start = time.perf_counter()

    async def handle_one():
        action_start = time.perf_counter()
        async with semaphore:
            tracker.enter()
            try:
                await asyncio.to_thread(_classify_sync_sleep, udf_latency_s)
            finally:
                tracker.exit()
        latencies.append(time.perf_counter() - action_start)

    tasks = [asyncio.create_task(handle_one()) for _ in range(num_actions)]
    await asyncio.gather(*tasks)
    wall = time.perf_counter() - start
    return Run(
        name=f'parallel-to-thread-{max_in_flight}',
        wall_clock_s=wall,
        per_action_latencies_s=latencies,
        peak_in_flight=tracker.peak,
        completed=len(latencies),
    )


# --- Driver ---


def fmt_run(r: Run) -> str:
    return (
        f'{r.name:35s} '
        f'wall={r.wall_clock_s:6.2f}s '
        f'tput={r.throughput:7.2f} a/s '
        f'p50={r.p50 * 1000:7.1f}ms '
        f'p95={r.p95 * 1000:7.1f}ms '
        f'peak_in_flight={r.peak_in_flight:4d}'
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-actions', type=int, default=50)
    parser.add_argument(
        '--udf-latency',
        type=float,
        default=0.5,
        help='Simulated UDF latency in seconds',
    )
    parser.add_argument(
        '--include-to-thread',
        action='store_true',
        help='Run the asyncio.to_thread variants too (slower).',
    )
    parser.add_argument(
        '--cap',
        type=int,
        nargs='*',
        default=[10, 50, 200],
        help='Semaphore values for parallel runs',
    )
    args = parser.parse_args()

    print(
        f'\nworkload: {args.num_actions} actions × {args.udf_latency:.2f}s simulated UDF '
        f'(theoretical-min wall = {args.udf_latency * args.num_actions:.2f}s sequential, '
        f'{args.udf_latency:.2f}s with full parallelism)\n'
    )

    runs: List[Run] = []

    runs.append(
        await run_sequential(args.num_actions, args.udf_latency, _classify_async_sleep)
    )
    for cap in args.cap:
        runs.append(
            await run_parallel_semaphore(
                args.num_actions, args.udf_latency, _classify_async_sleep, cap
            )
        )
    runs.append(
        await run_unbounded(args.num_actions, args.udf_latency, _classify_async_sleep)
    )

    if args.include_to_thread:
        runs.append(await run_sequential_to_thread(args.num_actions, args.udf_latency))
        for cap in args.cap:
            runs.append(
                await run_parallel_to_thread(args.num_actions, args.udf_latency, cap)
            )

    for r in runs:
        print(fmt_run(r))

    print()


if __name__ == '__main__':
    asyncio.run(main())
