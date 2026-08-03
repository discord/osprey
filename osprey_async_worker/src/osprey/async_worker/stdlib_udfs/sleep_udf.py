"""Test-only async UDF that injects latency by awaiting asyncio.sleep.

Used to model a long-running UDF (e.g. an LLM classification call) locally so we
can exercise the windowed worker/coordinator path with realistic per-action latency.
"""

import asyncio

from osprey.async_worker.adaptor.interfaces import AsyncUDFBase
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr


class Arguments(ArgumentsBase):
    seconds: ConstExpr[float]


class SleepUdf(AsyncUDFBase[Arguments, bool]):  # type: ignore[misc]
    """Awaits asyncio.sleep(seconds), then returns True. Yields the event loop, so
    other in-flight actions on the same worker continue while this one is parked.
    Returns a bool so it can be used directly in a Rule's when_all."""

    category = UdfCategories.ENGINE

    @classmethod
    def _get_udf_base_args(cls):
        return (Arguments, bool)

    async def async_execute(self, execution_context: ExecutionContext, arguments: Arguments) -> bool:
        await asyncio.sleep(arguments.seconds.value)
        return True
