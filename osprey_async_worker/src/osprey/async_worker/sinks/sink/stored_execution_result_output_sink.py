"""Async output sink that persists execution results to an execution result store."""

import asyncio

from osprey.async_worker.adaptor.interfaces import AsyncBaseOutputSink
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.storage.stored_execution_result import ExecutionResultStore, StoredExecutionResult


class AsyncStoredExecutionResultOutputSink(AsyncBaseOutputSink):
    """Persists every execution result to one `ExecutionResultStore`.

    Register one sink per store to write the same results to several backends.
    """

    # Store writes are blocking and may fail transiently, so they run off-loop with retries.
    timeout: float = 5.0
    max_retries: int = 2

    def __init__(self, storage_backend: ExecutionResultStore) -> None:
        self._storage_backend = storage_backend

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    async def push(self, result: ExecutionResult) -> None:
        await asyncio.to_thread(StoredExecutionResult.persist_from_execution_result, result, self._storage_backend)

    async def stop(self) -> None:
        pass
