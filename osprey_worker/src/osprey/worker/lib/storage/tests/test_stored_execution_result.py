from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.worker.lib.storage.stored_execution_result import (
    ExecutionResultPersistOutcome,
    ExecutionResultStorageService,
    ExecutionResultStore,
    ExecutionResultWrite,
)


def make_result(action_id: int) -> ExecutionResult:
    return ExecutionResult(
        extracted_features={'ActionName': 'message_sent', 'id': action_id},
        action=Action(
            action_id=action_id,
            action_name='message_sent',
            data={'id': action_id},
            timestamp=datetime(2026, 8, 19, 12, 0, action_id, tzinfo=timezone.utc),
        ),
        effects={},
        error_infos=[],
    )


class RecordingStore(ExecutionResultStore):
    def __init__(self) -> None:
        self.inserted: List[int] = []

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        return None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        return []

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        self.inserted.append(action_id)
        if action_id == 2:
            raise RuntimeError('row 2 failed')


def test_insert_many_default_fallback_preserves_order_and_errors() -> None:
    store = RecordingStore()
    writes = [ExecutionResultWrite.from_execution_result(make_result(i)) for i in (1, 2, 3)]

    outcomes: List[ExecutionResultPersistOutcome] = store.insert_many(writes)

    assert store.inserted == [1, 2, 3]
    assert [outcome.succeeded for outcome in outcomes] == [True, False, True]
    with pytest.raises(RuntimeError, match='row 2 failed'):
        outcomes[1].raise_for_error()


def test_service_persist_writes_preserves_input_order() -> None:
    store = RecordingStore()
    service = ExecutionResultStorageService(store)
    writes = [ExecutionResultWrite.from_execution_result(make_result(i)) for i in (3, 1)]

    outcomes = service.persist_writes(writes)

    assert store.inserted == [3, 1]
    assert all(outcome.succeeded for outcome in outcomes)


def test_service_execution_result_convenience_method_delegates_to_writes() -> None:
    store = RecordingStore()
    service = ExecutionResultStorageService(store)

    outcomes = service.persist_many_from_execution_results([make_result(2)])

    assert store.inserted == [2]
    assert outcomes[0].succeeded is False


def test_write_payload_size_counts_the_four_utf8_values() -> None:
    write = ExecutionResultWrite.from_execution_result(make_result(1))

    assert write.payload_size_bytes == sum(
        len(value.encode('utf-8'))
        for value in (
            write.extracted_features_json,
            write.error_traces_json,
            write.timestamp.isoformat(),
            write.action_data_json,
        )
    )
