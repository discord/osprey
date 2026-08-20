from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, call

import pytest
from google.rpc import code_pb2
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.worker.lib.storage import stored_execution_result as stored_result_module
from osprey.worker.lib.storage.stored_execution_result import (
    ExecutionResultPersistOutcome,
    ExecutionResultStorageService,
    ExecutionResultStore,
    ExecutionResultWrite,
    StoredExecutionResultBigTable,
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


def test_write_from_execution_result_preserves_serialized_fields() -> None:
    write = ExecutionResultWrite.from_execution_result(make_result(1))

    assert write == ExecutionResultWrite(
        action_id=1,
        extracted_features_json='{"ActionName": "message_sent", "id": 1}',
        error_traces_json='[]',
        timestamp=datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc),
        action_data_json='{"id": 1}',
    )


def test_write_payload_size_counts_non_ascii_utf8_bytes() -> None:
    write = ExecutionResultWrite(
        action_id=1,
        extracted_features_json='"é"',
        error_traces_json='"€"',
        timestamp=datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc),
        action_data_json='"🦅"',
    )

    assert write.payload_size_bytes == 40


def status(code: int, message: str = '') -> MagicMock:
    return MagicMock(code=code, message=message)


def test_bigtable_single_and_bulk_use_identical_row_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    table = MagicMock()
    first_row = MagicMock(row_key=b'prefix:1')
    second_row = MagicMock(row_key=b'prefix:1')
    first_row.get_mutations_size.return_value = 128
    second_row.get_mutations_size.return_value = 128
    table.row.side_effect = [first_row, second_row]
    table.mutate_rows.side_effect = [[status(code_pb2.OK)], [status(code_pb2.OK)]]
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', lambda _name: table)
    store = StoredExecutionResultBigTable()
    write = ExecutionResultWrite.from_execution_result(make_result(1))

    store.insert(
        action_id=write.action_id,
        extracted_features_json=write.extracted_features_json,
        error_traces_json=write.error_traces_json,
        timestamp=write.timestamp,
        action_data_json=write.action_data_json,
    )
    store.insert_many([write])

    expected_cells = [
        call(
            'execution_result',
            b'extracted_features',
            write.extracted_features_json.encode(),
            timestamp=write.timestamp,
        ),
        call(
            'execution_result',
            b'error_traces',
            write.error_traces_json.encode(),
            timestamp=write.timestamp,
        ),
        call(
            'execution_result',
            b'timestamp',
            write.timestamp.isoformat().encode(),
            timestamp=write.timestamp,
        ),
        call('execution_result', b'action_data', write.action_data_json.encode(), timestamp=write.timestamp),
    ]
    assert first_row.set_cell.call_args_list == expected_cells
    assert second_row.set_cell.call_args_list == expected_cells
    assert table.row.call_args_list[0] == table.row.call_args_list[1]


def test_bigtable_bulk_sends_one_request_and_aligns_partial_statuses(monkeypatch: pytest.MonkeyPatch) -> None:
    table = MagicMock()
    rows = [MagicMock(), MagicMock(), MagicMock()]
    for row in rows:
        row.get_mutations_size.return_value = 128
    table.row.side_effect = rows
    table.mutate_rows.return_value = [
        status(code_pb2.OK),
        status(code_pb2.UNAVAILABLE, 'retry exhausted'),
        status(code_pb2.OK),
    ]
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', lambda _name: table)

    outcomes = StoredExecutionResultBigTable().insert_many(
        [ExecutionResultWrite.from_execution_result(make_result(i)) for i in (1, 2, 3)]
    )

    table.mutate_rows.assert_called_once_with(rows, retry=StoredExecutionResultBigTable.retry_policy)
    assert [outcome.succeeded for outcome in outcomes] == [True, False, True]
    with pytest.raises(RuntimeError, match='code=14.*retry exhausted'):
        outcomes[1].raise_for_error()


def test_bigtable_transport_error_maps_to_every_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    table = MagicMock()
    rows = [MagicMock(row_key=b'prefix:1'), MagicMock(row_key=b'prefix:2')]
    for row in rows:
        row.get_mutations_size.return_value = 128
    table.row.side_effect = rows
    table.mutate_rows.side_effect = RuntimeError('transport down')
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', lambda _name: table)

    outcomes = StoredExecutionResultBigTable().insert_many(
        [ExecutionResultWrite.from_execution_result(make_result(i)) for i in (1, 2)]
    )

    assert [str(outcome.error) for outcome in outcomes] == ['transport down', 'transport down']


def test_bigtable_status_count_mismatch_fails_every_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    table = MagicMock()
    rows = [MagicMock(row_key=b'prefix:1'), MagicMock(row_key=b'prefix:2')]
    for row in rows:
        row.get_mutations_size.return_value = 128
    table.row.side_effect = rows
    table.mutate_rows.return_value = [status(code_pb2.OK)]
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', lambda _name: table)

    outcomes = StoredExecutionResultBigTable().insert_many(
        [ExecutionResultWrite.from_execution_result(make_result(i)) for i in (1, 2)]
    )

    assert [outcome.succeeded for outcome in outcomes] == [False, False]
    assert all('returned 1 statuses for 2 rows' in str(outcome.error) for outcome in outcomes)


def test_bigtable_row_build_error_maps_to_every_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    table = MagicMock()
    table.row.side_effect = RuntimeError('cannot build row')
    mock_metrics = MagicMock()
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', lambda _name: table)
    monkeypatch.setattr(stored_result_module, 'metrics', mock_metrics)

    outcomes = StoredExecutionResultBigTable().insert_many(
        [ExecutionResultWrite.from_execution_result(make_result(i)) for i in (1, 2)]
    )

    assert [str(outcome.error) for outcome in outcomes] == ['cannot build row', 'cannot build row']
    table.mutate_rows.assert_not_called()
    mock_metrics.increment.assert_called_once_with(
        'stored_execution_result.row_outcome', 2, tags=['outcome:batch_error']
    )


def test_bigtable_metrics_table_reuse_missing_status_and_idempotency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = MagicMock()
    rows = [MagicMock(row_key=b'prefix:1'), MagicMock(row_key=b'prefix:1')]
    for row in rows:
        row.get_mutations_size.return_value = 128
    table.row.side_effect = rows
    table.mutate_rows.return_value = [status(code_pb2.OK), None]
    table_lookup = MagicMock(return_value=table)
    mock_metrics = MagicMock()
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', table_lookup)
    monkeypatch.setattr(stored_result_module, 'metrics', mock_metrics)
    write = ExecutionResultWrite.from_execution_result(make_result(1))

    outcomes = StoredExecutionResultBigTable().insert_many([write, write])

    table_lookup.assert_called_once_with('stored_execution_result')
    assert table.row.call_args_list == [
        call(StoredExecutionResultBigTable._encode_action_id(1)),
        call(StoredExecutionResultBigTable._encode_action_id(1)),
    ]
    assert [outcome.succeeded for outcome in outcomes] == [True, False]
    mock_metrics.histogram.assert_any_call('stored_execution_result.batch_rows', 2)
    mock_metrics.histogram.assert_any_call(
        'stored_execution_result.batch_bytes',
        sum(len(row.row_key) + row.get_mutations_size() for row in rows),
    )
    mock_metrics.increment.assert_any_call('stored_execution_result.row_outcome', tags=['outcome:success'])
    mock_metrics.increment.assert_any_call(
        'stored_execution_result.row_outcome', tags=['outcome:row_error', 'code:missing']
    )
