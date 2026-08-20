from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import ServiceUnavailable
from google.cloud.bigtable.row import DirectRow
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
            timestamp=datetime(2026, 8, 19, 12, 0, action_id % 60, tzinfo=timezone.utc),
        ),
        effects={},
        error_infos=[],
    )


def make_write(action_id: int) -> ExecutionResultWrite:
    return ExecutionResultWrite.from_execution_result(make_result(action_id))


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
    writes = [make_write(i) for i in (1, 2, 3)]

    outcomes: List[ExecutionResultPersistOutcome] = store.insert_many(writes)

    assert store.inserted == [1, 2, 3]
    assert [outcome.succeeded for outcome in outcomes] == [True, False, True]
    with pytest.raises(RuntimeError, match='row 2 failed'):
        outcomes[1].raise_for_error()


def test_service_persist_writes_preserves_input_order() -> None:
    store = RecordingStore()
    service = ExecutionResultStorageService(store)
    writes = [make_write(i) for i in (3, 1)]

    outcomes = service.persist_writes(writes)

    assert store.inserted == [3, 1]
    assert all(outcome.succeeded for outcome in outcomes)


def test_write_from_execution_result_preserves_serialized_fields() -> None:
    write = make_write(1)

    assert write == ExecutionResultWrite(
        action_id=1,
        extracted_features_json='{"ActionName": "message_sent", "id": 1}',
        error_traces_json='[]',
        timestamp=datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc),
        action_data_json='{"id": 1}',
    )


def test_write_payload_size_matches_bulk_byte_accounting() -> None:
    write = ExecutionResultWrite(
        action_id=1,
        extracted_features_json='"é"',
        error_traces_json='"€"',
        timestamp=datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc),
        action_data_json='"🦅"',
    )

    row = StoredExecutionResultBigTable._build_row(write)
    assert write.payload_size_bytes == len(row.row_key) + row.get_mutations_size()
    # The row key and cell encoding overhead must be included, not just the values.
    raw_value_bytes = sum(
        len(value.encode('utf-8'))
        for value in (
            write.extracted_features_json,
            write.error_traces_json,
            write.timestamp.isoformat(),
            write.action_data_json,
        )
    )
    assert write.payload_size_bytes > raw_value_bytes


def status(code: int, message: str = '') -> MagicMock:
    return MagicMock(code=code, message=message)


def sent_rows(table: MagicMock, call_index: int = 0) -> List[DirectRow]:
    return list(table.mutate_rows.call_args_list[call_index].args[0])


def make_table(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    table = MagicMock()
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', lambda _name: table)
    return table


def test_mutation_retry_policy_retries_bigtable_retryable_errors() -> None:
    from google.cloud.bigtable.table import _BigtableRetryableError

    assert StoredExecutionResultBigTable.mutation_retry_policy._predicate(_BigtableRetryableError())


def test_bigtable_single_and_bulk_use_identical_row_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.side_effect = [[status(code_pb2.OK)], [status(code_pb2.OK)]]
    store = StoredExecutionResultBigTable()
    write = make_write(1)

    store.insert(
        action_id=write.action_id,
        extracted_features_json=write.extracted_features_json,
        error_traces_json=write.error_traces_json,
        timestamp=write.timestamp,
        action_data_json=write.action_data_json,
    )
    store.insert_many([write])

    (single_row,) = sent_rows(table, 0)
    (bulk_row,) = sent_rows(table, 1)
    assert single_row.row_key == bulk_row.row_key == StoredExecutionResultBigTable._encode_action_id(1)
    assert single_row._get_mutations() == bulk_row._get_mutations()
    assert [mutation.set_cell.column_qualifier for mutation in single_row._get_mutations()] == [
        b'extracted_features',
        b'error_traces',
        b'timestamp',
        b'action_data',
    ]


def test_bigtable_rollback_to_base_insert_many_does_not_recurse(monkeypatch: pytest.MonkeyPatch) -> None:
    """Removing the insert_many override must leave a working one-by-one path."""
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(code_pb2.OK)]

    outcomes = ExecutionResultStore.insert_many(StoredExecutionResultBigTable(), [make_write(1), make_write(2)])

    assert [outcome.succeeded for outcome in outcomes] == [True, True]
    assert table.mutate_rows.call_count == 2


def test_bigtable_bulk_sends_one_request_and_aligns_partial_statuses(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [
        status(code_pb2.OK),
        status(code_pb2.UNAVAILABLE, 'retry exhausted'),
        status(code_pb2.OK),
    ]

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2, 3)])

    table.mutate_rows.assert_called_once()
    assert table.mutate_rows.call_args.kwargs == {'retry': StoredExecutionResultBigTable.mutation_retry_policy}
    assert len(sent_rows(table)) == 3
    assert [outcome.succeeded for outcome in outcomes] == [True, False, True]
    with pytest.raises(ServiceUnavailable, match='retry exhausted'):
        outcomes[1].raise_for_error()


def test_bigtable_bulk_chunks_at_row_cap_and_isolates_chunk_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    monkeypatch.setattr(StoredExecutionResultBigTable, 'MAX_ROWS_PER_BULK_CALL', 2)
    table.mutate_rows.side_effect = [
        [status(code_pb2.OK), status(code_pb2.OK)],
        RuntimeError('chunk two down'),
        [status(code_pb2.OK)],
    ]

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2, 3, 4, 5)])

    assert [len(sent_rows(table, i)) for i in range(3)] == [2, 2, 1]
    assert [outcome.succeeded for outcome in outcomes] == [True, True, False, False, True]
    assert str(outcomes[2].error) == str(outcomes[3].error) == 'chunk two down'


def test_bigtable_transport_error_maps_to_every_outcome_and_times_the_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.side_effect = RuntimeError('transport down')
    mock_metrics = MagicMock()
    monkeypatch.setattr(stored_result_module, 'metrics', mock_metrics)

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2)])

    assert [str(outcome.error) for outcome in outcomes] == ['transport down', 'transport down']
    timing_calls = [c for c in mock_metrics.timing.call_args_list if c.args[0] == 'stored_execution_result.commit_ms']
    assert len(timing_calls) == 1
    # batch_rows/batch_bytes count completed MutateRows calls, so a raised call emits neither.
    mock_metrics.histogram.assert_not_called()
    mock_metrics.increment.assert_called_once_with(
        'stored_execution_result.row_outcome', 2, tags=['outcome:batch_error']
    )


def test_bigtable_status_count_mismatch_fails_every_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(code_pb2.OK)]

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2)])

    assert [outcome.succeeded for outcome in outcomes] == [False, False]
    assert all('returned 1 statuses for 2 rows' in str(outcome.error) for outcome in outcomes)


def test_bigtable_row_build_error_maps_to_every_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    make_table(monkeypatch)
    mock_metrics = MagicMock()
    monkeypatch.setattr(stored_result_module, 'metrics', mock_metrics)
    monkeypatch.setattr(stored_result_module, 'DirectRow', MagicMock(side_effect=RuntimeError('cannot build row')))

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2)])

    assert [str(outcome.error) for outcome in outcomes] == ['cannot build row', 'cannot build row']
    mock_metrics.increment.assert_called_once_with(
        'stored_execution_result.row_outcome', 2, tags=['outcome:batch_error']
    )


def test_bigtable_metrics_table_reuse_missing_status_and_idempotency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = MagicMock()
    table.mutate_rows.return_value = [status(code_pb2.OK), None]
    table_lookup = MagicMock(return_value=table)
    mock_metrics = MagicMock()
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', table_lookup)
    monkeypatch.setattr(stored_result_module, 'metrics', mock_metrics)
    write = make_write(1)

    outcomes = StoredExecutionResultBigTable().insert_many([write, write])

    table_lookup.assert_called_once_with('stored_execution_result')
    rows = sent_rows(table)
    assert rows[0].row_key == rows[1].row_key == StoredExecutionResultBigTable._encode_action_id(1)
    assert [outcome.succeeded for outcome in outcomes] == [True, False]
    mock_metrics.histogram.assert_any_call('stored_execution_result.batch_rows', 2)
    mock_metrics.histogram.assert_any_call(
        'stored_execution_result.batch_bytes',
        sum(write.payload_size_bytes for _ in rows),
    )
    mock_metrics.increment.assert_any_call('stored_execution_result.row_outcome', 1, tags=['outcome:success'])
    mock_metrics.increment.assert_any_call(
        'stored_execution_result.row_outcome', tags=['outcome:row_error', 'code:missing']
    )


def test_bigtable_row_error_tags_use_grpc_code_names(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(code_pb2.UNAVAILABLE), status(999)]
    mock_metrics = MagicMock()
    monkeypatch.setattr(stored_result_module, 'metrics', mock_metrics)

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2)])

    assert [outcome.succeeded for outcome in outcomes] == [False, False]
    mock_metrics.increment.assert_any_call(
        'stored_execution_result.row_outcome', tags=['outcome:row_error', 'code:UNAVAILABLE']
    )
    mock_metrics.increment.assert_any_call(
        'stored_execution_result.row_outcome', tags=['outcome:row_error', 'code:999']
    )


def test_bigtable_single_insert_counts_row_failure_without_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(code_pb2.UNAVAILABLE, 'retry exhausted')]
    mock_metrics = MagicMock()
    monkeypatch.setattr(stored_result_module, 'metrics', mock_metrics)
    write = make_write(1)

    StoredExecutionResultBigTable().insert(
        action_id=write.action_id,
        extracted_features_json=write.extracted_features_json,
        error_traces_json=write.error_traces_json,
        timestamp=write.timestamp,
        action_data_json=write.action_data_json,
    )

    mock_metrics.increment.assert_called_once_with(
        'stored_execution_result.row_outcome', tags=['outcome:row_error', 'code:UNAVAILABLE']
    )
    # The single-row path emits no batch histograms; batch_* describe insert_many calls.
    mock_metrics.histogram.assert_not_called()


def test_bigtable_single_insert_propagates_call_wide_exceptions(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.side_effect = RuntimeError('transport down')
    write = make_write(1)

    with pytest.raises(RuntimeError, match='transport down'):
        StoredExecutionResultBigTable().insert(
            action_id=write.action_id,
            extracted_features_json=write.extracted_features_json,
            error_traces_json=write.error_traces_json,
            timestamp=write.timestamp,
            action_data_json=write.action_data_json,
        )
