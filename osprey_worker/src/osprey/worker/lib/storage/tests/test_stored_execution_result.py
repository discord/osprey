from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence
from unittest.mock import MagicMock, call

import pytest
from google.api_core import gapic_v1
from google.api_core import timeout as api_core_timeout
from google.api_core.exceptions import ServiceUnavailable
from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.table import _MAX_BULK_MUTATIONS, DEFAULT_RETRY, Table, _BigtableRetryableError
from google.cloud.bigtable_v2.services.bigtable import BigtableClient
from google.cloud.bigtable_v2.types.bigtable import MutateRowsRequest, MutateRowsResponse
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


def make_write(action_id: int) -> ExecutionResultWrite:
    return ExecutionResultWrite.from_execution_result(
        ExecutionResult(
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
    )


def insert_write(store: ExecutionResultStore, write: ExecutionResultWrite) -> None:
    store.insert(
        action_id=write.action_id,
        extracted_features_json=write.extracted_features_json,
        error_traces_json=write.error_traces_json,
        timestamp=write.timestamp,
        action_data_json=write.action_data_json,
    )


def status(code: int, message: str = '') -> MagicMock:
    return MagicMock(code=code, message=message)


def sent_rows(table: MagicMock, call_index: int = 0) -> List[DirectRow]:
    return list(table.mutate_rows.call_args_list[call_index].args[0])


def make_table(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    table = MagicMock()
    table.name = 'projects/test-project/instances/test-instance/tables/stored_execution_result'
    table._app_profile_id = None
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', lambda _name: table)
    return table


def mock_metrics(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    metrics = MagicMock()
    monkeypatch.setattr(stored_result_module, 'metrics', metrics)
    return metrics


def clearing_mutate_rows(statuses: Sequence[Any]) -> Callable[..., Sequence[Any]]:
    """A `Table.mutate_rows` stand-in that clears every row it commits.

    google-cloud-bigtable does this — `_do_mutate_retryable_rows` calls `self.rows[index].clear()`
    for each entry whose status code is 0 — and a plain MagicMock does not, so byte accounting read
    after the call looks correct under mocks and collapses to the row keys in production.
    """

    def _mutate_rows(rows: Sequence[DirectRow], **_: Any) -> Sequence[Any]:
        for row, row_status in zip(rows, statuses):
            if row_status is not None and row_status.code == code_pb2.OK:
                row.clear()
        return statuses

    return _mutate_rows


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

    outcomes: List[ExecutionResultPersistOutcome] = store.insert_many([make_write(i) for i in (1, 2, 3)])

    assert store.inserted == [1, 2, 3]
    assert [outcome.succeeded for outcome in outcomes] == [True, False, True]
    with pytest.raises(RuntimeError, match='row 2 failed'):
        outcomes[1].raise_for_error()


def test_service_persist_writes_preserves_input_order() -> None:
    store = RecordingStore()

    outcomes = ExecutionResultStorageService(store).persist_writes([make_write(i) for i in (3, 1)])

    assert store.inserted == [3, 1]
    assert all(outcome.succeeded for outcome in outcomes)


def test_write_from_execution_result_preserves_serialized_fields() -> None:
    assert make_write(1) == ExecutionResultWrite(
        action_id=1,
        extracted_features_json='{"ActionName": "message_sent", "id": 1}',
        error_traces_json='[]',
        timestamp=datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc),
        action_data_json='{"id": 1}',
    )


def test_write_payload_size_matches_bulk_accounting_and_encodes_once(monkeypatch: pytest.MonkeyPatch) -> None:
    write = ExecutionResultWrite(
        action_id=1,
        extracted_features_json='"é"',
        error_traces_json='"€"',
        timestamp=datetime(2026, 8, 19, 12, 0, 1, tzinfo=timezone.utc),
        action_data_json='"🦅"',
    )
    build_row = MagicMock(side_effect=StoredExecutionResultBigTable._build_row)
    monkeypatch.setattr(StoredExecutionResultBigTable, '_build_row', build_row)
    row = build_row.side_effect(write)
    entry = MutateRowsRequest.Entry(row_key=row.row_key, mutations=row._get_mutations())
    expected_entry_bytes = MutateRowsRequest.pb(MutateRowsRequest(entries=[entry])).ByteSize()

    assert write.payload_size_bytes == expected_entry_bytes
    assert write.payload_size_bytes > len(row.row_key) + row.get_mutations_size()
    # A batching caller sizes every write, so the encode must not repeat per lookup.
    assert write.payload_size_bytes == write.payload_size_bytes
    build_row.assert_called_once_with(write)


def test_mutation_retry_matches_bigtable_errors_and_is_bounded() -> None:
    """`mutate_rows` only signals retryable work via `_BigtableRetryableError`, and the sinks
    both abandon a write after 5s without being able to cancel its thread — so the in-call retry
    must match that error and stay well inside that budget."""
    policy = StoredExecutionResultBigTable.mutation_retry_policy

    assert policy._predicate(_BigtableRetryableError())
    assert policy._deadline == StoredExecutionResultBigTable.MUTATION_RETRY_DEADLINE_SECONDS <= 2.0
    assert policy._deadline < DEFAULT_RETRY._deadline


def test_bigtable_effective_transport_rpc_timeout_is_two_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the pinned Table/worker/GAPIC wrappers through to the transport callable."""
    same_tick = datetime(2026, 8, 20, tzinfo=timezone.utc)
    monkeypatch.setattr(api_core_timeout.datetime_helpers, 'utcnow', lambda: same_tick)
    transport_mutate_rows = MagicMock(
        return_value=[MutateRowsResponse(entries=[MutateRowsResponse.Entry(index=0, status={'code': code_pb2.OK})])]
    )
    transport = MagicMock()
    transport.mutate_rows = transport_mutate_rows
    transport._wrapped_methods = {
        transport_mutate_rows: gapic_v1.method.wrap_method(transport_mutate_rows, client_info=None)
    }
    table_data_client = object.__new__(BigtableClient)
    table_data_client._transport = transport
    client = MagicMock(project='test-project', table_data_client=table_data_client)
    instance = MagicMock(instance_id='test-instance', _client=client)
    table = Table('stored_execution_result', instance)
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', lambda _name: table)
    mock_metrics(monkeypatch)

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(1)])

    assert outcomes[0].succeeded
    assert transport_mutate_rows.call_args.kwargs['timeout'] == 2.0
    assert StoredExecutionResultBigTable.mutation_retry_policy._deadline == 2.0


def test_bulk_row_cap_keeps_requests_under_the_client_mutation_limit() -> None:
    """Fails if a fifth cell is added to the row format without lowering the row cap."""
    mutations_per_row = len(StoredExecutionResultBigTable._build_row(make_write(1))._get_mutations())

    assert StoredExecutionResultBigTable.MAX_ROWS_PER_BULK_CALL * mutations_per_row <= _MAX_BULK_MUTATIONS


def test_bigtable_single_and_bulk_use_identical_row_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.side_effect = [[status(code_pb2.OK)], [status(code_pb2.OK)]]
    store = StoredExecutionResultBigTable()
    write = make_write(1)

    insert_write(store, write)
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
    assert table.mutate_rows.call_args.kwargs == {
        'retry': StoredExecutionResultBigTable.mutation_retry_policy,
        'timeout': StoredExecutionResultBigTable.MUTATE_ROWS_TIMEOUT_ARGUMENT_SECONDS,
    }
    assert len(sent_rows(table)) == 3
    assert [outcome.succeeded for outcome in outcomes] == [True, False, True]
    with pytest.raises(ServiceUnavailable, match='retry exhausted'):
        outcomes[1].raise_for_error()


def test_bigtable_bulk_byte_metric_is_measured_before_the_client_clears_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: batch_bytes read after mutate_rows() reports only the row keys."""
    table = make_table(monkeypatch)
    writes = [make_write(i) for i in (1, 2, 3)]
    expected_rows = [StoredExecutionResultBigTable._build_row(write, table) for write in writes]
    expected_entries = [
        MutateRowsRequest.Entry(row_key=row.row_key, mutations=row._get_mutations()) for row in expected_rows
    ]
    expected_request = MutateRowsRequest(
        table_name=table.name,
        app_profile_id=table._app_profile_id,
        entries=expected_entries,
    )
    expected_bytes = MutateRowsRequest.pb(expected_request).ByteSize()
    table.mutate_rows.side_effect = clearing_mutate_rows([status(code_pb2.OK)] * 3)
    metrics = mock_metrics(monkeypatch)

    outcomes = StoredExecutionResultBigTable().insert_many(writes)

    assert all(outcome.succeeded for outcome in outcomes)
    metrics.histogram.assert_any_call('stored_execution_result.batch_rows', 3)
    metrics.histogram.assert_any_call('stored_execution_result.batch_bytes', expected_bytes)
    # Proves the rows really were cleared, so the assertion above is not measuring a no-op.
    cleared_entries = [
        MutateRowsRequest.Entry(row_key=row.row_key, mutations=row._get_mutations()) for row in sent_rows(table)
    ]
    cleared_request = MutateRowsRequest(
        table_name=table.name,
        app_profile_id=table._app_profile_id,
        entries=cleared_entries,
    )
    assert MutateRowsRequest.pb(cleared_request).ByteSize() < expected_bytes


def test_bigtable_size_measurement_failure_does_not_cost_the_batch_its_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Byte accounting is instrumentation, so a failure to measure must not drop the rows."""
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(code_pb2.OK), status(code_pb2.OK)]
    metrics = mock_metrics(monkeypatch)
    monkeypatch.setattr(
        stored_result_module, '_encoded_request_size', MagicMock(side_effect=RuntimeError('cannot measure'))
    )

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(1), make_write(2)])

    assert [outcome.succeeded for outcome in outcomes] == [True, True]
    table.mutate_rows.assert_called_once()
    # batch_rows still reports the completed call; only the byte histogram is skipped.
    metrics.histogram.assert_called_once_with('stored_execution_result.batch_rows', 2)


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
    metrics = mock_metrics(monkeypatch)

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2)])

    assert [str(outcome.error) for outcome in outcomes] == ['transport down', 'transport down']
    assert metrics.timing.call_args_list == [
        call('stored_execution_result.commit_ms', metrics.timing.call_args.args[1], tags=['path:bulk'])
    ]
    # batch_rows/batch_bytes count completed MutateRows calls, so a raised call emits neither.
    metrics.histogram.assert_not_called()
    metrics.increment.assert_called_once_with(
        'stored_execution_result.row_outcome', 2, tags=['outcome:batch_error', 'path:bulk']
    )


def test_bigtable_status_count_mismatch_fails_every_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(code_pb2.OK)]

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2)])

    assert [outcome.succeeded for outcome in outcomes] == [False, False]
    assert all('returned 1 statuses for 2 rows' in str(outcome.error) for outcome in outcomes)


def test_bigtable_row_build_error_maps_to_every_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    make_table(monkeypatch)
    metrics = mock_metrics(monkeypatch)
    monkeypatch.setattr(stored_result_module, 'DirectRow', MagicMock(side_effect=RuntimeError('cannot build row')))

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in (1, 2)])

    assert [str(outcome.error) for outcome in outcomes] == ['cannot build row', 'cannot build row']
    metrics.increment.assert_called_once_with(
        'stored_execution_result.row_outcome', 2, tags=['outcome:batch_error', 'path:bulk']
    )


def test_bigtable_reuses_one_table_and_reports_a_missing_status(monkeypatch: pytest.MonkeyPatch) -> None:
    table = MagicMock()
    table.name = 'projects/test-project/instances/test-instance/tables/stored_execution_result'
    table._app_profile_id = None
    table.mutate_rows.return_value = [status(code_pb2.OK), None]
    table_lookup = MagicMock(return_value=table)
    monkeypatch.setattr(stored_result_module.osprey_bigtable, 'table', table_lookup)
    metrics = mock_metrics(monkeypatch)
    write = make_write(1)

    outcomes = StoredExecutionResultBigTable().insert_many([write, write])

    table_lookup.assert_called_once_with('stored_execution_result')
    rows = sent_rows(table)
    # Same action ID twice is the same row key, so a redelivery stays idempotent.
    assert rows[0].row_key == rows[1].row_key == StoredExecutionResultBigTable._encode_action_id(1)
    assert [outcome.succeeded for outcome in outcomes] == [True, False]
    expected_entries = [MutateRowsRequest.Entry(row_key=row.row_key, mutations=row._get_mutations()) for row in rows]
    expected_request = MutateRowsRequest(
        table_name=table.name,
        app_profile_id=table._app_profile_id,
        entries=expected_entries,
    )
    metrics.histogram.assert_any_call(
        'stored_execution_result.batch_bytes', MutateRowsRequest.pb(expected_request).ByteSize()
    )
    metrics.increment.assert_any_call(
        'stored_execution_result.row_outcome', 1, tags=['outcome:row_error', 'code:missing', 'path:bulk']
    )


def test_bigtable_row_outcomes_aggregate_per_distinct_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rows failing the same way collapse into one increment instead of one per row."""
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(c) for c in (code_pb2.OK, 14, 14, code_pb2.OK, code_pb2.OK)]
    metrics = mock_metrics(monkeypatch)

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(i) for i in range(5)])

    assert [outcome.succeeded for outcome in outcomes] == [True, False, False, True, True]
    assert metrics.increment.call_args_list == [
        call('stored_execution_result.row_outcome', 3, tags=['outcome:success', 'path:bulk']),
        call('stored_execution_result.row_outcome', 2, tags=['outcome:row_error', 'code:UNAVAILABLE', 'path:bulk']),
    ]


def test_bigtable_row_error_tags_fall_back_to_the_numeric_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """`code_pb2.Code.Name` raises on out-of-enum values, so the tag falls back to the int."""
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(999)]
    metrics = mock_metrics(monkeypatch)

    outcomes = StoredExecutionResultBigTable().insert_many([make_write(1)])

    assert not outcomes[0].succeeded
    metrics.increment.assert_called_once_with(
        'stored_execution_result.row_outcome', 1, tags=['outcome:row_error', 'code:999', 'path:bulk']
    )


def test_bigtable_single_insert_counts_row_failure_without_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.return_value = [status(code_pb2.UNAVAILABLE, 'retry exhausted')]
    metrics = mock_metrics(monkeypatch)

    insert_write(StoredExecutionResultBigTable(), make_write(1))

    metrics.increment.assert_called_once_with(
        'stored_execution_result.row_outcome', 1, tags=['outcome:row_error', 'code:UNAVAILABLE', 'path:single']
    )
    assert metrics.timing.call_args.args[0] == 'stored_execution_result.commit_ms'
    assert metrics.timing.call_args.kwargs['tags'] == ['path:single']
    # The single-row path emits no batch histograms; batch_* describe insert_many calls.
    metrics.histogram.assert_not_called()


def test_bigtable_single_insert_propagates_call_wide_exceptions(monkeypatch: pytest.MonkeyPatch) -> None:
    table = make_table(monkeypatch)
    table.mutate_rows.side_effect = RuntimeError('transport down')

    with pytest.raises(RuntimeError, match='transport down'):
        insert_write(StoredExecutionResultBigTable(), make_write(1))
