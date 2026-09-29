from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
from osprey.engine.executor.execution_context import Action, ExecutionResult
from osprey.worker._stdlibplugin import execution_result_store_chooser
from osprey.worker.lib.storage import ExecutionResultStorageBackendType
from osprey.worker.lib.storage.stored_execution_result import ExecutionResultStore
from osprey.worker.sinks.sink.stored_execution_result_output_sink import StoredExecutionResultOutputSink


class RecordingStore(ExecutionResultStore):
    def __init__(self) -> None:
        self.inserted: List[Dict[str, Any]] = []

    def select_one(self, action_id: int) -> None:
        return None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        return []

    def insert(self, action_id, extracted_features_json, error_traces_json, timestamp, action_data_json) -> None:
        self.inserted.append({'action_id': action_id, 'action_data_json': action_data_json})


def _result(action_id: int) -> ExecutionResult:
    return ExecutionResult(
        extracted_features={},
        action=Action(action_id=action_id, action_name='test', data={'k': 'v'}, timestamp=datetime.utcnow()),
        effects={},
        error_infos=[],
        validator_results=None,
        sample_rate=100,
    )


def test_sink_writes_to_an_injected_store() -> None:
    store = RecordingStore()

    StoredExecutionResultOutputSink(storage_backend=store).push(_result(5))

    assert store.inserted == [{'action_id': 5, 'action_data_json': '{"k": "v"}'}]


def test_plugin_backend_returns_the_registered_store(monkeypatch: pytest.MonkeyPatch) -> None:
    store = RecordingStore()
    monkeypatch.setattr(execution_result_store_chooser, 'CONFIG', MagicMock())
    monkeypatch.setattr(execution_result_store_chooser, 'bootstrap_execution_result_store', lambda config: store)

    chosen = execution_result_store_chooser.get_rules_execution_result_storage_backend(
        ExecutionResultStorageBackendType.PLUGIN
    )

    assert chosen is store


def test_plugin_backend_without_a_registered_store_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(execution_result_store_chooser, 'CONFIG', MagicMock())
    monkeypatch.setattr(execution_result_store_chooser, 'bootstrap_execution_result_store', lambda config: None)

    with pytest.raises(AssertionError):
        execution_result_store_chooser.get_rules_execution_result_storage_backend(
            ExecutionResultStorageBackendType.PLUGIN
        )
