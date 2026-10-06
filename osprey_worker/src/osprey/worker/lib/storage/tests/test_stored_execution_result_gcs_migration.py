import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest
from osprey.worker._stdlibplugin import execution_result_store_chooser
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage import ExecutionResultStorageBackendType, stored_execution_result
from osprey.worker.lib.storage.stored_execution_result import (
    ExecutionResultReadError,
    ExecutionResultStore,
    StoredExecutionResultGCS,
    StoredExecutionResultGCSMigration,
    bootstrap_execution_result_storage_service,
)
from osprey.worker.lib.storage.tests.test_stored_execution_result_gcs import _FakeClient, _FakeGCS

_EPOCH_MS = 1420070400000
_METRIC = 'execution_result.migration'
_NOW = datetime(2026, 9, 24, 12, 34, 20, tzinfo=timezone.utc)


def _action_id(moment: datetime, sequence: int = 0) -> int:
    unix_ms = int(moment.timestamp() * 1000)
    return ((unix_ms - _EPOCH_MS) << 22) | sequence


# Ten minutes old: past the 300 s threshold, so a miss counts as age:over_5m.
_OLD = _action_id(_NOW - timedelta(minutes=10))


class _FakeStore(ExecutionResultStore):
    def __init__(self) -> None:
        self.records: Dict[int, Dict[str, Any]] = {}
        self.selected: List[List[int]] = []
        self.flushes = 0
        self.fail_insert = False
        self.fail_select = False
        # Raise a read error that carries the records found.
        self.partial_select = False

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        if self.fail_insert:
            raise RuntimeError('simulated insert failure')
        self.records[action_id] = {
            'id': action_id,
            'extracted_features': extracted_features_json,
            'error_traces': error_traces_json,
            'timestamp': timestamp,
            'action_data': action_data_json,
        }

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        results = self.select_many([action_id])
        return results[0] if results else None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        self.selected.append(list(action_ids))
        if self.fail_select:
            raise RuntimeError('simulated select failure')
        found = [self.records[i] for i in action_ids if i in self.records]
        if self.partial_select:
            raise ExecutionResultReadError(partial_results=found, failed_prefixes=1)
        return found

    def flush(self) -> None:
        self.flushes += 1

    def put(self, action_id: int, marker: str) -> None:
        self.insert(action_id, '{}', '[]', _NOW, marker)

    def selected_ids(self) -> List[int]:
        return [i for call in self.selected for i in call]


class _FakeMetrics:
    def __init__(self) -> None:
        self.events: List[Tuple[str, float, List[str]]] = []

    def increment(self, metric: str, value: float = 1, tags: Optional[List[str]] = None) -> None:
        self.events.append((metric, value, list(tags or [])))

    # The real batched store also emits these; totals only read the gcs migration metrics.
    histogram = gauge = timing = increment

    @contextlib.contextmanager
    def timed(self, metric: str, tags: Optional[List[str]] = None) -> Iterator[None]:
        yield
        self.events.append((metric, 0, list(tags or [])))

    def total(self, suffix: str, *tags: str) -> float:
        return sum(
            value
            for metric, value, event_tags in self.events
            if metric == f'{_METRIC}{suffix}' and all(tag in event_tags for tag in tags)
        )

    def timed_stores(self) -> List[str]:
        return [tags[0] for metric, _, tags in self.events if metric == f'{_METRIC}.read.duration']


@pytest.fixture(autouse=True)
def config() -> Iterator[None]:
    config = CONFIG.instance()
    config.unconfigure_for_tests()
    config.configure({'SNOWFLAKE_EPOCH': _EPOCH_MS})
    yield
    config.unconfigure_for_tests()


@pytest.fixture
def fake_metrics(monkeypatch: pytest.MonkeyPatch) -> _FakeMetrics:
    recorder = _FakeMetrics()
    monkeypatch.setattr(stored_execution_result, 'metrics', recorder)
    return recorder


@pytest.fixture
def gcs_store() -> _FakeStore:
    return _FakeStore()


@pytest.fixture
def bigtable_store() -> _FakeStore:
    return _FakeStore()


def _gcs_migration_store(
    gcs_store: _FakeStore,
    bigtable_store: _FakeStore,
    bigtable_write_enabled: bool = True,
) -> StoredExecutionResultGCSMigration:
    return StoredExecutionResultGCSMigration(
        gcs_store,
        bigtable_store,
        bigtable_write_enabled=bigtable_write_enabled,
    )


def _insert(store: ExecutionResultStore, action_id: int) -> None:
    store.insert(action_id, '{"ActionName": "test"}', '[]', _NOW, '{}')


def test_insert_writes_both(gcs_store: _FakeStore, bigtable_store: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    _insert(_gcs_migration_store(gcs_store, bigtable_store), _OLD)
    assert list(gcs_store.records) == [_OLD]
    assert list(bigtable_store.records) == [_OLD]
    assert fake_metrics.total('.write', 'store:gcs', 'outcome:ok') == 1


def test_gcs_insert_failure_is_swallowed(
    gcs_store: _FakeStore, bigtable_store: _FakeStore, fake_metrics: _FakeMetrics
) -> None:
    gcs_store.fail_insert = True
    _insert(_gcs_migration_store(gcs_store, bigtable_store), _OLD)
    assert list(bigtable_store.records) == [_OLD]
    assert fake_metrics.total('.write', 'store:gcs', 'outcome:error') == 1
    assert fake_metrics.total('.write', 'store:bigtable', 'outcome:ok') == 1


def test_bigtable_insert_failure_propagates(
    gcs_store: _FakeStore, bigtable_store: _FakeStore, fake_metrics: _FakeMetrics
) -> None:
    bigtable_store.fail_insert = True
    with pytest.raises(RuntimeError, match='simulated insert failure'):
        _insert(_gcs_migration_store(gcs_store, bigtable_store), _OLD)
    assert list(gcs_store.records) == [_OLD]
    assert fake_metrics.total('.write', 'store:bigtable', 'outcome:error') == 1
    assert fake_metrics.total('.write', 'store:bigtable', 'outcome:ok') == 0


def test_bigtable_write_disabled_skips_bigtable(
    gcs_store: _FakeStore, bigtable_store: _FakeStore, fake_metrics: _FakeMetrics
) -> None:
    bigtable_store.fail_insert = True
    _insert(_gcs_migration_store(gcs_store, bigtable_store, bigtable_write_enabled=False), _OLD)
    assert list(gcs_store.records) == [_OLD]
    assert bigtable_store.records == {}
    assert fake_metrics.total('.write', 'store:bigtable') == 0


def test_read_results_by_store(gcs_store: _FakeStore, bigtable_store: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    in_gcs, in_bigtable, nowhere = (_action_id(_NOW - timedelta(minutes=10), s) for s in range(3))
    gcs_store.put(in_gcs, 'gcs')
    bigtable_store.put(in_gcs, 'bigtable')
    bigtable_store.put(in_bigtable, 'bigtable')

    results = _gcs_migration_store(gcs_store, bigtable_store).select_many([in_gcs, in_bigtable, nowhere])

    assert [(r['id'], r['action_data']) for r in results] == [(in_gcs, 'gcs'), (in_bigtable, 'bigtable')]
    assert gcs_store.selected == [[in_gcs, in_bigtable, nowhere]]
    assert bigtable_store.selected == [[in_bigtable, nowhere]]
    assert fake_metrics.total('.read.results', 'store:gcs') == 1
    assert fake_metrics.total('.read.results', 'store:bigtable') == 1
    # An id with no result is counted by the caller, which knows whether it is still being saved.
    assert fake_metrics.total('.read.results') == 2
    assert fake_metrics.timed_stores() == ['store:gcs', 'store:bigtable']


def test_select_one_prefers_gcs(gcs_store: _FakeStore, bigtable_store: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    gcs_store.put(_OLD, 'gcs')
    bigtable_store.put(_OLD, 'bigtable')
    record = _gcs_migration_store(gcs_store, bigtable_store).select_one(_OLD)
    assert record is not None and record['action_data'] == 'gcs'
    assert bigtable_store.selected == []


def test_gcs_select_failure_falls_back(
    gcs_store: _FakeStore, bigtable_store: _FakeStore, fake_metrics: _FakeMetrics
) -> None:
    gcs_store.fail_select = True
    ids = [_action_id(_NOW - timedelta(minutes=10), s) for s in range(3)]
    for action_id in ids:
        bigtable_store.put(action_id, 'bigtable')

    results = _gcs_migration_store(gcs_store, bigtable_store).select_many(ids)

    assert [r['id'] for r in results] == ids
    assert bigtable_store.selected == [ids]
    assert fake_metrics.total('.read.results', 'store:bigtable') == 3
    assert fake_metrics.total('.read.gcs_errors') == 1


def test_partial_gcs_read_keeps_its_results_and_falls_back_for_the_rest(
    gcs_store: _FakeStore, bigtable_store: _FakeStore, fake_metrics: _FakeMetrics
) -> None:
    gcs_store.partial_select = True
    in_gcs, unread = (_action_id(_NOW - timedelta(minutes=10), s) for s in range(2))
    gcs_store.put(in_gcs, 'gcs')
    for action_id in (in_gcs, unread):
        bigtable_store.put(action_id, 'bigtable')

    results = _gcs_migration_store(gcs_store, bigtable_store).select_many([in_gcs, unread])

    assert [(r['id'], r['action_data']) for r in results] == [(in_gcs, 'gcs'), (unread, 'bigtable')]
    assert bigtable_store.selected == [[unread]]
    assert fake_metrics.total('.read.gcs_errors') == 1
    assert fake_metrics.total('.read.results', 'store:gcs') == 1
    assert fake_metrics.total('.read.results', 'store:bigtable') == 1


def test_flush_reaches_both_stores(gcs_store: _FakeStore, bigtable_store: _FakeStore) -> None:
    _gcs_migration_store(gcs_store, bigtable_store).flush()
    assert (gcs_store.flushes, bigtable_store.flushes) == (1, 1)


class _FakeBatched(_FakeStore):
    instances: List['_FakeBatched'] = []

    def __init__(self) -> None:
        super().__init__()
        self.started = False
        _FakeBatched.instances.append(self)

    def start(self) -> None:
        self.started = True


class _FakeBigTable(_FakeStore):
    instances: List['_FakeBigTable'] = []

    def __init__(self) -> None:
        super().__init__()
        _FakeBigTable.instances.append(self)


def test_chooser_builds_gcs_migration_store(monkeypatch: pytest.MonkeyPatch, fake_metrics: _FakeMetrics) -> None:
    monkeypatch.setattr(_FakeBatched, 'instances', [])
    monkeypatch.setattr(_FakeBigTable, 'instances', [])
    monkeypatch.setattr(execution_result_store_chooser, 'StoredExecutionResultGCS', _FakeBatched)
    monkeypatch.setattr(execution_result_store_chooser, 'StoredExecutionResultBigTable', _FakeBigTable)
    CONFIG.instance().unconfigure_for_tests()
    CONFIG.instance().configure(
        {
            'SNOWFLAKE_EPOCH': _EPOCH_MS,
            'OSPREY_EXECUTION_RESULT_BIGTABLE_WRITE_ENABLED': 'false',
        }
    )

    store = execution_result_store_chooser.get_rules_execution_result_storage_backend(
        ExecutionResultStorageBackendType.GCS_MIGRATION
    )

    assert isinstance(store, StoredExecutionResultGCSMigration)
    [batched], [bigtable] = _FakeBatched.instances, _FakeBigTable.instances
    # The batched store starts its own timer on first insert.
    assert not batched.started
    _insert(store, _OLD)
    assert list(batched.records) == [_OLD]
    assert bigtable.records == {}


def test_chooser_builds_batched_store_without_starting_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_FakeBatched, 'instances', [])
    monkeypatch.setattr(execution_result_store_chooser, 'StoredExecutionResultGCS', _FakeBatched)

    store = execution_result_store_chooser.get_rules_execution_result_storage_backend(
        ExecutionResultStorageBackendType.GCS
    )

    [batched] = _FakeBatched.instances
    assert store is batched
    assert not batched.started


def test_service_reads_the_real_batched_store_through_gcs_migration(
    monkeypatch: pytest.MonkeyPatch, fake_metrics: _FakeMetrics
) -> None:
    gcs = _FakeGCS()
    monkeypatch.setattr(stored_execution_result.storage, 'Client', lambda project: _FakeClient(gcs))
    monkeypatch.setattr(_FakeBigTable, 'instances', [])
    monkeypatch.setattr(execution_result_store_chooser, 'StoredExecutionResultBigTable', _FakeBigTable)
    CONFIG.instance().unconfigure_for_tests()
    CONFIG.instance().configure(
        {
            'SNOWFLAKE_EPOCH': _EPOCH_MS,
            'OSPREY_EXECUTION_RESULT_STORAGE_BACKEND': 'gcs_migration',
            'OSPREY_GCS_EXECUTION_RESULTS_BUCKET': 'test-bucket',
        }
    )

    service = bootstrap_execution_result_storage_service()
    gcs_migration = service._storage_backend
    assert isinstance(gcs_migration, StoredExecutionResultGCSMigration)
    batched = gcs_migration._gcs
    assert isinstance(batched, StoredExecutionResultGCS)
    assert batched._timer_thread is None
    try:
        # Past the 5-minute miss age but not late, so the records land on time and a GCS miss would
        # count as age:over_5m.
        moment = datetime.now(timezone.utc) - timedelta(seconds=400)
        ids = [_action_id(moment, sequence) for sequence in range(2)]
        for action_id in ids:
            gcs_migration.insert(action_id, '{"ActionName": "test"}', '[]', moment, '{}')
        service.flush()
        assert len(gcs.objects) == 1

        assert sorted(result.id for result in service.get_many(ids)) == sorted(ids)
        assert fake_metrics.total('.read.results', 'store:gcs') == 2

        def _fail_list(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError('simulated GCS list outage')

        monkeypatch.setattr(_FakeClient, 'list_blobs', _fail_list)

        assert sorted(result.id for result in service.get_many(ids)) == sorted(ids)
        assert fake_metrics.total('.read.gcs_errors') == 1
        assert fake_metrics.total('.read.results', 'store:bigtable') == 2
        [bigtable] = _FakeBigTable.instances
        assert bigtable.selected == [ids]
    finally:
        batched.close()


def test_chooser_returns_plugin_store(monkeypatch: pytest.MonkeyPatch) -> None:
    registered = _FakeStore()
    monkeypatch.setattr(execution_result_store_chooser, 'bootstrap_execution_result_store', lambda config: registered)
    store = execution_result_store_chooser.get_rules_execution_result_storage_backend(
        ExecutionResultStorageBackendType.PLUGIN
    )
    assert store is registered
