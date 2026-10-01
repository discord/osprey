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
_METRIC = 'execution_result_gcs_migration'
_NOW = datetime(2026, 9, 24, 12, 34, 20, tzinfo=timezone.utc)


def _action_id(moment: datetime, sequence: int = 0) -> int:
    unix_ms = int(moment.timestamp() * 1000)
    return ((unix_ms - _EPOCH_MS) << 22) | sequence


# Ten minutes old: past the 300 s threshold, so a miss is unexpected.
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

    def timed_backends(self) -> List[str]:
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
def primary() -> _FakeStore:
    return _FakeStore()


@pytest.fixture
def legacy() -> _FakeStore:
    return _FakeStore()


def _gcs_migration_store(
    primary: _FakeStore,
    legacy: _FakeStore,
    legacy_write_enabled: bool = True,
) -> StoredExecutionResultGCSMigration:
    return StoredExecutionResultGCSMigration(
        primary,
        legacy,
        legacy_write_enabled=legacy_write_enabled,
        clock=_NOW.timestamp,
    )


def _insert(store: ExecutionResultStore, action_id: int) -> None:
    store.insert(action_id, '{"ActionName": "test"}', '[]', _NOW, '{}')


def test_insert_writes_both(primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    _insert(_gcs_migration_store(primary, legacy), _OLD)
    assert list(primary.records) == [_OLD]
    assert list(legacy.records) == [_OLD]
    assert fake_metrics.total('.write', 'target:gcs', 'outcome:ok') == 1


def test_primary_insert_failure_is_swallowed(
    primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics
) -> None:
    primary.fail_insert = True
    _insert(_gcs_migration_store(primary, legacy), _OLD)
    assert list(legacy.records) == [_OLD]
    assert fake_metrics.total('.write', 'target:gcs', 'outcome:error') == 1
    assert fake_metrics.total('.write', 'target:legacy', 'outcome:ok') == 1


def test_legacy_insert_failure_propagates(primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    legacy.fail_insert = True
    with pytest.raises(RuntimeError, match='simulated insert failure'):
        _insert(_gcs_migration_store(primary, legacy), _OLD)
    assert list(primary.records) == [_OLD]
    assert fake_metrics.total('.write', 'target:legacy', 'outcome:error') == 1
    assert fake_metrics.total('.write', 'target:legacy', 'outcome:ok') == 0


def test_legacy_write_disabled_skips_legacy(
    primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics
) -> None:
    legacy.fail_insert = True
    _insert(_gcs_migration_store(primary, legacy, legacy_write_enabled=False), _OLD)
    assert list(primary.records) == [_OLD]
    assert legacy.records == {}
    assert fake_metrics.total('.write', 'target:legacy') == 0


def test_read_sources(primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    in_gcs, in_legacy, nowhere = (_action_id(_NOW - timedelta(minutes=10), s) for s in range(3))
    primary.put(in_gcs, 'gcs')
    legacy.put(in_gcs, 'legacy')
    legacy.put(in_legacy, 'legacy')

    results = _gcs_migration_store(primary, legacy).select_many([in_gcs, in_legacy, nowhere])

    assert [(r['id'], r['action_data']) for r in results] == [(in_gcs, 'gcs'), (in_legacy, 'legacy')]
    assert primary.selected == [[in_gcs, in_legacy, nowhere]]
    assert legacy.selected == [[in_legacy, nowhere]]
    assert fake_metrics.total('.read.source', 'source:gcs') == 1
    assert fake_metrics.total('.read.source', 'source:legacy') == 1
    assert fake_metrics.total('.read.source', 'source:miss') == 1
    assert fake_metrics.timed_backends() == ['backend:gcs', 'backend:legacy']


def test_select_one_prefers_primary(primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    primary.put(_OLD, 'gcs')
    legacy.put(_OLD, 'legacy')
    record = _gcs_migration_store(primary, legacy).select_one(_OLD)
    assert record is not None and record['action_data'] == 'gcs'
    assert legacy.selected == []


def test_expected_and_unexpected_misses(primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    young = _action_id(_NOW - timedelta(minutes=2))
    store = _gcs_migration_store(primary, legacy)

    store.select_many([_OLD])
    assert fake_metrics.total('.read.gcs_unexpected_miss') == 1
    assert fake_metrics.total('.read.gcs_expected_miss') == 0

    store.select_many([young])
    assert fake_metrics.total('.read.gcs_unexpected_miss') == 1
    assert fake_metrics.total('.read.gcs_expected_miss') == 1


def test_primary_select_failure_falls_back(primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics) -> None:
    primary.fail_select = True
    ids = [_action_id(_NOW - timedelta(minutes=10), s) for s in range(3)]
    for action_id in ids:
        legacy.put(action_id, 'legacy')

    results = _gcs_migration_store(primary, legacy).select_many(ids)

    assert [r['id'] for r in results] == ids
    assert legacy.selected == [ids]
    assert fake_metrics.total('.read.source', 'source:legacy') == 3
    assert fake_metrics.total('.read.gcs_error') == 1
    assert fake_metrics.total('.read.gcs_unexpected_miss') == 0
    assert fake_metrics.total('.read.gcs_expected_miss') == 0


def test_partial_primary_read_keeps_its_results_and_falls_back_for_the_rest(
    primary: _FakeStore, legacy: _FakeStore, fake_metrics: _FakeMetrics
) -> None:
    primary.partial_select = True
    in_gcs, unread = (_action_id(_NOW - timedelta(minutes=10), s) for s in range(2))
    primary.put(in_gcs, 'gcs')
    for action_id in (in_gcs, unread):
        legacy.put(action_id, 'legacy')

    results = _gcs_migration_store(primary, legacy).select_many([in_gcs, unread])

    assert [(r['id'], r['action_data']) for r in results] == [(in_gcs, 'gcs'), (unread, 'legacy')]
    assert legacy.selected == [[unread]]
    assert fake_metrics.total('.read.gcs_error') == 1
    assert fake_metrics.total('.read.gcs_unexpected_miss') == 0
    assert fake_metrics.total('.read.gcs_expected_miss') == 0
    assert fake_metrics.total('.read.source', 'source:gcs') == 1
    assert fake_metrics.total('.read.source', 'source:legacy') == 1


def test_flush_reaches_both_stores(primary: _FakeStore, legacy: _FakeStore) -> None:
    _gcs_migration_store(primary, legacy).flush()
    assert (primary.flushes, legacy.flushes) == (1, 1)


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
            'OSPREY_EXECUTION_RESULT_LEGACY_WRITE_ENABLED': 'false',
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
    batched = gcs_migration._primary
    assert isinstance(batched, StoredExecutionResultGCS)
    assert batched._timer_thread is None
    try:
        # Past the unexpected-miss age but not late, so the records land on time and a GCS miss would
        # count as unexpected.
        moment = datetime.now(timezone.utc) - timedelta(seconds=400)
        ids = [_action_id(moment, sequence) for sequence in range(2)]
        for action_id in ids:
            gcs_migration.insert(action_id, '{"ActionName": "test"}', '[]', moment, '{}')
        service.flush()
        assert len(gcs.objects) == 1

        assert sorted(result.id for result in service.get_many(ids)) == sorted(ids)
        assert fake_metrics.total('.read.source', 'source:gcs') == 2

        def _fail_list(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError('simulated GCS list outage')

        monkeypatch.setattr(_FakeClient, 'list_blobs', _fail_list)

        assert sorted(result.id for result in service.get_many(ids)) == sorted(ids)
        assert fake_metrics.total('.read.gcs_error') == 1
        assert fake_metrics.total('.read.gcs_unexpected_miss') == 0
        assert fake_metrics.total('.read.source', 'source:legacy') == 2
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
