import contextlib
import gzip
import json
import random
import re
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

import pytest
from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage import stored_execution_result
from osprey.worker.lib.storage.stored_execution_result import (
    MAX_PENDING_BATCHES,
    ExecutionResultReadError,
    StoredExecutionResultGCSBatched,
)

_EPOCH_MS = 1420070400000
_METRIC = 'gcs_stored_execution_result_batched'
# 2026-09-24 12:34:20 UTC: mid-minute, so small offsets stay in the 12:34 slot.
_BASE = datetime(2026, 9, 24, 12, 34, 20, tzinfo=timezone.utc)
_BASE_CONFIG: Dict[str, Any] = {
    'SNOWFLAKE_EPOCH': _EPOCH_MS,
    'OSPREY_GCS_EXECUTION_RESULTS_BATCH_BUCKET': 'test-bucket',
}


def _action_id(moment: datetime, sequence: int = 0) -> int:
    unix_ms = int(moment.timestamp() * 1000)
    return ((unix_ms - _EPOCH_MS) << 22) | sequence


class _FakeGCS:
    """Shared object store behind the fake client, bucket, and blobs."""

    def __init__(self, page_size: int = 2):
        self.page_size = page_size
        self.objects: Dict[str, Tuple[bytes, Dict[str, str]]] = {}
        self.listed_prefixes: List[str] = []
        self.downloads: List[str] = []
        self.upload_threads: List[str] = []
        self.fail_uploads = False
        # Store the next upload but raise as if its response was lost.
        self.lose_next_response = False
        # Uploads wait on this gate; clear it to hold them.
        self.upload_gate = threading.Event()
        self.upload_gate.set()
        self.fail_list_prefixes: Set[str] = set()
        self.lock = threading.Lock()


class _FakeBlob:
    def __init__(self, gcs: _FakeGCS, name: str, metadata: Optional[Dict[str, str]] = None):
        self._gcs = gcs
        self.name = name
        self.metadata = metadata

    def upload_from_string(
        self,
        data: bytes,
        content_type: str,
        if_generation_match: Optional[int] = None,
        retry: Any = 'library default',
        timeout: float = 60,
    ) -> None:
        assert content_type == 'application/gzip'
        # The store's loop must be the only retry, with a bounded per-request timeout.
        assert retry is None
        assert timeout == 30
        assert self._gcs.upload_gate.wait(timeout=5)
        with self._gcs.lock:
            self._gcs.upload_threads.append(threading.current_thread().name)
            if self._gcs.fail_uploads:
                raise RuntimeError('simulated GCS outage')
            if if_generation_match == 0 and self.name in self._gcs.objects:
                raise PreconditionFailed(f'{self.name} exists')
            self._gcs.objects[self.name] = (data, dict(self.metadata or {}))
            if self._gcs.lose_next_response:
                self._gcs.lose_next_response = False
                raise ServiceUnavailable('response lost after the write')

    def download_as_bytes(self) -> bytes:
        with self._gcs.lock:
            self._gcs.downloads.append(self.name)
            return self._gcs.objects[self.name][0]


class _FakeBucket:
    def __init__(self, gcs: _FakeGCS):
        self._gcs = gcs

    def blob(self, name: str) -> _FakeBlob:
        return _FakeBlob(self._gcs, name)


class _FakeBlobIterator:
    def __init__(self, blobs: List[_FakeBlob], page_size: int):
        self._blobs = blobs
        self._page_size = page_size

    @property
    def pages(self) -> Iterator[List[_FakeBlob]]:
        for start in range(0, len(self._blobs), self._page_size):
            yield self._blobs[start : start + self._page_size]


class _FakeClient:
    def __init__(self, gcs: _FakeGCS):
        self._gcs = gcs

    def bucket(self, name: str) -> _FakeBucket:
        assert name == 'test-bucket'
        return _FakeBucket(self._gcs)

    def list_blobs(self, bucket_or_name: str, prefix: str, fields: str) -> _FakeBlobIterator:
        assert bucket_or_name == 'test-bucket'
        assert fields == 'items(name,metadata),nextPageToken'
        with self._gcs.lock:
            self._gcs.listed_prefixes.append(prefix)
            if prefix in self._gcs.fail_list_prefixes:
                raise RuntimeError(f'simulated list failure for {prefix}')
            blobs = [
                _FakeBlob(self._gcs, name, dict(metadata))
                for name, (_, metadata) in sorted(self._gcs.objects.items())
                if name.startswith(prefix)
            ]
        return _FakeBlobIterator(blobs, self._gcs.page_size)


class _FakeMetrics:
    def __init__(self) -> None:
        self.events: List[Tuple[str, str, float, List[str]]] = []
        self._lock = threading.Lock()

    def _record(self, kind: str, metric: str, value: float, tags: Optional[List[str]]) -> None:
        with self._lock:
            self.events.append((kind, metric, value, list(tags or [])))

    def increment(self, metric: str, value: float = 1, tags: Optional[List[str]] = None) -> None:
        self._record('increment', metric, value, tags)

    def histogram(self, metric: str, value: float, tags: Optional[List[str]] = None) -> None:
        self._record('histogram', metric, value, tags)

    def gauge(self, metric: str, value: float, tags: Optional[List[str]] = None) -> None:
        self._record('gauge', metric, value, tags)

    def timing(self, metric: str, value: float, tags: Optional[List[str]] = None) -> None:
        self._record('timing', metric, value, tags)

    @contextlib.contextmanager
    def timed(self, metric: str, tags: Optional[List[str]] = None) -> Iterator[None]:
        yield
        self._record('timing', metric, 0, tags)

    def values(self, kind: str, suffix: str, tag: Optional[str] = None) -> List[float]:
        return [
            value
            for event_kind, metric, value, tags in self.events
            if event_kind == kind and metric == f'{_METRIC}{suffix}' and (tag is None or tag in tags)
        ]

    def total(self, suffix: str, tag: Optional[str] = None) -> float:
        return sum(self.values('increment', suffix, tag))

    def last_gauge(self, suffix: str) -> float:
        return self.values('gauge', suffix)[-1]


class _FakeClock:
    def __init__(self, now: datetime):
        self.now = now.timestamp()

    def __call__(self) -> float:
        return self.now

    def set(self, moment: datetime) -> None:
        self.now = moment.timestamp()


@pytest.fixture
def gcs() -> _FakeGCS:
    return _FakeGCS()


@pytest.fixture
def fake_metrics(monkeypatch: pytest.MonkeyPatch) -> _FakeMetrics:
    recorder = _FakeMetrics()
    monkeypatch.setattr(stored_execution_result, 'metrics', recorder)
    return recorder


@pytest.fixture
def clock() -> _FakeClock:
    # Five seconds after the base action time: on time.
    return _FakeClock(_BASE + timedelta(seconds=5))


@pytest.fixture
def make_store(
    gcs: _FakeGCS, fake_metrics: _FakeMetrics, clock: _FakeClock, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Callable[..., StoredExecutionResultGCSBatched]]:
    monkeypatch.setattr(StoredExecutionResultGCSBatched, '_UPLOAD_RETRY_DELAYS_SECONDS', (0.0, 0.0))
    config = CONFIG.instance()
    stores: List[StoredExecutionResultGCSBatched] = []

    def _make(**config_overrides: Any) -> StoredExecutionResultGCSBatched:
        config.unconfigure_for_tests()
        config.configure({**_BASE_CONFIG, **config_overrides})
        store = StoredExecutionResultGCSBatched(clock=clock, client_factory=lambda: _FakeClient(gcs))
        stores.append(store)
        return store

    yield _make

    for store in stores:
        store.close()
    config.unconfigure_for_tests()


def _insert(
    store: StoredExecutionResultGCSBatched,
    action_id: int,
    timestamp: datetime = _BASE,
    features: str = '{"ActionName": "test"}',
) -> None:
    store.insert(
        action_id=action_id,
        extracted_features_json=features,
        error_traces_json='[]',
        timestamp=timestamp,
        action_data_json=f'{{"action_id": {action_id}}}',
    )


def _object_names(gcs: _FakeGCS) -> List[str]:
    return sorted(gcs.objects)


def _seq(name: str) -> int:
    match = re.search(r'-(\d{8})\.jsonl\.gz$', name)
    assert match is not None
    return int(match.group(1))


def test_constructor_requires_snowflake_epoch(make_store: Callable[..., StoredExecutionResultGCSBatched]) -> None:
    with pytest.raises(ValueError, match='SNOWFLAKE_EPOCH'):
        make_store(SNOWFLAKE_EPOCH=0)


@pytest.mark.parametrize('width, expected_prefix', [(1, 'v1/20260924/1234/'), (5, 'v1/20260924/1230/')])
def test_on_time_record_lands_under_its_floored_minute(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, width: int, expected_prefix: str
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_BUCKET_WIDTH_MINUTES=width)
    _insert(store, _action_id(_BASE))
    store.flush()

    (name,) = _object_names(gcs)
    assert name.startswith(expected_prefix)


def test_record_older_than_late_threshold_lands_under_late(
    make_store: Callable[..., StoredExecutionResultGCSBatched],
    gcs: _FakeGCS,
    fake_metrics: _FakeMetrics,
    clock: _FakeClock,
) -> None:
    store = make_store()
    clock.set(_BASE + timedelta(seconds=601))
    _insert(store, _action_id(_BASE))
    store.flush()

    (name,) = _object_names(gcs)
    assert name.startswith('v1/20260924/late/12/')
    assert fake_metrics.total('.insert', 'slot:late') == 1
    assert fake_metrics.total('.insert', 'slot:on_time') == 0


def test_object_names_are_sanitized_and_seq_increments_without_overwrite(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('HOSTNAME', 'pod/with:bad chars')
    # One upload thread keeps seq assignment in submission order.
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS=2, OSPREY_GCS_EXECUTION_RESULTS_UPLOAD_THREADS=1)
    for sequence in range(4):
        _insert(store, _action_id(_BASE, sequence))
    store.flush()

    first, second = _object_names(gcs)
    pattern = r'^v1/20260924/1234/(pod_with_bad_chars-\d+-[0-9a-f]{8})-\d{8}\.jsonl\.gz$'
    first_match = re.match(pattern, first)
    second_match = re.match(pattern, second)
    assert first_match is not None and second_match is not None
    # One store keeps one token, so only seq tells its objects apart.
    assert first_match.group(1) == second_match.group(1)
    assert _seq(second) == _seq(first) + 1
    assert [metadata['n'] for _, metadata in gcs.objects.values()] == ['2', '2']


def test_max_records_closes_the_batch_off_the_caller_thread(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, fake_metrics: _FakeMetrics
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS=3)
    for sequence in range(3):
        _insert(store, _action_id(_BASE, sequence))
    store.flush()  # nothing left open; waits for the in-flight upload

    ((_, metadata),) = gcs.objects.values()
    assert metadata['n'] == '3'
    assert fake_metrics.values('histogram', '.flush.records', 'reason:max_records') == [3]
    assert fake_metrics.values('histogram', '.flush.records', 'reason:shutdown') == []
    assert all(name.startswith('gcs-batched-upload') for name in gcs.upload_threads)


def test_max_compressed_bytes_closes_the_batch(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, fake_metrics: _FakeMetrics
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_COMPRESSED_BYTES=1)
    rng = random.Random(0)
    for sequence in range(2):
        # Incompressible enough that deflate emits output on the first write.
        features = json.dumps({'ActionName': 'test', 'noise': rng.randbytes(64 * 1024).hex()})
        _insert(store, _action_id(_BASE, sequence), features=features)
    store.flush()

    assert [metadata['n'] for _, metadata in gcs.objects.values()] == ['1', '1']
    assert fake_metrics.values('histogram', '.flush.records', 'reason:max_bytes') == [1, 1]


def test_on_time_batch_closes_only_after_bucket_end_plus_grace(
    make_store: Callable[..., StoredExecutionResultGCSBatched],
    gcs: _FakeGCS,
    fake_metrics: _FakeMetrics,
    clock: _FakeClock,
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_CLOSE_GRACE_SECONDS=15)
    # Created early in the slot, so the batch is past the 60 s reopen floor before bucket end + grace.
    action_time = datetime(2026, 9, 24, 12, 34, 5, tzinfo=timezone.utc)
    clock.set(action_time + timedelta(seconds=5))
    _insert(store, _action_id(action_time))
    bucket_end = datetime(2026, 9, 24, 12, 35, tzinfo=timezone.utc)

    clock.set(bucket_end + timedelta(seconds=14.9))
    store._tick()
    assert fake_metrics.last_gauge('.open_batches') == 1
    assert fake_metrics.last_gauge('.buffered_records') == 1

    clock.set(bucket_end + timedelta(seconds=15))
    store._tick()
    assert fake_metrics.last_gauge('.open_batches') == 0
    store.flush()

    assert len(gcs.objects) == 1
    assert fake_metrics.values('histogram', '.flush.records', 'reason:bucket_closed') == [1]


def test_reopened_slot_waits_for_the_minimum_age_before_closing(
    make_store: Callable[..., StoredExecutionResultGCSBatched],
    gcs: _FakeGCS,
    fake_metrics: _FakeMetrics,
    clock: _FakeClock,
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_CLOSE_GRACE_SECONDS=15)
    # The 12:34 slot is already past bucket end + grace, but the record is not yet late.
    created = datetime(2026, 9, 24, 12, 35, 30, tzinfo=timezone.utc)
    clock.set(created)
    _insert(store, _action_id(_BASE))

    store._tick()
    assert fake_metrics.last_gauge('.open_batches') == 1
    clock.set(created + timedelta(seconds=59.9))
    store._tick()
    assert fake_metrics.last_gauge('.open_batches') == 1

    clock.set(created + timedelta(seconds=60))
    store._tick()
    assert fake_metrics.last_gauge('.open_batches') == 0
    store.flush()

    (name,) = _object_names(gcs)
    assert name.startswith('v1/20260924/1234/')
    assert fake_metrics.values('histogram', '.flush.records', 'reason:bucket_closed') == [1]


def test_late_batch_closes_after_default_flush_tick(
    make_store: Callable[..., StoredExecutionResultGCSBatched],
    gcs: _FakeGCS,
    fake_metrics: _FakeMetrics,
    clock: _FakeClock,
) -> None:
    store = make_store()
    created = _BASE + timedelta(seconds=700)
    clock.set(created)
    _insert(store, _action_id(_BASE))

    clock.set(created + timedelta(seconds=299.9))
    store._tick()
    assert fake_metrics.last_gauge('.open_batches') == 1

    clock.set(created + timedelta(seconds=300))
    store._tick()
    assert fake_metrics.last_gauge('.open_batches') == 0
    store.flush()

    (name,) = _object_names(gcs)
    assert name.startswith('v1/20260924/late/12/')
    assert fake_metrics.values('histogram', '.flush.records', 'reason:tick') == [1]


def test_every_inserted_record_round_trips_after_flush(
    make_store: Callable[..., StoredExecutionResultGCSBatched], fake_metrics: _FakeMetrics
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS=4)
    moments = [_BASE, _BASE + timedelta(minutes=3)]
    ids = [_action_id(moment, sequence) for moment in moments for sequence in range(5)]
    for action_id in ids:
        _insert(store, action_id, features=f'{{"ActionName": "test", "id": {action_id}}}')
    store.flush()

    results = {result['id']: result for result in store.select_many(ids)}

    assert set(results) == set(ids)
    for action_id in ids:
        assert results[action_id] == {
            'id': action_id,
            'extracted_features': f'{{"ActionName": "test", "id": {action_id}}}',
            'error_traces': '[]',
            'timestamp': _BASE,
            'action_data': f'{{"action_id": {action_id}}}',
        }
        assert isinstance(results[action_id]['timestamp'], datetime)
    assert fake_metrics.total('.select.found') == len(ids)
    assert fake_metrics.total('.select.missing') == 0


def test_missing_id_returns_none(
    make_store: Callable[..., StoredExecutionResultGCSBatched], fake_metrics: _FakeMetrics
) -> None:
    store = make_store()
    _insert(store, _action_id(_BASE))
    store.flush()

    assert store.select_many([]) == []
    assert store.select_one(_action_id(_BASE, sequence=999)) is None
    assert fake_metrics.total('.select.missing') == 1


def test_reader_lists_minute_and_late_hour_prefixes_and_finds_late_records(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, clock: _FakeClock
) -> None:
    store = make_store()
    clock.set(_BASE + timedelta(hours=2))
    action_ids = [_action_id(_BASE), _action_id(_BASE + timedelta(minutes=1))]
    for action_id in action_ids:
        _insert(store, action_id)
    store.flush()

    results = store.select_many(action_ids)

    assert sorted(result['id'] for result in results) == sorted(action_ids)
    # Both ids share the 12:00 hour, so its late prefix is listed once.
    assert sorted(gcs.listed_prefixes) == ['v1/20260924/1234/', 'v1/20260924/1235/', 'v1/20260924/late/12/']


def test_bloom_prefilter_downloads_only_candidate_objects(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, fake_metrics: _FakeMetrics
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS=10)
    other_minute = _BASE + timedelta(minutes=1)
    for moment in (_BASE, other_minute):
        for sequence in range(30):
            _insert(store, _action_id(moment, sequence))
    store.flush()
    assert len(gcs.objects) == 6

    wanted = _action_id(_BASE, sequence=17)
    result = store.select_one(wanted)

    assert result is not None
    assert result['id'] == wanted
    (downloaded,) = gcs.downloads
    assert downloaded.startswith('v1/20260924/1234/')
    # Three objects in the 12:34 slot at page size 2; the late prefix is empty.
    assert fake_metrics.total('.select.objects_listed') == 3
    assert fake_metrics.total('.select.list_pages') == 2
    assert fake_metrics.total('.select.candidates') == 1
    assert fake_metrics.total('.select.bloom_false_positives') == 0


def test_unparseable_bloom_metadata_makes_only_that_blob_a_candidate(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS=10)
    for sequence in range(30):
        _insert(store, _action_id(_BASE, sequence))
    store.flush()
    garbage_id = _action_id(_BASE, sequence=500)
    garbage_line = json.dumps(
        {'id': garbage_id, 'ts': _BASE.isoformat(), 'extracted_features': '{}', 'error_traces': '[]', 'action_data': ''}
    )
    garbage_name = 'v1/20260924/1234/other-writer-00000000.jsonl.gz'
    gcs.objects[garbage_name] = (gzip.compress(garbage_line.encode()), {'bloom': 'not base64!!', 'bloom_k': '7'})

    results = store.select_many([_action_id(_BASE, sequence=17), garbage_id])

    assert {result['id'] for result in results} == {_action_id(_BASE, sequence=17), garbage_id}
    # The garbage blob plus the one good blob whose filter matches.
    assert len(gcs.downloads) == 2
    assert garbage_name in gcs.downloads


def test_list_failure_raises_with_results_from_the_other_prefixes(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, fake_metrics: _FakeMetrics
) -> None:
    store = make_store()
    good_id = _action_id(_BASE)
    unlistable_id = _action_id(_BASE + timedelta(minutes=1))
    _insert(store, good_id)
    _insert(store, unlistable_id)
    store.flush()
    gcs.fail_list_prefixes.add('v1/20260924/1235/')

    with pytest.raises(ExecutionResultReadError) as raised:
        store.select_many([good_id, unlistable_id])

    assert [result['id'] for result in raised.value.partial_results] == [good_id]
    assert raised.value.failed_prefixes == 1
    assert fake_metrics.total('.select.found') == 1
    assert fake_metrics.total('.select.missing') == 1


def test_download_failure_raises_with_results_from_the_other_objects(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store()
    good_id = _action_id(_BASE)
    unreadable_id = _action_id(_BASE + timedelta(minutes=1))
    _insert(store, good_id)
    _insert(store, unreadable_id)
    store.flush()
    download = _FakeBlob.download_as_bytes

    def _fail_1235(blob: _FakeBlob) -> bytes:
        if blob.name.startswith('v1/20260924/1235/'):
            raise RuntimeError('simulated download failure')
        return download(blob)

    monkeypatch.setattr(_FakeBlob, 'download_as_bytes', _fail_1235)

    with pytest.raises(ExecutionResultReadError) as raised:
        store.select_many([good_id, unreadable_id])

    assert [result['id'] for result in raised.value.partial_results] == [good_id]
    assert raised.value.failed_prefixes == 1


def test_duplicate_id_returns_the_latest_timestamp(make_store: Callable[..., StoredExecutionResultGCSBatched]) -> None:
    store = make_store()
    action_id = _action_id(_BASE)
    # A replay re-publishes the action stamped with a later publish time. Write it first so write order
    # cannot decide the winner.
    replayed_at = _BASE + timedelta(minutes=5)
    _insert(store, action_id, timestamp=replayed_at, features='{"ActionName": "replay"}')
    store.flush()
    _insert(store, action_id, timestamp=_BASE)
    store.flush()

    results = store.select_many([action_id])

    assert len(results) == 1
    assert results[0]['timestamp'] == replayed_at
    assert results[0]['extracted_features'] == '{"ActionName": "replay"}'


def test_duplicate_id_with_equal_timestamp_returns_one_record(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS
) -> None:
    store = make_store()
    action_id = _action_id(_BASE)
    # A re-execution keeps the action timestamp, so both copies carry the same `ts`.
    for _ in range(2):
        _insert(store, action_id)
        store.flush()
    assert len(gcs.objects) == 2

    results = store.select_many([action_id])

    assert len(results) == 1
    assert results[0]['id'] == action_id


def test_upload_failure_drops_the_batch_and_counts_it(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, fake_metrics: _FakeMetrics
) -> None:
    gcs.fail_uploads = True
    store = make_store()
    for sequence in range(3):
        _insert(store, _action_id(_BASE, sequence))  # must not raise
    store.flush()

    assert gcs.objects == {}
    assert len(gcs.upload_threads) == 3
    assert fake_metrics.total('.upload_retry') == 2
    assert fake_metrics.total('.dropped_records') == 3
    assert fake_metrics.total('.flush_error') == 3
    assert fake_metrics.values('histogram', '.flush.records') == []


def test_precondition_failed_on_retry_counts_as_stored(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, fake_metrics: _FakeMetrics
) -> None:
    gcs.lose_next_response = True
    store = make_store()
    action_id = _action_id(_BASE)
    _insert(store, action_id)
    store.flush()

    assert len(gcs.upload_threads) == 2
    assert fake_metrics.total('.upload_retry') == 1
    assert fake_metrics.total('.dropped_records') == 0
    assert fake_metrics.total('.flush_error') == 0
    assert fake_metrics.values('histogram', '.flush.records', 'reason:shutdown') == [1]
    assert store.select_one(action_id) is not None


def test_precondition_failed_on_first_attempt_drops_without_retry(
    make_store: Callable[..., StoredExecutionResultGCSBatched],
    gcs: _FakeGCS,
    fake_metrics: _FakeMetrics,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _collide(*_args: Any, **_kwargs: Any) -> None:
        gcs.upload_threads.append(threading.current_thread().name)
        raise PreconditionFailed('name collision')

    monkeypatch.setattr(_FakeBlob, 'upload_from_string', _collide)
    store = make_store()
    _insert(store, _action_id(_BASE))
    store.flush()

    assert len(gcs.upload_threads) == 1
    assert fake_metrics.total('.upload_retry') == 0
    assert fake_metrics.total('.dropped_records') == 1
    assert fake_metrics.values('histogram', '.flush.records') == []


def test_backlog_past_max_pending_batches_is_dropped(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS, fake_metrics: _FakeMetrics
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS=1, OSPREY_GCS_EXECUTION_RESULTS_UPLOAD_THREADS=1)
    gcs.upload_gate.clear()
    # Each insert closes a one-record batch; the first MAX_PENDING_BATCHES fill the backlog.
    for sequence in range(MAX_PENDING_BATCHES + 1):
        _insert(store, _action_id(_BASE, sequence))
    store._tick()

    assert fake_metrics.last_gauge('.pending_batches') == MAX_PENDING_BATCHES
    assert fake_metrics.total('.dropped_records') == 1
    assert fake_metrics.total('.flush_error') == 1

    gcs.upload_gate.set()
    store.flush()

    assert len(gcs.objects) == MAX_PENDING_BATCHES
    assert fake_metrics.total('.dropped_records') == 1


def test_unexpected_upload_error_is_counted_as_dropped(
    make_store: Callable[..., StoredExecutionResultGCSBatched],
    gcs: _FakeGCS,
    fake_metrics: _FakeMetrics,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _broken_writer_id(_token: str) -> str:
        raise RuntimeError('simulated bug outside the retry loop')

    monkeypatch.setattr(stored_execution_result, '_writer_id', _broken_writer_id)
    store = make_store()
    for sequence in range(2):
        _insert(store, _action_id(_BASE, sequence))
    store.flush()

    assert gcs.objects == {}
    assert fake_metrics.total('.dropped_records') == 2


def test_object_metadata_stays_under_gcs_limit(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS
) -> None:
    store = make_store()
    for sequence in range(1000):
        _insert(store, _action_id(_BASE, sequence))
    store.flush()

    ((data, metadata),) = gcs.objects.values()
    assert metadata['schema'] == '1'
    assert metadata['n'] == '1000'
    assert metadata['bloom_k'] == '7'
    assert sum(len(key) + len(value) for key, value in metadata.items()) < 8192
    assert len(gzip.decompress(data).splitlines()) == 1000


def test_concurrent_inserts_do_not_lose_writes(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS
) -> None:
    """The async worker calls insert() through asyncio.to_thread, so inserts race on real OS threads."""
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS=7)
    thread_count = 16
    inserts_per_thread = 25
    ids_by_thread = [
        [_action_id(_BASE, thread_index * inserts_per_thread + i) for i in range(inserts_per_thread)]
        for thread_index in range(thread_count)
    ]

    def _insert_all(ids: List[int]) -> None:
        for action_id in ids:
            _insert(store, action_id)

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        for future in [executor.submit(_insert_all, ids) for ids in ids_by_thread]:
            future.result()
    store.flush()

    all_ids = [action_id for ids in ids_by_thread for action_id in ids]
    assert sum(int(metadata['n']) for _, metadata in gcs.objects.values()) == len(all_ids)
    assert {result['id'] for result in store.select_many(all_ids)} == set(all_ids)


def test_start_is_idempotent_and_close_uploads_open_batches(
    make_store: Callable[..., StoredExecutionResultGCSBatched], gcs: _FakeGCS
) -> None:
    store = make_store()
    store.start()
    thread = store._timer_thread
    assert thread is not None and thread.is_alive()
    store.start()
    assert store._timer_thread is thread

    _insert(store, _action_id(_BASE))
    store.close()

    assert not thread.is_alive()
    assert len(gcs.objects) == 1


def test_constructing_a_store_starts_no_thread_and_the_first_insert_starts_the_timer(
    make_store: Callable[..., StoredExecutionResultGCSBatched],
) -> None:
    before = set(threading.enumerate())
    store = make_store()
    assert set(threading.enumerate()) - before == set()
    assert store._timer_thread is None

    _insert(store, _action_id(_BASE))

    timer = store._timer_thread
    assert timer is not None and timer.is_alive()
    assert timer in set(threading.enumerate()) - before


def test_close_cancels_queued_uploads_and_counts_them_as_dropped(
    make_store: Callable[..., StoredExecutionResultGCSBatched],
    gcs: _FakeGCS,
    fake_metrics: _FakeMetrics,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = make_store(OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS=1, OSPREY_GCS_EXECUTION_RESULTS_UPLOAD_THREADS=1)
    gcs.upload_gate.clear()
    # One upload runs and waits at the gate; the other two stay queued.
    for sequence in range(3):
        _insert(store, _action_id(_BASE, sequence))
    uploads = list(store._in_flight)
    monkeypatch.setattr(store, 'flush', lambda: StoredExecutionResultGCSBatched.flush(store, timeout=0.1))

    store.close()

    assert fake_metrics.total('.dropped_records') == 2
    assert [upload.cancelled() for upload in uploads] == [False, True, True]
    assert store._in_flight.keys() == {uploads[0]}
    gcs.upload_gate.set()
    # Only the running upload can finish; wait() never sees a cancel()led future as done.
    wait(uploads[:1], timeout=5)
    assert len(gcs.objects) == 1
