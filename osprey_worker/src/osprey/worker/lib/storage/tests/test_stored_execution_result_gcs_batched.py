import gzip
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pytest
from osprey.worker.lib.storage.stored_execution_result import StoredExecutionResultGCSBatched

# An action id is a snowflake: `timestamp_ms << 22 | sequence`. Building ids this way (with a
# sequence of 0) is enough to exercise the real `Snowflake.to_key_prefix`/`to_timestamp` math
# the class buckets writes by, without needing a real snowflake generator.
_MINUTE_MS = 60_000


def _action_id(timestamp_ms: int, sequence: int = 0) -> int:
    return (timestamp_ms << 22) | sequence


# Aligned to a minute boundary so offsets added in individual tests can't accidentally cross
# into the next minute bucket.
_BASE_MINUTE_MS = 1_700_000_000_000 - (1_700_000_000_000 % _MINUTE_MS)


class _FakeBlob:
    def __init__(self, bucket: '_FakeBucket', name: str):
        self._bucket = bucket
        self.name = name
        self.content_encoding: Optional[str] = None

    def upload_from_string(self, data: bytes, content_type: str) -> None:
        self._bucket.objects[self.name] = data

    def download_as_bytes(self) -> bytes:
        return self._bucket.objects[self.name]


class _FakeBucket:
    def __init__(self):
        self.objects: Dict[str, bytes] = {}

    def blob(self, name: str) -> _FakeBlob:
        return _FakeBlob(self, name)

    def list_blobs(self, prefix: str) -> List[_FakeBlob]:
        return [self.blob(name) for name in sorted(self.objects) if name.startswith(prefix)]


class _FakeGCSClient:
    def __init__(self):
        self.bucket_ = _FakeBucket()

    def bucket(self, _name: str) -> _FakeBucket:
        return self.bucket_


@pytest.fixture
def fake_bucket() -> _FakeBucket:
    return _FakeBucket()


@pytest.fixture
def store(fake_bucket: _FakeBucket) -> StoredExecutionResultGCSBatched:
    instance = StoredExecutionResultGCSBatched(bucket_name='test-bucket', max_batch_size=3, flush_interval_seconds=60)
    client = _FakeGCSClient()
    client.bucket_ = fake_bucket
    instance._gcs_client = client  # bypass lazy client construction; see _get_gcs_client
    return instance


def _insert(store: StoredExecutionResultGCSBatched, action_id: int, marker: str = 'features') -> None:
    store.insert(
        action_id=action_id,
        extracted_features_json=f'{{"ActionName": "test", "marker": "{marker}"}}',
        error_traces_json='[]',
        timestamp=datetime.now(timezone.utc),
        action_data_json='{}',
    )


def test_insert_below_max_batch_size_does_not_flush(store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket):
    _insert(store, _action_id(_BASE_MINUTE_MS))
    _insert(store, _action_id(_BASE_MINUTE_MS, sequence=1))

    assert fake_bucket.objects == {}
    # Not visible to reads either: nothing has been flushed yet.
    assert store.select_one(_action_id(_BASE_MINUTE_MS)) is None


def test_insert_flushes_automatically_at_max_batch_size(
    store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket
):
    ids = [_action_id(_BASE_MINUTE_MS, sequence=i) for i in range(3)]  # max_batch_size == 3
    for action_id in ids:
        _insert(store, action_id)

    assert len(fake_bucket.objects) == 1
    for action_id in ids:
        result = store.select_one(action_id)
        assert result is not None
        assert result['id'] == action_id


def test_flush_uploads_partial_batches(store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket):
    action_id = _action_id(_BASE_MINUTE_MS)
    _insert(store, action_id)
    assert fake_bucket.objects == {}

    store.flush()

    assert len(fake_bucket.objects) == 1
    result = store.select_one(action_id)
    assert result is not None
    assert result['id'] == action_id
    assert result['extracted_features'] == '{"ActionName": "test", "marker": "features"}'
    assert result['error_traces'] == '[]'
    assert result['action_data'] == '{}'
    assert isinstance(result['timestamp'], datetime)


def test_flush_clears_buffers(store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket):
    _insert(store, _action_id(_BASE_MINUTE_MS))
    store.flush()
    assert store._buffers == {}

    # A second flush with nothing buffered must not write another object.
    store.flush()
    assert len(fake_bucket.objects) == 1


def test_select_one_not_found_returns_none(store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket):
    _insert(store, _action_id(_BASE_MINUTE_MS))
    store.flush()

    other_action_id = _action_id(_BASE_MINUTE_MS, sequence=999)
    assert store.select_one(other_action_id) is None


def test_select_many_spans_different_minute_buckets(store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket):
    same_minute_id = _action_id(_BASE_MINUTE_MS)
    later_minute_id = _action_id(_BASE_MINUTE_MS + 5 * _MINUTE_MS)

    _insert(store, same_minute_id, marker='a')
    _insert(store, later_minute_id, marker='b')
    store.flush()

    # Two different minute buckets, so two separate flush objects.
    assert len(fake_bucket.objects) == 2

    results = store.select_many([same_minute_id, later_minute_id])
    assert {r['id'] for r in results} == {same_minute_id, later_minute_id}


def test_select_many_with_missing_and_empty_input(store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket):
    assert store.select_many([]) == []

    present_id = _action_id(_BASE_MINUTE_MS)
    missing_id = _action_id(_BASE_MINUTE_MS, sequence=42)
    _insert(store, present_id)
    store.flush()

    results = store.select_many([present_id, missing_id])
    assert len(results) == 1
    assert results[0]['id'] == present_id


def test_flushed_objects_are_gzip_compressed_json_lines(
    store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket
):
    _insert(store, _action_id(_BASE_MINUTE_MS))
    store.flush()

    (raw,) = fake_bucket.objects.values()
    decompressed = gzip.decompress(raw)
    lines = [line for line in decompressed.splitlines() if line]
    assert len(lines) == 1


def test_flush_error_drops_the_batch_without_raising(
    store: StoredExecutionResultGCSBatched, fake_bucket: _FakeBucket, monkeypatch: pytest.MonkeyPatch
):
    def _raise(*_args, **_kwargs):
        raise RuntimeError('simulated GCS outage')

    monkeypatch.setattr(_FakeBlob, 'upload_from_string', _raise)

    _insert(store, _action_id(_BASE_MINUTE_MS))
    store.flush()  # must not raise

    assert fake_bucket.objects == {}
    assert store._buffers == {}


def test_start_periodic_flush_is_idempotent(store: StoredExecutionResultGCSBatched):
    store.start_periodic_flush()
    greenlet = store._flush_greenlet
    assert greenlet is not None

    store.start_periodic_flush()
    assert store._flush_greenlet is greenlet

    store.stop_periodic_flush()
    assert store._flush_greenlet is None
    assert greenlet.dead
