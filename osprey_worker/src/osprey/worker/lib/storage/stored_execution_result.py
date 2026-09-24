from __future__ import annotations

import base64
import gzip
import hashlib
import itertools
import json
import os
import re
import socket
import threading
import time
import zlib
from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, cast
from uuid import uuid4

import gevent
import google.cloud.storage as storage
import pytz
from google.api_core import retry
from google.api_core.exceptions import PreconditionFailed
from google.cloud.bigtable import row_filters, row_set
from google.cloud.bigtable.row import Row
from minio import Minio
from minio.error import S3Error
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.snowflake import Snowflake
from osprey.worker.lib.storage import ExecutionResultStorageBackendType, postgres
from osprey.worker.lib.storage.bigtable import osprey_bigtable
from osprey.worker.lib.storage.bloom_filter import BloomFilter
from osprey.worker.lib.storage.pg_stored_execution import PgStoredExecutionResult
from pydantic.main import BaseModel

logger = get_logger()


if TYPE_CHECKING:
    from osprey.worker.ui_api.osprey.lib.abilities import DataCensorAbility

BIGTABLE_CONCURRENCY_LIMIT = 100
GCS_CONCURRENCY_LIMIT = 100
MINIO_CONCURRENCY_LIMIT = 100


class ExecutionResultStore(ABC):
    """Abstract base class for execution result storage backends."""

    @abstractmethod
    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a single execution result by action ID."""
        pass

    @abstractmethod
    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        """Retrieve multiple execution results by action IDs."""
        pass

    @abstractmethod
    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        """Insert an execution result."""
        pass

    def flush(self) -> None:
        """Upload or persist buffered writes. Default: nothing buffered."""
        return None


class ErrorTrace(BaseModel):
    rules_source_location: str
    traceback: str


class StoredExecutionResult(BaseModel):
    """
    Represents a stored execution result with methods to persist and retrieve it using a storage backend.
    """

    # NOTE: These fields must match the database column names exactly.
    id: int
    extracted_features: Dict[str, Any]
    error_traces: Sequence[ErrorTrace]
    timestamp: datetime
    action_data: Optional[Dict[str, Any]] = None

    @classmethod
    def persist_from_execution_result(
        cls, execution_result: ExecutionResult, storage_backend: ExecutionResultStore
    ) -> None:
        """Persist execution result using the provided storage backend."""
        storage_backend.insert(
            action_id=execution_result.action.action_id,
            extracted_features_json=execution_result.extracted_features_json,
            error_traces_json=execution_result.error_traces_json,
            action_data_json=execution_result.action.data_json,
            timestamp=execution_result.action.timestamp,
        )

    @classmethod
    def get_one_with_action_data(
        cls,
        event_record_id: int,
        storage_backend: ExecutionResultStore,
        data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = (),
    ) -> Optional['StoredExecutionResult']:
        """Get execution result from the provided storage backend."""
        result = storage_backend.select_one(event_record_id)
        if result:
            return StoredExecutionResult.parse_from_query_result(result, data_censor_abilities)
        return None

    @classmethod
    def get_many(
        cls,
        action_ids: List[int],
        storage_backend: ExecutionResultStore,
        data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = (),
    ) -> List['StoredExecutionResult']:
        """Get execution results from the provided storage backend."""
        results = storage_backend.select_many(action_ids)

        return sorted(
            [StoredExecutionResult.parse_from_query_result(result, data_censor_abilities) for result in results],
            key=lambda r: pytz.utc.localize(r.timestamp) if r.timestamp.tzinfo is None else r.timestamp,
            reverse=True,
        )

    @classmethod
    def parse_from_query_result(
        cls, result: Dict[str, Any], data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]]
    ) -> 'StoredExecutionResult':
        # Apply the data censors
        from osprey.worker.ui_api.osprey.lib.abilities import (
            CanViewActionData,
            CanViewFeatureData,
            DataCensorAbility,
        )

        def _censor_data(
            data: Dict[str, Any],
            field: str,
            data_censor_abilities: List[DataCensorAbility[Any, Any]],
            action_name: str,
        ) -> Optional[Dict[str, Any]]:
            data_at_field = data.get(field)
            if not data_at_field:
                return None
            data_copy: Dict[str, Any] = json.loads(data_at_field)
            if not data_censor_abilities:
                return DataCensorAbility.censor_all_leafs(data_copy)
            for censor in data_censor_abilities:
                censor.censor_data(data_copy, action_name)
            assert isinstance(data_copy, dict)
            return data_copy

        action_name: Optional[str] = None
        extracted_features: Optional[Any] = result.get('extracted_features')
        if extracted_features:
            action_name = json.loads(extracted_features).get('ActionName')
        assert action_name is not None, f'Action name could not be parsed from query result: {str(result)}'

        action_data_censors: List[DataCensorAbility[Any, Any]] = [
            censor for censor in data_censor_abilities if censor and isinstance(censor, CanViewActionData)
        ]
        feature_data_censors: List[DataCensorAbility[Any, Any]] = [
            censor for censor in data_censor_abilities if censor and isinstance(censor, CanViewFeatureData)
        ]
        censored_action_data = _censor_data(result, 'action_data', action_data_censors, action_name)
        censored_feature_data = _censor_data(result, 'extracted_features', feature_data_censors, action_name)
        # Continue as normally
        raw_error_traces = result.get('error_traces')
        if raw_error_traces is None:
            error_traces = []
        else:
            error_traces = json.loads(raw_error_traces)

        assert censored_feature_data is not None
        return cls.construct(
            id=result['id'],
            extracted_features=censored_feature_data,
            error_traces=error_traces,
            timestamp=result['timestamp'],
            action_data=censored_action_data,
        )


# TODO: Add tests
class StoredExecutionResultBigTable(ExecutionResultStore):
    retry_policy = retry.Retry(initial=1.0, maximum=2.0, multiplier=1.25, deadline=120.0)

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        row = osprey_bigtable.table('stored_execution_result').read_row(
            StoredExecutionResultBigTable._encode_action_id(action_id), row_filters.CellsColumnLimitFilter(1)
        )
        if not row:
            return None

        return StoredExecutionResultBigTable._execution_result_dict_from_row(row)

    # TODO: Add `select_*_minimal` methods

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        if not action_ids:
            return []

        row_set_obj = row_set.RowSet()
        for action_id in action_ids:
            row_set_obj.add_row_key(StoredExecutionResultBigTable._encode_action_id(action_id))

        rows = osprey_bigtable.table('stored_execution_result').read_rows(
            row_set=row_set_obj,
            filter_=row_filters.CellsColumnLimitFilter(1),
            retry=self.retry_policy,
        )

        results: List[Dict[str, Any]] = []
        for row in rows:
            if not row:
                continue
            results.append(StoredExecutionResultBigTable._execution_result_dict_from_row(row))

        return results

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        row = osprey_bigtable.table('stored_execution_result').row(
            StoredExecutionResultBigTable._encode_action_id(action_id)
        )
        row.set_cell('execution_result', b'extracted_features', extracted_features_json.encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'error_traces', error_traces_json.encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'timestamp', timestamp.isoformat().encode(), timestamp=timestamp)
        row.set_cell('execution_result', b'action_data', action_data_json.encode(), timestamp=timestamp)
        osprey_bigtable.table('stored_execution_result').mutate_rows([row], retry=self.retry_policy)

    @staticmethod
    def _encode_action_id(action_id_snowflake: int) -> bytes:
        """Constructs a bigtable key for a given snowflake."""
        key_prefix = Snowflake(action_id_snowflake).to_key_prefix()
        return f'{key_prefix}:{action_id_snowflake}'.encode()

    @staticmethod
    def _decode_action_id(bigtable_key: bytes) -> int:
        """Extracts the snowflake portion of a bigtable key produced by `to_bigtable_key`"""
        _prefix, _, snowflake = bigtable_key.decode('utf-8').partition(':')
        return int(snowflake)

    @staticmethod
    def _execution_result_dict_from_row(row: Row) -> Dict[str, Any]:
        # row.cells doesn't have the right type information setup (at least in this version of bt), so its ignored here.
        extracted_features = row.cells['execution_result'][b'extracted_features'][0].value.decode('utf-8')  # type: ignore[attr-defined]
        error_traces = row.cells['execution_result'][b'error_traces'][0].value.decode('utf-8')  # type: ignore[attr-defined]
        # This is really dumb but I couldn't get the timestamp value to parse from bytes -> int -> epoch -> datetime
        timestamp = row.cells['execution_result'][b'timestamp'][0].timestamp  # type: ignore[attr-defined]

        execution_result_dict = {
            'id': StoredExecutionResultBigTable._decode_action_id(row.row_key),
            'extracted_features': extracted_features,
            'error_traces': error_traces,
            'timestamp': timestamp,
            'action_data': None,
        }

        action_data = row.cells['execution_result'].get(b'action_data')  # type: ignore[attr-defined]
        if action_data:
            execution_result_dict['action_data'] = action_data[0].value.decode('utf-8')

        return execution_result_dict


class StoredExecutionResultGCS(ExecutionResultStore):
    def __init__(self):
        self._gcs_client: storage.Client | None = None
        self._bucket_name: str | None = None

    def _get_gcs_client(self) -> storage.Client:
        if self._gcs_client is None:
            from osprey.worker.lib.singletons import CONFIG

            config = CONFIG.instance()
            project_id = config.get_str('OSPREY_GCP_PROJECT_ID', 'osprey-dev')
            self._gcs_client = storage.Client(project=project_id)
        return self._gcs_client

    def _get_bucket_name(self) -> str:
        if self._bucket_name is None:
            from osprey.worker.lib.singletons import CONFIG

            config = CONFIG.instance()
            self._bucket_name = config.get_str('OSPREY_GCS_EXECUTION_RESULTS_BUCKET', 'osprey-execution-results-stg')
        return self._bucket_name

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        try:
            with metrics.timed('gcs_stored_execution_result.get_one'):
                object_name = StoredExecutionResultGCS._encode_action_id(action_id)
                bucket = self._get_gcs_client().bucket(self._get_bucket_name())
                blob = bucket.get_blob(object_name)
                if not blob:
                    metrics.increment(
                        'gcs_stored_execution_result.select_one.not_found', tags=[f'action_id:{action_id}']
                    )
                    return None

                raw_data = blob.download_as_bytes()
                data = json.loads(raw_data.decode('utf-8'))

                result = StoredExecutionResultGCS._execution_result_dict_from_gcs_data(data)
                return result
        except Exception as e:
            logger.error(f'Failed to retrieve execution result from GCS for action_id {action_id}: {e}')
            return None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        results = [
            result
            for result in gevent.pool.Pool(GCS_CONCURRENCY_LIMIT).imap(self.select_one, action_ids)
            if result is not None
        ]

        return results

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        try:
            with metrics.timed('gcs_stored_execution_result.insert'):
                object_name = StoredExecutionResultGCS._encode_action_id(action_id)
                data = {
                    'id': action_id,
                    'extracted_features': extracted_features_json,
                    'error_traces': error_traces_json,
                    'timestamp': timestamp.isoformat(),
                    'action_data': action_data_json,
                }

                json_data = json.dumps(data)
                compressed_data = gzip.compress(json_data.encode('utf-8'))

                bucket = self._get_gcs_client().bucket(self._get_bucket_name())
                blob = bucket.blob(object_name)

                blob.content_encoding = 'gzip'

                blob.upload_from_string(compressed_data, content_type='application/json')

        except Exception as e:
            logger.error(f'Failed to insert execution result into GCS for action_id {action_id}: {e}')

    @staticmethod
    def _encode_action_id(action_id_snowflake: int) -> str:
        """Constructs a GCS object key for a given snowflake using the same distribution logic as BigTable."""
        key_prefix = Snowflake(action_id_snowflake).to_key_prefix()
        return f'{key_prefix}:{action_id_snowflake}.json'

    @staticmethod
    def _execution_result_dict_from_gcs_data(data: Dict[str, Any]) -> Dict[str, Any]:
        execution_result_dict = {
            'id': data['id'],
            'extracted_features': data['extracted_features'],
            'error_traces': data['error_traces'],
            'timestamp': datetime.fromisoformat(data['timestamp']),
            'action_data': None,
        }

        action_data = data.get('action_data')
        if action_data:
            execution_result_dict['action_data'] = action_data

        return execution_result_dict


# Caps closed batches waiting on the uploader (about 100 MB at 3 MB each) so a GCS outage cannot grow
# memory without bound; past it, new closes are dropped.
MAX_PENDING_BATCHES = 32

_BATCHED_METRIC = 'gcs_stored_execution_result_batched'
_LATE_SLOT = 'late'
_WRITER_ID_INVALID_CHARS = re.compile(r'[^A-Za-z0-9._-]')
# One counter per process, shared by every store instance, so no two uploads from this process pick
# the same object name. `next()` on `itertools.count` is atomic under the GIL.
_OBJECT_SEQ = itertools.count()


def _writer_id(token: str) -> str:
    # A restarted container keeps its hostname and often its pid, and seq restarts at 0; the token keeps
    # its names distinct. Read the pid on every call so a store built before a fork differs per child.
    host = os.environ.get('HOSTNAME') or socket.gethostname()
    return _WRITER_ID_INVALID_CHARS.sub('_', f'{host}-{os.getpid()}-{token}')


def _object_prefix(day: str, slot: str) -> str:
    return f'v1/{day}/{slot}/'


@dataclass
class _Batch:
    key: Tuple[str, str]
    created_at: float
    # None for late batches: they close by age, not by bucket end.
    bucket_end: Optional[float]
    compressor: Any = field(default_factory=lambda: zlib.compressobj(6, zlib.DEFLATED, 31))
    compressed: bytearray = field(default_factory=bytearray)
    bloom: BloomFilter = field(default_factory=BloomFilter)
    records: int = 0
    raw_bytes: int = 0


class StoredExecutionResultGCSBatched(ExecutionResultStore):
    """Buffers execution results in-process and writes them to GCS as gzip JSON-lines bundles.

    Objects live under `v1/{YYYYMMDD}/{HHMM}/` (the action id's snowflake time, floored to the bucket
    width) or under `v1/{YYYYMMDD}/late/` when the action is older than the late threshold. Each object
    carries a Bloom filter of its ids in custom metadata, so a reader finds a record from the id alone:
    list two prefixes, download only the objects whose filter matches.

    A write is not durable or readable until its batch uploads. A crash loses the open batches, the
    same trade-off as a dropped write in `StoredExecutionResultGCS.insert()`.

    Uses `threading`, not `gevent`. The async worker calls every store method through
    `asyncio.to_thread`, so inserts arrive on real OS threads and nothing there drives a gevent hub.
    """

    # Sleeps between upload attempts; one more attempt than entries.
    _UPLOAD_RETRY_DELAYS_SECONDS: Tuple[float, ...] = (0.5, 1.0)

    def __init__(
        self,
        clock: Callable[[], float] = time.time,
        client_factory: Optional[Callable[[], storage.Client]] = None,
    ) -> None:
        from osprey.worker.lib.singletons import CONFIG

        config = CONFIG.instance()
        # Object names come from the snowflake timestamp, so a zero epoch files every record under 1970.
        if config.get_int('SNOWFLAKE_EPOCH', 0) == 0:
            raise ValueError('SNOWFLAKE_EPOCH must be set and non-zero to use StoredExecutionResultGCSBatched')
        self._bucket_name = config.expect_str('OSPREY_GCS_EXECUTION_RESULTS_BATCH_BUCKET')
        self._bucket_width_minutes = config.get_int('OSPREY_GCS_EXECUTION_RESULTS_BUCKET_WIDTH_MINUTES', 1)
        if self._bucket_width_minutes <= 0 or 60 % self._bucket_width_minutes != 0:
            raise ValueError('OSPREY_GCS_EXECUTION_RESULTS_BUCKET_WIDTH_MINUTES must divide 60')
        self._max_records = config.get_int('OSPREY_GCS_EXECUTION_RESULTS_MAX_RECORDS', 1000)
        self._max_compressed_bytes = config.get_int('OSPREY_GCS_EXECUTION_RESULTS_MAX_COMPRESSED_BYTES', 16777216)
        self._close_grace_seconds = config.get_int('OSPREY_GCS_EXECUTION_RESULTS_CLOSE_GRACE_SECONDS', 15)
        self._late_threshold_seconds = config.get_int('OSPREY_GCS_EXECUTION_RESULTS_LATE_THRESHOLD_SECONDS', 600)
        self._flush_tick_seconds = config.get_int('OSPREY_GCS_EXECUTION_RESULTS_FLUSH_TICK_SECONDS', 60)
        upload_threads = config.get_int('OSPREY_GCS_EXECUTION_RESULTS_UPLOAD_THREADS', 2)
        self._read_threads = config.get_int('OSPREY_GCS_EXECUTION_RESULTS_READ_THREADS', 16)
        project_id = config.get_str('OSPREY_GCP_PROJECT_ID', 'osprey-dev')

        self._clock = clock
        self._client_factory = client_factory or (lambda: storage.Client(project=project_id))
        self._client: Optional[storage.Client] = None
        self._client_lock = threading.Lock()
        self._writer_token = uuid4().hex[:8]

        self._lock = threading.Lock()
        self._batches: Dict[Tuple[str, str], _Batch] = {}
        self._uploader = ThreadPoolExecutor(max_workers=upload_threads, thread_name_prefix='gcs-batched-upload')
        self._in_flight: Set[Future[None]] = set()
        self._in_flight_lock = threading.Lock()
        self._timer_thread: Optional[threading.Thread] = None
        self._stop_timer = threading.Event()

    def _get_client(self) -> storage.Client:
        with self._client_lock:
            if self._client is None:
                self._client = self._client_factory()
            return self._client

    def _on_time_slot(self, snowflake_seconds: float) -> Tuple[str, str, float]:
        """Returns (YYYYMMDD, HHMM, bucket end) for a snowflake time, floored to the bucket width."""
        moment = datetime.fromtimestamp(snowflake_seconds, tz=timezone.utc)
        start = moment.replace(
            minute=moment.minute - moment.minute % self._bucket_width_minutes, second=0, microsecond=0
        )
        return start.strftime('%Y%m%d'), start.strftime('%H%M'), start.timestamp() + self._bucket_width_minutes * 60

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        try:
            for cell, value in (
                ('extracted_features', extracted_features_json),
                ('error_traces', error_traces_json),
                ('action_data', action_data_json),
            ):
                metrics.histogram(f'{_BATCHED_METRIC}.value_bytes', len(value.encode('utf-8')), tags=[f'cell:{cell}'])

            snowflake_seconds = Snowflake(action_id).to_timestamp()
            now = self._clock()
            day, hhmm, bucket_end = self._on_time_slot(snowflake_seconds)
            late = now - snowflake_seconds > self._late_threshold_seconds
            key = (day, _LATE_SLOT if late else hhmm)
            line = (
                json.dumps(
                    {
                        'id': action_id,
                        'ts': timestamp.isoformat(),
                        'extracted_features': extracted_features_json,
                        'error_traces': error_traces_json,
                        'action_data': action_data_json,
                    }
                )
                + '\n'
            ).encode('utf-8')

            closed: Optional[Tuple[_Batch, str]] = None
            with self._lock:
                batch = self._batches.get(key)
                if batch is None:
                    batch = _Batch(key=key, created_at=now, bucket_end=None if late else bucket_end)
                    self._batches[key] = batch
                batch.compressed += batch.compressor.compress(line)
                batch.bloom.add(action_id)
                batch.records += 1
                batch.raw_bytes += len(line)
                if batch.records >= self._max_records:
                    closed = (self._batches.pop(key), 'max_records')
                elif len(batch.compressed) >= self._max_compressed_bytes:
                    closed = (self._batches.pop(key), 'max_bytes')

            metrics.increment(f'{_BATCHED_METRIC}.insert', tags=[f'slot:{"late" if late else "on_time"}'])
            if closed is not None:
                self._submit(*closed)
        except Exception:
            logger.exception(f'Failed to buffer execution result {action_id} for GCS')
            metrics.increment(f'{_BATCHED_METRIC}.dropped_records')

    def start(self) -> None:
        """Starts the daemon thread that closes due batches. Idempotent."""
        if self._timer_thread is not None:
            return
        self._stop_timer.clear()
        self._timer_thread = threading.Thread(target=self._run_timer, name='gcs-batched-timer', daemon=True)
        self._timer_thread.start()

    def close(self) -> None:
        """Stops the timer, uploads every open batch, and shuts the uploader down."""
        if self._timer_thread is not None:
            self._stop_timer.set()
            self._timer_thread.join()
            self._timer_thread = None
        self.flush()
        # flush() already bounded the wait; do not block again on uploads that overran it.
        self._uploader.shutdown(wait=False)

    def flush(self, timeout: float = 25.0) -> None:
        """Uploads every open batch and waits up to `timeout` for all in-flight uploads."""
        with self._lock:
            batches = list(self._batches.values())
            self._batches = {}
        for batch in batches:
            self._submit(batch, 'shutdown')
        with self._in_flight_lock:
            pending = list(self._in_flight)
        _, not_done = wait(pending, timeout=timeout)
        if not_done:
            logger.error(f'{len(not_done)} execution result uploads to GCS did not finish within {timeout}s')

    def _run_timer(self) -> None:
        # Event.wait doubles as the sleep and the stop signal, so close() does not wait a full tick.
        while not self._stop_timer.wait(1.0):
            try:
                self._tick()
            except Exception:
                logger.exception('Failed to close due execution result batches')

    def _tick(self) -> None:
        now = self._clock()
        due: List[Tuple[_Batch, str]] = []
        with self._lock:
            for key, batch in list(self._batches.items()):
                if batch.bucket_end is None:
                    if now - batch.created_at >= self._flush_tick_seconds:
                        due.append((self._batches.pop(key), 'tick'))
                elif now >= batch.bucket_end + self._close_grace_seconds:
                    due.append((self._batches.pop(key), 'bucket_closed'))
            buffered_records = sum(batch.records for batch in self._batches.values())
            open_batches = len(self._batches)
        with self._in_flight_lock:
            pending_batches = len(self._in_flight)
        metrics.gauge(f'{_BATCHED_METRIC}.buffered_records', buffered_records)
        metrics.gauge(f'{_BATCHED_METRIC}.open_batches', open_batches)
        metrics.gauge(f'{_BATCHED_METRIC}.pending_batches', pending_batches)
        for batch, reason in due:
            self._submit(batch, reason)

    def _submit(self, batch: _Batch, reason: str) -> None:
        # Never call this while holding self._lock: the future can finish before we return.
        with self._in_flight_lock:
            if len(self._in_flight) >= MAX_PENDING_BATCHES:
                logger.error(f'Dropped {batch.records} execution results: {MAX_PENDING_BATCHES} batches await upload')
                metrics.increment(f'{_BATCHED_METRIC}.flush_error', batch.records)
                metrics.increment(f'{_BATCHED_METRIC}.dropped_records', batch.records)
                return
            try:
                future = self._uploader.submit(self._upload, batch, reason)
            except RuntimeError:
                # The uploader refuses work after close().
                logger.error(f'Dropped {batch.records} execution results: the GCS uploader is shut down')
                metrics.increment(f'{_BATCHED_METRIC}.dropped_records', batch.records)
                return
            self._in_flight.add(future)
        # Outside the lock: if the future is already done, the callback runs here and takes the lock.
        future.add_done_callback(self._forget_upload)

    def _forget_upload(self, future: Future[None]) -> None:
        with self._in_flight_lock:
            self._in_flight.discard(future)

    def _upload(self, batch: _Batch, reason: str) -> None:
        # Nobody reads the future's result, so an escaped exception would lose the batch silently.
        try:
            self._upload_batch(batch, reason)
        except Exception:
            logger.exception(f'Dropped {batch.records} execution results: unexpected error while uploading')
            metrics.increment(f'{_BATCHED_METRIC}.dropped_records', batch.records)

    def _upload_batch(self, batch: _Batch, reason: str) -> None:
        started = time.monotonic()
        batch.compressed += batch.compressor.flush()
        day, slot = batch.key
        object_name = f'{_object_prefix(day, slot)}{_writer_id(self._writer_token)}-{next(_OBJECT_SEQ):08d}.jsonl.gz'
        metadata = {
            'schema': '1',
            'n': str(batch.records),
            'bloom': base64.b64encode(batch.bloom.to_bytes()).decode('ascii'),
            'bloom_k': '7',
        }
        body = bytes(batch.compressed)
        for attempt in range(len(self._UPLOAD_RETRY_DELAYS_SECONDS) + 1):
            try:
                blob = self._get_client().bucket(self._bucket_name).blob(object_name)
                blob.metadata = metadata
                # retry=None keeps this loop the only retry, so one attempt is one request and a 412 on
                # the first attempt really is a collision.
                blob.upload_from_string(
                    body, content_type='application/gzip', if_generation_match=0, retry=None, timeout=30
                )
                break
            except PreconditionFailed:
                if attempt > 0:
                    # Names are unique, so a 412 on a retry means an earlier attempt stored the object
                    # and only its response was lost.
                    logger.info(f'GCS object {object_name} already exists on retry; an earlier attempt stored it')
                    break
                # A 412 on the first attempt is a real name collision; retrying would hit it again.
                self._drop_failed_upload(batch, object_name)
                return
            except Exception:
                if attempt == len(self._UPLOAD_RETRY_DELAYS_SECONDS):
                    self._drop_failed_upload(batch, object_name)
                    return
                metrics.increment(f'{_BATCHED_METRIC}.upload_retry')
                time.sleep(self._UPLOAD_RETRY_DELAYS_SECONDS[attempt])

        tags = [f'reason:{reason}']
        metrics.timing(f'{_BATCHED_METRIC}.flush.duration', time.monotonic() - started, tags=tags)
        metrics.histogram(f'{_BATCHED_METRIC}.flush.records', batch.records, tags=tags)
        metrics.histogram(f'{_BATCHED_METRIC}.flush.raw_bytes', batch.raw_bytes, tags=tags)
        metrics.histogram(f'{_BATCHED_METRIC}.flush.compressed_bytes', len(body), tags=tags)

    @staticmethod
    def _drop_failed_upload(batch: _Batch, object_name: str) -> None:
        logger.exception(f'Failed to upload {batch.records} execution results to GCS object {object_name}')
        metrics.increment(f'{_BATCHED_METRIC}.flush_error', batch.records)
        metrics.increment(f'{_BATCHED_METRIC}.dropped_records', batch.records)

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        results = self.select_many([action_id])
        return results[0] if results else None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        if not action_ids:
            return []

        with metrics.timed(f'{_BATCHED_METRIC}.select.duration'):
            # The writer's clock decides on-time vs late, so the reader checks both. The late prefix
            # covers a whole day and collects every wanted id from that day.
            wanted_by_prefix: Dict[str, Set[int]] = {}
            for action_id in action_ids:
                day, hhmm, _ = self._on_time_slot(Snowflake(action_id).to_timestamp())
                for slot in (hhmm, _LATE_SLOT):
                    wanted_by_prefix.setdefault(_object_prefix(day, slot), set()).add(action_id)

            found: Dict[int, Dict[str, Any]] = {}
            with ThreadPoolExecutor(max_workers=self._read_threads) as pool:
                candidates = [
                    candidate
                    for listed in pool.map(self._list_candidates, wanted_by_prefix.items())
                    for candidate in listed
                ]
                metrics.increment(f'{_BATCHED_METRIC}.select.candidates', len(candidates))
                for records in pool.map(self._read_candidate, candidates):
                    for record in records:
                        current = found.get(record['id'])
                        # A retried action can land twice; the latest execution wins.
                        if current is None or record['timestamp'] > current['timestamp']:
                            found[record['id']] = record

            metrics.increment(f'{_BATCHED_METRIC}.select.found', len(found))
            metrics.increment(f'{_BATCHED_METRIC}.select.missing', len(set(action_ids)) - len(found))
            return list(found.values())

    def _list_candidates(self, prefix_and_wanted: Tuple[str, Set[int]]) -> List[Tuple[Any, Set[int]]]:
        prefix, wanted = prefix_and_wanted
        candidates: List[Tuple[Any, Set[int]]] = []
        try:
            iterator = self._get_client().list_blobs(
                self._bucket_name, prefix=prefix, fields='items(name,metadata),nextPageToken'
            )
            for page in iterator.pages:
                listed = 0
                for blob in page:
                    listed += 1
                    if self._may_contain(blob.metadata, wanted):
                        candidates.append((blob, wanted))
                metrics.increment(f'{_BATCHED_METRIC}.select.list_pages')
                metrics.increment(f'{_BATCHED_METRIC}.select.objects_listed', listed)
        except Exception:
            logger.exception(f'Failed to list batched execution results under GCS prefix {prefix}')
            return []
        return candidates

    @staticmethod
    def _may_contain(metadata: Optional[Dict[str, str]], wanted: Set[int]) -> bool:
        # An object without a usable filter could hold anything, so it stays a candidate.
        if not metadata or 'bloom' not in metadata:
            return True
        try:
            bloom = BloomFilter.from_bytes(base64.b64decode(metadata['bloom']), int(metadata['bloom_k']))
        except (KeyError, TypeError, ValueError):
            return True
        return any(action_id in bloom for action_id in wanted)

    def _read_candidate(self, candidate: Tuple[Any, Set[int]]) -> List[Dict[str, Any]]:
        blob, wanted = candidate
        records: List[Dict[str, Any]] = []
        try:
            data = blob.download_as_bytes()
            metrics.increment(f'{_BATCHED_METRIC}.select.bytes_downloaded', len(data))
            for line in gzip.decompress(data).splitlines():
                record = json.loads(line)
                if record['id'] in wanted:
                    records.append(
                        {
                            'id': record['id'],
                            'extracted_features': record['extracted_features'],
                            'error_traces': record['error_traces'],
                            'timestamp': datetime.fromisoformat(record['ts']),
                            'action_data': record.get('action_data') or None,
                        }
                    )
        except Exception:
            logger.exception(f'Failed to read batched execution results from GCS object {blob.name}')
            return []
        if not records:
            metrics.increment(f'{_BATCHED_METRIC}.select.bloom_false_positives')
        return records


class StoredExecutionResultMinIO(ExecutionResultStore):
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool, bucket_name: str):
        self._minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self._bucket_name = bucket_name

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        try:
            with metrics.timed('minio_stored_execution_result.get_one'):
                object_name = StoredExecutionResultMinIO._encode_action_id(action_id)

                try:
                    response = self._minio_client.get_object(self._bucket_name, object_name)
                    raw_data = response.read()
                    response.close()
                    response.release_conn()

                    data = json.loads(raw_data.decode('utf-8'))
                    result = StoredExecutionResultMinIO._execution_result_dict_from_minio_data(data)
                    return result

                except S3Error as e:
                    if e.code == 'NoSuchKey':
                        metrics.increment(
                            'minio_stored_execution_result.select_one.not_found', tags=[f'action_id:{action_id}']
                        )
                        return None
                    raise

        except Exception as e:
            logger.error(f'Failed to retrieve execution result from MinIO for action_id {action_id}: {e}')
            return None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        results = [
            result
            for result in gevent.pool.Pool(MINIO_CONCURRENCY_LIMIT).imap(self.select_one, action_ids)
            if result is not None
        ]
        return results

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        try:
            with metrics.timed('minio_stored_execution_result.insert'):
                object_name = StoredExecutionResultMinIO._encode_action_id(action_id)
                data = {
                    'id': action_id,
                    'extracted_features': extracted_features_json,
                    'error_traces': error_traces_json,
                    'timestamp': timestamp.isoformat(),
                    'action_data': action_data_json,
                }

                json_data = json.dumps(data)

                data_stream = BytesIO(json_data.encode('utf-8'))

                self._minio_client.put_object(
                    self._bucket_name,
                    object_name,
                    data_stream,
                    length=len(json_data.encode('utf-8')),
                    content_type='application/json',
                )

        except Exception as e:
            logger.error(f'Failed to insert execution result into MinIO for action_id {action_id}: {e}')

    @staticmethod
    def _encode_action_id(action_id_snowflake: int) -> str:
        """Constructs a MinIO object key for a given snowflake using the same distribution logic as BigTable."""
        key_prefix = Snowflake(action_id_snowflake).to_key_prefix()
        return f'{key_prefix}:{action_id_snowflake}.json'

    @staticmethod
    def _execution_result_dict_from_minio_data(data: Dict[str, Any]) -> Dict[str, Any]:
        execution_result_dict = {
            'id': data['id'],
            'extracted_features': data['extracted_features'],
            'error_traces': data['error_traces'],
            'timestamp': datetime.fromisoformat(data['timestamp']),
            'action_data': None,
        }

        action_data = data.get('action_data')
        if action_data:
            execution_result_dict['action_data'] = action_data

        return execution_result_dict


class StoredExecutionResultPostgres(ExecutionResultStore):
    def __init__(self) -> None:
        postgres.init_from_config('osprey_db')

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        try:
            with metrics.timed('pg_stored_execution_result.get_one'):
                result = PgStoredExecutionResult.select_one(action_id)
                if not result:
                    metrics.increment(
                        'pg_stored_execution_result.select_one.not_found', tags=[f'action_id:{action_id}']
                    )
                    return None
                payload = cast(Dict[str, Any], result.payload)
                return StoredExecutionResultPostgres._execution_result_dict_from_pg_data(payload)

        except Exception as e:
            logger.error(f'Failed to retrieve execution result from PG for action_id {action_id}: {e}')
            return None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        try:
            with metrics.timed('pg_stored_execution_result.get_many'):
                results = PgStoredExecutionResult.select_many(action_ids)
                return [
                    StoredExecutionResultPostgres._execution_result_dict_from_pg_data(
                        cast(Dict[str, Any], result.payload)
                    )
                    for result in results
                ]
        except Exception as e:
            logger.error(f'Failed to retrieve execution results from PG for action_ids {action_ids}: {e}')
            return []

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        try:
            with metrics.timed('pg_stored_execution_result.insert'):
                payload: Dict[str, Any] = {
                    'id': action_id,
                    'extracted_features': extracted_features_json,
                    'error_traces': error_traces_json,
                    'timestamp': timestamp.isoformat(),
                    'action_data': action_data_json,
                }
                PgStoredExecutionResult.insert(action_id, payload)
        except Exception as e:
            logger.error(f'Failed to insert execution result into PG for action_id {action_id}: {e}')

    @staticmethod
    def _execution_result_dict_from_pg_data(data: Dict[str, Any]) -> Dict[str, Any]:
        execution_result_dict = {
            'id': data['id'],
            'extracted_features': data['extracted_features'],
            'error_traces': data['error_traces'],
            'timestamp': datetime.fromisoformat(data['timestamp']),
            'action_data': None,
        }

        action_data = data.get('action_data')
        if action_data:
            execution_result_dict['action_data'] = action_data

        return execution_result_dict


_ROUTING_METRIC = 'execution_result_routing'
# The batched writer closes a slot about 15 s after it ends, then uploads. A sampled id older than this
# should be readable, so a miss means lost or unreadable data rather than a write still in the buffer.
_UNEXPECTED_MISS_AGE_SECONDS = 300


def in_gcs_write_sample(action_id: int, percent: float) -> bool:
    """Returns whether `action_id` falls in the GCS write sample.

    Hashes the id, so writers and readers agree without shared state, and a reader can tell an expected
    GCS miss from an unexpected one.
    """
    if percent <= 0:
        return False
    if percent >= 100:
        return True
    digest = hashlib.blake2b(action_id.to_bytes(8, 'big'), digest_size=4).digest()
    return int.from_bytes(digest, 'big') % 10000 < int(percent * 100)


class RoutingExecutionResultStore(ExecutionResultStore):
    """Moves execution results from a legacy store (BigTable) to a primary store (batched GCS) in steps.

    Writes go to the primary for a percent of action ids and to the legacy store while it stays the
    system of record. Reads try the primary for ids at or above `cutover_id` and fall back to the legacy
    store. Primary failures never propagate; legacy failures do, so the sink keeps its retry semantics.
    """

    def __init__(
        self,
        primary: ExecutionResultStore,
        legacy: ExecutionResultStore,
        gcs_write_percent: float,
        legacy_write_enabled: bool,
        legacy_read_fallback: bool,
        cutover_id: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._primary = primary
        self._legacy = legacy
        self._gcs_write_percent = gcs_write_percent
        self._legacy_write_enabled = legacy_write_enabled
        self._legacy_read_fallback = legacy_read_fallback
        self._cutover_id = cutover_id
        self._clock = clock

    @classmethod
    def from_config(cls, primary: ExecutionResultStore, legacy: ExecutionResultStore) -> RoutingExecutionResultStore:
        from osprey.worker.lib.singletons import CONFIG

        config = CONFIG.instance()
        return cls(
            primary,
            legacy,
            gcs_write_percent=config.get_float('OSPREY_EXECUTION_RESULT_GCS_WRITE_PERCENT', 0.0),
            legacy_write_enabled=config.get_bool('OSPREY_EXECUTION_RESULT_LEGACY_WRITE_ENABLED', True),
            legacy_read_fallback=config.get_bool('OSPREY_EXECUTION_RESULT_LEGACY_READ_FALLBACK', True),
            cutover_id=config.get_int('OSPREY_EXECUTION_RESULT_ROUTING_CUTOVER_ID', 0),
        )

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        outcome = 'skipped'
        if in_gcs_write_sample(action_id, self._gcs_write_percent):
            try:
                self._primary.insert(
                    action_id=action_id,
                    extracted_features_json=extracted_features_json,
                    error_traces_json=error_traces_json,
                    timestamp=timestamp,
                    action_data_json=action_data_json,
                )
                outcome = 'ok'
            except Exception:
                logger.exception(f'Failed to write execution result {action_id} to GCS')
                outcome = 'error'
        metrics.increment(f'{_ROUTING_METRIC}.write', tags=['target:gcs', f'outcome:{outcome}'])

        if not self._legacy_write_enabled:
            return
        try:
            self._legacy.insert(
                action_id=action_id,
                extracted_features_json=extracted_features_json,
                error_traces_json=error_traces_json,
                timestamp=timestamp,
                action_data_json=action_data_json,
            )
        except Exception:
            metrics.increment(f'{_ROUTING_METRIC}.write', tags=['target:legacy', 'outcome:error'])
            raise
        metrics.increment(f'{_ROUTING_METRIC}.write', tags=['target:legacy', 'outcome:ok'])

    def select_one(self, action_id: int) -> Optional[Dict[str, Any]]:
        results = self.select_many([action_id])
        return results[0] if results else None

    def select_many(self, action_ids: List[int]) -> List[Dict[str, Any]]:
        action_ids = list(dict.fromkeys(action_ids))
        # The primary holds nothing written before the cutover, so skip it for older ids. Ids are never
        # negative, so a cutover of 0 makes every id eligible.
        eligible = [i for i in action_ids if i >= self._cutover_id]
        ineligible = [i for i in action_ids if i < self._cutover_id]

        found: Dict[int, Dict[str, Any]] = {}
        if eligible:
            try:
                with metrics.timed(f'{_ROUTING_METRIC}.read.duration', tags=['backend:gcs']):
                    found = {record['id']: record for record in self._primary.select_many(eligible)}
            except Exception:
                logger.exception(f'Failed to read {len(eligible)} execution results from GCS')

        misses = [i for i in eligible if i not in found]
        now = self._clock()
        unexpected = sum(
            1
            for i in misses
            if in_gcs_write_sample(i, self._gcs_write_percent)
            and now - Snowflake(i).to_timestamp() > _UNEXPECTED_MISS_AGE_SECONDS
        )
        if unexpected:
            metrics.increment(f'{_ROUTING_METRIC}.read.gcs_unexpected_miss', unexpected)
        if len(misses) > unexpected:
            metrics.increment(f'{_ROUTING_METRIC}.read.gcs_expected_miss', len(misses) - unexpected)

        fallback: Dict[int, Dict[str, Any]] = {}
        if self._legacy_read_fallback and (misses or ineligible):
            with metrics.timed(f'{_ROUTING_METRIC}.read.duration', tags=['backend:legacy']):
                fallback = {record['id']: record for record in self._legacy.select_many(misses + ineligible)}

        results: List[Dict[str, Any]] = []
        sources = {'gcs': 0, 'legacy': 0, 'miss': 0}
        for action_id in action_ids:
            # During dual write both stores can hold an id; the primary's record wins.
            if action_id in found:
                results.append(found[action_id])
                sources['gcs'] += 1
            elif action_id in fallback:
                results.append(fallback[action_id])
                sources['legacy'] += 1
            else:
                sources['miss'] += 1
        for source, count in sources.items():
            if count:
                metrics.increment(f'{_ROUTING_METRIC}.read.source', count, tags=[f'source:{source}'])
        return results

    def flush(self) -> None:
        try:
            self._primary.flush()
        except Exception:
            logger.exception('Failed to flush buffered execution results to GCS')
        self._legacy.flush()


class ExecutionResultStorageService:
    """Service class that provides execution result operations with a configured backend."""

    def __init__(self, storage_backend: ExecutionResultStore):
        self._storage_backend = storage_backend

    def persist_from_execution_result(self, execution_result: ExecutionResult) -> None:
        """Persist execution result using the configured storage backend."""
        StoredExecutionResult.persist_from_execution_result(execution_result, self._storage_backend)

    def get_one_with_action_data(
        self, event_record_id: int, data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = ()
    ) -> Optional[StoredExecutionResult]:
        """Get execution result from the configured storage backend."""
        return StoredExecutionResult.get_one_with_action_data(
            event_record_id, self._storage_backend, data_censor_abilities
        )

    def get_many(
        self, action_ids: List[int], data_censor_abilities: Sequence[Optional[DataCensorAbility[Any, Any]]] = ()
    ) -> List[StoredExecutionResult]:
        """Get execution results from the configured storage backend."""
        return StoredExecutionResult.get_many(action_ids, self._storage_backend, data_censor_abilities)

    def flush(self) -> None:
        """Upload or persist the backend's buffered writes."""
        self._storage_backend.flush()


def bootstrap_execution_result_storage_service() -> ExecutionResultStorageService:
    """Create an ExecutionResultStorageService with the configured storage backend."""
    from osprey.worker._stdlibplugin.execution_result_store_chooser import get_rules_execution_result_storage_backend
    from osprey.worker.lib.singletons import CONFIG

    config = CONFIG.instance()

    storage_backend_type = ExecutionResultStorageBackendType(
        config.get_str('OSPREY_EXECUTION_RESULT_STORAGE_BACKEND', 'none').lower()
    )
    storage_backend = get_rules_execution_result_storage_backend(backend_type=storage_backend_type)

    if storage_backend is None:
        raise AssertionError('No storage backend registered')

    return ExecutionResultStorageService(storage_backend)
