from __future__ import annotations

import gzip
import json
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from io import BytesIO
from time import monotonic
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, cast

import gevent
import google.cloud.storage as storage
import pytz
from google.api_core import retry
from google.api_core.exceptions import from_grpc_status
from google.cloud.bigtable import row_filters, row_set
from google.cloud.bigtable.row import DirectRow, Row
from google.cloud.bigtable.table import DEFAULT_RETRY, Table
from google.rpc import code_pb2
from minio import Minio
from minio.error import S3Error
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.snowflake import Snowflake
from osprey.worker.lib.storage import ExecutionResultStorageBackendType, postgres
from osprey.worker.lib.storage.bigtable import osprey_bigtable
from osprey.worker.lib.storage.pg_stored_execution import PgStoredExecutionResult
from pydantic.main import BaseModel

logger = get_logger()


if TYPE_CHECKING:
    from osprey.worker.ui_api.osprey.lib.abilities import DataCensorAbility

BIGTABLE_CONCURRENCY_LIMIT = 100
GCS_CONCURRENCY_LIMIT = 100
MINIO_CONCURRENCY_LIMIT = 100


def _grpc_code_name(code: int) -> str:
    """Symbolic gRPC status name for metric tags (e.g. UNAVAILABLE instead of 14)."""
    try:
        return code_pb2.Code.Name(code)
    except ValueError:
        return str(code)


def _encoded_request_size(rows: Iterable[DirectRow]) -> int:
    """Bytes these rows add to a MutateRows request: row keys plus cell mutations.

    Read this BEFORE the rows are committed. `Table.mutate_rows` clears the mutations of
    every row it commits, so a row that has already been through it measures as its key
    alone — about four bytes.
    """
    return sum(len(row.row_key) + row.get_mutations_size() for row in rows)


def _classify_row_status(row_status: Optional[Any]) -> Tuple[Optional[BaseException], Tuple[str, ...]]:
    """Classifies one returned row status as (error or None, metric tags describing it).

    A missing status is a failure: `mutate_rows` pre-fills its status list with None and
    returns it as-is when the retry deadline hits, so those entries do occur.
    """
    if row_status is None:
        error: Optional[BaseException] = RuntimeError('Bigtable stored-result mutation ended without a row status')
        return error, ('outcome:row_error', 'code:missing')
    if row_status.code == code_pb2.OK:
        return None, ('outcome:success',)
    return from_grpc_status(row_status.code, row_status.message), (
        'outcome:row_error',
        f'code:{_grpc_code_name(row_status.code)}',
    )


@dataclass(frozen=True)
class ExecutionResultWrite:
    action_id: int
    extracted_features_json: str
    error_traces_json: str
    timestamp: datetime
    action_data_json: str

    @classmethod
    def from_execution_result(cls, execution_result: ExecutionResult) -> 'ExecutionResultWrite':
        return cls(
            action_id=execution_result.action.action_id,
            extracted_features_json=execution_result.extracted_features_json,
            error_traces_json=execution_result.error_traces_json,
            timestamp=execution_result.action.timestamp,
            action_data_json=execution_result.action.data_json,
        )

    @cached_property
    def payload_size_bytes(self) -> int:
        """Encoded Bigtable size of this write (row key plus cell mutations) — the same
        accounting `insert_many` reports as `stored_execution_result.batch_bytes`, so
        callers sizing batches against request limits use one measure.

        Cached: a batching caller asks every write for its size and `insert_many` then builds
        the row again to send it, so this keeps it at one encode per write."""
        return _encoded_request_size([StoredExecutionResultBigTable._build_row(self)])


@dataclass(frozen=True)
class ExecutionResultPersistOutcome:
    error: Optional[BaseException] = None

    @property
    def succeeded(self) -> bool:
        return self.error is None

    def raise_for_error(self) -> None:
        if self.error is not None:
            raise self.error


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

    def insert_many(self, writes: Sequence[ExecutionResultWrite]) -> List[ExecutionResultPersistOutcome]:
        """One outcome per write, aligned to input order.

        An outcome only reflects whether insert() raised, so backends that catch and log write
        errors internally (GCS, MinIO, Postgres) always report success here.

        Subclasses overriding this must NOT implement insert() by delegating to insert_many():
        this fallback calls insert(), so that cycle recurses once the override is removed.
        """
        outcomes: List[ExecutionResultPersistOutcome] = []
        for write in writes:
            try:
                self.insert(
                    action_id=write.action_id,
                    extracted_features_json=write.extracted_features_json,
                    error_traces_json=write.error_traces_json,
                    timestamp=write.timestamp,
                    action_data_json=write.action_data_json,
                )
            except Exception as error:
                outcomes.append(ExecutionResultPersistOutcome(error=error))
            else:
                outcomes.append(ExecutionResultPersistOutcome())
        return outcomes


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
    # mutate_rows signals retryable work only by raising _BigtableRetryableError, which
    # DEFAULT_RETRY's predicate matches and retry_policy's transient-error predicate never does —
    # reusing retry_policy here disables mutation retries entirely.
    #
    # Bounded well inside the tightest caller budget, because DEFAULT_RETRY's own 120s deadline
    # outlasts every caller: the async sink abandons a write after 5s (sync counterpart 2s) and
    # runs it in asyncio.to_thread(), which cannot cancel the thread. Longer recovery is the
    # sinks' job, not ours.
    MUTATION_RETRY_DEADLINE_SECONDS = 2.0
    mutation_retry_policy = DEFAULT_RETRY.with_delay(initial=0.1, maximum=0.5, multiplier=2.0).with_deadline(
        MUTATION_RETRY_DEADLINE_SECONDS
    )
    # Bigtable rejects MutateRows requests above 100,000 mutations (google-cloud-bigtable
    # raises TooManyMutationsError client-side, which is not transient and would fail the
    # same batch on every retry); each write is four SetCell mutations.
    MAX_ROWS_PER_BULK_CALL = 25_000

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

    @staticmethod
    def _build_row(write: ExecutionResultWrite, table: Optional[Table] = None) -> DirectRow:
        row = DirectRow(StoredExecutionResultBigTable._encode_action_id(write.action_id), table)
        row.set_cell(
            'execution_result',
            b'extracted_features',
            write.extracted_features_json.encode(),
            timestamp=write.timestamp,
        )
        row.set_cell('execution_result', b'error_traces', write.error_traces_json.encode(), timestamp=write.timestamp)
        row.set_cell('execution_result', b'timestamp', write.timestamp.isoformat().encode(), timestamp=write.timestamp)
        row.set_cell('execution_result', b'action_data', write.action_data_json.encode(), timestamp=write.timestamp)
        return row

    def insert_many(self, writes: Sequence[ExecutionResultWrite]) -> List[ExecutionResultPersistOutcome]:
        """Sends the writes in MutateRows calls of at most MAX_ROWS_PER_BULK_CALL rows each.

        Chunking bounds the mutation count, not the encoded request size, which Bigtable also
        caps — size byte-sensitive batches with ExecutionResultWrite.payload_size_bytes. A
        call-wide failure maps to every row of its chunk; partial statuses stay independent.
        """
        if not writes:
            return []

        try:
            table = osprey_bigtable.table('stored_execution_result')
            rows = [self._build_row(write, table) for write in writes]
        except Exception as error:
            metrics.increment(
                'stored_execution_result.row_outcome', len(writes), tags=['outcome:batch_error', 'path:bulk']
            )
            return [ExecutionResultPersistOutcome(error=error) for _ in writes]

        outcomes: List[ExecutionResultPersistOutcome] = []
        for start in range(0, len(rows), self.MAX_ROWS_PER_BULK_CALL):
            chunk = rows[start : start + self.MAX_ROWS_PER_BULK_CALL]
            encoded_bytes = _encoded_request_size(chunk)
            try:
                chunk_outcomes = self._commit_rows(table, chunk, path='bulk')
            except Exception as error:
                metrics.increment(
                    'stored_execution_result.row_outcome', len(chunk), tags=['outcome:batch_error', 'path:bulk']
                )
                outcomes.extend(ExecutionResultPersistOutcome(error=error) for _ in chunk)
                continue
            # Emitted only once the call has returned, so batch_rows.count counts completed
            # bulk calls and the dashboard's calls-per-row formula excludes raised ones.
            metrics.histogram('stored_execution_result.batch_rows', len(chunk))
            metrics.histogram('stored_execution_result.batch_bytes', encoded_bytes)
            outcomes.extend(chunk_outcomes)
        return outcomes

    def _commit_rows(
        self, table: Table, rows: Sequence[DirectRow], *, path: Literal['single', 'bulk']
    ) -> List[ExecutionResultPersistOutcome]:
        """Issues one MutateRows call and returns outcomes aligned to `rows`.

        Call-wide exceptions propagate to the caller. `path` tags the metrics so the two write
        paths stay separable while both are live; batch sizing belongs to `insert_many`, the
        only caller that batches.
        """
        path_tag = f'path:{path}'
        started = monotonic()
        try:
            statuses = table.mutate_rows(rows, retry=self.mutation_retry_policy)
        finally:
            metrics.timing('stored_execution_result.commit_ms', (monotonic() - started) * 1000, tags=[path_tag])

        if len(statuses) != len(rows):
            error = RuntimeError(f'Bigtable returned {len(statuses)} statuses for {len(rows)} rows')
            metrics.increment(
                'stored_execution_result.row_outcome', len(rows), tags=['outcome:status_count_error', path_tag]
            )
            return [ExecutionResultPersistOutcome(error=error) for _ in rows]

        classified = [_classify_row_status(row_status) for row_status in statuses]
        # One increment per distinct outcome rather than per row: a chunk of 25,000 rows
        # failing the same way is one datagram, not 25,000.
        for outcome_tags, count in Counter(tags for _, tags in classified).items():
            metrics.increment('stored_execution_result.row_outcome', count, tags=[*outcome_tags, path_tag])
        return [ExecutionResultPersistOutcome(error=error) for error, _ in classified]

    def insert(
        self,
        action_id: int,
        extracted_features_json: str,
        error_traces_json: str,
        timestamp: datetime,
        action_data_json: str,
    ) -> None:
        # Deliberately not routed through insert_many(): the base fallback calls insert(), so
        # that cycle would recurse if the insert_many() override were removed as a rollback.
        write = ExecutionResultWrite(
            action_id=action_id,
            extracted_features_json=extracted_features_json,
            error_traces_json=error_traces_json,
            timestamp=timestamp,
            action_data_json=action_data_json,
        )
        table = osprey_bigtable.table('stored_execution_result')
        # Preserves the pre-bulk contract: call-wide exceptions propagate into the sinks' retry
        # path, while a failed row status is counted but not raised — raising costs three sink
        # attempts plus a Sentry event per row, at a rate nothing has measured yet.
        self._commit_rows(table, [self._build_row(write, table)], path='single')

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


class ExecutionResultStorageService:
    """Service class that provides execution result operations with a configured backend."""

    def __init__(self, storage_backend: ExecutionResultStore):
        self._storage_backend = storage_backend

    def persist_from_execution_result(self, execution_result: ExecutionResult) -> None:
        """Persist execution result using the configured storage backend."""
        StoredExecutionResult.persist_from_execution_result(execution_result, self._storage_backend)

    def persist_writes(self, writes: Sequence[ExecutionResultWrite]) -> List[ExecutionResultPersistOutcome]:
        return self._storage_backend.insert_many(writes)

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
