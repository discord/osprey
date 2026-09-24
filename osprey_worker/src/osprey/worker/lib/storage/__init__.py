from enum import StrEnum, auto

# Import all models to ensure they're registered with SQLAlchemy
# This is required for metadata.create_all() to create all tables
from .bulk_action_task import BulkActionJob, BulkActionTask  # noqa: F401
from .bulk_label_task import BulkLabelTask  # noqa: F401
from .queries import Query, SavedQuery  # noqa: F401
from .temporary_ability_token import TemporaryAbilityToken  # noqa: F401


class ExecutionResultStorageBackendType(StrEnum):
    """Type of store used for execution results."""

    BIGTABLE = auto()
    """
    Bigtable execution result store
    """

    GCS = auto()
    """
    Google Cloud Storage execution result store
    """

    GCS_BATCHED = auto()
    """
    Google Cloud Storage execution result store that buffers writes and flushes many of them
    into a single object, instead of one object per write. See StoredExecutionResultGCSBatched.
    """

    ROUTING = auto()
    """
    Writes go to BigTable and, for a configurable percent of action ids, to the batched GCS store. Reads
    try GCS, then fall back to BigTable. See RoutingExecutionResultStore.
    """

    MINIO = auto()
    """
    Minio execution result store
    """

    POSTGRES = auto()
    """
    Postgres execution result store
    """

    PLUGIN = auto()
    """
    Execution result store that is defined via register_execution_result_store
    """

    NONE = auto()
    """
    Disable execution results from being stored. This may cause certain elements of Osprey to break, such as the events stream and individual event details in the UI
    """
