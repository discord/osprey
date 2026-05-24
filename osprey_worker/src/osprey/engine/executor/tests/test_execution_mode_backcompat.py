"""Back-compat tests for Action.execution_mode plumbing (Task 1.5)."""
from datetime import datetime
from unittest.mock import MagicMock


def test_action_defaults_execution_mode_to_unspecified():
    """Existing code constructing Action without execution_mode keyword must continue to work."""
    from osprey.engine.executor.execution_context import Action
    a = Action(
        action_id=1,
        action_name="USER_REGISTER_ATTEMPTED",
        data={"foo": "bar"},
        timestamp=datetime(2026, 1, 1),
    )
    assert a.execution_mode == "unspecified"


def test_action_accepts_sync_mode():
    from osprey.engine.executor.execution_context import Action
    a = Action(
        action_id=1,
        action_name="USER_REGISTER_ATTEMPTED",
        data={},
        timestamp=datetime(2026, 1, 1),
        execution_mode="sync",
    )
    assert a.execution_mode == "sync"


def test_action_accepts_async_mode():
    from osprey.engine.executor.execution_context import Action
    a = Action(
        action_id=1,
        action_name="GUILD_JOINED",
        data={},
        timestamp=datetime(2026, 1, 1),
        execution_mode="async",
    )
    assert a.execution_mode == "async"


def test_action_to_dict_roundtrips_execution_mode():
    from osprey.engine.executor.execution_context import Action
    a = Action(
        action_id=1,
        action_name="GUILD_JOIN_ATTEMPTED",
        data={"x": 1},
        timestamp=datetime(2026, 1, 1),
        execution_mode="sync",
    )
    d = a.to_dict()
    assert d.get("execution_mode") == "sync"


def test_action_from_dict_defaults_when_absent():
    """Older serialized Actions (no execution_mode key) must deserialize cleanly."""
    from osprey.engine.executor.execution_context import Action
    a = Action.from_dict({
        "action_id": 42,
        "action_name": "USER_LOGIN_ATTEMPTED",
        "data": {},
        "timestamp": "2026-01-01T00:00:00",
    })
    assert a.execution_mode == "unspecified"


def test_action_from_dict_reads_execution_mode_when_present():
    from osprey.engine.executor.execution_context import Action
    a = Action.from_dict({
        "action_id": 42,
        "action_name": "GUILD_JOINED",
        "data": {},
        "timestamp": "2026-01-01T00:00:00",
        "execution_mode": "async",
    })
    assert a.execution_mode == "async"


def test_proto_mode_str_conversion():
    """Verify proto enum int values map to expected strings."""
    from osprey.async_worker.lib.coordinator_input_stream import _proto_mode_to_str
    assert _proto_mode_to_str(0) == 'unspecified'
    assert _proto_mode_to_str(1) == 'sync'
    assert _proto_mode_to_str(2) == 'async'
    assert _proto_mode_to_str(99) == 'unspecified'  # unknown future variant


def _make_mock_coordinator_action(action_id, action_name, json_data, timestamp_seconds, mode):
    """Build a mock OspreyCoordinatorAction suitable for _create_osprey_engine_action."""
    mock_msg = MagicMock()
    mock_msg.ack_id = 1
    mock_msg.action_id = action_id
    mock_msg.action_name = action_name
    mock_msg.json_action_data = json_data if isinstance(json_data, bytes) else json_data.encode()
    mock_msg.WhichOneof = lambda field: 'json_action_data'
    mock_msg.timestamp.ToDatetime.return_value = datetime.utcfromtimestamp(timestamp_seconds)
    # HasField for json_secret_data should return False so secret_data stays empty
    mock_msg.HasField.return_value = False
    mock_msg.mode = mode
    return mock_msg


def _make_input_stream():
    """Create a minimal OspreyCoordinatorInputStream instance for unit testing."""
    from osprey.async_worker.lib.coordinator_input_stream import OspreyCoordinatorInputStream
    instance = object.__new__(OspreyCoordinatorInputStream)
    return instance


def test_create_osprey_engine_action_threads_sync_mode():
    """Sync-stamped proto messages produce execution_mode='sync' on the Action."""
    stream = _make_input_stream()
    mock_msg = _make_mock_coordinator_action(
        action_id=100,
        action_name="USER_REGISTER_ATTEMPTED",
        json_data=b'{"foo": "bar"}',
        timestamp_seconds=1700000000,
        mode=1,  # SYNC
    )

    action = stream._create_osprey_engine_action(mock_msg)

    assert action is not None
    assert action.execution_mode == 'sync'
    assert action.action_name == 'USER_REGISTER_ATTEMPTED'


def test_create_osprey_engine_action_threads_async_mode():
    stream = _make_input_stream()
    mock_msg = _make_mock_coordinator_action(
        action_id=100,
        action_name="GUILD_JOINED",
        json_data=b'{}',
        timestamp_seconds=1700000000,
        mode=2,  # ASYNC
    )

    action = stream._create_osprey_engine_action(mock_msg)

    assert action is not None
    assert action.execution_mode == 'async'


def test_create_osprey_engine_action_unspecified_for_legacy_messages():
    """An older coordinator that doesn't stamp mode produces UNSPECIFIED (0), which
    must translate to 'unspecified' so the engine applies no tier filtering."""
    stream = _make_input_stream()
    mock_msg = _make_mock_coordinator_action(
        action_id=100,
        action_name="USER_LOGIN_ATTEMPTED",
        json_data=b'{}',
        timestamp_seconds=1700000000,
        mode=0,  # UNSPECIFIED (legacy)
    )

    action = stream._create_osprey_engine_action(mock_msg)

    assert action is not None
    assert action.execution_mode == 'unspecified'
