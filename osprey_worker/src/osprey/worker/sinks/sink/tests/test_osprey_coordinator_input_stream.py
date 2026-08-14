from typing import Optional
from unittest.mock import patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from osprey.rpc.osprey_coordinator.bidirectional_stream.v1.service_pb2 import OspreyCoordinatorAction
from osprey.worker.sinks.sink.osprey_coordinator_input_stream import OspreyCoordinatorInputStream


def create_coordinator_action(secret_data: Optional[bytes]) -> OspreyCoordinatorAction:
    action = OspreyCoordinatorAction(
        action_id=123,
        action_name='test_action',
        json_action_data=b'{"public":"value"}',
        timestamp=Timestamp(seconds=1_700_000_000),
    )
    if secret_data is not None:
        action.json_secret_data = secret_data
    return action


@pytest.mark.parametrize(
    ('encoded_secret_data', 'expected_secret_data'),
    [
        (None, {}),
        (b'{"private":"secret"}', {'private': 'secret'}),
    ],
)
def test_create_engine_action_keeps_secret_data_separate(
    encoded_secret_data: Optional[bytes], expected_secret_data: dict[str, str]
) -> None:
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)

    action = stream._create_osprey_engine_action(create_coordinator_action(encoded_secret_data))

    assert action is not None
    assert action.data == {'public': 'value'}
    assert action.secret_data == expected_secret_data


def test_create_engine_action_rejects_malformed_secret_json() -> None:
    stream = OspreyCoordinatorInputStream.__new__(OspreyCoordinatorInputStream)

    with patch('osprey.worker.sinks.sink.osprey_coordinator_input_stream.sentry_sdk.capture_exception'):
        action = stream._create_osprey_engine_action(create_coordinator_action(b'not-json'))

    assert action is None
