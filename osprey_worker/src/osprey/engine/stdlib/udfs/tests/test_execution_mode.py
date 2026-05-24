"""Tests for the ExecutionMode() UDF."""
from typing import Any, Callable, List

import pytest

from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.execution_mode import ExecutionMode
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(ExecutionMode)),
]


def test_execution_mode_returns_sync(execute: ExecuteFunction) -> None:
    sources = {
        'main.sml': "CurrentMode: str = ExecutionMode()",
    }
    result = execute(sources, data={}, execution_mode='sync')
    assert result == {'CurrentMode': 'sync'}


def test_execution_mode_returns_async(execute: ExecuteFunction) -> None:
    sources = {
        'main.sml': "CurrentMode: str = ExecutionMode()",
    }
    result = execute(sources, data={}, execution_mode='async')
    assert result == {'CurrentMode': 'async'}


def test_execution_mode_returns_unspecified_for_legacy(execute: ExecuteFunction) -> None:
    """When no mode is passed to the fixture (default 'unspecified'),
    the UDF returns 'unspecified'."""
    sources = {
        'main.sml': "CurrentMode: str = ExecutionMode()",
    }
    result = execute(sources, data={})
    assert result == {'CurrentMode': 'unspecified'}
