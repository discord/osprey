"""Tests for FakeSlowClassifier — the test-only SLOW UDF used in tier-routing tests."""
from typing import Any, Callable, List

import pytest

from osprey.engine.conftest import ExecuteFunction
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.tests.fake_slow_udf import FakeSlowClassifier
from osprey.engine.udf.registry import UDFRegistry


pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(JsonData, FakeSlowClassifier)),
]


def test_fake_slow_classifier_latency_tier_is_slow():
    """Required for the validator to recognize it as slow."""
    assert FakeSlowClassifier.latency_tier == 'slow'


def test_fake_slow_classifier_with_fixed_score(execute: ExecuteFunction) -> None:
    """fixed_score is returned directly (no sleep)."""
    sources = {
        'main.sml': '''
            UserId: str = JsonData(path='$.user_id')
            Score: float = FakeSlowClassifier(user_id=UserId, fixed_score=0.85)
        ''',
    }
    result = execute(sources, data={'user_id': '12345'})
    assert result['Score'] == 0.85
    assert result['UserId'] == '12345'


def test_fake_slow_classifier_default_score(execute: ExecuteFunction) -> None:
    """Without fixed_score, the UDF sleeps briefly and returns 0.5."""
    sources = {
        'main.sml': '''
            UserId: str = JsonData(path='$.user_id')
            Score: float = FakeSlowClassifier(user_id=UserId)
        ''',
    }
    result = execute(sources, data={'user_id': '12345'})
    assert result['Score'] == 0.5
