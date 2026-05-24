"""Tests for the tier kwarg on WhenRules (Task 2.2 — compile-time validation, no runtime filter yet)."""
from typing import Any, Callable, List

import pytest

from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, RunValidationFunction
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.stdlib.udfs.verdicts import DeclareVerdict
from osprey.engine.udf.registry import UDFRegistry


pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(JsonData, Rule, WhenRules, DeclareVerdict)
    ),
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
]


def test_when_rules_accepts_no_tier_kwarg(execute: ExecuteFunction) -> None:
    """Back-compat: existing rules without a tier kwarg continue to compile + run."""
    sources = {
        'main.sml': '''
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="test rule")
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict="ok")],
            )
        ''',
    }
    result = execute(sources, data={'flag': True})
    assert 'Flag' in result


@pytest.mark.parametrize('tier', ['sync', 'async', 'both', 'legacy'])
def test_when_rules_accepts_all_valid_tiers(execute: ExecuteFunction, tier: str) -> None:
    sources = {
        'main.sml': f'''
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="test rule")
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict="ok")],
                tier="{tier}",
            )
        ''',
    }
    result = execute(sources, data={'flag': True})
    assert 'Flag' in result


def test_when_rules_rejects_invalid_tier(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    """An invalid tier value must produce a compile-time error."""
    with check_failure():
        run_validation('''
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="test rule")
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict="ok")],
                tier="nonsense",
            )
        ''')


def test_when_rules_rejects_tier_typo(
    run_validation: RunValidationFunction, check_failure: CheckFailureFunction
) -> None:
    """Catches common typos like 'syc' / 'asyn' / 'sycn'."""
    with check_failure():
        run_validation('''
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="test rule")
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict="ok")],
                tier="syc",
            )
        ''')
