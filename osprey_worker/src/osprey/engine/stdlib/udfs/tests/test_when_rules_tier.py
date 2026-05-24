"""Tests for the tier kwarg on WhenRules (Task 2.2 — compile-time validation, Task 2.3 — runtime filter)."""
from typing import Any, Callable, List

import pytest

from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import CheckFailureFunction, ExecuteFunction, ExecuteWithResultFunction, RunValidationFunction
from osprey.engine.language_types.verdicts import VerdictEffect
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


# --- Runtime tier filtering matrix (Task 2.3) ---


@pytest.mark.parametrize(
    'tier,execution_mode,should_fire',
    [
        # Legacy tier — fires everywhere
        ('legacy', 'sync', True),
        ('legacy', 'async', True),
        ('legacy', 'unspecified', True),
        # Both tier — fires everywhere
        ('both', 'sync', True),
        ('both', 'async', True),
        ('both', 'unspecified', True),
        # sync tier — fires only on sync
        ('sync', 'sync', True),
        ('sync', 'async', False),  # skipped_by_tier
        ('sync', 'unspecified', True),  # back-compat: no filtering when mode unknown
        # async tier — fires only on async
        ('async', 'sync', False),  # skipped_by_tier
        ('async', 'async', True),
        ('async', 'unspecified', True),  # back-compat
    ],
)
def test_tier_filtering_matrix(
    execute_with_result: ExecuteWithResultFunction, tier: str, execution_mode: str, should_fire: bool
) -> None:
    """Verify the full tier × execution_mode filtering matrix."""
    sources = {
        'main.sml': f'''
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="test rule")
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict="filter_test_result")],
                tier="{tier}",
            )
        ''',
    }
    result = execute_with_result(sources, data={'flag': True}, execution_mode=execution_mode)
    verdict_effects = result.effects.get(VerdictEffect, [])
    fired = any(e.verdict == 'filter_test_result' for e in verdict_effects)
    assert fired == should_fire, (
        f'tier={tier} mode={execution_mode}: expected fire={should_fire}, got fired={fired}'
    )


def test_filtered_block_records_audit_entry(execute_with_result: ExecuteWithResultFunction) -> None:
    """A tier-skipped WhenRules MUST record an audit entry with skipped_by_tier=True,
    so debugging is observable."""
    sources = {
        'main.sml': '''
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="async-only rule")
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict="async_only")],
                tier="async",
            )
        ''',
    }
    # Submit on the sync path — the async-tier block must be filtered.
    result = execute_with_result(sources, data={'flag': True}, execution_mode='sync')
    skipped = [e for e in result.rule_audit_entries if e.skipped_by_tier]
    assert len(skipped) == 1, f'expected 1 tier-skipped audit entry, got {len(skipped)}'
    # Should have NOT emitted the verdict
    verdicts = result.effects.get(VerdictEffect, [])
    assert not any(e.verdict == 'async_only' for e in verdicts)


def test_mixed_file_only_filters_per_block(execute_with_result: ExecuteWithResultFunction) -> None:
    """A file with both tier='sync' and tier='async' WhenRules: each is filtered
    independently. On sync path: sync block fires, async block skips. On async
    path: vice versa."""
    sources = {
        'main.sml': '''
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="shared rule")
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict="sync_only")],
                tier="sync",
            )
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict="async_only")],
                tier="async",
            )
        ''',
    }

    sync_result = execute_with_result(sources, data={'flag': True}, execution_mode='sync')
    sync_verdicts = [e.verdict for e in sync_result.effects.get(VerdictEffect, [])]
    assert 'sync_only' in sync_verdicts and 'async_only' not in sync_verdicts

    async_result = execute_with_result(sources, data={'flag': True}, execution_mode='async')
    async_verdicts = [e.verdict for e in async_result.effects.get(VerdictEffect, [])]
    assert 'async_only' in async_verdicts and 'sync_only' not in async_verdicts
