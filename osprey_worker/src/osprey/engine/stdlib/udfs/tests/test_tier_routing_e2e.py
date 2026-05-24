"""End-to-end engine tests for the tier routing worked example.

Verifies:
1. Sync invocation: existing-style sync WhenRules fires; async-only WhenRules does NOT;
   the SLOW UDF is NOT called (Require(require_if=...) gates the whole file out).
2. Async invocation: existing sync WhenRules does NOT fire (skipped_by_tier=True);
   async-only WhenRules fires; SLOW UDF IS called.
3. Below-threshold async: SLOW UDF runs but the rule condition fails => no effect.
4. Legacy/unspecified mode: existing sync WhenRules fires (legacy back-compat);
   async file gated out (Require's require_if returns False because mode != 'async');
   no SLOW UDF call.
5. Audit trail surfaces skipped_by_tier=True for filtered blocks.
"""
import json
from typing import Any, Callable, List

import pytest

from osprey.engine.conftest import ExecuteWithResultFunction
from osprey.engine.language_types.labels import LabelEffect
from osprey.engine.stdlib.udfs.entity import Entity
from osprey.engine.stdlib.udfs.execution_mode import ExecutionMode
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.labels import LabelAdd
from osprey.engine.stdlib.udfs.require import Require
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.stdlib.udfs.tests.fake_slow_udf import FakeSlowClassifier
from osprey.engine.udf.registry import UDFRegistry


pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(
            JsonData, Entity, Rule, WhenRules, LabelAdd, ExecutionMode,
            Require, FakeSlowClassifier,
        )
    ),
]

# Labels referenced in the worked example — must be declared in config.yaml so
# the auto-registered ValidateLabels validator doesn't reject them.
_LABELS_CONFIG = json.dumps({
    'labels': {
        'test_sync_label': {'valid_for': ['User']},
        'test_async_label': {'valid_for': ['User']},
    }
})


# --- Sources for the worked example ---
#
# The engine requires 'main.sml' as the entry-point. The main file Require()s
# the action file, which in turn conditionally Require()s the async-only slow
# UDF file.

WORKED_EXAMPLE_SOURCES = {
    # Label config required by the auto-registered ValidateLabels validator.
    'config.yaml': _LABELS_CONFIG,

    # Entry point: routes to the action file
    'main.sml': '''
        Require(rule="actions/test_action_attempted.sml")
    ''',

    # Action file — existing sync rules + conditional Require for async-only file
    'actions/test_action_attempted.sml': '''
        UserId: str = JsonData(path='$.user_id')
        BadSignal: bool = JsonData(path='$.bad_signal')
        ActionUser: Entity[str] = Entity(type='User', id=UserId)

        # Existing sync rule (tier='sync' for clarity).
        SyncRule = Rule(when_all=[BadSignal], description="sync rule fires on bad signal")
        WhenRules(
            rules_any=[SyncRule],
            then=[LabelAdd(entity=ActionUser, label="test_sync_label")],
            tier="sync",
        )

        # Coarse gate: only load the async classifier file on async path.
        # On the sync path, this Require's require_if returns False, so the
        # entire async_classifier.sml file is dropped from the topo sorter
        # — the FakeSlowClassifier is NEVER invoked.
        Require(
            rule="workflows/async_classifier.sml",
            require_if=ExecutionMode() == "async",
        )
    ''',

    # Async-only file with the SLOW UDF + tier='async' WhenRules.
    # Uses distinct feature names (AsyncUserId, FixedScore, AsyncUser) to avoid
    # UniqueStoredNames validation errors — all sources are parsed statically
    # even if Require(require_if=...) gates them at runtime.
    'workflows/async_classifier.sml': '''
        AsyncUserId: str = JsonData(path='$.user_id')
        FixedScore: float = JsonData(path='$.fixed_score')
        AsyncUser: Entity[str] = Entity(type='User', id=AsyncUserId)

        # SLOW UDF — must only be referenced on the async path.
        # The Require(require_if=...) in the root file prevents this from
        # loading on the sync path.
        AsyncScore: float = FakeSlowClassifier(user_id=AsyncUserId, fixed_score=FixedScore)

        HighScore = Rule(when_all=[AsyncScore > 0.9], description="high async score")
        WhenRules(
            rules_any=[HighScore],
            then=[LabelAdd(entity=AsyncUser, label="test_async_label")],
            tier="async",
        )
    ''',
}


def _labels_emitted(result) -> list:
    """Extract label name strings from LabelEffect emissions in the execution result."""
    return [e.name for e in result.effects.get(LabelEffect, [])]


def test_sync_invocation_skips_async_file_entirely(execute_with_result: ExecuteWithResultFunction) -> None:
    """On the sync path:
       - Require(require_if=...) gates out the async file entirely
       - AsyncScore feature is NOT computed (SLOW UDF never invoked)
       - Existing sync rule fires (LabelAdd emitted)
       - Async-only label NOT emitted."""
    result = execute_with_result(
        WORKED_EXAMPLE_SOURCES,
        data={'user_id': '12345', 'bad_signal': True, 'fixed_score': 0.99},
        execution_mode='sync',
    )
    extracted = result.extracted_features
    # SLOW UDF never invoked → AsyncScore not in extracted features
    assert 'AsyncScore' not in extracted, (
        f'SLOW UDF must not run on sync path. Got extracted features: {sorted(extracted.keys())}'
    )
    labels = _labels_emitted(result)
    # Sync label fired
    assert 'test_sync_label' in labels, f'sync rule must fire. Got: {labels}'
    # Async-only label did NOT fire
    assert 'test_async_label' not in labels, (
        f'async-only label must NOT appear on sync. Got: {labels}'
    )


def test_async_invocation_runs_slow_udf_and_fires_async_rule(
    execute_with_result: ExecuteWithResultFunction,
) -> None:
    """On the async path:
       - Require gates the async file IN (require_if evaluates True)
       - AsyncScore is computed (SLOW UDF invoked, fixed_score=0.99)
       - Async-only WhenRules fires (LabelAdd emitted)
       - Sync WhenRules is FILTERED (skipped_by_tier=True; no LabelAdd)."""
    result = execute_with_result(
        WORKED_EXAMPLE_SOURCES,
        data={'user_id': '12345', 'bad_signal': True, 'fixed_score': 0.99},
        execution_mode='async',
    )
    extracted = result.extracted_features
    # SLOW UDF DID run → AsyncScore computed
    assert extracted.get('AsyncScore') == 0.99, (
        f'expected AsyncScore=0.99 on async path. Got: {extracted.get("AsyncScore")}'
    )
    labels = _labels_emitted(result)
    # Async-only label fired
    assert 'test_async_label' in labels, (
        f'async-only label must fire on async path. Got: {labels}'
    )
    # Sync-tier label did NOT fire (filtered)
    assert 'test_sync_label' not in labels, (
        f'tier=sync rule must NOT fire on async path (would be duplicate emission). '
        f'Got: {labels}'
    )


def test_async_below_threshold_does_not_emit_label(
    execute_with_result: ExecuteWithResultFunction,
) -> None:
    """If the score is below threshold, the async WhenRules condition fails
    even though the SLOW UDF ran."""
    result = execute_with_result(
        WORKED_EXAMPLE_SOURCES,
        data={'user_id': '12345', 'bad_signal': True, 'fixed_score': 0.50},
        execution_mode='async',
    )
    # SLOW UDF did run
    assert result.extracted_features.get('AsyncScore') == 0.50
    # But the rule condition (AsyncScore > 0.9) failed
    labels = _labels_emitted(result)
    assert 'test_async_label' not in labels


def test_unspecified_mode_preserves_legacy_behavior(
    execute_with_result: ExecuteWithResultFunction,
) -> None:
    """An older coordinator that doesn't stamp mode produces execution_mode='unspecified'.
       - tier-filtering bypassed (legacy back-compat)
       - Require(require_if=ExecutionMode() == 'async') returns False, so async file
         is NOT loaded
       - Sync rule fires (existing behavior preserved)
       - Async label does NOT fire (file never loaded)"""
    result = execute_with_result(
        WORKED_EXAMPLE_SOURCES,
        data={'user_id': '12345', 'bad_signal': True, 'fixed_score': 0.99},
        execution_mode='unspecified',
    )
    labels = _labels_emitted(result)
    # Sync label fires (back-compat)
    assert 'test_sync_label' in labels
    # Async label does NOT — file was never loaded
    assert 'test_async_label' not in labels
    # SLOW UDF was NOT invoked — file was gated out
    assert 'AsyncScore' not in result.extracted_features


def test_audit_trail_shows_filtered_block_on_async(
    execute_with_result: ExecuteWithResultFunction,
) -> None:
    """Verify the audit log surfaces tier-filtered blocks for debugging."""
    result = execute_with_result(
        WORKED_EXAMPLE_SOURCES,
        data={'user_id': '1', 'bad_signal': True, 'fixed_score': 0.99},
        execution_mode='async',
    )
    skipped = [e for e in result.rule_audit_entries if e.skipped_by_tier]
    # On async, the tier=sync WhenRules is filtered
    assert len(skipped) >= 1, (
        f'expected at least one audit entry with skipped_by_tier=True. '
        f'Got audit entries: {result.rule_audit_entries}'
    )
