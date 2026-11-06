import json
from typing import Any, Iterator, Mapping, Sequence

import pytest
from osprey.engine.ast.sources import Sources
from osprey.engine.executor.execution_context import WhenRulesAuditEntry
from osprey.engine.executor.rule_controls import RuleControls, set_rule_controls
from osprey.engine.language_types.labels import LabelEffect

SML = {
    'main.sml': """
        Paused = Rule(when_all=[True], description='paused rule')
        Other = Rule(when_all=[True], description='other rule')
        E = Entity(type='MyEntity', id='entity 1')
        WhenRules(rules_any=[Paused], then=[LabelAdd(entity=E, label='paused_label')])
        WhenRules(rules_any=[Other], then=[LabelAdd(entity=E, label='other_label')])
    """,
    'config.yaml': json.dumps(
        {'labels': {'paused_label': {'valid_for': ['MyEntity']}, 'other_label': {'valid_for': ['MyEntity']}}}
    ),
}


class _Controls(RuleControls):
    def is_rule_suppressed(self, rule_name: str) -> bool:
        return rule_name == 'Paused'

    def annotate_execution(
        self, sources: Sources, rule_audit_entries: Sequence[WhenRulesAuditEntry]
    ) -> Mapping[str, Any]:
        return {
            '__rules_version': {
                'ruleset_hash': sources.hash(),
                'matched': sorted(name for entry in rule_audit_entries for name in entry.rules_matched),
            }
        }


@pytest.fixture(autouse=True)
def _reset_controls() -> Iterator[None]:
    yield
    set_rule_controls(None)


@pytest.mark.asyncio
async def test_the_async_executor_skips_suppressed_rules_and_annotates_its_results(async_execute_with_result) -> None:
    set_rule_controls(_Controls())

    result = await async_execute_with_result(dict(SML))

    labels = [effect.name for effect in result.effects.get(LabelEffect, []) if isinstance(effect, LabelEffect)]
    assert labels == ['other_label']
    assert result.extracted_features['Paused'] is True
    assert sorted(name for entry in result.rule_audit_entries for name in entry.rules_suppressed) == ['Paused']
    stamp = result.extracted_features['__rules_version']
    assert stamp['matched'] == ['Other']
    assert len(stamp['ruleset_hash']) == 64


@pytest.mark.asyncio
async def test_the_async_executor_does_nothing_extra_by_default(async_execute_with_result) -> None:
    result = await async_execute_with_result(dict(SML))

    labels = sorted(effect.name for effect in result.effects.get(LabelEffect, []) if isinstance(effect, LabelEffect))
    assert labels == ['other_label', 'paused_label']
    assert '__rules_version' not in result.extracted_features
