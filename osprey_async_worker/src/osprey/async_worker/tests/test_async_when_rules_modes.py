import json

import pytest
from osprey.engine.language_types.labels import LabelEffect

_CONFIG = json.dumps({'labels': {'foo': {'valid_for': ['MyEntity']}}})
_ENTITY_COUNT = 100


@pytest.fixture(autouse=True)
def modes_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('OSPREY_WHEN_RULES_MODES', '1')


def _sources(when_rules_args: str) -> dict:
    entities = '\n'.join(f"E{i} = Entity(type='MyEntity', id='entity-{i}')" for i in range(_ENTITY_COUNT))
    labels = ', '.join(f"LabelAdd(entity=E{i}, label='foo')" for i in range(_ENTITY_COUNT))
    return {
        'main.sml': f"""
{entities}
R = Rule(when_all=[True], description='always')
WhenRules(rules_any=[R], then=[{labels}], {when_rules_args})
""",
        'config.yaml': _CONFIG,
    }


@pytest.mark.asyncio
async def test_async_shadow_suppresses_and_records(async_execute_with_result) -> None:
    result = await async_execute_with_result(_sources("name='d', mode='shadow'"))

    assert not result.effects
    [entry] = result.rule_audit_entries
    assert (entry.name, entry.mode, entry.effects_emitted) == ('d', 'shadow', [])
    assert len(entry.effects_suppressed) == _ENTITY_COUNT


@pytest.mark.asyncio
async def test_async_rollout_emits_only_in_bucket_entities(async_execute_with_result) -> None:
    result = await async_execute_with_result(
        _sources("name='d', mode='rollout', rollout_percent=50, revision_id='rev1', release_id='rel1'")
    )

    [entry] = result.rule_audit_entries
    in_bucket = {r.entity for r in entry.rollout_effects if r.in_bucket}
    emitted = {f'MyEntity/{e.entity.id}' for e in result.effects[LabelEffect]}
    assert 0 < len(in_bucket) < _ENTITY_COUNT
    assert emitted == in_bucket
    assert {(e.when_rules_mode, e.revision_id, e.release_id) for e in result.effects[LabelEffect]} == {
        ('rollout', 'rev1', 'rel1')
    }
