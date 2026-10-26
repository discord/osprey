import json
from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validation_context import ValidationFailed
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.conftest import ExecuteWithResultFunction, RunValidationFunction
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.engine.language_types.labels import LabelEffect
from osprey.engine.stdlib.udfs.entity import Entity
from osprey.engine.stdlib.udfs.labels import LabelAdd
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.udf.registry import UDFRegistry

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(Entity, Rule, WhenRules, LabelAdd)),
    pytest.mark.use_validators([ValidateCallKwargs, UniqueStoredNames]),
]

_CONFIG = json.dumps({'labels': {'foo': {'valid_for': ['MyEntity']}}})
_ENTITY_COUNT = 300


@pytest.fixture(autouse=True)
def modes_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('OSPREY_WHEN_RULES_MODES', '1')


def _run(execute_with_result: ExecuteWithResultFunction, when_rules_args: str, entities: int = 1) -> ExecutionResult:
    entity_defs = '\n'.join(f"E{i} = Entity(type='MyEntity', id='entity-{i}')" for i in range(entities))
    labels = ', '.join(f"LabelAdd(entity=E{i}, label='foo')" for i in range(entities))
    return execute_with_result(
        {
            'main.sml': f"""
                R = Rule(when_all=[True], description='always')
                {entity_defs}
                WhenRules(rules_any=[R], then=[{labels}]{when_rules_args})
            """,
            'config.yaml': _CONFIG,
        }
    )


def _emitted_entities(result: ExecutionResult) -> set[str]:
    return {str(e.entity.id) for e in result.effects.get(LabelEffect, []) if isinstance(e, LabelEffect)}


def test_unchanged_corpus_has_no_stamps(execute_with_result: ExecuteWithResultFunction) -> None:
    result = _run(execute_with_result, '')

    [effect] = result.effects[LabelEffect]
    assert (effect.when_rules_name, effect.when_rules_mode, effect.revision_id, effect.release_id) == (None,) * 4
    [entry] = result.rule_audit_entries
    assert (entry.name, entry.mode, entry.effects_suppressed, entry.rollout_effects) == (None, None, [], [])
    assert entry.effects_emitted == ['LabelEffect']


def test_flag_off_ignores_declared_mode(
    execute_with_result: ExecuteWithResultFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv('OSPREY_WHEN_RULES_MODES')

    result = _run(execute_with_result, ", name='d', mode='off'")

    [effect] = result.effects[LabelEffect]
    assert effect.when_rules_name is None
    assert result.rule_audit_entries[0].mode is None


def test_enforce_stamps_effects_and_audit(execute_with_result: ExecuteWithResultFunction) -> None:
    result = _run(execute_with_result, ", name='d', revision_id='rev1', release_id='rel1'")

    [effect] = result.effects[LabelEffect]
    assert (effect.when_rules_name, effect.when_rules_mode, effect.revision_id, effect.release_id) == (
        'd',
        'enforce',
        'rev1',
        'rel1',
    )
    [entry] = result.rule_audit_entries
    assert (entry.name, entry.mode, entry.revision_id, entry.release_id) == ('d', 'enforce', 'rev1', 'rel1')
    assert entry.effects_emitted == ['LabelEffect']


def test_off_emits_nothing(execute_with_result: ExecuteWithResultFunction) -> None:
    result = _run(execute_with_result, ", name='d', mode='off'")

    assert not result.effects
    [entry] = result.rule_audit_entries
    assert (entry.mode, entry.rules_matched, entry.effects_emitted, entry.effects_suppressed) == ('off', ['R'], [], [])


def test_shadow_records_suppressed_effects(execute_with_result: ExecuteWithResultFunction) -> None:
    result = _run(execute_with_result, ", name='d', mode='shadow'")

    assert not result.effects
    [entry] = result.rule_audit_entries
    assert entry.mode == 'shadow'
    assert entry.effects_emitted == []
    assert [(r.effect_type, r.bucket, r.in_bucket) for r in entry.effects_suppressed] == [('LabelEffect', None, None)]


def test_rollout_emits_only_entities_under_percent(execute_with_result: ExecuteWithResultFunction) -> None:
    result = _run(execute_with_result, ", name='d', mode='rollout', rollout_percent=50", entities=_ENTITY_COUNT)

    [entry] = result.rule_audit_entries
    in_bucket = {r.entity for r in entry.rollout_effects if r.in_bucket}
    assert 0 < len(in_bucket) < _ENTITY_COUNT
    assert {f'MyEntity/{i}' for i in _emitted_entities(result)} == in_bucket
    assert len(entry.rollout_effects) == _ENTITY_COUNT
    assert {r.entity for r in entry.effects_suppressed} == {r.entity for r in entry.rollout_effects if not r.in_bucket}
    assert all(r.bucket is not None and (r.bucket < 5000) == r.in_bucket for r in entry.rollout_effects)
    assert {e.when_rules_mode for e in result.effects[LabelEffect]} == {'rollout'}


def test_rollout_buckets_are_stable_as_percent_rises(execute_with_result: ExecuteWithResultFunction) -> None:
    emitted = {
        percent: _emitted_entities(
            _run(
                execute_with_result,
                f", name='d', mode='rollout', rollout_percent={percent}",
                entities=_ENTITY_COUNT,
            )
        )
        for percent in (0, 10, 50, 100)
    }

    assert emitted[0] == set()
    assert emitted[0] < emitted[10] < emitted[50] < emitted[100]
    assert len(emitted[100]) == _ENTITY_COUNT


@pytest.mark.parametrize(
    'args, expected',
    [
        ("mode='bogus', name='d'", 'unknown WhenRules mode'),
        ("mode='shadow'", '`name` is required'),
        ("mode='rollout', name='d'", '`rollout_percent` is required'),
        ("mode='rollout', name='d', rollout_percent=101", 'between 0 and 100'),
        ("mode='rollout', name='d', rollout_percent=-1", 'between 0 and 100'),
    ],
)
def test_invalid_mode_arguments(run_validation: RunValidationFunction, args: str, expected: str) -> None:
    with pytest.raises(ValidationFailed) as e:
        run_validation(
            f"""
            R = Rule(when_all=[True], description='')
            E = Entity(type='MyEntity', id='1')
            WhenRules(rules_any=[R], then=[LabelAdd(entity=E, label='foo')], {args})
            """
        )
    assert expected in e.value.rendered()


def test_name_must_be_unique_across_corpus(run_validation: RunValidationFunction) -> None:
    block = "WhenRules(rules_any=[R], then=[LabelAdd(entity=E, label='foo')], name='{}')"
    with pytest.raises(ValidationFailed) as e:
        run_validation(
            f"""
            R = Rule(when_all=[True], description='')
            E = Entity(type='MyEntity', id='1')
            {block.format('dup')}
            {block.format('dup')}
            """
        )
    assert 'must be unique across all rule files' in e.value.rendered()
