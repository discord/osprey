import json
from datetime import datetime
from typing import Any, Callable, Iterator, List, Mapping, Optional, Sequence, Set
from unittest.mock import MagicMock

import pytest
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.conftest import into_sources
from osprey.engine.executor.execution_context import Action, ExecutionResult, WhenRulesAuditEntry
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.executor import execute as osprey_execute
from osprey.engine.executor.rule_controls import RuleControls, get_rule_controls, set_rule_controls
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.language_types.labels import LabelEffect
from osprey.engine.stdlib import get_config_registry
from osprey.engine.stdlib.udfs.entity import Entity
from osprey.engine.stdlib.udfs.labels import LabelAdd
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.udf.registry import UDFRegistry
from osprey.worker.adaptor.plugin_manager import bootstrap_ast_validators
from pytest_mock import MockFixture

REGISTRY = UDFRegistry.with_udfs(Entity, Rule, WhenRules, LabelAdd)

SML = """
    Paused = Rule(when_all=[True], description='paused rule')
    Other = Rule(when_all=[True], description='other rule')
    Dependent = Rule(when_all=[not (not Paused)], description='depends on Paused')
    E = Entity(type='MyEntity', id='entity 1')
    WhenRules(rules_any=[Paused], then=[LabelAdd(entity=E, label='paused_label')])
    WhenRules(rules_any=[Paused, Other], then=[LabelAdd(entity=E, label='shared_label')])
    WhenRules(rules_any=[Dependent], then=[LabelAdd(entity=E, label='dependent_label')])
"""
CONFIG = json.dumps(
    {'labels': {name: {'valid_for': ['MyEntity']} for name in ('paused_label', 'shared_label', 'dependent_label')}}
)


class _Suppressing(RuleControls):
    def __init__(self, suppressed: Set[str]) -> None:
        self.suppressed = suppressed

    def is_rule_suppressed(self, rule_name: str) -> bool:
        return rule_name in self.suppressed


class _Annotating(RuleControls):
    def __init__(self, annotate: Callable[[Sources, Sequence[WhenRulesAuditEntry]], Mapping[str, Any]]) -> None:
        self._annotate = annotate

    def annotate_execution(
        self, sources: Sources, rule_audit_entries: Sequence[WhenRulesAuditEntry]
    ) -> Mapping[str, Any]:
        return self._annotate(sources, rule_audit_entries)


@pytest.fixture(autouse=True)
def _reset_controls() -> Iterator[None]:
    yield
    set_rule_controls(None)


@pytest.fixture()
def graph() -> ExecutionGraph:
    bootstrap_ast_validators()
    config_validator = get_config_registry().get_validator()
    validator_registry = ValidatorRegistry.get_instance().instance_with_additional_validators(config_validator)
    sources = into_sources({'main.sml': SML, 'config.yaml': CONFIG})
    return compile_execution_graph(validate_sources(sources, REGISTRY, validator_registry))


def _run(graph: ExecutionGraph) -> ExecutionResult:
    action = Action(action_id=1, data={}, action_name='test', timestamp=datetime.utcnow())
    return osprey_execute(graph, UDFHelpers(), action, None)


def _labels(result: ExecutionResult) -> Mapping[str, List[str]]:
    """Each applied label, and the names of the rules it carries."""
    return {
        effect.name: sorted(rule.name for rule in effect.rules)
        for effect in result.effects.get(LabelEffect, [])
        if isinstance(effect, LabelEffect)
    }


def test_controls_do_nothing_by_default(graph: ExecutionGraph) -> None:
    assert type(get_rule_controls()) is RuleControls

    result = _run(graph)

    assert _labels(result) == {
        'paused_label': ['Paused'],
        'shared_label': ['Other', 'Paused'],
        'dependent_label': ['Dependent'],
    }
    assert all(entry.rules_suppressed == [] for entry in result.rule_audit_entries)
    assert not [name for name in result.extracted_features if name.startswith('__rule')]


def test_a_suppressed_rule_emits_no_effects_and_still_evaluates_for_its_dependents(graph: ExecutionGraph) -> None:
    set_rule_controls(_Suppressing({'Paused'}))

    result = _run(graph)

    assert _labels(result) == {'shared_label': ['Other'], 'dependent_label': ['Dependent']}
    assert result.extracted_features['Paused'] is True
    assert result.extracted_features['Dependent'] is True
    by_rules = {tuple(entry.rules_evaluated): entry for entry in result.rule_audit_entries}
    assert by_rules[('Paused',)].rules_matched == []
    assert by_rules[('Paused',)].rules_suppressed == ['Paused']
    assert by_rules[('Paused',)].effects_emitted == []
    assert by_rules[('Paused', 'Other')].rules_matched == ['Other']
    assert by_rules[('Paused', 'Other')].rules_suppressed == ['Paused']
    assert by_rules[('Dependent',)].rules_suppressed == []


def test_suppression_follows_the_installed_check_on_the_same_compiled_graph(graph: ExecutionGraph) -> None:
    controls = _Suppressing({'Paused', 'Other'})
    set_rule_controls(controls)
    assert _labels(_run(graph)) == {'dependent_label': ['Dependent']}

    controls.suppressed = set()

    assert _labels(_run(graph)) == {
        'paused_label': ['Paused'],
        'shared_label': ['Other', 'Paused'],
        'dependent_label': ['Dependent'],
    }


def test_a_check_that_raises_suppresses_nothing_and_is_counted(graph: ExecutionGraph, mocker: MockFixture) -> None:
    increment: MagicMock = mocker.patch('osprey.engine.executor.rule_controls.metrics.increment')

    class _Broken(RuleControls):
        def is_rule_suppressed(self, rule_name: str) -> bool:
            raise RuntimeError('broken check')

    set_rule_controls(_Broken())

    result = _run(graph)

    assert set(_labels(result)) == {'paused_label', 'shared_label', 'dependent_label'}
    assert all(entry.rules_suppressed == [] for entry in result.rule_audit_entries)
    assert any(call.args[0] == 'osprey.rule_controls.suppression_check_error' for call in increment.call_args_list)


def test_the_annotation_sees_the_compiled_sources_and_the_audit_entries_and_lands_in_extracted_features(
    graph: ExecutionGraph,
) -> None:
    seen: List[Any] = []

    def annotate(sources: Sources, entries: Sequence[WhenRulesAuditEntry]) -> Mapping[str, Any]:
        seen.append((sources.hash(), sorted(name for entry in entries for name in entry.rules_matched)))
        return {'__rules_version': {'ruleset_hash': sources.hash()}}

    set_rule_controls(_Annotating(annotate))

    result = _run(graph)

    sources_hash = graph.validated_sources.sources.hash()
    assert seen == [(sources_hash, ['Dependent', 'Other', 'Paused', 'Paused'])]
    assert result.extracted_features['__rules_version'] == {'ruleset_hash': sources_hash}
    assert json.loads(result.extracted_features_json)['__rules_version'] == {'ruleset_hash': sources_hash}


def test_annotations_never_replace_a_feature_or_use_an_unreserved_name(
    graph: ExecutionGraph, mocker: MockFixture
) -> None:
    increment: MagicMock = mocker.patch('osprey.engine.executor.rule_controls.metrics.increment')
    set_rule_controls(
        _Annotating(lambda sources, entries: {'__action_id': 99, 'Paused': False, '__stamp': 'kept', 'plain': 1})
    )

    result = _run(graph)

    assert result.extracted_features['__action_id'] == 1
    assert result.extracted_features['Paused'] is True
    assert result.extracted_features['__stamp'] == 'kept'
    assert 'plain' not in result.extracted_features
    dropped = [call for call in increment.call_args_list if call.args[0] == 'osprey.rule_controls.annotation_dropped']
    assert len(dropped) == 3


def test_an_annotation_that_raises_leaves_the_result_unannotated(graph: ExecutionGraph, mocker: MockFixture) -> None:
    increment: MagicMock = mocker.patch('osprey.engine.executor.rule_controls.metrics.increment')

    def annotate(sources: Sources, entries: Sequence[WhenRulesAuditEntry]) -> Optional[Mapping[str, Any]]:
        raise RuntimeError('broken annotation')

    set_rule_controls(_Annotating(annotate))  # type: ignore[arg-type]

    result = _run(graph)

    assert set(_labels(result)) == {'paused_label', 'shared_label', 'dependent_label'}
    assert not [name for name in result.extracted_features if name.startswith('__rule')]
    assert any(call.args[0] == 'osprey.rule_controls.annotation_error' for call in increment.call_args_list)
