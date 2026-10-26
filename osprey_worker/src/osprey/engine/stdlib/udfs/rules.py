import copy
import logging
import os
import re
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, cast

import sentry_sdk
import typing_inspect
from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_utils import add_must_assign_to_variable_error
from osprey.engine.ast_validator.validators.validate_when_rules_names import ValidateWhenRulesNames
from osprey.engine.executor.execution_context import EffectAuditRecord, WhenRulesAuditEntry
from osprey.engine.executor.node_executor.call_executor import CallExecutor
from osprey.engine.language_types.effects import EffectBase
from osprey.engine.language_types.entities import EntityT
from osprey.engine.language_types.rules import RuleT
from osprey.engine.udf.type_helpers import validate_kwarg_node_type
from osprey.worker.lib.instruments import metrics

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories
from .experiments import Experiment

logger = logging.getLogger(__name__)

_HAS_FORMAT_STRING_RE = re.compile(r'\{([^\d\W]\w*)\}')
_ENFORCEMENT_GAP_METRIC = 'osprey.enforcement_gap'
_ENFORCEMENT_GAP_TRUE_TAG = 'gap:true'
_EFFECTS_METRIC = 'osprey.whenrules_effects'
_MODE_IGNORED_METRIC = 'osprey.whenrules_mode_ignored'
# Gates WhenRules `name`/`mode`/`rollout_percent`/`revision_id`/`release_id`. Default OFF: a non-enforce `mode` fails
# validation, and other blocks behave as today with no stamps on effects or audit entries. The flag is read from each
# validating process's environment, so every process that loads a corpus must set it the same way.
_MODES_ENABLED_ENV = 'OSPREY_WHEN_RULES_MODES'
_TRUTHY = {'1', 'true', 'yes', 'on'}
_DISPOSITION_EMITTED = 'emitted'
_DISPOSITION_SUPPRESSED = 'suppressed'
_DISPOSITION_NO_ENTITY = 'no_entity'
# Keeps decision buckets apart from `Experiment()` cohorts that share a name.
_BUCKET_KEY_PREFIX = 'whenrules/'
_MODE_ARGUMENTS = ('name', 'mode', 'rollout_percent', 'revision_id', 'release_id')
_reported_unbucketable: Set[Tuple[str, str]] = set()


class WhenRulesMode(str, Enum):
    OFF = 'off'
    SHADOW = 'shadow'
    ROLLOUT = 'rollout'
    ENFORCE = 'enforce'


def _modes_enabled() -> bool:
    return os.environ.get(_MODES_ENABLED_ENV, '').strip().lower() in _TRUTHY


def _udf_takes_entity(validation_context: ValidationContext, item: grammar.ASTNode) -> bool:
    """True if a `then` item is a direct call to a UDF with an `entity` argument, which rollout needs to bucket by."""
    if not isinstance(item, grammar.Call) or not isinstance(item.func, grammar.Name):
        return False
    udf_class = validation_context.udf_registry.get(item.func.identifier)
    if udf_class is None:
        # ValidateCallKwargs reports the unknown function.
        return True
    entity_type = udf_class.get_arguments_type().items().get('entity')
    origin = typing_inspect.get_origin(entity_type) or entity_type
    return isinstance(origin, type) and issubclass(origin, EntityT)


@dataclass(frozen=True)
class _ModeConfig:
    name: Optional[str]
    mode: WhenRulesMode
    rollout_units: int
    revision_id: Optional[str]
    release_id: Optional[str]


class RuleArguments(ArgumentsBase):
    # TODO - Should we require that this is a list literal? If so, should we require it be non-empty?
    when_all: List[bool]
    description: str


class Rule(UDFBase[RuleArguments, RuleT]):
    """Defines a named rule with conditions. Evaluates to true when all conditions in when_all are met."""

    name: Optional[str] = None
    category = UdfCategories.ENGINE

    def __init__(self, validation_context: ValidationContext, arguments: RuleArguments):
        super().__init__(validation_context, arguments)
        # Figure out what the rule name is, by traversing up the AST.
        # We want to make sure that our rule is in the form of
        # FeatureName = Rule(...).
        call_node = arguments.get_call_node()
        parent = call_node.parent
        if isinstance(parent, grammar.Assign):
            if parent.target.is_local:
                validation_context.add_error(
                    message='rules must be stored in non-local features',
                    span=parent.span,
                    hint=(
                        f'this rule is being stored in the local variable `{parent.target.identifier}`'
                        "\nrules must be in non-local features (eg that don't start with `_`)"
                    ),
                )
            else:
                self.name = parent.target.identifier

        else:
            add_must_assign_to_variable_error(
                validation_context,
                message='`Rule(...)` must be assigned to a variable',
                node=call_node,
                example_variable_name='SomeRule',
            )

        # We want to make sure that the rule description is a string or fstring literal.
        description_ast = validate_kwarg_node_type(
            context=validation_context,
            udf=self,
            arguments=arguments,
            kwarg='description',
            message_ending='requires either a string literal or an f-string',
            expected_type=(grammar.String, grammar.FormatString),
            expected_str='`String` or `FormatString` literal',
        )

        # If our description ast is a string, do a lint, and see if the author meant
        # to use an f-string, because variables were attempted to be interpolated.
        if isinstance(description_ast, grammar.String):
            matches = _HAS_FORMAT_STRING_RE.findall(description_ast.value)

            if matches:
                found_variables = ', '.join(f'`{match}`' for match in matches)
                validation_context.add_error(
                    message='variable interpolation attempted in non-format string',
                    span=description_ast.span,
                    hint=(
                        'this string contains what looks like variable interpolation, but is not an f-string'
                        f'\nfound: {found_variables}'
                        '\nconsider prefixing with `f`'
                    ),
                )

    def execute(self, execution_context: ExecutionContext, arguments: RuleArguments) -> RuleT:
        assert self.name, 'BUG: Rule execution started with un-assigned name.'

        description_ast = arguments.get_argument_ast('description')
        if isinstance(description_ast, grammar.String):
            description = description_ast.value
            features = {}
        elif isinstance(description_ast, grammar.FormatString):
            description = description_ast.format_string
            features = {name.identifier: str(execution_context.resolved(name)) for name in description_ast.names}
        else:
            raise TypeError(f'BUG: Unknown type {description_ast!r}, should have been caught by validator.')

        value_result = all(arguments.when_all)

        # Rules prefixed with "metric" are used by AD for monitoring, but is an anti-pattern.
        if self.name.lower().startswith('metric'):
            metrics.increment(
                'execute',
                tags=[
                    f'udf:{self.__class__.__name__}',
                    f'rule:{self.name}',
                    f'result:{value_result}',
                    f'action:{execution_context.get_action_name()}',
                ],
            )
        return RuleT(name=self.name, value=value_result, description=description, features=features)


class WhenRulesArguments(ArgumentsBase):
    # TODO - Should we require this be non-empty?
    rules_any: List[RuleT]
    # TODO - Should we require this be non-empty?
    then: List[EffectBase]
    name: ConstExpr[str] = ConstExpr.for_default('name', '')
    """Unique name of the decision. Required when `mode` is not `enforce`."""
    mode: ConstExpr[str] = ConstExpr.for_default('mode', WhenRulesMode.ENFORCE.value)
    """One of `off`, `shadow`, `rollout`, `enforce`."""
    rollout_percent: ConstExpr[float] = ConstExpr.for_default('rollout_percent', 0)
    """Percentage of entities, 0 to 100, that receive effects in `rollout` mode."""
    revision_id: ConstExpr[str] = ConstExpr.for_default('revision_id', '')
    release_id: ConstExpr[str] = ConstExpr.for_default('release_id', '')


class WhenRules(UDFBase[WhenRulesArguments, None]):
    """Binds rules to effects. When any of the referenced rules fire, the then= effects are applied."""

    category = UdfCategories.ENGINE

    def __init__(self, validation_context: ValidationContext, arguments: WhenRulesArguments):
        super().__init__(validation_context, arguments)
        self._mode_config = self._validate_mode_arguments(validation_context, arguments)

    @staticmethod
    def _validate_mode_arguments(
        validation_context: ValidationContext, arguments: WhenRulesArguments
    ) -> Optional[_ModeConfig]:
        """Validates the mode arguments and returns the runtime config, or None if the block should behave as today."""
        declared = set(_MODE_ARGUMENTS) & set(arguments.get_call_node().argument_dict())
        if not declared:
            return None

        name = arguments.name.value if 'name' in declared else None
        mode_str = arguments.mode.value
        valid = True

        if name is not None and not name.strip():
            validation_context.add_error(message='`name` must not be empty', span=arguments.name.argument_span)
            valid = False

        try:
            mode = WhenRulesMode(mode_str)
        except ValueError:
            allowed = ', '.join(f'`{m.value}`' for m in WhenRulesMode)
            validation_context.add_error(
                message=f'unknown WhenRules mode `{mode_str}`',
                span=arguments.mode.argument_span,
                hint=f'mode must be one of {allowed}',
            )
            return None

        if mode is not WhenRulesMode.ENFORCE and name is None:
            validation_context.add_error(
                message=f'`name` is required when mode is `{mode.value}`',
                span=arguments.mode.argument_span,
            )
            valid = False

        if mode is not WhenRulesMode.ROLLOUT and 'rollout_percent' in declared:
            validation_context.add_error(
                message=f'`rollout_percent` is only valid when mode is `rollout`, not `{mode.value}`',
                span=arguments.rollout_percent.argument_span,
            )
            valid = False

        rollout_units = 0
        if mode is WhenRulesMode.ROLLOUT:
            then_ast = arguments.get_argument_ast('then')
            then_items = then_ast.items if isinstance(then_ast, grammar.List) else [then_ast]
            for item in then_items:
                if not _udf_takes_entity(validation_context, item):
                    validation_context.add_error(
                        message='`rollout` requires every `then` effect to be a direct call with an `entity` argument',
                        span=item.span,
                        hint='write the effect call inline in `then=[...]`; rollout buckets each effect by its entity',
                    )
                    valid = False
            if 'rollout_percent' not in declared:
                validation_context.add_error(
                    message='`rollout_percent` is required when mode is `rollout`',
                    span=arguments.mode.argument_span,
                )
                valid = False
            elif not 0 <= arguments.rollout_percent.value <= 100:
                validation_context.add_error(
                    message='`rollout_percent` must be between 0 and 100 inclusive',
                    span=arguments.rollout_percent.argument_span,
                )
                valid = False
            elif Decimal(str(arguments.rollout_percent.value)) * 100 % 1 != 0:
                validation_context.add_error(
                    message='`rollout_percent` precision is too high, use at most two decimal places',
                    span=arguments.rollout_percent.argument_span,
                )
                valid = False
            else:
                rollout_units = Experiment.percentage_to_bucket_units(arguments.rollout_percent.value)

        if name is not None and name.strip():
            seen: Dict[str, grammar.Span] = validation_context.get_validator_input(ValidateWhenRulesNames, {})
            validation_context.set_validator_input(ValidateWhenRulesNames, seen)
            prior_span = seen.get(name)
            if prior_span is not None:
                validation_context.add_error(
                    message=f'`name` must be unique across all rule files, `{name}` is already used',
                    span=arguments.name.argument_span,
                    additional_spans_message='first used here:',
                    additional_spans=[prior_span],
                )
                valid = False
            else:
                seen[name] = arguments.name.argument_span

        if not valid:
            return None

        if not _modes_enabled():
            if mode is not WhenRulesMode.ENFORCE:
                # Fail closed: running a shadow, rollout, or off decision as enforce would apply it to everyone.
                validation_context.add_error(
                    message=f'mode `{mode.value}` is disabled',
                    span=arguments.mode.argument_span,
                    hint=f'set {_MODES_ENABLED_ENV}=1 to enable WhenRules modes',
                )
                return None
            logger.warning(
                'when_rules_mode_ignored',
                extra={'when_rules_name': name, 'when_rules_mode': mode.value, 'flag': _MODES_ENABLED_ENV},
            )
            metrics.increment(_MODE_IGNORED_METRIC, tags=[f'mode:{mode.value}'])
            return None

        return _ModeConfig(
            name=name,
            mode=mode,
            rollout_units=rollout_units,
            revision_id=arguments.revision_id.value if 'revision_id' in declared else None,
            release_id=arguments.release_id.value if 'release_id' in declared else None,
        )

    def resolve_arguments(self, execution_context: ExecutionContext, call_executor: CallExecutor) -> WhenRulesArguments:
        # WhenRules has some custom resolve logic, in order to tolerate a failure in the `rules_any` dependency chain.

        # 1. Grab the raw nodes, that correspond to each key in `WhenRulesArgument`.
        then_node = call_executor.dependent_node_dict['then']
        rules_any_node = call_executor.dependent_node_dict['rules_any']

        # 2. Perform special resolution of the `then` list. Essentially, we are semi-circumventing the
        #    `ListExecutor`, in order to peek inside, and grab each non-failed item within the list.
        #    This is important because if we have a list of LabelAdd() calls, we can end up invalidating
        #    them all if any of the calls fails.
        then_value = []
        then_failed = 0
        assert isinstance(then_node, grammar.List), 'BUG: `then` node is not a list!'
        for item in then_node.items:
            resolved = execution_context.resolved(item, return_none_for_failed_values=True)
            if resolved is not None:
                then_value.append(resolved)
            else:
                then_failed += 1

        # 3. Perform special resolution of the `rules_any` list. Essentially, we are semi-circumventing the
        #    `ListExecutor`, in order to peek inside, and grab each non-failed item within the list.
        rules_any_value = []
        rules_any_failed = 0
        failed_rule_names: List[str] = []
        assert isinstance(rules_any_node, grammar.List), 'BUG: `rules_any node is not a List!'
        for item in rules_any_node.items:
            resolved = execution_context.resolved(item, return_none_for_failed_values=True)
            if resolved is not None:
                rules_any_value.append(resolved)
            else:
                rules_any_failed += 1
                if isinstance(item, grammar.Name):
                    failed_rule_names.append(item.identifier)
                else:
                    failed_rule_names.append('<unknown>')

        # 4. Store audit state for execute() to consume.
        # Safe because WhenRules is synchronous (execute_async=False) and CallExecutor.execute()
        # calls resolve_arguments() then execute() without yielding to the event loop.
        self._failed_rule_names = failed_rule_names
        self._then_failed = then_failed

        # 5. Emit completeness metrics for this WhenRules block.
        action_name = execution_context.get_action_name()
        is_degraded = rules_any_failed > 0 or then_failed > 0
        metrics.increment(
            'osprey.whenrules_completeness',
            tags=[f'action:{action_name}', f'degraded:{is_degraded}'],
        )

        # 6. Construct the resolved arguments, based on our custom argument resolution.
        return cast(
            WhenRulesArguments,
            call_executor.unresolved_arguments.update_with_resolved({'then': then_value, 'rules_any': rules_any_value}),
        )

    def execute(self, execution_context: ExecutionContext, arguments: WhenRulesArguments) -> None:
        all_rule_names = [rule.name for rule in arguments.rules_any]
        passing_rules = [rule for rule in arguments.rules_any if rule.value]
        passing_names = [rule.name for rule in passing_rules]
        assert hasattr(self, '_failed_rule_names'), 'BUG: resolve_arguments() must be called before execute()'
        failed_rule_names: List[str] = self._failed_rule_names
        then_failed: int = self._then_failed
        config = self._mode_config

        effects_emitted: List[str] = []
        effects_suppressed: List[EffectAuditRecord] = []
        rollout_effects: List[EffectAuditRecord] = []
        suppressed_ids: Set[int] = set()
        if passing_rules:
            effects = [o for o in arguments.then if isinstance(o, EffectBase)]
            if config is None:
                effects_emitted = [type(o).__name__ for o in effects]
            else:
                for effect in effects:
                    emit, record = self._decide(config, effect)
                    if emit:
                        effects_emitted.append(record.effect_type)
                    else:
                        suppressed_ids.add(id(effect))
                        if config.mode is not WhenRulesMode.OFF:
                            effects_suppressed.append(record)
                    if config.mode is WhenRulesMode.ROLLOUT:
                        rollout_effects.append(record)

        entry = WhenRulesAuditEntry(
            rules_evaluated=all_rule_names,
            rules_matched=passing_names,
            rules_failed=failed_rule_names,
            effects_emitted=effects_emitted,
            effects_failed=then_failed,
            is_degraded=len(failed_rule_names) > 0,
            name=config.name if config else None,
            mode=config.mode.value if config else None,
            revision_id=config.revision_id if config else None,
            release_id=config.release_id if config else None,
            effects_suppressed=effects_suppressed,
            rollout_effects=rollout_effects,
        )
        execution_context.add_rule_audit_entry(entry)

        has_gap = bool(passing_rules) and entry.effects_failed > 0 and not entry.is_degraded
        if has_gap:
            metrics.increment(
                _ENFORCEMENT_GAP_METRIC,
                tags=[f'action:{execution_context.get_action_name()}', _ENFORCEMENT_GAP_TRUE_TAG],
            )

        if not passing_rules:
            return

        for outcome in arguments.then:
            if isinstance(outcome, EffectBase):
                if id(outcome) in suppressed_ids:
                    continue
                if config is not None:
                    outcome = self._stamp(config, outcome)
                outcome.add_rules(passing_rules)
                execution_context.add_effect(outcome)
            else:
                raise TypeError(f'BUG: Non-EffectBase type for WhenRules: {outcome!r} (type: {type(outcome)})')

    @staticmethod
    def _decide(config: _ModeConfig, effect: EffectBase) -> Tuple[bool, EffectAuditRecord]:
        """Decides whether a matched effect is emitted under the block's mode, and builds its audit record.

        Each effect is bucketed by its own entity, so one decision can emit for some entities and not others.
        """
        effect_type = type(effect).__name__
        mode = config.mode
        entity = effect.primary_entity()
        to_str = getattr(effect, 'to_str', None)
        record = EffectAuditRecord(
            effect_type=effect_type,
            entity=str(entity) if entity is not None else None,
            detail=to_str() if callable(to_str) else None,
        )
        emit = mode is WhenRulesMode.ENFORCE
        disposition = _DISPOSITION_EMITTED if emit else _DISPOSITION_SUPPRESSED

        if mode is WhenRulesMode.ROLLOUT:
            assert config.name is not None
            if entity is None:
                # Validation rejects this; fail closed if a plugin effect still lacks an entity at runtime.
                disposition = _DISPOSITION_NO_ENTITY
                record.in_bucket = False
                if (config.name, effect_type) not in _reported_unbucketable:
                    _reported_unbucketable.add((config.name, effect_type))
                    logger.error(
                        'when_rules_rollout_effect_without_entity',
                        extra={'when_rules_name': config.name, 'effect_type': effect_type},
                    )
                    sentry_sdk.capture_message(
                        f'WhenRules rollout `{config.name}` has {effect_type} effects with no primary entity;'
                        ' they are suppressed',
                        level='error',
                    )
            else:
                record.bucket = Experiment.hash_mod(f'{_BUCKET_KEY_PREFIX}{config.name}', f'{entity.type}/{entity.id}')
                record.in_bucket = record.bucket < config.rollout_units
                emit = record.in_bucket
                disposition = _DISPOSITION_EMITTED if emit else _DISPOSITION_SUPPRESSED

        if mode is not WhenRulesMode.OFF:
            metrics.increment(_EFFECTS_METRIC, tags=[f'mode:{mode.value}', f'disposition:{disposition}'])
        return emit, record

    @staticmethod
    def _stamp(config: _ModeConfig, effect: EffectBase) -> EffectBase:
        """Returns a copy of the effect carrying the block's identity, so an effect shared by two blocks keeps both."""
        stamped = copy.copy(effect)
        stamped.rules = []
        stamped.when_rules_name = config.name
        stamped.when_rules_mode = config.mode.value
        stamped.revision_id = config.revision_id
        stamped.release_id = config.release_id
        return stamped
