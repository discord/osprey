from dataclasses import FrozenInstanceError, dataclass
from datetime import datetime, timezone
from textwrap import dedent
from typing import Optional
from unittest.mock import patch

import pytest
from osprey.engine.ast.grammar import Assign, Call, Name
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.executor.execution_context import Action, ExecutionContext, ExpectedUdfException
from osprey.engine.executor.execution_graph import ExecutionGraph, compile_execution_graph
from osprey.engine.executor.executor import execute
from osprey.engine.executor.graph_specializer import SpecializedExecutionGraph
from osprey.engine.executor.node_executor.call_executor import CallExecutor
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.language_types.post_execution_convertible import PostExecutionConvertible
from osprey.engine.schema.schema_loader import ActionSchema
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import UDFBase
from osprey.engine.udf.registry import UDFRegistry
from result import Ok, UnwrapError


class _IntValueArguments(ArgumentsBase):
    value: ConstExpr[int]


class _IntValue(UDFBase[_IntValueArguments, int]):
    def execute(self, execution_context: ExecutionContext, arguments: _IntValueArguments) -> int:
        return arguments.value.value


class _FailValue(UDFBase[ArgumentsBase, int]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> int:
        raise ExpectedUdfException()


@dataclass(frozen=True)
class _Convertible(PostExecutionConvertible[str]):
    value: str

    def to_post_execution_value(self) -> str:
        return self.value


class _ConvertibleValue(UDFBase[ArgumentsBase, _Convertible]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> _Convertible:
        return _Convertible('converted')


@dataclass(frozen=True)
class _FailingConvertible(PostExecutionConvertible[str]):
    def to_post_execution_value(self) -> str:
        raise UnwrapError('convert boom')


class _FailingConvertibleValue(UDFBase[ArgumentsBase, _FailingConvertible]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> _FailingConvertible:
        return _FailingConvertible()


class _ResolveAllArguments(ArgumentsBase):
    direct: int
    loaded: int
    nullable: Optional[int]
    converted: str


class _ResolveAll(UDFBase[_ResolveAllArguments, str]):
    def execute(self, execution_context: ExecutionContext, arguments: _ResolveAllArguments) -> str:
        return f'{arguments.direct}|{arguments.loaded}|{arguments.nullable}|{arguments.converted}'


class _RequiredValueArguments(ArgumentsBase):
    value: int


class _RequiredValue(UDFBase[_RequiredValueArguments, int]):
    def execute(self, execution_context: ExecutionContext, arguments: _RequiredValueArguments) -> int:
        return arguments.value


class _OptionalValueArguments(ArgumentsBase):
    value: Optional[int]


class _OptionalValue(UDFBase[_OptionalValueArguments, int]):
    def execute(self, execution_context: ExecutionContext, arguments: _OptionalValueArguments) -> int:
        return -1 if arguments.value is None else arguments.value


class _OptionalConvertedArguments(ArgumentsBase):
    value: Optional[str]


class _OptionalConverted(UDFBase[_OptionalConvertedArguments, str]):
    def execute(self, execution_context: ExecutionContext, arguments: _OptionalConvertedArguments) -> str:
        return 'fallback' if arguments.value is None else arguments.value


class _RequiredConvertedArguments(ArgumentsBase):
    value: str


class _RequiredConverted(UDFBase[_RequiredConvertedArguments, str]):
    def execute(self, execution_context: ExecutionContext, arguments: _RequiredConvertedArguments) -> str:
        return arguments.value


class _CustomResolveArguments(ArgumentsBase):
    value: int


class _CustomResolve(UDFBase[_CustomResolveArguments, int]):
    def resolve_arguments(
        self, execution_context: ExecutionContext, call_executor: CallExecutor
    ) -> _CustomResolveArguments:
        failed_value = execution_context.resolved(
            call_executor.dependent_node_dict['value'], return_none_for_failed_values=True
        )
        assert failed_value is None
        return call_executor.unresolved_arguments.update_with_resolved({'value': 41})  # type: ignore[return-value]

    def execute(self, execution_context: ExecutionContext, arguments: _CustomResolveArguments) -> int:
        return arguments.value + 1


_REGISTRY = UDFRegistry.with_udfs(
    _IntValue,
    _FailValue,
    _ConvertibleValue,
    _FailingConvertibleValue,
    _ResolveAll,
    _RequiredValue,
    _OptionalValue,
    _OptionalConverted,
    _RequiredConverted,
    _CustomResolve,
)
_VALIDATORS = ValidatorRegistry.from_validator_classes(
    [
        ValidateCallKwargs,
        ValidateDynamicCallsHaveAnnotatedRValue,
        ImportsMustNotHaveCycles,
        UniqueStoredNames,
        VariablesMustBeDefined,
        ValidateStaticTypes,
    ]
)


def _compile(source: str) -> ExecutionGraph:
    sources = Sources.from_dict({'main.sml': dedent(source)})
    return compile_execution_graph(validate_sources(sources, _REGISTRY, _VALIDATORS))


def _call_executor(graph: ExecutionGraph, udf_type: type[UDFBase]) -> CallExecutor:
    plan = graph.get_execution_plan()
    assert plan is not None
    matches = [
        chain.executor
        for chain in plan.chains
        if isinstance(chain.executor, CallExecutor) and isinstance(chain.executor._udf, udf_type)
    ]
    assert len(matches) == 1
    return matches[0]


def _execute(graph: ExecutionGraph):
    return execute(
        graph,
        UDFHelpers(),
        Action(
            action_id=1,
            action_name='recipe_test',
            data={},
            timestamp=datetime(2026, 8, 5, tzinfo=timezone.utc),
        ),
        async_pool=None,
    )


def _schema() -> ActionSchema:
    return ActionSchema(
        action='recipe_test',
        provides_groups=frozenset(),
        absent_groups=frozenset(),
        provides_field_types={},
        optional_for={},
    )


def test_full_graph_compiles_ordered_immutable_argument_resolution_recipe() -> None:
    graph = _compile(
        """
        Loaded = _IntValue(value=7)
        Result = _ResolveAll(
            direct=_IntValue(value=3),
            loaded=Loaded,
            nullable=_FailValue(),
            converted=_ConvertibleValue(),
        )
        """
    )
    executor = _call_executor(graph, _ResolveAll)
    dependent_nodes = executor.dependent_node_dict
    loaded_name = dependent_nodes['loaded']
    assert isinstance(loaded_name, Name)
    loaded_result_node = graph.get_assignment_dependency_chain(loaded_name).executor.node

    recipe = executor.resolution_recipe

    assert isinstance(recipe, tuple)
    assert tuple(step.kwarg_name for step in recipe) == ('direct', 'loaded', 'nullable', 'converted')
    assert tuple(step.node for step in recipe) == (
        dependent_nodes['direct'],
        loaded_result_node,
        dependent_nodes['nullable'],
        dependent_nodes['converted'],
    )
    assert isinstance(recipe[0].node, Call)
    assert isinstance(recipe[1].node, Assign)
    assert tuple(step.node_key for step in recipe) == tuple(id(step.node) for step in recipe)
    assert tuple(step.return_none_on_failure for step in recipe) == (False, False, True, False)
    assert tuple(step.should_unwrap for step in recipe) == (False, False, False, True)

    with pytest.raises(FrozenInstanceError):
        recipe[0].node_key = 0  # type: ignore[misc]
    with pytest.raises(TypeError):
        recipe[0] = recipe[-1]  # type: ignore[index]


def test_default_resolver_uses_recipe_for_direct_name_nullable_required_and_converted_values() -> None:
    graph = _compile(
        """
        Loaded = _IntValue(value=7)
        Result = _ResolveAll(
            direct=_IntValue(value=3),
            loaded=Loaded,
            nullable=_FailValue(),
            converted=_ConvertibleValue(),
        )
        RequiredFailure = _RequiredValue(value=_FailValue())
        """
    )

    with (
        patch.object(ExecutionContext, 'resolved', side_effect=AssertionError('legacy node resolution used')),
        patch.object(
            _ResolveAllArguments, 'kwarg_can_be_none', side_effect=AssertionError('runtime nullability lookup used')
        ),
        patch.object(
            _RequiredValueArguments,
            'kwarg_can_be_none',
            side_effect=AssertionError('runtime nullability lookup used'),
        ),
    ):
        result = _execute(graph)

    assert result.error_infos == []
    assert result.extracted_features['Loaded'] == 7
    assert result.extracted_features['Result'] == '3|7|None|converted'
    assert result.extracted_features['RequiredFailure'] is None


def test_specialized_context_uses_recipe_for_pruned_nullable_required_and_folded_values() -> None:
    graph = _compile(
        """
        NullablePruned = _OptionalValue(value=_IntValue(value=1))
        Folded = _RequiredValue(value=_IntValue(value=2))
        RequiredPruned = _RequiredValue(value=_IntValue(value=3))
        """
    )
    plan = graph.get_execution_plan()
    assert plan is not None
    int_value_executors = {
        chain.executor.unresolved_arguments.value.value: chain.executor
        for chain in plan.chains
        if isinstance(chain.executor, CallExecutor) and isinstance(chain.executor._udf, _IntValue)
    }
    assert set(int_value_executors) == {1, 2, 3}
    specialized = SpecializedExecutionGraph(
        full_graph=graph,
        pruned_keys=frozenset({id(int_value_executors[1].node), id(int_value_executors[3].node)}),
        schema=_schema(),
        fold_values={id(int_value_executors[2].node): Ok(22)},
    )

    with (
        patch.object(ExecutionContext, 'resolved', side_effect=AssertionError('legacy node resolution used')),
        patch.object(
            _OptionalValueArguments,
            'kwarg_can_be_none',
            side_effect=AssertionError('runtime nullability lookup used'),
        ),
        patch.object(
            _RequiredValueArguments,
            'kwarg_can_be_none',
            side_effect=AssertionError('runtime nullability lookup used'),
        ),
    ):
        result = _execute(specialized)

    assert result.error_infos == []
    assert result.extracted_features['NullablePruned'] == -1
    assert result.extracted_features['Folded'] == 22
    assert result.extracted_features['RequiredPruned'] is None


def test_nullable_converter_unwrap_error_becomes_none_and_execution_continues() -> None:
    graph = _compile('Result = _OptionalConverted(value=_FailingConvertibleValue())')

    result = _execute(graph)

    assert result.error_infos == []
    assert result.extracted_features['Result'] == 'fallback'


def test_required_converter_unwrap_error_propagates_without_error_info() -> None:
    graph = _compile('Result = _RequiredConverted(value=_FailingConvertibleValue())')

    result = _execute(graph)

    assert result.error_infos == []
    assert result.extracted_features['Result'] is None


def test_custom_resolver_keeps_its_override_path() -> None:
    graph = _compile('Custom = _CustomResolve(value=_FailValue())')

    result = _execute(graph)

    assert result.error_infos == []
    assert result.extracted_features['Custom'] == 42
