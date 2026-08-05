from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

from osprey.engine.ast.grammar import ASTNode, Call, Name

from ..node_executor_registry import NodeExecutorRegistry
from ._base_node_executor import BaseNodeExecutor

if TYPE_CHECKING:
    from ddtrace.span import Span
    from osprey.engine.ast_validator.validation_context import ValidatedSources
    from osprey.engine.udf.arguments import ArgumentsBase
    from osprey.engine.udf.base import UDFBase

    from ..execution_context import ExecutionContext
    from ..execution_graph import ExecutionGraph


@dataclass(frozen=True, slots=True)
class ArgumentResolutionStep:
    """Immutable metadata for resolving one call kwarg at execution time."""

    kwarg_name: str
    node: ASTNode
    node_key: int
    return_none_on_failure: bool
    should_unwrap: bool


@NodeExecutorRegistry.register_globally
class CallExecutor(BaseNodeExecutor[Call, Any]):
    node_type = Call
    unresolved_arguments: 'ArgumentsBase'

    _udf: 'UDFBase[Any, Any]'
    _resolution_recipe: Optional[Tuple[ArgumentResolutionStep, ...]]

    def __init__(self, node: Call, sources: 'ValidatedSources'):
        from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs

        super().__init__(node=node, sources=sources)
        udf_map = sources.get_validator_result(ValidateCallKwargs)
        self._udf, self.unresolved_arguments = udf_map[id(node)]
        self.dependent_node_dict = self.unresolved_arguments.get_dependent_node_dict()
        self._resolution_recipe = None

    def prepare_resolution_recipe(self, execution_graph: 'ExecutionGraph') -> None:
        """Compile stable argument lookup metadata after the full graph is complete."""
        if self._resolution_recipe is not None:
            raise RuntimeError('argument resolution recipe was already prepared')

        steps = []
        for kwarg_name, argument_node in self.dependent_node_dict.items():
            result_node = argument_node
            if isinstance(argument_node, Name):
                result_node = execution_graph.get_assignment_dependency_chain(argument_node).executor.node
            steps.append(
                ArgumentResolutionStep(
                    kwarg_name=kwarg_name,
                    node=result_node,
                    node_key=id(result_node),
                    return_none_on_failure=self.unresolved_arguments.kwarg_can_be_none(kwarg_name),
                    should_unwrap=execution_graph.should_unwrap(argument_node),
                )
            )
        self._resolution_recipe = tuple(steps)

    @property
    def resolution_recipe(self) -> Tuple[ArgumentResolutionStep, ...]:
        if self._resolution_recipe is None:
            raise RuntimeError('argument resolution recipe has not been prepared')
        return self._resolution_recipe

    def set_tracing_tags(self, span: 'Span') -> None:
        span.set_tag('udf', self._udf.__class__.__name__)

    def execute(self, execution_context: 'ExecutionContext') -> Any:
        resolved_arguments = self._udf.resolve_arguments(execution_context, self)
        result = self._udf.execute(execution_context, resolved_arguments)
        return self._udf.check_result_type(result)

    def get_dependent_nodes(self) -> List[ASTNode]:
        return list(self.dependent_node_dict.values())

    @property
    def execute_async(self) -> bool:
        return self._udf.execute_async
