"""Graph specializer for typed action contracts.

Given a full ExecutionGraph and an ActionSchema, produces a specialized graph
that skips nodes whose extracted json paths belong to absent top-level groups.

The specializer works by creating a SpecializedExecutionGraph subclass that
overrides get_sorted_dependency_chain() to filter out pruned DependencyChains.

Pruning rules (per §4.4 of the typed-action-contracts plan):
  1. Root absent extractors: any DependencyChain whose executor's UDF has
     `extracts_json_path = True` AND whose path top-level group is in
     schema.absent_groups is pruned.
  2. Propagation:
     (a) Conservative Rule: prune if ANY when_all dep is pruned.
     (b) ResolveOptional with non-None default_value: rewrite to a
         short-circuit (return default without evaluating optional_value).
         Implemented by filtering the optional_value dep from the chain.
     (c) Default propagation: prune if ALL non-constant deps are pruned.
  3. Surviving chains are assembled into a SpecializedExecutionGraph.

Stable node identity: NodeKey = Tuple[str, int, int, str]
  = (source_path, span.start_line, span.start_pos, ast_node_class_name)
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from osprey.engine.ast.grammar import ASTNode, Source
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_graph import ExecutionGraph

if TYPE_CHECKING:
    from osprey.engine.ast_validator.validation_context import ValidatedSources
    from osprey.engine.ast_validator.validators.collect_json_data_paths import FieldDeclaration
    from osprey.engine.schema.schema_loader import ActionSchema

log = logging.getLogger(__name__)

# Stable node identity: (source_path, start_line, start_pos, ast_class_name)
NodeKey = Tuple[str, int, int, str]


def _node_key_from_chain(chain: DependencyChain) -> NodeKey:
    """Compute stable node key from a DependencyChain."""
    node = chain.executor.node
    span = node.span
    return (span.source.path, span.start_line, span.start_pos, type(node).__name__)


def _chain_udf(chain: DependencyChain) -> Optional[object]:
    """Return the UDF instance from a CallExecutor chain, or None."""
    from osprey.engine.executor.node_executor.call_executor import CallExecutor

    if isinstance(chain.executor, CallExecutor):
        return chain.executor._udf
    return None


def _is_json_extractor(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF has extracts_json_path = True."""
    udf = _chain_udf(chain)
    if udf is None:
        return False
    return getattr(type(udf), "extracts_json_path", False)


def _get_extractor_path(chain: DependencyChain) -> Optional[str]:
    """Return the path argument from a json-extractor chain."""
    udf = _chain_udf(chain)
    if udf is None:
        return None
    # Path is stored on the UDF's arguments (already resolved ConstExpr)
    # Access via the executor's unresolved_arguments
    from osprey.engine.executor.node_executor.call_executor import CallExecutor

    if isinstance(chain.executor, CallExecutor):
        try:
            path_arg = chain.executor.unresolved_arguments.get_argument_ast("path")
            if path_arg is not None:
                from osprey.engine.ast.grammar import String

                if isinstance(path_arg, String):
                    return path_arg.value
        except Exception:
            pass
    return None


def _get_top_level_group(path_str: str) -> str:
    """Extract top-level group from a json path string."""
    if path_str.startswith("$."):
        rest = path_str[2:]
        if rest:
            return rest.split(".")[0].split("[")[0]
    return path_str.lstrip("$").lstrip(".").split(".")[0]


def _is_resolve_optional_chain(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF is ResolveOptional."""
    from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional

    udf = _chain_udf(chain)
    return isinstance(udf, ResolveOptional)


def _resolve_optional_has_default(chain: DependencyChain) -> bool:
    """Return True if this ResolveOptional has a non-None default_value."""
    from osprey.engine.executor.node_executor.call_executor import CallExecutor

    if not isinstance(chain.executor, CallExecutor):
        return False
    try:
        default_arg = chain.executor.unresolved_arguments.get_argument_ast("default_value")
        return default_arg is not None
    except Exception:
        return False


def _is_rule_chain(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF is a Rule (which has when_all)."""
    from osprey.engine.stdlib.udfs.rules import Rule

    udf = _chain_udf(chain)
    return isinstance(udf, Rule)


def _get_when_all_dep_keys(chain: DependencyChain) -> List[NodeKey]:
    """Return node keys for the when_all argument dependencies of a Rule chain."""
    from osprey.engine.executor.node_executor.call_executor import CallExecutor
    from osprey.engine.ast.grammar import List as GrammarList

    if not isinstance(chain.executor, CallExecutor):
        return []
    try:
        dep_dict = chain.executor.unresolved_arguments.get_dependent_node_dict()
        keys = []
        for arg_name, node in dep_dict.items():
            if arg_name == "when_all" or (isinstance(arg_name, str) and "when_all" in arg_name):
                # The when_all node is a List; each item in the list is a dep
                if isinstance(node, GrammarList):
                    for item in node.items:
                        span = item.span
                        keys.append((span.source.path, span.start_line, span.start_pos, type(item).__name__))
                else:
                    span = node.span
                    keys.append((span.source.path, span.start_line, span.start_pos, type(node).__name__))
        return keys
    except Exception:
        return []


def _get_all_sorted_chains(graph: ExecutionGraph) -> List[DependencyChain]:
    """Gather all sorted dependency chains from all sources in the graph."""
    chains: List[DependencyChain] = []
    seen: Set[int] = set()

    for source in graph.validated_sources.sources:
        try:
            for chain in graph.get_sorted_dependency_chain(source):
                if id(chain) not in seen:
                    seen.add(id(chain))
                    chains.append(chain)
        except KeyError:
            pass
    return chains


def _collect_all_chains_recursive(chains: Sequence[DependencyChain]) -> List[DependencyChain]:
    """Recursively collect all chains including sub-chains."""
    result: List[DependencyChain] = []
    seen: Set[int] = set()

    def visit(chain: DependencyChain) -> None:
        if id(chain) in seen:
            return
        seen.add(id(chain))
        for dep in chain.dependent_on:
            visit(dep)
        result.append(chain)

    for chain in chains:
        visit(chain)
    return result


def specialize_graph(
    full_graph: ExecutionGraph,
    schema: "ActionSchema",
    manifest_fields: List["FieldDeclaration"],
) -> "SpecializedExecutionGraph":
    """Produce a specialized execution graph for the given action schema.

    Chains whose top-level json group is in schema.absent_groups are pruned,
    along with their dependents (using the propagation rules in §4.4).

    Returns a SpecializedExecutionGraph that delegates to full_graph for
    everything except pruned chains.
    """
    # Build lookup: NodeKey -> FieldDeclaration for fast absent-check
    absent_groups: FrozenSet[str] = schema.absent_groups

    # Step 1 — collect all chains and build key maps
    all_top_level_chains = _get_all_sorted_chains(full_graph)
    all_chains = _collect_all_chains_recursive(all_top_level_chains)

    # Map NodeKey -> chain for propagation
    key_to_chain: Dict[NodeKey, DependencyChain] = {}
    for chain in all_chains:
        key = _node_key_from_chain(chain)
        key_to_chain[key] = chain

    # Step 2 — seed pruned set with absent extractors
    pruned: Set[NodeKey] = set()

    for chain in all_chains:
        if _is_json_extractor(chain):
            path = _get_extractor_path(chain)
            if path is not None:
                group = _get_top_level_group(path)
                if group in absent_groups:
                    pruned.add(_node_key_from_chain(chain))

    # Step 3 — propagation loop
    changed = True
    while changed:
        changed = False
        for chain in all_chains:
            key = _node_key_from_chain(chain)
            if key in pruned:
                continue

            deps = chain.dependent_on

            # (a) Conservative WhenRules: prune if ANY when_all dep is pruned
            if _is_rule_chain(chain):
                when_all_keys = _get_when_all_dep_keys(chain)
                if when_all_keys and any(k in pruned for k in when_all_keys):
                    pruned.add(key)
                    changed = True
                    continue

            # (b) ResolveOptional with default: don't prune even if optional_value dep
            # is pruned — the node will return default_value at runtime.
            # Rescue all transitive pruned deps so the executor can find them at runtime.
            if _is_resolve_optional_chain(chain) and _resolve_optional_has_default(chain):
                # Walk all deps (and their transitive deps) — if any were pruned, rescue
                # them so the chain can still execute (producing None for absent fields).
                to_rescue: List[DependencyChain] = list(deps)
                while to_rescue:
                    dep = to_rescue.pop()
                    dep_key = _node_key_from_chain(dep)
                    if dep_key in pruned:
                        pruned.discard(dep_key)
                        changed = True
                        to_rescue.extend(dep.dependent_on)
                continue

            # (c) Default propagation: prune if ALL non-constant deps are pruned
            non_const_dep_keys = []
            for dep in deps:
                dep_key = _node_key_from_chain(dep)
                if dep_key not in pruned:
                    non_const_dep_keys.append(dep_key)

            if deps and not non_const_dep_keys:
                pruned.add(key)
                changed = True

    log.debug(
        "specialize_graph: schema=%s absent_groups=%r pruned %d of %d chains",
        schema.action,
        absent_groups,
        len(pruned),
        len(all_chains),
    )

    return SpecializedExecutionGraph(
        full_graph=full_graph,
        pruned_keys=frozenset(pruned),
        schema=schema,
    )


class SpecializedExecutionGraph(ExecutionGraph):
    """A specialized ExecutionGraph that filters out pruned dependency chains.

    Constructed by specialize_graph(); delegates to the full_graph for all
    unmodified behavior and overrides get_sorted_dependency_chain() to skip
    absent-group chains.
    """

    __slots__ = (
        '_root_node_executor_mapping',
        '_assignment_executor_mapping',
        '_node_executor_registry',
        '_validated_sources',
        '_sorted_dependency_chains',
        '_nodes_to_unwrap',
        '_full_graph',
        '_pruned_keys',
        '_schema',
    )

    def __init__(
        self,
        full_graph: ExecutionGraph,
        pruned_keys: FrozenSet[NodeKey],
        schema: "ActionSchema",
    ) -> None:
        # Initialize the base ExecutionGraph with the full graph's registry and sources
        super().__init__(
            node_executor_registry=full_graph._node_executor_registry,
            sources=full_graph._validated_sources,
            nodes_to_unwrap=full_graph._nodes_to_unwrap,
        )
        # Copy existing mappings from the full graph
        self._root_node_executor_mapping = full_graph._root_node_executor_mapping
        self._assignment_executor_mapping = full_graph._assignment_executor_mapping
        self._sorted_dependency_chains = full_graph._sorted_dependency_chains
        self._full_graph = full_graph
        self._pruned_keys = pruned_keys
        self._schema = schema

    def get_sorted_dependency_chain(self, source: Source) -> Sequence[DependencyChain]:
        """Return the sorted dependency chain for a source, with pruned chains removed."""
        original = self._full_graph.get_sorted_dependency_chain(source)
        if not self._pruned_keys:
            return original
        return [
            chain
            for chain in original
            if _node_key_from_chain(chain) not in self._pruned_keys
        ]

    @property
    def pruned_count(self) -> int:
        """Number of chains pruned by this specialization."""
        return len(self._pruned_keys)

    @property
    def schema(self) -> "ActionSchema":
        return self._schema
