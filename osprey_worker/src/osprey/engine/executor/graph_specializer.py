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
     (a) Conservative Rule pruning: a Rule chain is pruned if ANY non-constant
         dep in chain.dependent_on is pruned.  Rule.execute calls all(when_all)
         via ListExecutor which resolves each dep WITHOUT return_none_for_failed;
         a pruned dep that was never executed will raise KeyError at runtime.
     (b) ResolveOptional with non-None default_value: rescued — not pruned
         even if its optional_value dep is pruned (returns default at runtime).
         The optional_value extractor chain and its transitive deps are rescued
         so the executor can execute and return None for the absent field.
     (c) Default propagation: prune if ALL non-constant (non-IsConstant) deps
         are pruned. Literal constants (String, Number, Boolean) do not block
         pruning of their parent, since they are always computable.
  3. Surviving chains are assembled into a SpecializedExecutionGraph.

Node identity: NodeKey = id(ast_node) — collision-free (see NodeKey definition).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, FrozenSet, List, Optional, Sequence, Set

from osprey.engine.ast.grammar import ASTNode, IsConstant, List as GrammarList, Source
from osprey.engine.ast.grammar import String
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.node_executor.call_executor import CallExecutor
from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional
from osprey.engine.stdlib.udfs.rules import Rule

if TYPE_CHECKING:
    from osprey.engine.schema.schema_loader import ActionSchema

log = logging.getLogger(__name__)

# Node identity = id() of the AST node object.
#
# A structural key (source_path, start_line, start_pos, class_name) is NOT unique:
# CPython gives a mixed-operator boolean expression `A and B or C` two
# BooleanOperation nodes (the outer Or and inner And) that share the leftmost
# child's line+col, and Osprey's Span carries no end position to disambiguate
# them. With a structural key, pruning the inner And (it reads an absent group)
# would also prune the surviving outer Or (same key) — silently dropping the
# feature/verdict even when the schema's `absent` set is correct.
#
# The specializer and the runtime (SpecializedExecutionGraph) operate on the SAME
# full_graph AST node objects, and the spec graph is rebuilt against a fresh graph
# on every recompile, so id() is stable for a specialization's lifetime and
# collision-free across distinct nodes.
NodeKey = int


def _node_key_from_node(node: ASTNode) -> NodeKey:
    """Collision-free node identity (id of the AST node object)."""
    return id(node)


def _node_key_from_chain(chain: DependencyChain) -> NodeKey:
    """Collision-free node identity from a DependencyChain's executor node."""
    return id(chain.executor.node)


def _chain_udf(chain: DependencyChain) -> Optional[object]:
    """Return the UDF instance from a CallExecutor chain, or None."""
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
    if isinstance(chain.executor, CallExecutor):
        try:
            path_arg = chain.executor.unresolved_arguments.get_argument_ast("path")
            if path_arg is not None:
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
    udf = _chain_udf(chain)
    return isinstance(udf, ResolveOptional)


def _is_rule_chain(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF is Rule.

    Rule.execute calls all(when_all) via ListExecutor which resolves each dep
    without return_none_for_failed_values=True. A pruned dep that was never
    executed will cause a KeyError at runtime, so Rule chains must be pruned
    conservatively (rule (a): prune if ANY non-constant dep is pruned).
    """
    udf = _chain_udf(chain)
    return isinstance(udf, Rule)


def _resolve_optional_has_default(chain: DependencyChain) -> bool:
    """Return True if this ResolveOptional has a non-None default_value."""
    if not isinstance(chain.executor, CallExecutor):
        return False
    try:
        default_arg = chain.executor.unresolved_arguments.get_argument_ast("default_value")
        return default_arg is not None
    except Exception:
        return False


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
) -> "SpecializedExecutionGraph":
    """Produce a specialized execution graph for the given action schema.

    Chains whose top-level json group is in schema.absent_groups are pruned,
    along with their dependents (using the propagation rules in §4.4).

    Returns a SpecializedExecutionGraph that delegates to full_graph for
    everything except pruned chains.
    """
    absent_groups: FrozenSet[str] = schema.absent_groups

    # Step 1 — collect all chains
    all_top_level_chains = _get_all_sorted_chains(full_graph)
    all_chains = _collect_all_chains_recursive(all_top_level_chains)

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

            # (a) Rule chains — conservative pruning: prune if ANY non-constant dep
            # (or dep-of-dep via the when_all List node) is pruned.
            # Rule.execute resolves when_all via ListExecutor which calls
            # execution_context.resolved(n) without return_none_for_failed_values=True.
            # A pruned (never-executed) dep would cause a KeyError at runtime.
            #
            # The Rule chain's dependent_on is [when_all_List_chain, description_String_chain].
            # The individual when_all conditions are deps of the List chain (one level deeper).
            # We must check both the direct deps AND the deps of the when_all List node.
            if _is_rule_chain(chain):
                should_prune = False
                for dep in deps:
                    if isinstance(dep.executor.node, IsConstant) and dep.executor.node.is_constant:
                        continue
                    dep_key = _node_key_from_chain(dep)
                    if dep_key in pruned:
                        should_prune = True
                        break
                    # If this dep is the when_all List node, check its own deps too.
                    if isinstance(dep.executor.node, GrammarList):
                        for when_all_dep in dep.dependent_on:
                            if isinstance(when_all_dep.executor.node, IsConstant) and when_all_dep.executor.node.is_constant:
                                continue
                            when_all_dep_key = _node_key_from_chain(when_all_dep)
                            if when_all_dep_key in pruned:
                                should_prune = True
                                break
                    if should_prune:
                        break
                if should_prune:
                    pruned.add(key)
                    changed = True
                continue  # Rule chains skip rules (b) and (c)

            # (b) ResolveOptional with default: don't prune even if optional_value dep
            # is pruned — the node will return default_value at runtime.
            # Rescue the pruned dep chain (optional_value) and its transitive deps,
            # stopping at chains that are not pruned or are constants.
            # `rescued` prevents re-visiting nodes in diamond-shaped dependency graphs.
            if _is_resolve_optional_chain(chain) and _resolve_optional_has_default(chain):
                to_rescue: List[DependencyChain] = list(deps)
                rescued: Set[NodeKey] = set()
                while to_rescue:
                    dep = to_rescue.pop()
                    dep_key = _node_key_from_chain(dep)
                    if dep_key in rescued:
                        continue
                    rescued.add(dep_key)
                    if dep_key in pruned:
                        pruned.discard(dep_key)
                        changed = True
                        to_rescue.extend(dep.dependent_on)
                continue

            # (c) Default propagation: prune if ALL non-constant deps are pruned.
            # A dep whose AST node is an IsConstant (String, Number, Boolean,
            # etc.) can never be pruned and should not block pruning of its
            # parent.  A dep that is NOT a constant but is not yet in `pruned`
            # is a live computed value that keeps the current chain alive.
            non_const_surviving_dep_keys = []
            for dep in deps:
                if isinstance(dep.executor.node, IsConstant) and dep.executor.node.is_constant:
                    # Literal constant — skip; cannot be pruned
                    continue
                dep_key = _node_key_from_chain(dep)
                if dep_key not in pruned:
                    non_const_surviving_dep_keys.append(dep_key)

            # Only prune if there is at least one non-constant dep (otherwise
            # the chain itself is effectively constant and should remain).
            has_non_const_dep = any(
                not (isinstance(dep.executor.node, IsConstant) and dep.executor.node.is_constant)
                for dep in deps
            )
            if has_non_const_dep and not non_const_surviving_dep_keys:
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

    def is_pruned_node(self, node: ASTNode) -> bool:
        """True if this node's chain was pruned by this specialization."""
        return _node_key_from_node(node) in self._pruned_keys

    @property
    def pruned_count(self) -> int:
        """Number of chains pruned by this specialization."""
        return len(self._pruned_keys)

    @property
    def schema(self) -> "ActionSchema":
        return self._schema
