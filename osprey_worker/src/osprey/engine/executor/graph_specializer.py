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
  2.5 Verdict-critical rescue: after propagation, the transitive closure of every
     WhenRules chain (rules_any -> Rules -> when_all -> extractors, plus then -> effects)
     is un-pruned. Enforcement-feeding nodes therefore always compute exactly as the full
     graph — a required=False extractor over an absent group resolves to None and flows
     through its comparison (`None != x` is True) — so pruning can NEVER change an emitted
     verdict/effect; it only ever drops analytics-only features (those feeding no effect).
     This supersedes (a) for any Rule bound to a WhenRules and is what makes a wrong
     `absent` entry (from a producer or consumer change) unable to silently miss enforcement.
  3. Surviving chains are assembled into a SpecializedExecutionGraph.

Node identity: NodeKey = id(ast_node) — collision-free (see NodeKey definition).
"""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, FrozenSet, List, Optional, Sequence, Set

from osprey.engine.ast.grammar import ASTNode, IsConstant, Source, String
from osprey.engine.ast.grammar import List as GrammarList
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_graph import ExecutionGraph
from osprey.engine.executor.node_executor.call_executor import CallExecutor
from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules

if TYPE_CHECKING:
    from osprey.engine.schema.schema_loader import ActionSchema

log = logging.getLogger(__name__)

# Node identity = id() of the AST node. A structural key (source/line/col/class) is
# NOT unique: `A and B or C` yields an Or and an And node sharing line+col (Span has
# no end position), so pruning the inner And would also drop the surviving Or. The
# specializer and runtime share the same full_graph node objects (rebuilt per
# recompile), so id() is stable for a specialization and collision-free.
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


def _is_whenrules_chain(chain: DependencyChain) -> bool:
    """Return True if this chain's UDF is WhenRules (binds rules to effects). A WhenRules
    chain's transitive closure is the entire enforcement output path (rules_any -> Rules ->
    when_all -> extractors, plus then -> effects), which the specializer must never prune."""
    return isinstance(_chain_udf(chain), WhenRules)


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


def _collect_chain_keys(chain: DependencyChain, out: "Set[NodeKey]") -> None:
    """Add `chain`'s node key and the keys of all its transitive deps to `out`."""
    key = _node_key_from_chain(chain)
    if key in out:
        return
    out.add(key)
    for dep in chain.dependent_on:
        _collect_chain_keys(dep, out)


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

            # (b) ResolveOptional-with-default: keep it (don't prune), and don't rescue
            # its optional_value subtree. optional_value is an Optional kwarg, so
            # resolve_arguments resolves a pruned dep to None WITHOUT running its
            # extractor (udf/base.py return_none_for_failed_values=True), and execute()
            # returns the default on None. Rescuing it (the old behavior) only made an
            # absent-group extractor run and raise ExpectedUdfException before
            # defaulting anyway — same result, wasted work, spurious expected error.
            # (Without a default it falls to rule (c): pruned -> None, also neutral.)
            if _is_resolve_optional_chain(chain) and _resolve_optional_has_default(chain):
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

    # Step 3b — rescue the enforcement output path. Everything a WhenRules transitively
    # depends on is verdict/effect-determining: the rules_any List, its Rules, each Rule's
    # when_all conditions and their extractors, and the then effects. Keep that entire
    # closure so the specialized graph computes verdicts/effects exactly as the full graph
    # — a required=False extractor over an absent group resolves to None and flows through
    # its comparison normally (`None != x` is True; the full graph fires, so the specialized
    # graph must too, instead of conservatively dropping the Rule). Only analytics-only nodes
    # (feeding no effect) stay pruned, so pruning can never change an emitted verdict/effect.
    protected: Set[NodeKey] = set()
    for chain in all_chains:
        if _is_whenrules_chain(chain):
            _collect_chain_keys(chain, protected)
    pruned -= protected

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


# RuleT carries a `features={...}` dict used only to render the human-readable label
# description; it can embed time-variant values (e.g. _AccountAge = SnowflakeAge(now()))
# and never affects enforcement. Strip it before comparing effects so a ~ms drift
# between the full and specialized passes is not mistaken for a divergence.
_EFFECT_DESC_META = re.compile(r"features=\{[^}]*\}")
# The enforcement-determining decision outputs (engine-injected, `__`-prefixed).
_DECISION_KEYS = ("__verdicts", "__classifications", "__entity_label_mutations")


def shadow_divergences(full_result: object, spec_result: object) -> List[str]:
    """ENFORCEMENT-divergence reasons between a full and specialized ExecutionResult
    (empty == equivalent).

    The bar is *enforcement equivalence* — emitted effects plus the decision keys — NOT
    feature-key identity. A pruned absent-group feature legitimately disappears, and an
    absent-group `: bool` feature legitimately becomes None instead of a fabricated False,
    so a changed/absent non-decision feature value is EXPECTED, not a divergence (matching
    the in-process equivalence harness and the RFC's "Pruning Semantics & Validation").
    Two things are still real bugs: a spec-only feature (pruning must only REMOVE, never
    ADD) and any change to the effects or decision outputs. Time-variant effect description
    metadata is normalized out (see `_EFFECT_DESC_META`).
    """
    issues: List[str] = []
    ff = getattr(full_result, "extracted_features", {}) or {}
    sf = getattr(spec_result, "extracted_features", {}) or {}
    # Pruning must only ever REMOVE features, never add them.
    extra = sorted(k for k in (set(sf) - set(ff)) if not k.startswith("__"))
    if extra:
        issues.append(f"spec-only features: {extra[:10]}")
    # Enforcement decision outputs must be identical.
    for k in _DECISION_KEYS:
        if ff.get(k) != sf.get(k):
            issues.append(f"decision changed: {k} ({ff.get(k)!r} != {sf.get(k)!r})")

    def _effects(result: object) -> List[str]:
        out: List[str] = []
        for effect_type, seq in (getattr(result, "effects", {}) or {}).items():
            for effect in seq:
                rendered = f"{getattr(effect_type, '__name__', effect_type)}:{effect!r}"
                out.append(_EFFECT_DESC_META.sub("features=<desc>", rendered))
        return sorted(out)

    fe, se = _effects(full_result), _effects(spec_result)
    if fe != se:
        issues.append(f"effects differ: full={fe[:6]} spec={se[:6]}")
    return issues


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
