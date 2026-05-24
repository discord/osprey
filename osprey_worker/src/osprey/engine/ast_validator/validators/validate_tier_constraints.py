"""Validator: enforces tier constraints on WhenRules blocks.

Two rules:
1. SLOW UDFs (latency_tier='slow') forbidden in tier=sync/both/legacy WhenRules.
2. State-mutating effects (mutates_state=True) forbidden in tier=both WhenRules.

State-mutating effects in tier=both would emit on both sync and async paths,
causing duplicate writes to downstream stores.

The validator walks each WhenRules call's transitive UDF dependency graph (for
constraint 1) and inspects the effects list (for constraint 2).
"""
from typing import Callable, Dict, List, Set, Tuple

from osprey.engine.ast.ast_utils import filter_nodes, iter_field_values
from osprey.engine.ast.grammar import ASTNode, Assign, Call, List as ASTList, Name, Source, String, Store
from osprey.engine.stdlib.udfs.tier_constants import SLOW_FORBIDDEN

from ..base_validator import SourceValidator

# Name of the WhenRules UDF — compare by name to avoid a circular import.
_WHEN_RULES_NAME = "WhenRules"


class ValidateTierConstraints(SourceValidator):
    """Enforces tier constraints on WhenRules blocks.

    Constraint 1: SLOW UDFs (latency_tier='slow') are forbidden in WhenRules
    with tier='sync', tier='both', or tier='legacy'. These tiers run on (or
    alongside) the sync latency budget; slow UDFs blow it.

    Constraint 2: State-mutating effects (mutates_state=True on the UDF class)
    are forbidden in WhenRules with tier='both'. In tier='both', the block fires
    on both the sync and async execution passes, causing duplicate writes.
    """

    def validate_source(self, source: Source) -> None:
        # Build a name→Assign index for this source so we can follow Name references.
        name_to_assign = self._build_name_index(source)

        for call_node in filter_nodes(source.ast_root, Call):
            if not isinstance(call_node.func, Name):
                continue
            if call_node.func.identifier != _WHEN_RULES_NAME:
                continue
            self._check_when_rules(call_node, name_to_assign)

    # ------------------------------------------------------------------
    # Per-WhenRules checks
    # ------------------------------------------------------------------

    def _check_when_rules(self, call: Call, name_to_assign: Dict[str, Assign]) -> None:
        tier = self._get_tier(call)

        # Constraint 1: SLOW UDFs forbidden in sync/both/legacy.
        # Walk BOTH `rules_any` and `then` — a SLOW UDF can appear as a direct
        # effect or buried inside a Rule's expression graph.
        if tier in SLOW_FORBIDDEN:
            slow_hits: List[Tuple[str, object]] = []
            seen: Set[int] = set()
            for kwarg_name in ("rules_any", "then"):
                kw = call.find_argument(kwarg_name)
                if kw is not None:
                    self._collect_matching_udfs(
                        kw.value, name_to_assign, slow_hits, seen,
                        predicate=lambda cls: getattr(cls, "latency_tier", "fast") == "slow",
                        recurse_into_call_args=True,
                    )
            for udf_name, span in slow_hits:
                self.context.add_error(
                    message=f"tier=`{tier}` WhenRules references SLOW UDF `{udf_name}`",
                    span=span,  # type: ignore[arg-type]
                    hint=(
                        f"`{udf_name}` is declared latency_tier=`slow`, but tier=`{tier}` "
                        "WhenRules run on the sync-latency-budget code path\n"
                        "either:\n"
                        "  - change this WhenRules to tier=`async`, or\n"
                        "  - move the slow UDF reference behind a Require() gated on async mode"
                    ),
                )

        # Constraint 2: state-mutating effects forbidden in tier=both.
        # Follow Name references the same way `_collect_slow_udfs` does, so
        # `then=[HelperEffect]` where HelperEffect = LabelAdd(...) still trips
        # the check.
        if tier == "both":
            then_kw = call.find_argument("then")
            if then_kw is not None:
                mutating_hits: List[Tuple[str, object]] = []
                seen_mut: Set[int] = set()
                self._collect_matching_udfs(
                    then_kw.value, name_to_assign, mutating_hits, seen_mut,
                    predicate=lambda cls: getattr(cls, "mutates_state", False),
                    recurse_into_call_args=False,
                )
                for udf_name, span in mutating_hits:
                    self.context.add_error(
                        message=f"tier=`both` WhenRules emits state-mutating effect `{udf_name}`",
                        span=span,  # type: ignore[arg-type]
                        hint=(
                            f"`{udf_name}` is declared mutates_state=True; "
                            "in tier=`both` WhenRules it would emit on both sync and async paths, "
                            "causing duplicate writes\n"
                            "pick a single tier:\n"
                            "  - tier=`sync` if the effect should fire in-line with the API request, or\n"
                            "  - tier=`async` if the effect should fire on the async post-processing pass"
                        ),
                    )

    # ------------------------------------------------------------------
    # Constraint walkers
    # ------------------------------------------------------------------

    def _collect_matching_udfs(
        self,
        node: object,
        name_to_assign: Dict[str, Assign],
        out: List[Tuple[str, object]],
        seen: Set[int],
        predicate: Callable[[type], bool],
        recurse_into_call_args: bool,
    ) -> None:
        """Walk the AST recursively, collecting (udf_name, span) for every Call
        whose UDF class matches `predicate`. Follows Name → Assign references
        one level deep so `Score = SlowUDF()` then `Rule(when_all=[Score > 0.5])`
        catches `SlowUDF`.

        `recurse_into_call_args` controls whether sub-arguments of a Call are
        walked. For SLOW UDF detection we recurse (a slow UDF buried in a Rule's
        expression graph is still scheduled). For state-mutating effects we do
        NOT recurse — only top-level entries of `then=[...]` actually emit
        effects, so a mutating UDF buried as a sub-argument is computed but
        never added to the execution context."""
        node_id = id(node)
        if node_id in seen:
            return
        seen.add(node_id)

        if isinstance(node, Call):
            if isinstance(node.func, Name):
                udf_cls = self.context.udf_registry.get(node.func.identifier)
                if udf_cls is not None and predicate(udf_cls):
                    out.append((node.func.identifier, node.span))
                if recurse_into_call_args:
                    for kw in node.arguments:
                        self._collect_matching_udfs(
                            kw.value, name_to_assign, out, seen, predicate, recurse_into_call_args
                        )
        elif isinstance(node, Name):
            if isinstance(node.context, Store):
                return
            assign = name_to_assign.get(node.identifier)
            if assign is not None:
                self._collect_matching_udfs(
                    assign.value, name_to_assign, out, seen, predicate, recurse_into_call_args
                )
        elif isinstance(node, ASTList):
            for item in node.items:
                self._collect_matching_udfs(
                    item, name_to_assign, out, seen, predicate, recurse_into_call_args
                )
        elif recurse_into_call_args and isinstance(node, ASTNode):
            # Walk arbitrary AST children (e.g. BinaryOp's left/right) so SLOW
            # UDFs nested in rule expressions are caught.
            for _field, value in iter_field_values(node):
                if isinstance(value, ASTNode):
                    self._collect_matching_udfs(
                        value, name_to_assign, out, seen, predicate, recurse_into_call_args
                    )
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, ASTNode):
                            self._collect_matching_udfs(
                                item, name_to_assign, out, seen, predicate, recurse_into_call_args
                            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_tier(self, call: Call) -> str:
        """Return the tier= kwarg value for this WhenRules call, defaulting to 'legacy'."""
        kw = call.find_argument("tier")
        if kw is not None and isinstance(kw.value, String):
            return kw.value.value
        return "legacy"

    def _build_name_index(self, source: Source) -> Dict[str, Assign]:
        """Build a mapping of identifier → Assign node for all top-level assignments
        in this source, so that Name references can be resolved one level deep.

        Cross-source references (Name defined in a Require()d file) are NOT
        followed — the SLOW UDF check is source-local. A SLOW UDF buried behind
        a cross-source name will not be caught by this validator; rely on the
        file-level Require(require_if=ExecutionMode()=='async') gating pattern
        for those cases."""
        index: Dict[str, Assign] = {}
        for assign in filter_nodes(source.ast_root, Assign):
            if isinstance(assign.target, Name):
                index[assign.target.identifier] = assign
        return index
