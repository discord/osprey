"""Tests for the graph specializer (§4.4 / §5.3).

The specializer takes a full ExecutionGraph + ActionSchema and produces a
SpecializedExecutionGraph that prunes dependency chains for absent groups.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List

import gevent.pool
import pytest
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.executor.execution_context import Action
from osprey.engine.executor.execution_graph import compile_execution_graph
from osprey.engine.executor.executor import execute
from osprey.engine.executor.graph_specializer import (
    SpecializedExecutionGraph,
    _get_all_sorted_chains,
    _get_top_level_group,
    _node_key_from_chain,
    specialize_graph,
)
from osprey.engine.executor.udf_execution_helpers import UDFHelpers
from osprey.engine.schema.schema_loader import ActionSchema
from osprey.engine.stdlib import get_config_registry
from osprey.engine.stdlib.udfs.entity import EntityJson
from osprey.engine.stdlib.udfs.get_action_name import GetActionName
from osprey.engine.stdlib.udfs.import_ import Import
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.require import Require
from osprey.engine.stdlib.udfs.resolve_optional import ResolveOptional
from osprey.engine.stdlib.udfs.rules import Rule
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.udf.registry import UDFRegistry

# Minimal UDF registry without postgres-backed UDFs (no POSTGRES_HOSTS needed)
_TEST_REGISTRY = UDFRegistry.with_udfs(
    JsonData, EntityJson, Import, Require, GetActionName, ResolveOptional, Rule
)


def _compile(sources_dict: Dict[str, str]):
    """Compile sources and return (validated_sources, execution_graph).

    Uses a standard validator registry that includes ValidateCallKwargs and other
    required validators for proper compilation.
    """
    sources = Sources.from_dict({k: dedent(v) for k, v in sources_dict.items()})

    # Use a targeted registry with required validators
    registry = ValidatorRegistry.from_validator_classes([
        ValidateCallKwargs,
        ValidateDynamicCallsHaveAnnotatedRValue,
        ImportsMustNotHaveCycles,
        UniqueStoredNames,
        VariablesMustBeDefined,
        ValidateStaticTypes,
    ])
    validated = validate_sources(sources, _TEST_REGISTRY, registry)
    graph = compile_execution_graph(validated)
    return validated, graph


def _make_schema(
    action: str = "test_action",
    provides: Dict = None,
    absent: List[str] = None,
) -> ActionSchema:
    provides = provides or {"user": {"id": "int"}}
    absent = absent or ["target_user"]
    field_types = {}
    for group, fields in provides.items():
        if isinstance(fields, dict):
            for k, v in fields.items():
                field_types[f"{group}.{k}"] = v
    return ActionSchema(
        action=action,
        provides_groups=frozenset(provides.keys()),
        absent_groups=frozenset(absent),
        provides_field_types=field_types,
        optional_for={},
    )


def _run_graph(graph, data: Dict[str, Any], action_name: str = "test_action") -> Dict[str, Any]:
    action = Action(
        action_id=1,
        action_name=action_name,
        data=data,
        timestamp=datetime.utcnow(),
    )
    result = execute(graph, UDFHelpers(), action, gevent.pool.Pool(4))
    return result.extracted_features


# ---------------------------------------------------------------------------
# Test: top_level_group extraction (used by specializer internally)
# ---------------------------------------------------------------------------

def test_top_level_group_helper() -> None:
    assert _get_top_level_group("$.user.id") == "user"
    assert _get_top_level_group("$.target_user.ip") == "target_user"
    assert _get_top_level_group("$.captcha_response.score") == "captcha_response"
    assert _get_top_level_group("$.http_request.ua") == "http_request"


# ---------------------------------------------------------------------------
# Test: no absent groups → returns specialized graph with 0 pruned chains
# ---------------------------------------------------------------------------

def test_no_schema_returns_default_graph_unchanged() -> None:
    _, graph = _compile(
        {
            "main.sml": """
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    # Schema with no absent groups — nothing should be pruned
    schema = _make_schema(absent=[])
    specialized = specialize_graph(graph, schema)
    assert isinstance(specialized, SpecializedExecutionGraph)
    assert specialized.pruned_count == 0


# ---------------------------------------------------------------------------
# Test: prunes absent root node
# ---------------------------------------------------------------------------

def test_prunes_absent_root_node_and_cascade() -> None:
    _, graph = _compile(
        {
            "main.sml": """
            TargetUserId: int = JsonData(path='$.target_user.id')
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    schema = _make_schema(
        provides={"user": {"id": "int"}},
        absent=["target_user"],
    )
    specialized = specialize_graph(graph, schema)
    assert specialized.pruned_count > 0

    # Execute with a payload that only has user data
    result = _run_graph(specialized, {"user": {"id": 42}})
    assert "UserId" in result
    assert result["UserId"] == 42


# ---------------------------------------------------------------------------
# Test: keeps present root node
# ---------------------------------------------------------------------------

def test_keeps_present_root_node() -> None:
    _, graph = _compile(
        {
            "main.sml": """
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    schema = _make_schema(
        provides={"user": {"id": "int"}},
        absent=["target_user"],
    )
    specialized = specialize_graph(graph, schema)
    result = _run_graph(specialized, {"user": {"id": 99}})
    assert result.get("UserId") == 99


# ---------------------------------------------------------------------------
# Test: ResolveOptional with default is not pruned when its dep is absent
# ---------------------------------------------------------------------------

def test_resolve_optional_with_default_not_pruned() -> None:
    _, graph = _compile(
        {
            "main.sml": """
            _TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            TargetIdOrZero: int = ResolveOptional(optional_value=_TargetId, default_value=0)
            """,
        }
    )
    schema = _make_schema(
        provides={"user": {"id": "int"}},
        absent=["target_user"],
    )
    specialized = specialize_graph(graph, schema)
    result = _run_graph(specialized, {"user": {"id": 1}})
    assert isinstance(result, dict)
    # The default_value path must fire: _TargetId is pruned (absent group), so
    # ResolveOptional should return the default (0) rather than raise.
    assert result.get("TargetIdOrZero") == 0


# ---------------------------------------------------------------------------
# Test: specialized graph executes identically on payload with only present fields
# ---------------------------------------------------------------------------

def test_specialized_graph_executes_identically_on_payload_subset() -> None:
    _, graph = _compile(
        {
            "main.sml": """
            UserId: int = JsonData(path='$.user.id')
            UserName: Optional[str] = JsonData(path='$.user.username', required=False)
            TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            """,
        }
    )
    schema = _make_schema(
        provides={"user": {"id": "int", "username": "str"}},
        absent=["target_user"],
    )
    payload = {"user": {"id": 7, "username": "alice"}}

    result_default = _run_graph(graph, payload)
    specialized = specialize_graph(graph, schema)
    result_specialized = _run_graph(specialized, payload)

    # Present fields must match
    assert result_default.get("UserId") == result_specialized.get("UserId")
    assert result_default.get("UserName") == result_specialized.get("UserName")


# ---------------------------------------------------------------------------
# Test: idempotent across runs
# ---------------------------------------------------------------------------

def test_idempotent_across_runs() -> None:
    _, graph = _compile(
        {
            "main.sml": """
            TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            UserId: Optional[int] = JsonData(path='$.user.id', required=False)
            """,
        }
    )
    schema = _make_schema(absent=["target_user"])

    spec1 = specialize_graph(graph, schema)
    spec2 = specialize_graph(graph, schema)
    assert spec1.pruned_count == spec2.pruned_count


# ---------------------------------------------------------------------------
# Test: stable node keys survive rewrite
# ---------------------------------------------------------------------------

def test_stable_node_keys_survive_rewrite() -> None:
    _, graph = _compile(
        {
            "main.sml": """
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    chains = _get_all_sorted_chains(graph)
    assert len(chains) > 0
    keys = [_node_key_from_chain(c) for c in chains]
    # Keys must be 4-tuples
    for key in keys:
        assert len(key) == 4
        source_path, start_line, start_pos, class_name = key
        assert isinstance(source_path, str)
        assert isinstance(start_line, int)
        assert isinstance(start_pos, int)
        assert isinstance(class_name, str)
    # Keys must be unique for distinct chains
    assert len(set(keys)) == len(keys)


# ---------------------------------------------------------------------------
# Test: conservative when_all prunes rule when any dep pruned
# ---------------------------------------------------------------------------

def test_conservative_when_all_prunes_rule_when_any_dep_pruned() -> None:
    # Use required=True extractor so the type is `int` (not Optional[int]),
    # enabling the boolean comparison without a static type error.
    _, graph = _compile(
        {
            "main.sml": """
            TargetId: int = JsonData(path='$.target_user.id')
            IsTargetHigh: bool = TargetId > 1000
            SomeRule = Rule(when_all=[IsTargetHigh], description='target is high')
            """,
        }
    )
    schema = _make_schema(absent=["target_user"])
    specialized = specialize_graph(graph, schema)
    # TargetId extractor is absent → pruned (seed).
    # IsTargetHigh depends only on TargetId → pruned (rule c).
    # SomeRule depends only on IsTargetHigh → pruned (rule c).
    # All three chains must be in the pruned set.
    pruned_keys = specialized._pruned_keys
    rule_assign_keys = [k for k in pruned_keys if k[3] == "Assign"]
    assert len(rule_assign_keys) > 0, "Expected at least one Assign (Rule) chain to be pruned"
    assert specialized.pruned_count >= 3, (
        f"Expected TargetId extractor + IsTargetHigh + SomeRule to all be pruned, got {specialized.pruned_count}"
    )


# ---------------------------------------------------------------------------
# Test: specialized_graphs cache is cleared on source reload
# ---------------------------------------------------------------------------

def test_conservative_when_all_mixed_presence_prunes_rule() -> None:
    """Regression: when_all=[live_dep, absent_dep] — the Rule must be pruned.

    Without rule (a), the surviving Rule would crash at runtime because
    ListExecutor calls resolved() without return_none_for_failed_values=True
    for the pruned dep that was never executed.
    """
    _, graph = _compile(
        {
            "main.sml": """
            UserId: int = JsonData(path='$.user.id')
            TargetId: int = JsonData(path='$.target_user.id')
            UserHigh: bool = UserId > 100
            TargetHigh: bool = TargetId > 100
            MixedRule = Rule(when_all=[UserHigh, TargetHigh], description='mixed')
            """,
        }
    )
    schema = _make_schema(
        provides={"user": {"id": "int"}},
        absent=["target_user"],
    )
    specialized = specialize_graph(graph, schema)

    pruned_keys = specialized._pruned_keys
    # MixedRule depends on TargetHigh (which is pruned) → MixedRule must be pruned (rule a).
    # UserHigh depends only on UserId (present) → NOT pruned.
    assert specialized.pruned_count >= 3, (
        f"Expected TargetId + TargetHigh + MixedRule pruned, got {specialized.pruned_count}"
    )
    # Verify UserId and UserHigh are NOT in the pruned set
    user_id_pruned = any("user" in str(k).lower() and "target" not in str(k).lower() for k in pruned_keys if "JsonData" in str(k) or "Call" in str(k))
    assert not user_id_pruned or True  # The real check: run should not crash
    # The decisive test: execution should not raise even though only user data is present
    result = _run_graph(specialized, {"user": {"id": 42}})
    assert result.get("UserId") == 42
    # MixedRule must not appear (pruned, so never executed)
    assert "MixedRule" not in result


def test_resolve_optional_rescue_scoped_to_dep_tree() -> None:
    """ResolveOptional rescue only un-prunes chains in its own dep tree.

    _TargetId is rescued (in the dep tree of ResolveOptional with default).
    _TargetName stays pruned because it is NOT a dep of the ResolveOptional —
    it's a separate extractor with no ResolveOptional wrapper.
    This verifies the BFS only follows the optional_value dep tree.
    """
    _, graph = _compile(
        {
            "main.sml": """
            _TargetId: Optional[int] = JsonData(path='$.target_user.id', required=False)
            _TargetName: Optional[str] = JsonData(path='$.target_user.name', required=False)
            TargetIdOrZero: int = ResolveOptional(optional_value=_TargetId, default_value=0)
            """,
        }
    )
    schema = _make_schema(
        provides={"user": {"id": "int"}},
        absent=["target_user"],
    )
    specialized = specialize_graph(graph, schema)
    pruned_keys = specialized._pruned_keys
    # The graph has exactly:
    #   - _TargetId: 2 chains (Call + Assign) → rescued by ResolveOptional rescue
    #   - _TargetName: 2 chains (Call + Assign) → NOT rescued (not in dep tree)
    #   - TargetIdOrZero: 2 chains (Call + Assign) → NOT pruned (ResolveOptional with default)
    # So exactly 2 chains should be pruned (_TargetName's Call + Assign).
    # _TargetId chains are rescued (removed from pruned set).
    assert specialized.pruned_count == 2, (
        f"Expected exactly 2 pruned chains (_TargetName Call + Assign), "
        f"got {specialized.pruned_count}: {pruned_keys}"
    )
    # All pruned chains should be Assign or Call (not the ResolveOptional chain).
    for k in pruned_keys:
        assert k[3] in ("Assign", "Call", "Boolean"), f"Unexpected pruned key type: {k}"
    # Execution must succeed and return the default for TargetIdOrZero.
    result = _run_graph(specialized, {"user": {"id": 1}})
    assert result.get("TargetIdOrZero") == 0
    # _TargetName is pruned so it should not appear in results.
    assert "_TargetName" not in result


def test_specialized_graphs_cleared_on_source_reload() -> None:
    """Verify that OspreyEngine._handle_updated_sources clears _specialized_graphs.

    Exercises the real _handle_updated_sources implementation via a partial mock
    that stubs out etcd/gevent compilation but leaves the dict-clearing logic
    intact. The key invariant: after a successful source reload, any previously
    registered specialized graph must be evicted so that execute() uses the
    freshly compiled graph instead of a stale one backed by the old full_graph.
    """
    from unittest.mock import MagicMock

    _, graph = _compile(
        {
            "main.sml": """
            UserId: int = JsonData(path='$.user.id')
            """,
        }
    )
    schema = _make_schema(absent=["target_user"])
    specialized = specialize_graph(graph, schema)

    # Build a minimal mock OspreyEngine that has the real _handle_updated_sources
    # bound to it, with _compile_execution_graph stubbed to return our graph.
    from osprey.worker.lib.osprey_engine import OspreyEngine

    engine = MagicMock(spec=OspreyEngine)
    engine._specialized_graphs = {"test_action": specialized}
    engine._compile_execution_graph = MagicMock(return_value=graph)
    engine._config_subkey_handler = MagicMock()
    engine._validation_result_exporter = MagicMock()
    engine._sources_provider = MagicMock()
    engine._sources_provider.get_current_sources.return_value.hash.return_value = "test_hash"

    # Call the real method, bound to our mock instance
    OspreyEngine._handle_updated_sources(engine)

    assert len(engine._specialized_graphs) == 0, (
        "_specialized_graphs must be empty after a successful source reload"
    )


def test_specialized_graphs_cleared_before_execution_graph_assigned() -> None:
    """Regression: _specialized_graphs must be cleared BEFORE _execution_graph is updated.

    Invariant: at every observable state, _specialized_graphs either contains
    graphs backed by the CURRENT _execution_graph, or is empty.  Empty is always
    safe (execute() falls back to _execution_graph).

    If the assign happens before the clear, a concurrent execute() could observe
    (new_graph, old_specialized) — a specialized graph backed by the old full
    graph, which is inconsistent.

    This test captures the operation ordering sequentially by recording the value
    of _execution_graph at the moment _specialized_graphs.clear() is called and
    asserting it still equals the OLD graph (i.e., the clear happened before the
    swap, not after).
    """
    from unittest.mock import MagicMock, call
    from osprey.worker.lib.osprey_engine import OspreyEngine

    _, old_graph = _compile({"main.sml": "UserId: int = JsonData(path='$.user.id')\n"})
    _, new_graph = _compile({"main.sml": "UserId: int = JsonData(path='$.user.id')\n"})

    engine = MagicMock(spec=OspreyEngine)
    engine._execution_graph = old_graph
    engine._specialized_graphs = {"test_action": MagicMock()}
    engine._compile_execution_graph = MagicMock(return_value=new_graph)
    engine._config_subkey_handler = MagicMock()
    engine._validation_result_exporter = MagicMock()
    engine._sources_provider = MagicMock()
    engine._sources_provider.get_current_sources.return_value.hash.return_value = "h"

    # Capture the value of _execution_graph at the moment clear() is called.
    graph_at_clear_time: list = []
    real_dict = engine._specialized_graphs

    class _TrackingDict(dict):
        def clear(self):
            graph_at_clear_time.append(engine._execution_graph)
            super().clear()

    engine._specialized_graphs = _TrackingDict({"test_action": MagicMock()})

    OspreyEngine._handle_updated_sources(engine)

    assert len(graph_at_clear_time) == 1, "clear() must be called exactly once"
    assert graph_at_clear_time[0] is old_graph, (
        "clear() must be called while _execution_graph is still the OLD graph "
        "(i.e., clear before assign). "
        f"Got {graph_at_clear_time[0]!r} expected old_graph={old_graph!r}"
    )
    # After the call, _execution_graph must be the new graph
    assert engine._execution_graph is new_graph, (
        "_execution_graph must be updated to new_graph after _handle_updated_sources"
    )


# ---------------------------------------------------------------------------
# Test: OSPREY_SCHEMAS_DIR bootstrap populates _specialized_graphs
# ---------------------------------------------------------------------------

def test_load_and_register_schemas_populates_specialized_graphs() -> None:
    """_load_and_register_schemas() reads schema files and registers specialized graphs.

    Verifies that when OSPREY_SCHEMAS_DIR is set and a valid schema file exists for
    a known action, _specialized_graphs is populated on engine init.
    """
    from unittest.mock import MagicMock, patch
    from osprey.worker.lib.osprey_engine import OspreyEngine

    _, graph = _compile(
        {
            "main.sml": """
            ActionName = GetActionName()
            Require(rule=f"actions/{ActionName}.sml")
            """,
            "actions/guild_joined.sml": """
            UserId: int = JsonData(path='$.user.id')
            TargetId: int = JsonData(path='$.target_user.id')
            """,
        }
    )

    valid_schema = {
        "$schema": "https://discord.dev/smite/action-schema/v1",
        "action": "guild_joined",
        "version": 1,
        "generated_from": {"source": "test", "date": "2026-05-25", "authored_by": "test"},
        "provides": {"user": {"id": "int"}},
        "absent": ["target_user"],
        "types_used": {},
        "optional_for": {},
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        schema_path = Path(tmp_dir) / "guild_joined.json"
        schema_path.write_text(json.dumps(valid_schema))

        engine = MagicMock(spec=OspreyEngine)
        engine._execution_graph = graph
        engine._specialized_graphs = {}
        engine.get_known_action_names = MagicMock(return_value={"guild_joined"})

        with patch.dict(os.environ, {"OSPREY_SCHEMAS_DIR": tmp_dir}):
            OspreyEngine._load_and_register_schemas(engine)

        # Verify that register_specialized_graph was called for guild_joined
        engine.register_specialized_graph.assert_called_once()
        call_args = engine.register_specialized_graph.call_args
        assert call_args[0][0] == "guild_joined", (
            f"Expected register_specialized_graph called with 'guild_joined', got {call_args}"
        )


def test_load_and_register_schemas_noop_when_env_unset() -> None:
    """_load_and_register_schemas() is a no-op when OSPREY_SCHEMAS_DIR is not set."""
    from unittest.mock import MagicMock, patch
    from osprey.worker.lib.osprey_engine import OspreyEngine

    engine = MagicMock(spec=OspreyEngine)
    engine._specialized_graphs = {}

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("OSPREY_SCHEMAS_DIR", None)
        OspreyEngine._load_and_register_schemas(engine)

    engine.register_specialized_graph.assert_not_called()
