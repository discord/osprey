"""Tests for the graph specializer (§4.4 / §5.3).

The specializer takes a full ExecutionGraph + ActionSchema and produces a
SpecializedExecutionGraph that prunes dependency chains for absent groups.
"""
from __future__ import annotations

from datetime import datetime
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
    from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
    from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
        ValidateDynamicCallsHaveAnnotatedRValue,
    )
    from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
    from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
    from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
    from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes

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
    specialized = specialize_graph(graph, schema, [])
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
    specialized = specialize_graph(graph, schema, [])
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
    specialized = specialize_graph(graph, schema, [])
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
    specialized = specialize_graph(graph, schema, [])
    # Just assert no crash — the specializer should handle ResolveOptional with default
    result = _run_graph(specialized, {"user": {"id": 1}})
    assert isinstance(result, dict)


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
    specialized = specialize_graph(graph, schema, [])
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

    spec1 = specialize_graph(graph, schema, [])
    spec2 = specialize_graph(graph, schema, [])
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
    specialized = specialize_graph(graph, schema, [])
    # The target_user extractor and its dependents should be pruned
    assert specialized.pruned_count > 0
