"""Tests for CountOver lowering in DruidQueryTransformer."""
from typing import Any, Callable, List

import pytest
from osprey.engine.ast_validator.validators.imports_must_not_have_cycles import ImportsMustNotHaveCycles
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_dynamic_calls_have_annotated_rvalue import (
    ValidateDynamicCallsHaveAnnotatedRValue,
)
from osprey.engine.ast_validator.validators.validate_static_types import ValidateStaticTypes
from osprey.engine.ast_validator.validators.variables_must_be_defined import VariablesMustBeDefined
from osprey.engine.conftest import CheckJsonOutputFunction
from osprey.engine.query_language import parse_query_to_validated_ast
from osprey.engine.query_language.ast_druid_translator import DruidQueryTransformer
from osprey.engine.query_language.tests.conftest import MakeRulesSourcesFunction
from osprey.engine.query_language.udfs.count_over import CountOver
from osprey.engine.udf.registry import UDFRegistry

# Validators and UDF registry setup for CountOver translator tests
pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_standard_rules_validators(),
    pytest.mark.use_validators(
        [
            UniqueStoredNames,
            ValidateStaticTypes,
            ValidateCallKwargs,
            ImportsMustNotHaveCycles,
            ValidateDynamicCallsHaveAnnotatedRValue,
            VariablesMustBeDefined,
        ]
    ),
    pytest.mark.use_udf_registry(UDFRegistry.with_udfs(CountOver)),
]


# Snapshot tests for all 13 cases (6 ops × 2 keying variants + AND filter case)


def test_count_over_gte_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_gt_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) > 10 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) > 10",
        make_rules_sources([('Endpoint', "'/foo'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_eq_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1' and Endpoint == '/foo', window='10m', key=UserId) == 10 (with key, AND predicate)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1' and Endpoint == '/foo', window='10m', key=UserId) == 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('Endpoint', "'/foo'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_neq_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) != 5 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) != 5",
        make_rules_sources([('Endpoint', "'/foo'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_lte_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) <= 10 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) <= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_lt_with_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) < 10 (with key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m', key=UserId) < 10",
        make_rules_sources([('Endpoint', "'/foo'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_gte_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 10 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_gt_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m') > 10 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m') > 10",
        make_rules_sources([('Endpoint', "'/foo'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_eq_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1' or Endpoint == '/foo', window='10m') == 10 (no key, OR predicate)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1' or Endpoint == '/foo', window='10m') == 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('Endpoint', "'/foo'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_neq_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m') != 5 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m') != 5",
        make_rules_sources([('Endpoint', "'/foo'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_lte_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') <= 10 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m') <= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_lt_no_key(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(predicate=Endpoint == '/foo', window='10m') < 10 (no key)"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=Endpoint == '/foo', window='10m') < 10",
        make_rules_sources([('Endpoint', "'/foo'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_with_and_filter(
    make_rules_sources: MakeRulesSourcesFunction, check_json_output: CheckJsonOutputFunction
) -> None:
    """CountOver(...) >= 10 and Country != 'US' - verifies AND-conjunct folding"""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10 and Country != 'US'",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'"), ('Country', "'someCountry'")]),
    )
    transformed_query = DruidQueryTransformer(validated_sources=validated_sources).transform()
    assert check_json_output(transformed_query)
    _assert_valid_count_over_sql(transformed_query)


def test_count_over_custom_datasource_name(
    make_rules_sources: MakeRulesSourcesFunction,
) -> None:
    """Callers can pass a real Druid datasource name (e.g. `smite.events`)
    and get executable SQL with that name interpolated and quoted."""
    validated_sources = parse_query_to_validated_ast(
        "CountOver(predicate=UserLoginIp == '1.1.1.1', window='10m', key=UserId) >= 10",
        make_rules_sources([('UserLoginIp', "'1.1.1.1'"), ('UserId', "'123'")]),
    )
    transformed_query = DruidQueryTransformer(
        validated_sources=validated_sources, datasource_name='smite.events'
    ).transform()
    sql = transformed_query['sql']
    assert 'FROM "smite.events"' in sql
    # Placeholder must not leak through when a real name was provided.
    assert 'FROM "datasource"' not in sql
    # Inner-subquery alias should still be present regardless of datasource name.
    assert ') AS __inner WHERE' in sql


def _assert_valid_count_over_sql(transformed_query: Any) -> None:
    """Assert the transformed query contains valid CountOver SQL.

    Checks:
    - Type is 'sql'
    - SQL string is present
    - No doubled FROM clauses (check by counting FROM after first SELECT *, before first WHERE in parentheses)
    - No literal {operand_str} f-string placeholders
    - Balanced parentheses
    """
    assert isinstance(transformed_query, dict), f"Expected dict, got {type(transformed_query)}"
    assert transformed_query.get('type') == 'sql', f"Expected type='sql', got {transformed_query.get('type')}"

    sql = transformed_query.get('sql', '')
    assert isinstance(sql, str) and sql, "Expected non-empty SQL string"

    # Check for doubled FROM clause bug (Critical 1)
    # The pattern should be: SELECT * FROM (SELECT *, LAG(...) FROM datasource WHERE ...) WHERE ...
    # Not: SELECT * FROM (SELECT *, LAG(...) FROM __default FROM datasource WHERE ...) WHERE ...
    # So we look for the doubling pattern specifically
    assert ' FROM __default FROM ' not in sql.upper(), f"Found doubled FROM clause (FROM __default FROM). SQL: {sql}"
    # The translator emits the datasource name double-quoted (`FROM "<name>"`)
    # so that names containing `.` parse under Calcite. The default placeholder
    # `"datasource"` shows up here; callers passing a real name get e.g.
    # `FROM "smite.events"`.
    assert 'FROM "datasource"' in sql, f"Expected 'FROM \"datasource\"' in SQL. SQL: {sql}"

    # Check for f-string bug (Critical 2) - literal {operand_str}
    assert '{operand_str}' not in sql, f"Found literal {{operand_str}} in SQL (f-string bug). SQL: {sql}"
    assert '{' not in sql or '}' not in sql, f"Found unresolved placeholder in SQL. SQL: {sql}"

    # Check parentheses are balanced
    paren_count = 0
    for char in sql:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        assert paren_count >= 0, f"Unbalanced parentheses in SQL (more closing than opening). SQL: {sql}"
    assert paren_count == 0, f"Unbalanced parentheses in SQL (unclosed). SQL: {sql}"



