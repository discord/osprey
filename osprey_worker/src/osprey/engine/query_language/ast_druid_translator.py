import re
from typing import Any, Dict, List, Optional, Tuple

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidatedSources
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.udf.base import QueryUdfBase
from osprey.engine.utils.osprey_unary_executor import OspreyUnaryExecutor
from osprey.engine.query_language.udfs.count_over import operator_metadata_for, CountOver


class DruidQueryTransformException(Exception):
    """Some error happened while trying to transform the Osprey AST into a Druid Query"""

    def __init__(self, node: grammar.ASTNode, error: str):
        super().__init__(f'{error}: {node.__class__.__name__}')
        self.node = node


class DruidQueryTransformer:
    """Given a osprey_ast node tree, transform it into a Druid query.

    For CountOver lowering, `datasource_name` is interpolated into the emitted
    SQL's `FROM` clause. The default `'datasource'` preserves the historical
    behavior where callers substitute the placeholder themselves; pass a real
    Druid datasource name (e.g. `'smite.events'`) to get executable SQL.
    The name is quoted in the SQL output to support names containing `.`.
    """

    def __init__(self, validated_sources: ValidatedSources, datasource_name: str = 'datasource'):
        try:
            self._udf_node_mapping = validated_sources.get_validator_result(ValidateCallKwargs)
        except KeyError:
            self._udf_node_mapping = {}

        assign_node = validated_sources.sources.get_entry_point().ast_root.statements[0]
        assert isinstance(assign_node, grammar.Assign)
        self._root = assign_node.value
        self._datasource_name = datasource_name

    def transform(self) -> Dict[str, Any]:
        """Transform AST to Druid query.

        Returns a tagged shape: {'type': 'sql', 'sql': '...'} for CountOver queries,
        or {'type': 'native', 'filter': {...}} for native queries.
        """
        # Pre-pass: detect top-level CountOver pattern
        count_over_info = self._detect_count_over(self._root)
        if count_over_info:
            predicate, window_seconds, key, comparator_type, threshold, other_conjuncts = count_over_info
            sql = self._compose_count_over_sql(predicate, window_seconds, key, comparator_type, threshold, other_conjuncts)
            return {'type': 'sql', 'sql': sql}

        # Fall through to native query transformation
        return {'type': 'native', 'filter': self._transform(self._root)}

    def _detect_count_over(
        self, node: grammar.ASTNode
    ) -> Optional[Tuple[grammar.ASTNode, int, Optional[str], type, int, List[grammar.ASTNode]]]:
        """Detect top-level CountOver(p, w, key) <op> N pattern.

        Returns (predicate, window_seconds, key, comparator_type, threshold, other_conjuncts) or None.
        """
        # Check if it's a binary comparison at the top level
        if isinstance(node, grammar.BinaryComparison):
            return self._try_extract_count_over_from_comparison(node, [])

        # Check if it's an AND that might contain a CountOver comparison
        if isinstance(node, grammar.BooleanOperation) and isinstance(node.operand, grammar.And):
            # Find which value is the CountOver comparison, if any
            count_over_comp: Optional[Tuple[grammar.ASTNode, int, Optional[str], type, int, List[grammar.ASTNode]]] = None
            other_values: List[grammar.ASTNode] = []
            for value in node.values:
                if isinstance(value, grammar.BinaryComparison):
                    result = self._try_extract_count_over_from_comparison(value, [])
                    if result:
                        count_over_comp = result
                    else:
                        other_values.append(value)
                else:
                    other_values.append(value)
            if count_over_comp:
                # Return the full tuple with other_values
                return (count_over_comp[0], count_over_comp[1], count_over_comp[2], count_over_comp[3], count_over_comp[4], other_values)

        return None

    def _try_extract_count_over_from_comparison(
        self, comp: grammar.BinaryComparison, accumulated: List[grammar.ASTNode]
    ) -> Optional[Tuple[grammar.ASTNode, int, Optional[str], type, int, List[grammar.ASTNode]]]:
        """Try to extract CountOver pattern from a BinaryComparison.

        Returns (predicate, window_seconds, key, comparator_type, threshold, accumulated) or None.
        """
        # Check if left side is a CountOver call
        left_call = None
        if isinstance(comp.left, grammar.Call):
            left_call = comp.left

        # Check if right side is a CountOver call
        right_call = None
        if isinstance(comp.right, grammar.Call):
            right_call = comp.right

        # We expect exactly one CountOver call
        count_over_call = left_call or right_call
        if not count_over_call:
            return None

        # Verify it's actually a CountOver call
        if not (isinstance(count_over_call.func, grammar.Name) and count_over_call.func.identifier == 'CountOver'):
            return None

        # Get the UDF to verify it's the CountOver UDF
        udf, _ = self._udf_node_mapping.get(id(count_over_call), (None, None))
        if not isinstance(udf, CountOver):
            return None

        # Extract the threshold literal (the other side of the comparison)
        threshold_node = comp.right if left_call else comp.left
        threshold = self._extract_literal_int(threshold_node)
        if threshold is None:
            return None

        # Extract CountOver arguments: predicate, window, key
        predicate = self._find_argument(count_over_call, 'predicate')
        if predicate is None:
            return None

        window_arg = self._find_argument(count_over_call, 'window')
        if window_arg is None:
            return None
        window_seconds = self._extract_window_seconds(window_arg)
        if window_seconds is None:
            return None

        key = self._extract_key(count_over_call)

        comparator_type = type(comp.comparator)

        return (predicate, window_seconds, key, comparator_type, threshold, accumulated)

    def _find_argument(self, call: grammar.Call, name: str) -> Optional[grammar.ASTNode]:
        """Find a named argument in a Call node."""
        keyword = call.find_argument(name)
        return keyword.value if keyword else None

    def _extract_literal_int(self, node: grammar.ASTNode) -> Optional[int]:
        """Extract an integer literal from an AST node."""
        if isinstance(node, grammar.Number) and isinstance(node.value, int):
            return node.value
        return None

    def _extract_window_seconds(self, node: grammar.ASTNode) -> Optional[int]:
        """Extract window duration in seconds from a time-delta string literal.

        Supports formats like '10m', '1h', '30s', '7d' with units: s, m, h, d.
        Returns total seconds, or None if the format is invalid.
        """
        if not isinstance(node, grammar.String):
            return None

        window_str = node.value
        # Match pattern: digits followed by one of (s, m, h, d)
        match = re.match(r'^(\d+)([smhd])$', window_str)
        if not match:
            return None

        amount_str, unit = match.groups()
        try:
            amount = int(amount_str)
        except ValueError:
            return None

        # Convert to seconds based on unit
        multipliers = {
            's': 1,
            'm': 60,
            'h': 3600,
            'd': 86400,
        }

        return amount * multipliers[unit]

    def _extract_key(self, call: grammar.Call) -> Optional[str]:
        """Extract the key argument (partition column name) from a CountOver call."""
        key_node = self._find_argument(call, 'key')
        if key_node is None:
            return None
        if isinstance(key_node, grammar.Name):
            return key_node.identifier
        if isinstance(key_node, grammar.String):
            return key_node.value
        return None

    def _compose_count_over_sql(
        self,
        predicate: grammar.ASTNode,
        window_seconds: int,
        key: Optional[str],
        comparator_type: type,
        threshold: int,
        other_conjuncts: List[grammar.ASTNode],
    ) -> str:
        """Compose the SQL string for a CountOver query."""
        # Get operator metadata (LAG offsets and post-filter template)
        metadata = operator_metadata_for(comparator_type, threshold)

        # Translate predicate and other conjuncts to WHERE clause SQL
        predicate_sql = self._predicate_to_sql(predicate)
        other_sqls = [self._predicate_to_sql(conj) for conj in other_conjuncts]
        where_conditions = [predicate_sql] + other_sqls
        where_clause = ' AND '.join(where_conditions)

        # Build the OVER clause (with or without PARTITION BY)
        if key:
            over_clause = f"OVER (PARTITION BY {key} ORDER BY __time)"
        else:
            over_clause = "OVER (ORDER BY __time)"

        # Build LAG column selections
        lag_columns = []
        for i, offset in enumerate(metadata.lag_offsets):
            pt_name = f"pt{i + 1}"
            lag_columns.append(f"LAG(__time, {offset}) {over_clause} AS {pt_name}")

        lag_select = ', '.join(lag_columns)

        # Build the inner SELECT (with LAG columns).
        # `datasource_name` defaults to a literal `datasource` placeholder so
        # legacy callers that substitute it themselves keep working; callers
        # that pass a real name get executable SQL directly. The name is
        # double-quoted in case it contains `.` (Druid datasources commonly
        # do, e.g. `smite.events`).
        quoted_datasource = f'"{self._datasource_name}"'
        inner_select = f"SELECT *, {lag_select} FROM {quoted_datasource} WHERE {where_clause}"

        # Build the post-filter SQL
        post_filter = metadata.post_filter_template.format(window_seconds=window_seconds)

        # Combine into outer SELECT. Druid's Calcite parser requires every
        # FROM-clause subquery to be aliased — `AS __inner` satisfies that.
        sql = f"SELECT * FROM ({inner_select}) AS __inner WHERE {post_filter}"

        return sql

    def _predicate_to_sql(self, node: grammar.ASTNode) -> str:
        """Convert a predicate AST node to SQL WHERE clause fragment.

        Supports: Name == 'literal', Name != 'literal', And, Or, and parenthesization.
        Raises NotImplementedError for unsupported constructs.
        """
        if isinstance(node, grammar.BinaryComparison):
            return self._binary_comparison_to_sql(node)
        elif isinstance(node, grammar.BooleanOperation):
            if isinstance(node.operand, grammar.And):
                operand_str = ' AND '
            elif isinstance(node.operand, grammar.Or):
                operand_str = ' OR '
            else:
                raise NotImplementedError(f"Unsupported boolean operand: {type(node.operand)}")
            parts = [self._predicate_to_sql(v) for v in node.values]
            return f"({operand_str.join(parts)})"
        elif isinstance(node, grammar.UnaryOperation):
            if isinstance(node.operator, grammar.Not):
                inner = self._predicate_to_sql(node.operand)
                return f"NOT ({inner})"
            else:
                raise NotImplementedError(f"Unsupported unary operator: {type(node.operator)}")
        else:
            raise NotImplementedError(f"Unsupported predicate node type: {type(node).__name__}")

    def _binary_comparison_to_sql(self, comp: grammar.BinaryComparison) -> str:
        """Convert a BinaryComparison to SQL (e.g., 'column == value')."""
        # Extract column name and value
        left_is_col = isinstance(comp.left, grammar.Name)
        right_is_col = isinstance(comp.right, grammar.Name)

        if left_is_col and not right_is_col:
            assert isinstance(comp.left, grammar.Name)
            col_name = comp.left.identifier
            value = self._get_ast_node_value(comp.right)
        elif right_is_col and not left_is_col:
            assert isinstance(comp.right, grammar.Name)
            col_name = comp.right.identifier
            value = self._get_ast_node_value(comp.left)
        else:
            raise NotImplementedError(f"Unsupported binary comparison: both or neither sides are columns")

        # Format the value
        value_sql = self._format_sql_value(value)

        # Map comparator to SQL operator
        if isinstance(comp.comparator, grammar.Equals):
            op = '='
        elif isinstance(comp.comparator, grammar.NotEquals):
            op = '!='
        elif isinstance(comp.comparator, grammar.GreaterThan):
            op = '>'
        elif isinstance(comp.comparator, grammar.GreaterThanEquals):
            op = '>='
        elif isinstance(comp.comparator, grammar.LessThan):
            op = '<'
        elif isinstance(comp.comparator, grammar.LessThanEquals):
            op = '<='
        else:
            raise NotImplementedError(f"Unsupported comparator: {type(comp.comparator)}")

        return f"{col_name} {op} {value_sql}"

    def _get_ast_node_value(self, node: grammar.ASTNode) -> Any:
        """Extract a Python value from an AST node (mirrors the existing get_ast_node_value)."""
        if isinstance(node, grammar.UnaryOperation):
            return OspreyUnaryExecutor(node).get_execution_value()
        elif isinstance(node, grammar.List):
            return [self._get_ast_node_value(i) for i in node.items]
        elif isinstance(node, grammar.None_):
            return None
        elif isinstance(node, (grammar.String, grammar.Number, grammar.Boolean)):
            return node.value
        else:
            raise NotImplementedError(f"Node has no known value: {type(node)}")

    def _format_sql_value(self, value: Any) -> str:
        """Format a Python value for use in SQL string."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Escape single quotes by doubling them
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        else:
            raise NotImplementedError(f"Cannot format value type: {type(value)}")

    def _transform(self, node: grammar.ASTNode) -> Dict[str, Any]:
        method = 'transform_' + node.__class__.__name__
        transformer = getattr(self, method, None)

        if not transformer:
            raise DruidQueryTransformException(node, 'Unknown AST Expression')

        ret = transformer(node)
        assert isinstance(ret, dict)
        return ret

    def transform_BooleanOperation(self, node: grammar.BooleanOperation) -> Dict[str, Any]:
        assert isinstance(node.operand, grammar.And) or isinstance(node.operand, grammar.Or)

        filter_type = 'and' if isinstance(node.operand, grammar.And) else 'or'
        values = [self._transform(v) for v in node.values]
        return {'type': filter_type, 'fields': values}

    def transform_BinaryComparison(self, node: grammar.BinaryComparison) -> Dict[str, Any]:
        if isinstance(node.left, grammar.Name) and isinstance(node.right, grammar.Name):
            column_comparison = {
                'type': 'columnComparison',
                'dimensions': [node.left.identifier, node.right.identifier],
            }
            if isinstance(node.comparator, grammar.Equals):
                return column_comparison
            elif isinstance(node.comparator, grammar.NotEquals):
                return {'type': 'not', 'field': column_comparison}
            else:
                raise DruidQueryTransformException(
                    node.comparator, 'When comparing two features, only the `==` and `!=` operators are supported'
                )

        dimension = get_comparison_dimension(node)
        value = get_comparison_value(node)

        if isinstance(node.comparator, grammar.Equals):
            return {'type': 'selector', 'dimension': dimension, 'value': value}
        elif isinstance(node.comparator, grammar.In):
            return get_in_query_by_value_type(node, dimension, value)
        elif isinstance(node.comparator, grammar.NotEquals):
            return {'type': 'not', 'field': {'type': 'selector', 'dimension': dimension, 'value': value}}
        elif isinstance(node.comparator, grammar.NotIn):
            return {'type': 'not', 'field': get_in_query_by_value_type(node, dimension, value)}

        bound_query = {
            'type': 'bound',
            'dimension': dimension,
            'ordering': get_value_bound_ordering(value),
            **get_druid_bound_query_props(node, value),
        }

        # greater than and less than queries require an explicit not null check
        return {
            'type': 'and',
            'fields': [
                {'type': 'not', 'field': {'type': 'selector', 'dimension': dimension, 'value': None}},
                bound_query,
            ],
        }

    def transform_UnaryOperation(self, node: grammar.UnaryOperation) -> Dict[str, Any]:
        if isinstance(node.operator, grammar.Not):
            return {'type': 'not', 'field': self._transform(node.operand)}
        else:
            raise DruidQueryTransformException(node, 'Unknown Unary Operator')

    def transform_Call(self, node: grammar.Call) -> Dict[str, Any]:
        udf, _ = self._udf_node_mapping[id(node)]

        if not isinstance(udf, QueryUdfBase):
            raise DruidQueryTransformException(node, 'Unknown function call type')

        return udf.to_druid_query()


def get_in_query_by_value_type(node: grammar.BinaryComparison, dimension: str, comparison_value: Any) -> Dict[str, Any]:
    if isinstance(comparison_value, str):
        return {
            'type': 'search',
            'dimension': dimension,
            'query': {'type': 'insensitive_contains', 'value': comparison_value},
        }
    elif isinstance(comparison_value, list):
        return {'type': 'in', 'dimension': dimension, 'values': comparison_value}
    else:
        raise DruidQueryTransformException(node, 'Invalid "in" comparison value type, must be string or list')


def get_druid_bound_query_props(node: grammar.BinaryComparison, comparison_value: Any) -> Dict[str, Any]:
    """Get the correct query properties for the various type of `bound` filters"""

    if isinstance(node.comparator, grammar.LessThan):
        return {'upper': comparison_value, 'upperStrict': True}
    elif isinstance(node.comparator, grammar.LessThanEquals):
        return {'upper': comparison_value}
    elif isinstance(node.comparator, grammar.GreaterThan):
        return {'lower': comparison_value, 'lowerStrict': True}
    elif isinstance(node.comparator, grammar.GreaterThanEquals):
        return {'lower': comparison_value}
    else:
        raise DruidQueryTransformException(node.comparator, 'Unknown Binary Comparator')


def get_comparison_dimension(node: grammar.BinaryComparison) -> str:
    """Extracts the dimension name for a binary comparison"""

    if isinstance(node.left, grammar.Name):
        return node.left.identifier
    elif isinstance(node.right, grammar.Name):
        return node.right.identifier
    else:
        raise DruidQueryTransformException(node, 'Binary Comparator must contain at least one column')


def get_comparison_value(node: grammar.BinaryComparison) -> Any:
    """Extracts the value for a binary comparison"""

    if isinstance(node.left, (grammar.Literal, grammar.UnaryOperation)):
        return get_ast_node_value(node.left)
    elif isinstance(node.right, (grammar.Literal, grammar.UnaryOperation)):
        return get_ast_node_value(node.right)


def get_ast_node_value(node: grammar.ASTNode) -> Any:
    """Gets the relevant value from any given expression type (Name or Literal)

    Unary operations can be evaluated into literals here (for negative Numbers)
    """

    if isinstance(node, grammar.UnaryOperation):
        return OspreyUnaryExecutor(node).get_execution_value()
    elif isinstance(node, grammar.List):
        return [get_ast_node_value(i) for i in node.items]
    elif isinstance(node, grammar.None_):
        return None
    elif isinstance(node, grammar.String) or isinstance(node, grammar.Number) or isinstance(node, grammar.Boolean):
        return node.value
    else:
        raise DruidQueryTransformException(node, 'Node has no known value attribute')


def get_value_bound_ordering(value: Any) -> str:
    """Given a value, return the appropriate comparator for the value to be used in a bound filter, throwing if it
    cannot be compared."""

    if isinstance(value, (int, float)):
        return 'numeric'
    elif isinstance(value, str):
        return 'lexicographic'

    raise TypeError(f'Cannot compare a {value.__class__.__name__}')
