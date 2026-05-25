from typing import Any

from osprey.engine.stdlib.udfs import json_utils
from osprey.engine.udf.markers import JsonExtractUDFMixin
from osprey.engine.udf.rvalue_type_checker import ExtractSecretTypeChecker, SecretTypeChecker

from ._prelude import ArgumentsBase, ConstExpr, ExecutionContext, UDFBase, ValidationContext
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    path: ConstExpr[str]
    """The path within the JSON to extract the data from.

    Must be a string literal and must be valid JSON path syntax.
    """

    required: bool = True
    """Whether or not the value is required to be in the action data.

    Defaults to `True`. If `False`, will gracefully handle missing, present-but-null, and
    (when `coerce_type` is set) present-but-uncoercible values by skipping dependent nodes.
    """

    coerce_type: bool = True
    """Whether to attempt to convert the value to the expected type.

    By default `JsonData` will attempt to coerce the value to the declared type (e.g., parse a number from a string).
    If coercion fails on a `required` field it raises `InvalidJsonType`; on a non-required field the value is
    treated as absent. Set to `False` to require exact type matches.
    """


class JsonData(JsonExtractUDFMixin, UDFBase[Arguments, Any]):
    """Extract a piece of data from the action's JSON.

    Has a dynamic return type, so the result must be stored into a type-annotated feature.
    """

    category = UdfCategories.ENGINE

    def __init__(self, validation_context: 'ValidationContext', arguments: Arguments):
        super().__init__(validation_context, arguments)
        self._expr = json_utils.parse_path(arguments.path)

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> Any:
        assert self._rvalue_type_checker is not None, 'Should have been set before getting here, or failed validation'

        is_secret = isinstance(self._rvalue_type_checker, SecretTypeChecker) or isinstance(
            self._rvalue_type_checker, ExtractSecretTypeChecker
        )

        data = execution_context.get_secret_data() if is_secret else execution_context.get_data()

        return json_utils.get_from_data(
            self._expr,
            data,
            required=arguments.required,
            coerce_type=arguments.coerce_type,
            rvalue_type_checker=self._rvalue_type_checker,
        )
