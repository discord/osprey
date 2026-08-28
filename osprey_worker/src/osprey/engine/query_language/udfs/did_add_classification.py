import re
from typing import Dict

from osprey.engine import shared_constants
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.query_language.udfs.registry import register
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import QueryUdfBase


class Arguments(ArgumentsBase):
    classification: ConstExpr[str]


@register
class DidAddClassification(QueryUdfBase[Arguments, bool]):
    def __init__(self, validation_context: ValidationContext, arguments: Arguments):
        super().__init__(validation_context, arguments)
        self.classification = arguments.classification.value.lower()

    def to_druid_query(self) -> Dict[str, object]:
        classification = re.escape(self.classification)
        return {
            'type': 'regex',
            'dimension': shared_constants.CLASSIFICATIONS_DIMENSION_NAME,
            'pattern': rf'^[^/]+/[^/]+/{classification}$',
        }
