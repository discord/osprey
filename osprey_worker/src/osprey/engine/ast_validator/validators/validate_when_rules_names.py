from typing import Dict

from osprey.engine.ast.grammar import Span

from ..base_validator import BaseValidator, HasInput
from ..validation_context import ValidationContext
from .validate_call_kwargs import ValidateCallKwargs


class ValidateWhenRulesNames(BaseValidator, HasInput[Dict[str, Span]]):
    """Holds the `WhenRules(name=...)` values seen so far, so the UDF can reject a name used twice in a corpus."""

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)

    def run(self) -> None:
        # The per-block checks live in the WhenRules UDF, which ValidateCallKwargs constructs.
        self.context.validator_depends_on(validator_classes=[ValidateCallKwargs])
