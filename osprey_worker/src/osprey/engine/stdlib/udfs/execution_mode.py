from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    pass


class ExecutionMode(UDFBase[Arguments, str]):
    """Returns the execution mode of the current action: 'sync', 'async', or 'unspecified'.

    Use in conjunction with Require(require_if=...) to gate entire files by tier:
        Require(rule='slow_classifiers.sml', require_if=ExecutionMode() == 'async')

    'unspecified' indicates older messages without a stamped mode (a coordinator
    that predates the Phase 1 proto change). Rule files using this UDF should
    treat 'unspecified' as a no-op (no filtering) rather than as a third tier."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return execution_context.get_execution_mode()
