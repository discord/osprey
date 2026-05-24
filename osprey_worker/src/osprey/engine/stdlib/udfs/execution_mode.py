from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories


class Arguments(ArgumentsBase):
    pass


class ExecutionMode(UDFBase[Arguments, str]):
    """Returns the execution mode of the current action: 'sync', 'async', or 'unspecified'.

    Use in conjunction with Require(require_if=...) to gate entire files by tier:
        Require(rule='slow_classifiers.sml', require_if=ExecutionMode() == 'async')

    'unspecified' indicates older messages without a stamped mode (a coordinator
    that predates the proto change). Rule authors should test for the
    AFFIRMATIVE case (e.g. `== 'async'`) — an explicit equality check against
    'sync' or 'async' will evaluate to False on 'unspecified', matching the
    legacy behavior where the file was never gated.

    Note: this is distinct from the WhenRules `tier` filter, which DOES treat
    'unspecified' as a bypass (all tier-tagged blocks fire). The asymmetry is
    intentional: rule authors need a falsy ExecutionMode comparison for older
    messages, but tier-tagged blocks should still fire on older messages to
    avoid silently disabling enforcement during a rollback."""

    category = UdfCategories.ENGINE

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return execution_context.get_execution_mode()
