"""Single source of truth for tier-routing constants.

Imported by:
- WhenRules (compile-time validation + runtime filter)
- ValidateTierConstraints (constraint matrix)

Adding a 5th tier? Update VALID_TIERS, decide whether it goes in
ALWAYS_FIRES (bypasses filtering) and/or SLOW_FORBIDDEN (latency budget).
"""
from typing import FrozenSet

VALID_TIERS: FrozenSet[str] = frozenset({"sync", "async", "both", "legacy"})

# Tiers that fire on every execution mode (no runtime filtering).
ALWAYS_FIRES: FrozenSet[str] = frozenset({"legacy", "both"})

# Tiers that forbid SLOW UDFs at compile time (the sync latency budget applies).
SLOW_FORBIDDEN: FrozenSet[str] = frozenset({"sync", "both", "legacy"})

# The execution mode treated as "no tier filtering applied" — for back-compat
# with older coordinator binaries that don't stamp mode on the bidi-stream
# message.
UNSPECIFIED_MODE: str = "unspecified"
