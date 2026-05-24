"""Test-only SLOW UDF used in tier-routing E2E and worked example tests.

This UDF mimics an LLM gRPC call by sleeping briefly (or returning immediately
when fixed_score is set). It is tagged latency_tier='slow' so it triggers the
ValidateTierConstraints validator the same way a real slow gRPC UDF would.

Not registered in the default stdlib registry — tests opt in explicitly via
UDFRegistry.with_udfs(FakeSlowClassifier)."""
from time import sleep
from typing import Optional

from osprey.engine.stdlib.udfs._prelude import ArgumentsBase, ExecutionContext, UDFBase
from osprey.engine.stdlib.udfs.categories import UdfCategories


class FakeSlowClassifierArguments(ArgumentsBase):
    user_id: str
    """The user ID being scored. Passed through to allow tests to assert on which
    entity was scored."""
    fixed_score: Optional[float] = None
    """If set, return this score immediately (no sleep). Used in unit tests.
    If None, the UDF sleeps briefly and returns 0.5 (test fixture behavior;
    do NOT use in production)."""


class FakeSlowClassifier(UDFBase[FakeSlowClassifierArguments, float]):
    """A test-only UDF tagged latency_tier='slow'. Mimics an LLM gRPC call.

    In unit tests, callers pass fixed_score to skip the simulated delay.
    In integration / E2E tests, the brief sleep demonstrates the latency-budget
    impact of running the UDF on the sync path."""

    category = UdfCategories.ENGINE
    latency_tier = 'slow'

    def execute(self, ctx: ExecutionContext, args: FakeSlowClassifierArguments) -> float:
        if args.fixed_score is not None:
            return args.fixed_score
        sleep(0.1)  # short stand-in for the 1-5s real-world call
        return 0.5
