"""Tests for ValidateTierConstraints.

Uses pytest.raises(ValidationFailed) directly rather than check_failure so
that no snapshot files are required.
"""
from typing import Any, Callable, ClassVar, List

import pytest

from osprey.engine.ast_validator.validation_context import ValidationFailed
from osprey.engine.ast_validator.validators.unique_stored_names import UniqueStoredNames
from osprey.engine.ast_validator.validators.validate_call_kwargs import ValidateCallKwargs
from osprey.engine.ast_validator.validators.validate_tier_constraints import ValidateTierConstraints
from osprey.engine.conftest import RunValidationFunction
from osprey.engine.language_types.effects import EffectBase
from osprey.engine.stdlib.udfs._prelude import ArgumentsBase, ExecutionContext, UDFBase
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.stdlib.udfs.json_data import JsonData
from osprey.engine.stdlib.udfs.rules import Rule, WhenRules
from osprey.engine.stdlib.udfs.verdicts import DeclareVerdict
from osprey.engine.udf.registry import UDFRegistry


# ---------------------------------------------------------------------------
# Test-only UDFs
# ---------------------------------------------------------------------------


class FakeSlowArguments(ArgumentsBase):
    pass


class FakeSlowUDF(UDFBase[FakeSlowArguments, float]):
    """Test-only SLOW UDF."""

    category = UdfCategories.ENGINE
    latency_tier = "slow"

    def execute(self, ctx: ExecutionContext, args: FakeSlowArguments) -> float:
        return 0.5


class FakeMutatingArguments(ArgumentsBase):
    pass


class FakeMutatingEffect(EffectBase):
    """Test-only effect emitted by FakeMutatingUDF."""


class FakeMutatingUDF(UDFBase[FakeMutatingArguments, EffectBase]):
    """Test-only UDF that emits a state-mutating effect."""

    category = UdfCategories.ENGINE
    mutates_state: ClassVar[bool] = True

    def execute(self, ctx: ExecutionContext, args: FakeMutatingArguments) -> EffectBase:
        return FakeMutatingEffect()


# ---------------------------------------------------------------------------
# Pytest marks
# ---------------------------------------------------------------------------

pytestmark: List[Callable[[Any], Any]] = [
    pytest.mark.use_validators([ValidateTierConstraints, ValidateCallKwargs, UniqueStoredNames]),
    pytest.mark.use_udf_registry(
        UDFRegistry.with_udfs(JsonData, Rule, WhenRules, DeclareVerdict, FakeSlowUDF, FakeMutatingUDF)
    ),
]


# ---------------------------------------------------------------------------
# Constraint 1: SLOW UDF checks
# ---------------------------------------------------------------------------


def test_fast_udf_in_sync_tier_passes(run_validation: RunValidationFunction) -> None:
    """A WhenRules with tier='sync' referencing only FAST UDFs validates clean."""
    run_validation("""
        Flag: bool = JsonData(path='$.flag')
        MyRule = Rule(when_all=[Flag], description="sync rule")
        WhenRules(
            rules_any=[MyRule],
            then=[DeclareVerdict(verdict="ok")],
            tier="sync",
        )
    """)


def test_slow_udf_in_async_tier_passes(run_validation: RunValidationFunction) -> None:
    """tier='async' is allowed to use SLOW UDFs — async path has no wall-clock budget."""
    run_validation("""
        Score: float = FakeSlowUDF()
        SlowRule = Rule(when_all=[Score > 0.5], description="async rule")
        WhenRules(
            rules_any=[SlowRule],
            then=[DeclareVerdict(verdict="ok")],
            tier="async",
        )
    """)


def test_slow_udf_in_sync_tier_fails(run_validation: RunValidationFunction) -> None:
    """tier='sync' must not reference SLOW UDFs — latency budget violation."""
    with pytest.raises(ValidationFailed) as exc_info:
        run_validation("""
            Score: float = FakeSlowUDF()
            SlowRule = Rule(when_all=[Score > 0.5], description="bad rule")
            WhenRules(
                rules_any=[SlowRule],
                then=[DeclareVerdict(verdict="block")],
                tier="sync",
            )
        """)
    rendered = exc_info.value.rendered()
    assert "slow" in rendered.lower()
    assert "FakeSlowUDF" in rendered


def test_slow_udf_in_legacy_tier_fails(run_validation: RunValidationFunction) -> None:
    """tier='legacy' (default) also forbidden to reference SLOW UDFs."""
    with pytest.raises(ValidationFailed) as exc_info:
        run_validation("""
            Score: float = FakeSlowUDF()
            SlowRule = Rule(when_all=[Score > 0.5], description="bad rule")
            WhenRules(
                rules_any=[SlowRule],
                then=[DeclareVerdict(verdict="block")],
            )
        """)
    assert "SLOW UDF" in exc_info.value.rendered()
    assert "FakeSlowUDF" in exc_info.value.rendered()


def test_slow_udf_in_both_tier_fails(run_validation: RunValidationFunction) -> None:
    """tier='both' also forbidden to reference SLOW UDFs."""
    with pytest.raises(ValidationFailed) as exc_info:
        run_validation("""
            Score: float = FakeSlowUDF()
            SlowRule = Rule(when_all=[Score > 0.5], description="bad rule")
            WhenRules(
                rules_any=[SlowRule],
                then=[DeclareVerdict(verdict="info")],
                tier="both",
            )
        """)
    assert "SLOW UDF" in exc_info.value.rendered()
    assert "FakeSlowUDF" in exc_info.value.rendered()


# ---------------------------------------------------------------------------
# Constraint 2: state-mutating effect checks
# ---------------------------------------------------------------------------


def test_state_mutating_effect_in_both_tier_fails(run_validation: RunValidationFunction) -> None:
    """tier='both' must not emit state-mutating effects — duplicate write risk."""
    with pytest.raises(ValidationFailed) as exc_info:
        run_validation("""
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="bad")
            WhenRules(
                rules_any=[MyRule],
                then=[FakeMutatingUDF()],
                tier="both",
            )
        """)
    rendered = exc_info.value.rendered()
    assert "state-mutating" in rendered
    assert "FakeMutatingUDF" in rendered


def test_verdict_only_in_both_tier_passes(run_validation: RunValidationFunction) -> None:
    """tier='both' with only DeclareVerdict (mutates_state=False) is fine."""
    run_validation("""
        Flag: bool = JsonData(path='$.flag')
        MyRule = Rule(when_all=[Flag], description="both rule")
        WhenRules(
            rules_any=[MyRule],
            then=[DeclareVerdict(verdict="info")],
            tier="both",
        )
    """)


def test_state_mutating_in_sync_or_async_tier_passes(run_validation: RunValidationFunction) -> None:
    """FakeMutatingUDF in tier='sync' or tier='async' is fine — only tier='both' is forbidden."""
    run_validation("""
        Flag: bool = JsonData(path='$.flag')
        MyRule = Rule(when_all=[Flag], description="ok")
        WhenRules(rules_any=[MyRule], then=[FakeMutatingUDF()], tier="sync")
        WhenRules(rules_any=[MyRule], then=[FakeMutatingUDF()], tier="async")
    """)


def test_existing_rules_without_tier_pass_validator(run_validation: RunValidationFunction) -> None:
    """Back-compat: existing rules without explicit tier (defaulting to 'legacy')
    that use state-mutating effects but only FAST UDFs pass the validator.
    The 'both' constraint only triggers on explicit tier='both'."""
    run_validation("""
        Flag: bool = JsonData(path='$.flag')
        MyRule = Rule(when_all=[Flag], description="existing")
        WhenRules(rules_any=[MyRule], then=[FakeMutatingUDF()])
    """)


def test_slow_udf_buried_in_then_kwarg_fails(run_validation: RunValidationFunction) -> None:
    """SLOW UDFs are caught when nested inside an Effect's arguments in then=,
    not only when referenced via rules_any. Without walking then=, a SLOW UDF
    smuggled in as a nested argument would slip past constraint 1."""
    with pytest.raises(ValidationFailed) as exc_info:
        run_validation("""
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="trivial")
            WhenRules(
                rules_any=[MyRule],
                then=[DeclareVerdict(verdict=FakeSlowUDF())],
                tier="sync",
            )
        """)
    rendered = exc_info.value.rendered()
    assert "FakeSlowUDF" in rendered
    assert "tier=`sync`" in rendered


def test_mutating_effect_via_name_reference_in_both_fails(run_validation: RunValidationFunction) -> None:
    """tier=`both` catches state-mutating effects even when they're referenced
    through a Name. then=[Helper] where Helper = FakeMutatingUDF() still fires
    the constraint."""
    with pytest.raises(ValidationFailed) as exc_info:
        run_validation("""
            Flag: bool = JsonData(path='$.flag')
            MyRule = Rule(when_all=[Flag], description="trivial")
            Helper = FakeMutatingUDF()
            WhenRules(
                rules_any=[MyRule],
                then=[Helper],
                tier="both",
            )
        """)
    rendered = exc_info.value.rendered()
    assert "FakeMutatingUDF" in rendered
    assert "tier=`both`" in rendered
