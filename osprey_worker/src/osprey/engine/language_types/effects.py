from abc import abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterable, List, Self

from osprey.engine.executor.custom_extracted_features import (
    CustomExtractedFeature,
    SerializableT,
)
from osprey.engine.language_types.rules import RuleT


@dataclass(kw_only=True)
class EffectBase:
    """
    This is the base class for all effects (`ExecutionResult.effects`). All effects should inherit from this class.

    Effects are outcomes of `WhenRules` blocks, and are appended to the `ExecutionResult.effects`.
    They can also be automatically serialized into a custom extracted feature (`ExecutionResult.extracted_features`)
    if they implement the `ListToCustomExtractedFeature[SerializableT]` protocol, i.e. implement a class method with spec:
    ```python
    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[SerializableT]:
        ...
    ```

    Examples:
    - :class:`osprey_engine.packages.osprey_language_types.verdicts.VerdictEffect`
      - Used in :class:`osprey_engine.packages.osprey_stdlib.udfs.verdicts.DeclareVerdict`
    - :class:`osprey_engine.packages.osprey_language_types.labels.LabelEffect`
      - Used in :class:`osprey_engine.packages.osprey_stdlib.udfs.labels.LabelAdd` and :class:`osprey_engine.packages.osprey_stdlib.udfs.labels.LabelRemove`
    """

    mutates_state: ClassVar[bool] = False
    """True if this effect type writes to persistent state (label store, classification
    record, Safety Signal pubsub, intervention queue, etc.).

    Note: ValidateTierConstraints currently reads `mutates_state` from the *UDF* class
    (e.g. LabelAdd, Classify, EmitSignal), not from the Effect class. The Effect-side
    declarations are documentation of intent and a forward-compatibility hook for
    future code paths that may want to dedupe or gate based on the emitted effect
    (e.g. a sink-time dedup pass). Keep both in sync when adding new state-mutating
    effects.

    Set True on:
    - LabelEffect / RemoveLabelEffect / LabelEffectWithExpiry
    - ClassificationEffect (Safety Record write)
    - SignalEffect (Safety Signal pubsub)
    - Any future effect that fans out to persistent storage or downstream consumers.

    Verdict effects keep this False — verdicts are a response channel, not state."""

    rules: List[RuleT] = field(default_factory=list)
    """
    The rules that caused this effect to be emitted.
    These are appended automatically during WhenRules execution.
    """

    def add_rule(self, rule: RuleT) -> Self:
        self.rules.append(rule)
        return self

    def add_rules(self, rules: Iterable[RuleT]) -> Self:
        self.rules.extend(rules)
        return self


@dataclass
class EffectToCustomExtractedFeatureBase(EffectBase, Generic[SerializableT]):
    """
    This is the base class for all effects that can be automatically serialized into a custom extracted feature.
    """

    @classmethod
    @abstractmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[SerializableT]:
        """
        This method should return a custom extracted feature from a list of its own type.
        It will be called by the executor to build the custom extracted feature to represent the effect
        and appended to the `ExecutionResult.extracted_features`~
        """
        ...
