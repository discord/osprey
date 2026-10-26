from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generic, Iterable, List, Optional, Self

from osprey.engine.executor.custom_extracted_features import (
    CustomExtractedFeature,
    SerializableT,
)
from osprey.engine.language_types.entities import EntityT
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

    rules: List[RuleT] = field(default_factory=list)
    """
    The rules that caused this effect to be emitted.
    These are appended automatically during WhenRules execution.
    """

    when_rules_name: Optional[str] = None
    """The `name` of the WhenRules block that emitted this effect. Stamped only for blocks that declare WhenRules modes."""

    when_rules_mode: Optional[str] = None
    """The `mode` of the WhenRules block that emitted this effect."""

    revision_id: Optional[str] = None
    """The `revision_id` of the WhenRules block that emitted this effect."""

    release_id: Optional[str] = None
    """The `release_id` of the WhenRules block that emitted this effect."""

    def primary_entity(self) -> Optional[EntityT[Any]]:
        """The entity used to bucket this effect for `rollout` mode: its `entity` field, or None if it has none."""
        entity = getattr(self, 'entity', None)
        return entity if isinstance(entity, EntityT) else None

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
