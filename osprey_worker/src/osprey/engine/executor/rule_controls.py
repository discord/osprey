"""Process-wide hooks that let a deployment control rules at run time, without recompiling them.

A deployment installs one ``RuleControls`` with ``set_rule_controls``. The base class does nothing, so an engine with
nothing installed behaves exactly as before.

- ``is_rule_suppressed`` is asked, in ``WhenRules``, about each rule that passed. A suppressed rule emits none of that
  block's effects and is recorded in the block's audit entry, but its value is unchanged, so rules and blocks that
  depend on it see what they always saw.
- ``annotate_execution`` is called once per execution, after it ran, with the sources the graph was compiled from and
  the execution's audit entries. Whatever it returns is added to the result's extracted features, so every sink that
  records them records the annotation too.

Both hooks run on the hot path and must be cheap. Neither can break an execution: a hook that raises is counted and
logged, a raising suppression check suppresses nothing, and a raising annotation adds nothing.
"""

import logging
from typing import Any, Dict, Mapping, Optional, Sequence

from osprey.engine.ast.sources import Sources
from osprey.engine.executor.execution_context import WhenRulesAuditEntry
from osprey.worker.lib.instruments import metrics

logger = logging.getLogger(__name__)

# SML stores no feature whose name starts with an underscore, so annotations under this prefix never shadow a rule's
# features. Existing names, such as the engine's own ``__action_id``, are never replaced.
ANNOTATION_PREFIX = '__'

SUPPRESSION_CHECK_ERROR_METRIC = 'osprey.rule_controls.suppression_check_error'
ANNOTATION_ERROR_METRIC = 'osprey.rule_controls.annotation_error'
ANNOTATION_DROPPED_METRIC = 'osprey.rule_controls.annotation_dropped'


class RuleControls:
    def is_rule_suppressed(self, rule_name: str) -> bool:
        return False

    def annotate_execution(
        self, sources: Sources, rule_audit_entries: Sequence[WhenRulesAuditEntry]
    ) -> Mapping[str, Any]:
        return {}


_NO_CONTROLS = RuleControls()
_controls: RuleControls = _NO_CONTROLS


def set_rule_controls(controls: Optional[RuleControls]) -> None:
    """Install ``controls`` for every execution in this process; None restores the defaults."""
    global _controls
    _controls = _NO_CONTROLS if controls is None else controls


def get_rule_controls() -> RuleControls:
    return _controls


def suppressed_rule_names(rule_names: Sequence[str]) -> Sequence[str]:
    """The names in ``rule_names`` the installed controls suppress; none when the check raises."""
    controls = _controls
    if controls is _NO_CONTROLS or not rule_names:
        return []
    try:
        return [name for name in rule_names if controls.is_rule_suppressed(name)]
    except Exception as error:
        metrics.increment(SUPPRESSION_CHECK_ERROR_METRIC, tags=[f'error:{type(error).__name__}'])
        logger.exception('rule suppression check failed; suppressing no rules')
        return []


def annotate_execution(
    extracted_features: Dict[str, Any], sources: Sources, rule_audit_entries: Sequence[WhenRulesAuditEntry]
) -> None:
    """Add the installed controls' annotation of one execution to its ``extracted_features``."""
    controls = _controls
    if controls is _NO_CONTROLS:
        return
    try:
        annotation = controls.annotate_execution(sources, rule_audit_entries)
    except Exception as error:
        metrics.increment(ANNOTATION_ERROR_METRIC, tags=[f'error:{type(error).__name__}'])
        logger.exception('execution annotation failed; the result is not annotated')
        return
    for name, value in annotation.items():
        if not name.startswith(ANNOTATION_PREFIX) or name in extracted_features:
            metrics.increment(ANNOTATION_DROPPED_METRIC)
            continue
        extracted_features[name] = value
