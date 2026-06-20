"""Schema loader for typed action contracts.

Loads per-action JSON schemas from a smite-rules checkout.
Schema format is defined in §4.3 of the typed-action-contracts plan.
"""
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional, Set

log = logging.getLogger(__name__)

_SCHEMA_VERSION = "https://discord.dev/smite/action-schema/v1"

# Activation gate for runtime absent-group pruning. Pruning is a behavior-
# changing optimization: it removes execution-graph chains for groups a schema
# declares `absent`. A WRONG `absent` entry on a group that is actually present
# (the set is fragile — see CollectJsonDataPaths) silently drops features /
# enforcement. So pruning is OFF unless this env var is explicitly truthy, even
# when schema files are present on disk. Loading schemas for the compile-time
# CollectJsonDataPaths warning is unaffected — only the runtime graph
# specialization is gated.
_PRUNING_ENV = "OSPREY_TYPED_CONTRACT_PRUNING"
_TRUTHY = {"1", "true", "yes", "on"}


def absent_pruning_enabled() -> bool:
    """True iff runtime absent-group pruning is explicitly enabled via env."""
    return os.environ.get(_PRUNING_ENV, "").strip().lower() in _TRUTHY


@dataclass(frozen=True)
class ActionSchema:
    """Parsed representation of a per-action schema JSON file."""

    action: str
    provides_groups: FrozenSet[str]       # keys of `provides`
    absent_groups: FrozenSet[str]         # from `absent`
    provides_field_types: Dict[str, str]   # "user.id" -> "int" (dot-notation, flattened)
    optional_for: Dict[str, List[str]]


class SchemaLoadError(Exception):
    """Raised when a schema file cannot be parsed or is invalid."""


def load_schema(schema_path: Path, schemas_dir: Optional[Path] = None) -> ActionSchema:
    """Load and parse a single action schema JSON file.

    Args:
        schema_path: Path to the <action_name>.json schema file.
        schemas_dir: Base directory for resolving $ref: paths. Defaults to
            the parent directory of schema_path.

    Returns:
        Parsed ActionSchema.

    Raises:
        SchemaLoadError: if the file is missing, malformed, or violates constraints.
    """
    if schemas_dir is None:
        schemas_dir = schema_path.parent

    try:
        raw = json.loads(schema_path.read_text())
    except FileNotFoundError:
        raise SchemaLoadError(f"Schema file not found: {schema_path}")
    except json.JSONDecodeError as e:
        raise SchemaLoadError(f"Invalid JSON in {schema_path}: {e}")

    schema_val = raw.get("$schema", "")
    if schema_val != _SCHEMA_VERSION:
        raise SchemaLoadError(
            f"Unsupported schema version in {schema_path}: {schema_val!r}. "
            f"Expected {_SCHEMA_VERSION!r}."
        )

    action = raw.get("action", "")
    if not action:
        raise SchemaLoadError(f"Missing 'action' field in {schema_path}")

    raw_provides: Dict[str, object] = raw.get("provides", {})
    absent_list: List[str] = raw.get("absent", [])
    types_used: Dict[str, str] = raw.get("types_used", {})
    optional_for: Dict[str, List[str]] = raw.get("optional_for", {})

    absent_groups: Set[str] = set(absent_list)
    provides_groups: Set[str] = set(raw_provides.keys())

    # Constraint: provides and absent must not overlap
    overlap = provides_groups & absent_groups
    if overlap:
        raise SchemaLoadError(
            f"Schema {schema_path}: groups {overlap!r} appear in both 'provides' and 'absent'."
        )

    # Resolve $ref: references — load referenced type files and merge into provides
    # $ref values point to types/<name>.json relative to schemas_dir
    _schemas_dir_resolved = schemas_dir.resolve()

    def _resolve_ref_path(ref_str: str) -> Path:
        """Resolve a $ref: path and assert it stays within schemas_dir."""
        ref_rel = ref_str[len("$ref:"):]
        ref_path = (schemas_dir / ref_rel).resolve()
        if not ref_path.is_relative_to(_schemas_dir_resolved):
            raise SchemaLoadError(
                f"$ref path escapes schemas directory: {ref_rel!r} resolves to {ref_path}"
            )
        return ref_path

    resolved_provides: Dict[str, object] = {}
    for group, value in raw_provides.items():
        if isinstance(value, str) and value.startswith("$ref:"):
            ref_path = _resolve_ref_path(value)
            try:
                ref_data = json.loads(ref_path.read_text())
            except FileNotFoundError:
                raise SchemaLoadError(f"Referenced type file not found: {ref_path} (from {schema_path})")
            except json.JSONDecodeError as e:
                raise SchemaLoadError(f"Invalid JSON in referenced file {ref_path}: {e}")
            resolved_provides[group] = ref_data
        else:
            resolved_provides[group] = value

    # Also resolve top-level $ref entries in types_used if they provide field definitions
    for group, ref_str in types_used.items():
        if isinstance(ref_str, str) and ref_str.startswith("$ref:") and group not in resolved_provides:
            try:
                ref_path = _resolve_ref_path(ref_str)
                ref_data = json.loads(ref_path.read_text())
                resolved_provides[group] = ref_data
                provides_groups.add(group)
            except SchemaLoadError:
                raise
            except (FileNotFoundError, json.JSONDecodeError):
                # types_used refs are informational; skip if the file doesn't exist yet
                pass

    # Flatten provides to dot-notation field types: "user.id" -> "int"
    # Scalar-only groups use the convention {"_scalar": "<type>"} to represent a
    # top-level field with no sub-fields (e.g. request_name: str rather than
    # request_name.field: str). Flatten these to the bare group name so that a
    # JsonData extraction of "$.request_name" matches "request_name" in
    # provides_field_types rather than the non-existent "request_name._scalar".
    provides_field_types: Dict[str, str] = {}
    for group, fields in resolved_provides.items():
        if isinstance(fields, dict):
            if list(fields.keys()) == ["_scalar"] and isinstance(fields["_scalar"], str):
                # Scalar group: flatten to bare group name
                provides_field_types[group] = fields["_scalar"]
                continue
            for field_name, field_type in fields.items():
                if isinstance(field_type, str):
                    provides_field_types[f"{group}.{field_name}"] = field_type

    return ActionSchema(
        action=action,
        provides_groups=frozenset(provides_groups),
        absent_groups=frozenset(absent_groups),
        provides_field_types=provides_field_types,
        optional_for=optional_for,
    )


def resolve_schemas_dir() -> Optional[Path]:
    """Discover the schemas directory from the runtime environment.

    Resolution order:
      1. ``OSPREY_SCHEMAS_DIR`` if set and points at an existing directory.
      2. ``<OSPREY_RULES_PATH>/schemas`` if that env var is set and the
         ``schemas`` subdirectory exists in the rules tree.

    Returns ``None`` if neither path resolves to an existing directory.

    The fallback to ``OSPREY_RULES_PATH/schemas`` lets typed contracts
    activate in environments that already point the worker at a smite-rules
    checkout, without requiring a separate env var or deployment change.
    """
    explicit = os.environ.get("OSPREY_SCHEMAS_DIR", "").strip()
    if explicit:
        candidate = Path(explicit)
        if candidate.is_dir():
            return candidate
        log.warning("OSPREY_SCHEMAS_DIR=%r is not a directory; ignoring", explicit)

    rules_path = os.environ.get("OSPREY_RULES_PATH", "").strip()
    if rules_path:
        candidate = Path(rules_path) / "schemas"
        if candidate.is_dir():
            return candidate

    return None


def load_schema_for_action(action_name: str, schemas_dir: Path) -> Optional[ActionSchema]:
    """Load the schema for a given action name from the schemas directory.

    Returns None if no schema file exists for that action (schema-less actions
    fall back to the default execution graph).
    """
    schema_path = schemas_dir / f"{action_name}.json"
    if not schema_path.exists():
        return None
    try:
        return load_schema(schema_path, schemas_dir=schemas_dir)
    except SchemaLoadError:
        log.exception("Failed to load schema for action %r from %s", action_name, schema_path)
        return None
