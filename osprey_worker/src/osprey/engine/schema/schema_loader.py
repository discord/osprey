"""Schema loader for typed action contracts.

Loads per-action JSON schemas from a smite-rules checkout.
Schema format is defined in §4.3 of the typed-action-contracts plan.
"""
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

log = logging.getLogger(__name__)

_SCHEMA_VERSION = "https://discord.dev/smite/action-schema/v1"


@dataclass(frozen=True)
class ActionSchema:
    """Parsed representation of a per-action schema JSON file."""

    action: str
    provides_groups: Set[str]       # keys of `provides`
    absent_groups: Set[str]         # from `absent`
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
    resolved_provides: Dict[str, object] = {}
    for group, value in raw_provides.items():
        if isinstance(value, str) and value.startswith("$ref:"):
            ref_path = schemas_dir / value[len("$ref:"):]
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
            ref_path = schemas_dir / ref_str[len("$ref:"):]
            try:
                ref_data = json.loads(ref_path.read_text())
                resolved_provides[group] = ref_data
                provides_groups.add(group)
            except (FileNotFoundError, json.JSONDecodeError):
                # types_used refs are informational; skip if the file doesn't exist yet
                pass

    # Flatten provides to dot-notation field types: "user.id" -> "int"
    provides_field_types: Dict[str, str] = {}
    for group, fields in resolved_provides.items():
        if isinstance(fields, dict):
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
