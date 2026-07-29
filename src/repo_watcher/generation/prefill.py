"""Auto-prefill skeleton fields from community CV data."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import esgvoc.api as ev

from repo_watcher.generation.context import detect_field_references
from repo_watcher.generation.report import TermFile


def prefill_from_cv_data(
    skeleton: dict,
    term_name: str,
    collection: str,
    descriptor: str,
    universe_repo: Path,
    project_name: str,
    field_refs: dict[str, str],
    collection_mapping: dict[str, str],
    cv_data: dict,
    cv_original_key: str | None,
    source_prefill: dict[str, dict[str, str]] | None,
    planned_ids: set[str],
) -> dict[str, object]:
    """Apply CV data to a universe skeleton, returning extra fields for the project link.

    Handles:
    - Auto-matching CV fields to skeleton fields
    - Explicit field renames from source_prefill config
    - Cross-reference detection and ID lowercasing
    - List wrapping and ID validation
    - Setting drs_name from original-case CV key

    Returns a dict of fields that don't belong in the universe skeleton
    (to be added to the project link instead).
    """
    project_extra_fields: dict[str, object] = {}

    # Build field mapping: auto-match + explicit renames
    field_pairs: list[tuple[str, str]] = []
    # Auto-match: CV fields that exist in skeleton or in universe context
    for cv_field in cv_data:
        if cv_field.startswith("_"):
            continue
        if cv_field in skeleton or cv_field in field_refs:
            field_pairs.append((cv_field, cv_field))
    # Explicit renames from source_prefill config
    if source_prefill and collection in source_prefill:
        for src_field, target_field in source_prefill[collection].items():
            field_pairs.append((src_field, target_field))

    for src_field, target_field in field_pairs:
        if src_field not in cv_data:
            continue
        value = cv_data[src_field]
        if value is None:
            continue
        # Skip when skeleton expects a dict but CV provides a scalar
        if target_field in skeleton and isinstance(skeleton[target_field], dict) and not isinstance(value, dict):
            continue
        # Determine if this field is a cross-reference
        ref_descriptor = _resolve_ref_descriptor(target_field, field_refs, collection_mapping)
        is_id_field = ref_descriptor is not None
        # Lowercase only ID references (not descriptions, labels, etc.)
        if is_id_field:
            if isinstance(value, str):
                value = value.lower()
            elif isinstance(value, list):
                value = [v.lower() if isinstance(v, str) else v for v in value]
        # Wrap scalar into list when target expects a list
        target_is_list = target_field in skeleton and isinstance(skeleton[target_field], list)
        if target_is_list and not isinstance(value, list):
            value = [value]
        # Validate list ID values
        if is_id_field and isinstance(value, list):
            ref_dir = universe_repo / ref_descriptor if ref_descriptor else None
            value = [
                v for v in value
                if ev.get_term_in_collection(project_name, target_field, v) is not None
                or v in planned_ids
                or (ref_dir is not None and (ref_dir / f"{v}.json").exists())
            ]
        if target_field in skeleton:
            skeleton[target_field] = value
        else:
            # Field not in universe skeleton — add to project link
            if isinstance(value, list) and not value:
                continue
            project_extra_fields[target_field] = value

    # Set drs_name from original-case CV key when not already filled
    if cv_original_key and "drs_name" in skeleton and not skeleton["drs_name"]:
        skeleton["drs_name"] = cv_original_key

    return project_extra_fields


def _resolve_ref_descriptor(
    target_field: str,
    field_refs: dict[str, str],
    collection_mapping: dict[str, str],
) -> str | None:
    """Determine the universe descriptor a field cross-references, if any."""
    ref_descriptor = field_refs.get(target_field)
    if ref_descriptor is None and target_field in collection_mapping:
        ref_descriptor = collection_mapping[target_field]
    if ref_descriptor is None and target_field.endswith("_id"):
        base_field = target_field.removesuffix("_id")
        ref_descriptor = field_refs.get(base_field)
        if ref_descriptor is None and base_field in field_refs.values():
            ref_descriptor = base_field
    return ref_descriptor
