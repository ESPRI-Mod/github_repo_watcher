"""JSON-LD context parsing for universe and project repos."""

from __future__ import annotations

import json
from pathlib import Path

CONTEXT_FILENAME = "000_context.jsonld"

# JSON-LD structural keys to skip when discovering term fields from context
JSONLD_STRUCTURAL_KEYS = {"@base", "@vocab", "id", "type"}


def detect_field_references(universe_repo: Path, descriptor: str) -> dict[str, str]:
    """Parse a universe descriptor's 000_context.jsonld to find fields that are cross-references.

    Returns e.g. {"activity": "activity", "required_model_components": "source_type"}.
    Only fields with "@type": "@id" and a nested @base URL are included.
    """
    context_file = universe_repo / descriptor / CONTEXT_FILENAME
    if not context_file.exists():
        return {}
    try:
        data = json.loads(context_file.read_text())
    except json.JSONDecodeError:
        return {}
    context = data.get("@context", {})
    refs: dict[str, str] = {}
    for field, spec in context.items():
        if not isinstance(spec, dict):
            continue
        if spec.get("@type") != "@id":
            continue
        nested = spec.get("@context", {})
        base_url = nested.get("@base", "")
        if base_url:
            ref_collection = base_url.rstrip("/").rsplit("/", 1)[-1]
            refs[field] = ref_collection
    return refs


def detect_collection_mapping(project_repo: Path) -> dict[str, str]:
    """Parse @base URLs from project 000_context.jsonld files to build collection->universe mapping.

    Returns e.g. {"source": "model", "institution": "organisation", "grid_label": "horizontal_grid_cell"}.
    Collections where the name matches the universe descriptor are included too.
    """
    mapping: dict[str, str] = {}
    for collection_dir in sorted(project_repo.iterdir()):
        if not collection_dir.is_dir():
            continue
        context_file = collection_dir / CONTEXT_FILENAME
        if not context_file.exists():
            continue
        try:
            data = json.loads(context_file.read_text())
        except json.JSONDecodeError:
            continue
        context = data.get("@context", {})
        base_url = context.get("@base", "")
        if not base_url:
            continue
        # Extract universe descriptor from @base URL like
        # "https://esgvoc.ipsl.fr/resource/universe/model/"
        descriptor = base_url.rstrip("/").rsplit("/", 1)[-1]
        mapping[collection_dir.name] = descriptor
    return mapping
