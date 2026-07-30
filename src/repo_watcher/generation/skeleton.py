"""Build blank universe and project JSON term skeletons."""

from __future__ import annotations

import json
from pathlib import Path

from repo_watcher.generation.context import CONTEXT_FILENAME


def sample_existing_term(descriptor_dir: Path) -> dict | None:
    """Read the first existing term JSON in a universe descriptor directory."""
    for f in sorted(descriptor_dir.iterdir()):
        if f.name == CONTEXT_FILENAME or not f.suffix == ".json":
            continue
        try:
            return json.loads(f.read_text())
        except json.JSONDecodeError:
            continue
    return None


def build_universe_skeleton(universe_repo: Path, descriptor: str, term_id: str) -> dict:
    """Build a skeleton universe term JSON by sampling an existing term for field discovery."""
    result: dict = {
        "@context": CONTEXT_FILENAME,
        "id": term_id,
        "type": descriptor,
    }
    descriptor_dir = universe_repo / descriptor

    # Sample an existing term to discover fields and their default types
    sample = sample_existing_term(descriptor_dir)
    if sample is None:
        return result

    for key, value in sample.items():
        if key in result:
            continue
        if isinstance(value, list):
            result[key] = []
        elif isinstance(value, dict):
            result[key] = {}
        elif isinstance(value, str):
            result[key] = ""
        else:
            result[key] = None

    return result


def build_project_link(descriptor: str, term_id: str) -> dict:
    """Build the minimal project link JSON (always 3 fields)."""
    return {
        "@context": CONTEXT_FILENAME,
        "id": term_id,
        "type": descriptor,
    }
