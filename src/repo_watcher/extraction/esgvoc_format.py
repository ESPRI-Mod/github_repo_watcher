"""Extract terms from esgvoc-format repos (one dir per collection, one JSON per term)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from repo_watcher.domain import Term

logger = logging.getLogger(__name__)


def extract_terms_esgvoc_format(
    repo_directory: Path,
    only_collections: list[str] | None = None,
    skip_collections: list[str] | None = None,
) -> set[Term]:
    """Extract terms from an esgvoc-format repo.

    Used for CMIP7-CVs and similar repos where each collection is a subdirectory
    and each term is a separate JSON file with an "@id" or "id" field.
    """
    result: set[Term] = set()

    for collection_dir in sorted(repo_directory.iterdir()):
        if not collection_dir.is_dir():
            continue
        collection = collection_dir.name
        if collection.startswith((".", "_", "scripts")):
            continue
        if skip_collections and collection in skip_collections:
            continue
        if only_collections and collection not in only_collections:
            continue

        for term_file in sorted(collection_dir.glob("*.json")):
            try:
                data = json.loads(term_file.read_text())
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSON: %s", term_file)
                continue
            term_id = data.get("@id") or data.get("id") or term_file.stem
            result.add(Term(name=term_id, collection=collection))

    return result
