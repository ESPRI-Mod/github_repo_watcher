"""Extract raw term data from community CV files (for generation prefill)."""

from __future__ import annotations

from pathlib import Path

from repo_watcher.extraction.formats import GLOBAL_EXCLUDED_COLLECTIONS, prepare_json, validate_typing


def extract_term_data(
    cv_directory: Path,
    project: str,
    collections: list[str] | None = None,
    cv_format: str | None = None,
) -> dict[str, dict[str, dict]]:
    """Extract raw term data from community CV files.

    Returns {collection: {term_key: {field: value, ...}}}.
    Used by generation to pre-fill skeleton fields from source data.
    """
    result: dict[str, dict[str, dict]] = {}

    for collection_path in sorted(cv_directory.glob("*.json")):
        if any(exc in collection_path.name for exc in GLOBAL_EXCLUDED_COLLECTIONS):
            continue

        contents = collection_path.read_text()
        validated = validate_typing(contents, project, cv_format)
        prepared = prepare_json(validated, project, collection_path, cv_format=cv_format)

        for collection_key, terms in prepared.items():
            if collections and collection_key not in collections:
                continue
            collection_data: dict[str, dict] = {}
            for term_key, term_content in terms.items():
                if isinstance(term_content, dict):
                    collection_data[term_key] = term_content
                elif isinstance(term_content, str):
                    collection_data[term_key] = {"_value": term_content, "_key": term_key}
            if collection_data:
                result[collection_key] = collection_data

    return result
