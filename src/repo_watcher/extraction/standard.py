"""Extract terms from classic CV JSON files (CMIP6, CORDEX, CMIP6Plus, input4MIPs)."""

from __future__ import annotations

from pathlib import Path

from repo_watcher.config.schema import FieldMappingConfig
from repo_watcher.domain import Term
from repo_watcher.extraction.formats import GLOBAL_EXCLUDED_COLLECTIONS, prepare_json, validate_typing


def _extract_field_level_terms(
    terms: dict,
    mapping: FieldMappingConfig,
) -> set[Term]:
    """Extract field-level terms for EMD-style checks."""
    result: set[Term] = set()
    target_collection = mapping["target_collection"]
    fields = mapping["fields"]
    for term_key, term_params in terms.items():
        if isinstance(term_params, dict):
            for source_field, target_field in fields.items():
                if source_field in term_params:
                    result.add(
                        Term(
                            name=term_key,
                            collection=target_collection,
                            field=target_field,
                            value=str(term_params[source_field]),
                        ),
                    )
    return result


def extract_terms(
    cv_directory: Path,
    project: str,
    only_collections: list[str] | None = None,
    skip_collections: list[str] | None = None,
    field_mappings: dict[str, FieldMappingConfig] | None = None,
    cv_format: str | None = None,
) -> set[Term]:
    """Extract terms from a local directory of JSON CV files."""
    result: set[Term] = set()

    for collection_path in sorted(cv_directory.glob("*.json")):
        if any(exc in collection_path.name for exc in GLOBAL_EXCLUDED_COLLECTIONS):
            continue

        contents = collection_path.read_text()
        validated = validate_typing(contents, project, cv_format)
        prepared = prepare_json(validated, project, collection_path, skip_collections, cv_format)

        for collection_key, terms in prepared.items():
            if field_mappings and collection_key in field_mappings:
                result |= _extract_field_level_terms(terms, field_mappings[collection_key])
                continue

            if only_collections and collection_key not in only_collections:
                continue

            for term_key in terms:
                result.add(Term(name=term_key, collection=collection_key))

    return result
