"""Layer 2 — Extraction: local directory of JSON CVs -> set[Term].

Pure parsing. No network, no esgvoc, no Prefect.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from models import FieldMappingConfig, Term

logger = logging.getLogger(__name__)

# Type aliases
type ParamStr = str
type ParamDict = dict[str, Any]
type TermKey = str
type TermDict = dict[TermKey, ParamStr | ParamDict]
type CollectionKey = str
type NotCollectionKey = str
type VocabularyDict = dict[CollectionKey, TermDict]

# Input types for validating the json files per project.
# Only covers the "legacy" format (one JSON file per collection).
# CMIP7 and EMD use a different layout (one directory per collection,
# one JSON file per term) and are handled by extract_terms_esgvoc_format()
# and extract_terms_emd() respectively — they don't need these types.
type CordexCMIP6InputDict = dict[CollectionKey, list[TermKey] | TermDict]
type CMIP6InputDict = dict[CollectionKey, list[TermKey] | TermDict]
type CMIP6PlusInputDict = dict[CollectionKey, TermKey | list[TermKey] | TermDict]
type Input4MipsInputDict = list[TermKey] | TermDict | dict[NotCollectionKey, TermDict]

INPUT_TYPES: dict[str, type] = {
    "cordex-cmip6": CordexCMIP6InputDict,
    "cmip6": CMIP6InputDict,
    "cmip6plus": CMIP6PlusInputDict,
    "input4mip": Input4MipsInputDict,
}

# Collections to always skip (applied before project-specific skips)
GLOBAL_EXCLUDED_COLLECTIONS = [
    "DRS",
    "required_global_attributes",
    "global_attrs",
]


def validate_typing(data: str, project: str) -> dict | list:
    """Validate JSON data against the expected type for a project."""
    if project not in INPUT_TYPES:
        msg = f"Unknown project: {project}"
        raise KeyError(msg)
    return TypeAdapter(INPUT_TYPES[project]).validate_json(data)


def prepare_json(
    data: dict | list,
    project: str,
    collection_path: Path,
    skip_collections: list[str] | None = None,
) -> VocabularyDict:
    """Normalize any project's JSON CV format into {collection: {term: params}}.

    The differences between projects are minimal:
    - CMIP6Plus has a `case str()` branch (single term as string value)
    - Input4MIPs derives collection name from filename and has a target_mip special case
    - Everything else (dict of dicts, dict of lists) is identical
    """
    all_skips = list(GLOBAL_EXCLUDED_COLLECTIONS)
    if skip_collections:
        all_skips.extend(skip_collections)

    if project == "input4mip":
        return _prepare_json_input4mip(data, collection_path, all_skips)
    return _prepare_json_standard(data, project, all_skips)


def _prepare_json_standard(
    data: dict,
    project: str,
    skip_collections: list[str],
) -> VocabularyDict:
    result: VocabularyDict = {}
    for collection_key, terms in data.items():
        if collection_key in skip_collections:
            continue
        result[collection_key] = {}
        match terms:
            case dict():
                for term_key, term_content in terms.items():
                    result[collection_key][term_key] = term_content
            case list():
                for term_key in terms:
                    result[collection_key][term_key] = {}
            case str() if project == "cmip6plus":
                result[collection_key][terms] = {}
    return result


def _prepare_json_input4mip(
    data: dict | list,
    collection_path: Path,
    skip_collections: list[str],
) -> VocabularyDict:
    result: VocabularyDict = {}
    collection = collection_path.stem.removeprefix("input4MIPs_")
    if collection in skip_collections:
        return result
    result[collection] = {}

    match data:
        case dict() if collection == "target_mip":
            for target_mips in data.values():
                for target_mip, target_mip_params in target_mips.items():
                    result["target_mip"][target_mip] = target_mip_params
        case dict():
            for term_key, term_content in data.items():
                result[collection][term_key] = term_content
        case list():
            for term_key in data:
                result[collection][term_key] = {}
    return result


def _extract_field_level_terms(
    terms: TermDict,
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
) -> set[Term]:
    """Extract terms from a local directory of JSON CV files."""
    result: set[Term] = set()

    for collection_path in sorted(cv_directory.glob("*.json")):
        if any(exc in collection_path.name for exc in GLOBAL_EXCLUDED_COLLECTIONS):
            continue

        contents = collection_path.read_text()
        validated = validate_typing(contents, project)
        prepared = prepare_json(validated, project, collection_path, skip_collections)

        for collection_key, terms in prepared.items():
            if field_mappings and collection_key in field_mappings:
                result |= _extract_field_level_terms(terms, field_mappings[collection_key])
                continue

            if only_collections and collection_key not in only_collections:
                continue

            for term_key in terms:
                result.add(Term(name=term_key, collection=collection_key))

    return result


def extract_term_data(
    cv_directory: Path,
    project: str,
    collections: list[str] | None = None,
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
        validated = validate_typing(contents, project)
        prepared = prepare_json(validated, project, collection_path)

        for collection_key, terms in prepared.items():
            if collections and collection_key not in collections:
                continue
            collection_data: dict[str, dict] = {}
            for term_key, term_content in terms.items():
                if isinstance(term_content, dict):
                    collection_data[term_key] = term_content
            if collection_data:
                result[collection_key] = collection_data

    return result


def extract_terms_esgvoc_format(
    repo_directory: Path,
    only_collections: list[str] | None = None,
    skip_collections: list[str] | None = None,
) -> set[Term]:
    """Extract terms from an esgvoc-format repo (one dir per collection, one JSON per term).

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


def extract_terms_emd(
    repo_directory: Path,
    field_mappings: dict[str, FieldMappingConfig],
) -> set[Term]:
    """Extract field-level terms from EMD src-data format.

    Each collection is a subdirectory with one JSON per term.
    field_mappings maps source collection to target collection + field name mappings.

    Example field_mappings:
        {"model": {"target_collection": "source",
                   "fields": {"validation_key": "drs_name", "description": "description"}}}
    """
    result: set[Term] = set()

    for source_collection, mapping in field_mappings.items():
        collection_dir = repo_directory / source_collection
        if not collection_dir.is_dir():
            continue

        target_collection = mapping["target_collection"]
        fields = mapping["fields"]

        for term_file in sorted(collection_dir.glob("*.json")):
            try:
                data = json.loads(term_file.read_text())
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSON: %s", term_file)
                continue
            raw_id = data.get("@id") or data.get("id") or term_file.stem
            term_id = raw_id.replace("-", "_")

            for source_field, target_field in fields.items():
                if source_field in data:
                    result.add(
                        Term(
                            name=term_id,
                            collection=target_collection,
                            field=target_field,
                            value=str(data[source_field]),
                        ),
                    )

    return result
