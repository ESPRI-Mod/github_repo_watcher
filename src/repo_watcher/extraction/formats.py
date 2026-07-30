"""CV format detection, validation, and normalization."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

# Type aliases
type ParamStr = str
type ParamDict = dict[str, Any]
type TermKey = str
type TermDict = dict[TermKey, ParamStr | ParamDict]
type CollectionKey = str
type NotCollectionKey = str
type VocabularyDict = dict[CollectionKey, TermDict]

# Input types for validating JSON files per project.
type CordexCMIP6InputDict = dict[CollectionKey, list[TermKey] | TermDict]
type CMIP6InputDict = dict[CollectionKey, list[TermKey] | TermDict]
type CMIP6PlusInputDict = dict[CollectionKey, TermKey | list[TermKey] | TermDict]
type Input4MipsInputDict = list[TermKey] | TermDict | dict[NotCollectionKey, TermDict]

INPUT_TYPES: dict[str, type] = {
    "cordex-cmip6": CordexCMIP6InputDict,
    "cmip6": CMIP6InputDict,
    "cmip6plus": CMIP6PlusInputDict,
    "input4mips": Input4MipsInputDict,
}

# Collections to always skip
GLOBAL_EXCLUDED_COLLECTIONS = [
    "DRS",
    "required_global_attributes",
    "global_attrs",
]


def _resolve_format(project: str, cv_format: str | None = None) -> str:
    """Resolve the CV format key: explicit cv_format wins, else fall back to project."""
    fmt = cv_format or project
    if fmt not in INPUT_TYPES:
        msg = f"Unknown CV format: {fmt}"
        raise KeyError(msg)
    return fmt


def validate_typing(data: str, project: str, cv_format: str | None = None) -> dict | list:
    """Validate JSON data against the expected type for a project."""
    fmt = _resolve_format(project, cv_format)
    return TypeAdapter(INPUT_TYPES[fmt]).validate_json(data)


def prepare_json(
    data: dict | list,
    project: str,
    collection_path: Path,
    skip_collections: list[str] | None = None,
    cv_format: str | None = None,
) -> VocabularyDict:
    """Normalize any project's JSON CV format into {collection: {term: params}}."""
    fmt = _resolve_format(project, cv_format)
    all_skips = list(GLOBAL_EXCLUDED_COLLECTIONS)
    if skip_collections:
        all_skips.extend(skip_collections)

    if fmt == "input4mips":
        return _prepare_json_input4mip(data, collection_path, all_skips)
    return _prepare_json_standard(data, fmt, all_skips)


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
