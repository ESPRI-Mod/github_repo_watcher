"""Configuration data shapes parsed from repos_config.yml."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Required, TypedDict


class SourceConfig(TypedDict, total=False):
    """source: / reference: block in repos_config.yml."""

    type: Required[str]  # "git", "git_esgvoc_format", "git_emd", "registry"
    owner: str  # GitHub org (git sources only)
    repo: str  # GitHub repo name (git sources only)
    branch: str  # branch to clone (git sources only)
    project: str  # esgvoc project id (registry only)
    cv_subdir: str  # subdirectory containing CV files (e.g. "CVs")
    cv_format: str  # JSON format for extraction (e.g. "input4mips")


class TermOverrideConfig(TypedDict, total=False):
    """Single override entry, e.g. tracking_id -> prefuuid with custom regex."""

    universe_term: Required[str]  # id of the universe term to link to
    fields: dict[str, str]  # field overrides (e.g. {"regex": "^hdl:..."})
    create_universe: bool  # scaffold a new universe term if missing


class MultiStepConfig(TypedDict, total=False):
    """Config for expanding one term into member + group + project link.

    E.g. institution_id "IUM-FDU" -> institution/ium + institution/fdu
         + organisation/ium-fdu + institution_id/ium-fdu
    """

    separator: str  # split term_id by this (e.g. "-")
    member_descriptor: Required[str]  # universe type for each part (e.g. "institution")
    group_descriptor: Required[str]  # universe type for the wrapper (e.g. "organisation")


class FieldMappingConfig(TypedDict):
    """Maps a source collection to a target collection with field name mappings.

    Used by EMD-style field-level checks.
    """

    target_collection: str  # registry collection to compare against
    fields: dict[str, str]  # source_field -> target_field


class TermIdTransform(TypedDict):
    """A single find/replace rule for normalizing term IDs."""

    pattern: str  # substring to find
    replace: str  # replacement string


@dataclass
class CheckConfig:
    """Configuration for a single consistency check, parsed from repos_config.yml."""

    name: str
    source: SourceConfig
    reference: SourceConfig
    project: str
    skip_collections: list[str] | None = None
    only_collections: list[str] | None = None
    # collection_name -> collection_name (rename before comparing)
    collection_mappings: dict[str, str] | None = None
    # source_collection -> field mapping config
    field_mappings: dict[str, FieldMappingConfig] | None = None
    # collection -> list of regex patterns to exclude
    exclude_terms: dict[str, list[str]] | None = None
    # collection -> universe descriptor override
    universe_type_mappings: dict[str, str] | None = None
    # collection -> {field: default_value}
    generation_defaults: dict[str, dict[str, Any]] | None = None
    # collection -> list of find/replace rules
    term_id_transforms: dict[str, list[TermIdTransform]] | None = None
    # collection -> {term_pattern: override config}
    term_overrides: dict[str, dict[str, TermOverrideConfig]] | None = None
    # collection -> multi-step expansion config
    multi_step_terms: dict[str, MultiStepConfig] | None = None
    # collection -> {source_field: target_field}
    source_prefill: dict[str, dict[str, str]] | None = None
    # Project-specific metadata sync run after generation (e.g. "cordex_cmip6")
    metadata_profile: str | None = None


@dataclass
class MergedConfig:
    """Merged configuration across multiple CheckConfigs for the same project.

    Used by the generation pipeline to consolidate options from multiple checks.
    """

    excludes: dict[str, list[str]] = field(default_factory=dict)
    universe_type_mappings: dict[str, str] = field(default_factory=dict)
    generation_defaults: dict[str, dict[str, Any]] = field(default_factory=dict)
    term_id_transforms: dict[str, list] = field(default_factory=dict)
    term_overrides: dict[str, dict] = field(default_factory=dict)
    multi_step_terms: dict[str, dict] = field(default_factory=dict)
    source_prefill: dict[str, dict[str, str]] = field(default_factory=dict)
    metadata_profile: str | None = None
