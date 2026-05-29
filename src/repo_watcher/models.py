"""Shared data models for the repo watcher."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Required, TypedDict


# ──────────────────────────────────────────────────────────
# TypedDicts for config structures
# ──────────────────────────────────────────────────────────


class SourceConfig(TypedDict, total=False):
    """source: / reference: block in repos_config.yml."""

    type: Required[str]  # "git", "git_esgvoc_format", "git_emd", "registry"
    owner: str  # GitHub org (git sources only)
    repo: str  # GitHub repo name (git sources only)
    branch: str  # branch to clone (git sources only)
    project: str  # esgvoc project id (registry only)


class TermOverrideConfig(TypedDict, total=False):
    """Single override entry, e.g. tracking_id → prefuuid with custom regex."""

    universe_term: Required[str]  # id of the universe term to link to
    fields: dict[str, str]  # field overrides (e.g. {"regex": "^hdl:..."})
    create_universe: bool  # scaffold a new universe term if missing


class MultiStepConfig(TypedDict, total=False):
    """Config for expanding one term into member + group + project link.

    E.g. institution_id "IUM-FDU" → institution/ium + institution/fdu
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
    fields: dict[str, str]  # source_field → target_field


class TermIdTransform(TypedDict):
    """A single find/replace rule for normalizing term IDs."""

    pattern: str  # substring to find
    replace: str  # replacement string


class ReportMode(str, Enum):
    """Which differences to show in the report."""

    missing_in_ref = "missing_in_ref"  # in source but not reference (default)
    missing_in_src = "missing_in_src"  # in reference but not source
    both = "both"  # both directions


@dataclass(frozen=True)
class Term:
    """A vocabulary term, the atom of every consistency check.

    For classic projects (cordex, cmip6, etc.), only name and collection are set.
    For EMD field-level checks, field and value are also set so that comparison
    can detect value mismatches via plain set operations.
    """

    name: str
    collection: str
    field: str | None = None
    value: str | None = None

    @property
    def key(self) -> str:
        parts = [self.collection.lower(), self.name.lower()]
        if self.field:
            parts.append(self.field.lower())
        if self.value is not None:
            parts.append(self.value.lower())
        return "/".join(parts)

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Term):
            return NotImplemented
        return self.key == other.key


@dataclass
class CheckConfig:
    """Configuration for a single consistency check, parsed from repos_config.yml."""

    name: str
    source: SourceConfig
    reference: SourceConfig
    project: str
    skip_collections: list[str] | None = None
    only_collections: list[str] | None = None
    # collection_name → collection_name (rename before comparing)
    collection_mappings: dict[str, str] | None = None
    # source_collection → field mapping config
    field_mappings: dict[str, FieldMappingConfig] | None = None
    # collection → list of regex patterns to exclude
    exclude_terms: dict[str, list[str]] | None = None
    # collection → universe descriptor override
    universe_type_mappings: dict[str, str] | None = None
    # collection → {field: default_value}
    generation_defaults: dict[str, dict[str, Any]] | None = None
    # collection → list of find/replace rules
    term_id_transforms: dict[str, list[TermIdTransform]] | None = None
    # collection → {term_pattern: override config}
    term_overrides: dict[str, dict[str, TermOverrideConfig]] | None = None
    # collection → multi-step expansion config
    multi_step_terms: dict[str, MultiStepConfig] | None = None
    # collection → {source_field: target_field}
    source_prefill: dict[str, dict[str, str]] | None = None


@dataclass
class ConsistencyResult:
    """Result of comparing two sets of terms."""

    source_label: str
    reference_label: str
    in_source_not_reference: set[Term] = field(default_factory=set)
    in_reference_not_source: set[Term] = field(default_factory=set)
    common: set[Term] = field(default_factory=set)

    @property
    def is_consistent(self) -> bool:
        return len(self.in_source_not_reference) == 0

    def _format_diff(self, terms: set[Term], header: str) -> list[str]:
        if not terms:
            return []
        lines = [header]
        grouped: defaultdict[str, list[str]] = defaultdict(list)
        for term in terms:
            label = term.name
            if term.field:
                label = f"{term.name}.{term.field}={term.value}"
            grouped[term.collection].append(label)
        for collection in sorted(grouped):
            lines.append(f"  {collection}:")
            for name in sorted(grouped[collection]):
                lines.append(f"    - {name}")
        return lines

    def format_report(self, mode: ReportMode = ReportMode.missing_in_ref) -> str:
        lines: list[str] = []
        if mode in (ReportMode.missing_in_ref, ReportMode.both):
            lines.extend(
                self._format_diff(
                    self.in_source_not_reference,
                    f"Terms in {self.source_label} not found in {self.reference_label}:",
                )
            )
        if mode in (ReportMode.missing_in_src, ReportMode.both):
            lines.extend(
                self._format_diff(
                    self.in_reference_not_source,
                    f"Terms in {self.reference_label} not found in {self.source_label}:",
                )
            )
        if not lines:
            lines.append(f"{self.source_label} and {self.reference_label} are consistent ({len(self.common)} terms)")
        return "\n".join(lines)


@dataclass
class ProjectResult:
    """Aggregated result of all checks for a single project."""

    project: str
    reference_label: str
    check_results: list[tuple[str, ConsistencyResult]] = field(default_factory=list)

    def _format_diff_grouped(self, terms: set[Term]) -> list[str]:
        if not terms:
            return []
        lines: list[str] = []
        grouped: defaultdict[str, list[str]] = defaultdict(list)
        for term in terms:
            label = term.name
            if term.field:
                label = f"{term.name}.{term.field}={term.value}"
            grouped[term.collection].append(label)
        for collection in sorted(grouped):
            lines.append(f"  {collection}:")
            for name in sorted(grouped[collection]):
                lines.append(f"    - {name}")
        return lines

    def format_report(self, mode: ReportMode = ReportMode.missing_in_ref, detail: bool = False) -> str:
        n_sources = len(self.check_results)
        lines = [f"[{self.project}] {n_sources} source(s) checked against {self.reference_label}", ""]

        # Aggregate across all checks
        all_in_source_not_ref: set[Term] = set()
        all_in_ref_not_source: set[Term] = set()
        all_common: set[Term] = set()
        for _label, r in self.check_results:
            all_in_source_not_ref |= r.in_source_not_reference
            all_in_ref_not_source |= r.in_reference_not_source
            all_common |= r.common

        has_diff = False
        if mode in (ReportMode.missing_in_ref, ReportMode.both) and all_in_source_not_ref:
            lines.append(f"Missing in {self.reference_label} (across all sources):")
            lines.extend(self._format_diff_grouped(all_in_source_not_ref))
            has_diff = True
        if mode in (ReportMode.missing_in_src, ReportMode.both) and all_in_ref_not_source:
            lines.append(f"Missing in sources (not covered by any source):")
            lines.extend(self._format_diff_grouped(all_in_ref_not_source))
            has_diff = True

        if not has_diff:
            lines.append(f"All consistent ({len(all_common)} terms)")

        # Per-source breakdown
        if detail:
            lines.extend(["", "Per source:"])
            for source_label, r in self.check_results:
                terms = set()
                if mode in (ReportMode.missing_in_ref, ReportMode.both):
                    terms |= r.in_source_not_reference
                if mode in (ReportMode.missing_in_src, ReportMode.both):
                    terms |= r.in_reference_not_source
                if terms:
                    lines.append(f"  {source_label}:")
                    lines.extend(f"  {line}" for line in self._format_diff_grouped(terms))
                else:
                    lines.append(f"  {source_label}: consistent ({len(r.common)} terms)")

        return "\n".join(lines)
