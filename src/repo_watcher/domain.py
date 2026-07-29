"""Domain objects: the core data types of the repo watcher."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum


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

        # Filter out field-level terms whose base term already exists in common
        # (these are value diffs on known terms, not actionable for PR generation)
        common_bases = {(t.collection.lower(), t.name.lower()) for t in all_common}
        all_in_source_not_ref = {
            t for t in all_in_source_not_ref
            if not t.field or (t.collection.lower(), t.name.lower()) not in common_bases
        }
        all_in_ref_not_source = {
            t for t in all_in_ref_not_source
            if not t.field or (t.collection.lower(), t.name.lower()) not in common_bases
        }

        has_diff = False
        if mode in (ReportMode.missing_in_ref, ReportMode.both) and all_in_source_not_ref:
            lines.append(f"Missing in {self.reference_label} (across all sources):")
            lines.extend(self._format_diff_grouped(all_in_source_not_ref))
            has_diff = True
        if mode in (ReportMode.missing_in_src, ReportMode.both) and all_in_ref_not_source:
            lines.append("Missing in sources (not covered by any source):")
            lines.extend(self._format_diff_grouped(all_in_ref_not_source))
            has_diff = True

        if not has_diff:
            lines.append(f"All consistent ({len(all_common)} terms)")

        # Per-source breakdown
        if detail:
            lines.extend(["", "Per source:"])
            for source_label, r in self.check_results:
                per_common = {(t.collection.lower(), t.name.lower()) for t in r.common}
                terms = set()
                if mode in (ReportMode.missing_in_ref, ReportMode.both):
                    terms |= r.in_source_not_reference
                if mode in (ReportMode.missing_in_src, ReportMode.both):
                    terms |= r.in_reference_not_source
                terms = {
                    t for t in terms
                    if not t.field or (t.collection.lower(), t.name.lower()) not in per_common
                }
                if terms:
                    lines.append(f"  {source_label}:")
                    lines.extend(f"  {line}" for line in self._format_diff_grouped(terms))
                else:
                    lines.append(f"  {source_label}: consistent ({len(r.common)} terms)")

        return "\n".join(lines)
