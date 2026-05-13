"""Shared data models for the repo watcher."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


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
    source: dict[str, Any]
    reference: dict[str, Any]
    project: str
    skip_collections: list[str] | None = None
    only_collections: list[str] | None = None
    collection_mappings: dict[str, str] | None = None
    field_mappings: dict[str, dict[str, Any]] | None = None


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

    def format_report(self) -> str:
        lines: list[str] = []
        if self.in_source_not_reference:
            lines.append(f"Terms in {self.source_label} not found in {self.reference_label}:")
            grouped: defaultdict[str, list[str]] = defaultdict(list)
            for term in self.in_source_not_reference:
                label = term.name
                if term.field:
                    label = f"{term.name}.{term.field}={term.value}"
                grouped[term.collection].append(label)
            for collection in sorted(grouped):
                lines.append(f"  {collection}:")
                for name in sorted(grouped[collection]):
                    lines.append(f"    - {name}")
        if self.in_reference_not_source:
            lines.append(f"Terms in {self.reference_label} not found in {self.source_label}:")
            grouped_ref: defaultdict[str, list[str]] = defaultdict(list)
            for term in self.in_reference_not_source:
                label = term.name
                if term.field:
                    label = f"{term.name}.{term.field}={term.value}"
                grouped_ref[term.collection].append(label)
            for collection in sorted(grouped_ref):
                lines.append(f"  {collection}:")
                for name in sorted(grouped_ref[collection]):
                    lines.append(f"    - {name}")
        if not lines:
            lines.append(f"{self.source_label} and {self.reference_label} are consistent ({len(self.common)} terms)")
        return "\n".join(lines)
