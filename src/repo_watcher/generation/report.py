"""Generation data structures: what to create, update, or skip."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TermFile:
    """A planned file operation (create or update)."""

    descriptor: str
    term_id: str
    content: dict


@dataclass(frozen=True)
class FieldChange:
    """A field value update on an existing term (new value None removes a project field)."""

    descriptor: str
    term_id: str
    changes: dict[str, tuple[Any, Any]]


@dataclass(frozen=True)
class ContextChange:
    """Term definitions to add to an existing project collection context."""

    collection: str
    terms: dict[str, Any]


@dataclass(frozen=True)
class TermRef:
    """A reference to an existing term (descriptor/collection + id)."""

    descriptor: str
    term_id: str


@dataclass(frozen=True)
class TermSkip:
    """A term skipped during generation with a reason."""

    collection: str
    term_id: str
    reason: str


@dataclass
class GenerationReport:
    """Report of what would be (or was) created by generate_missing_terms."""

    universe_to_create: list[TermFile] = field(default_factory=list)
    project_to_create: list[TermFile] = field(default_factory=list)
    universe_to_update: list[FieldChange] = field(default_factory=list)
    project_to_update: list[FieldChange] = field(default_factory=list)
    project_context_to_update: list[ContextChange] = field(default_factory=list)
    universe_existed: list[TermRef] = field(default_factory=list)
    project_existed: list[TermRef] = field(default_factory=list)
    skipped: list[TermSkip] = field(default_factory=list)
    # Points needing a manual decision (no file change is planned for them)
    notices: list[str] = field(default_factory=list)
    # Which steps planned changes per repo: "generic" and/or "metadata_profile"
    changed_by: dict[str, set[str]] = field(default_factory=lambda: {"universe": set(), "project": set()})

    @property
    def has_work(self) -> bool:
        return bool(
            self.universe_to_create
            or self.project_to_create
            or self.universe_to_update
            or self.project_to_update
            or self.project_context_to_update,
        )

    def format_report(self) -> str:
        lines: list[str] = []

        if not self.has_work:
            return "Nothing to generate — all terms are up to date."

        if self.universe_to_create:
            lines.append(f"Universe terms to create ({len(self.universe_to_create)}):")
            for tf in sorted(self.universe_to_create, key=lambda t: (t.descriptor, t.term_id)):
                lines.append(f"  {tf.descriptor}/{tf.term_id}.json:")
                lines.append(json.dumps(tf.content, indent=2, ensure_ascii=False))

        if self.project_to_create:
            lines.append(f"Project links to create ({len(self.project_to_create)}):")
            for tf in sorted(self.project_to_create, key=lambda t: (t.descriptor, t.term_id)):
                lines.append(f"  {tf.descriptor}/{tf.term_id}.json:")
                lines.append(json.dumps(tf.content, indent=2, ensure_ascii=False))

        for title, updates in (
            ("Universe terms to update", self.universe_to_update),
            ("Project terms to update", self.project_to_update),
        ):
            if not updates:
                continue
            lines.append(f"{title} ({len(updates)}):")
            for fc in sorted(updates, key=lambda t: (t.descriptor, t.term_id)):
                lines.append(f"  {fc.descriptor}/{fc.term_id}.json:")
                for field_name in sorted(fc.changes):
                    old_val, new_val = fc.changes[field_name]
                    lines.append(f"    {field_name}: {old_val!r} -> {new_val!r}")

        if self.project_context_to_update:
            lines.append(f"Project contexts to extend ({len(self.project_context_to_update)}):")
            for cc in sorted(self.project_context_to_update, key=lambda c: c.collection):
                lines.append(f"  {cc.collection}/000_context.jsonld:")
                for term, definition in cc.terms.items():
                    lines.append(f"    + {term}: {json.dumps(definition)}")

        if self.universe_existed:
            lines.append(f"Already in universe ({len(self.universe_existed)}):")
            grouped_e: defaultdict[str, list[str]] = defaultdict(list)
            for ref in self.universe_existed:
                grouped_e[ref.descriptor].append(ref.term_id)
            for descriptor in sorted(grouped_e):
                lines.append(f"  {descriptor}/: {', '.join(sorted(grouped_e[descriptor]))}")

        if self.project_existed:
            lines.append(f"Already in project ({len(self.project_existed)}):")
            grouped_pe: defaultdict[str, list[str]] = defaultdict(list)
            for ref in self.project_existed:
                grouped_pe[ref.descriptor].append(ref.term_id)
            for descriptor in sorted(grouped_pe):
                lines.append(f"  {descriptor}/: {', '.join(sorted(grouped_pe[descriptor]))}")

        if self.skipped:
            lines.append(f"Skipped ({len(self.skipped)}):")
            for skip in self.skipped:
                lines.append(f"  {skip.collection}/{skip.term_id}: {skip.reason}")

        if self.notices:
            lines.append(f"Notices ({len(self.notices)}):")
            lines.extend(f"  {notice}" for notice in self.notices)

        return "\n".join(lines)
