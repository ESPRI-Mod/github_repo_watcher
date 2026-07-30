"""Generation data structures: what to create, update, or skip."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TermFile:
    """A planned file operation (create or update)."""

    descriptor: str
    term_id: str
    content: dict


@dataclass(frozen=True)
class FieldChange:
    """A field value update on an existing universe term."""

    descriptor: str
    term_id: str
    changes: dict[str, tuple[str, str]]


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
    universe_existed: list[TermRef] = field(default_factory=list)
    project_existed: list[TermRef] = field(default_factory=list)
    skipped: list[TermSkip] = field(default_factory=list)

    @property
    def has_work(self) -> bool:
        return bool(self.universe_to_create or self.project_to_create or self.universe_to_update)

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

        if self.universe_to_update:
            lines.append(f"Universe terms to update ({len(self.universe_to_update)}):")
            for fc in sorted(self.universe_to_update, key=lambda t: (t.descriptor, t.term_id)):
                lines.append(f"  {fc.descriptor}/{fc.term_id}.json:")
                for field_name in sorted(fc.changes):
                    old_val, new_val = fc.changes[field_name]
                    lines.append(f"    {field_name}: {old_val!r} -> {new_val!r}")

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

        return "\n".join(lines)
