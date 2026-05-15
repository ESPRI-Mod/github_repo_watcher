"""Layer 5 — Generation: scaffold missing term JSON files in universe and project repos.

Pure file I/O. No network, no esgvoc, no Prefect.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from models import Term

CONTEXT_FILENAME = "000_context.jsonld"

# JSON-LD structural keys to skip when discovering term fields from context
_JSONLD_STRUCTURAL_KEYS = {"@base", "@vocab", "id", "type"}

# Fields that get empty string defaults instead of null
_STRING_DEFAULT_FIELDS = {"description", "drs_name", "name"}


def detect_collection_mapping(project_repo: Path) -> dict[str, str]:
    """Parse @base URLs from project 000_context.jsonld files to build collection->universe mapping.

    Returns e.g. {"source": "model", "institution": "organisation", "grid_label": "horizontal_grid_cell"}.
    Collections where the name matches the universe descriptor are included too.
    """
    mapping: dict[str, str] = {}
    for collection_dir in sorted(project_repo.iterdir()):
        if not collection_dir.is_dir():
            continue
        context_file = collection_dir / CONTEXT_FILENAME
        if not context_file.exists():
            continue
        try:
            data = json.loads(context_file.read_text())
        except json.JSONDecodeError:
            continue
        context = data.get("@context", {})
        base_url = context.get("@base", "")
        if not base_url:
            continue
        # Extract universe descriptor from @base URL like
        # "https://esgvoc.ipsl.fr/resource/universe/model/"
        descriptor = base_url.rstrip("/").rsplit("/", 1)[-1]
        mapping[collection_dir.name] = descriptor
    return mapping


def _build_universe_skeleton(universe_repo: Path, descriptor: str, term_id: str) -> dict:
    """Build a skeleton universe term JSON by parsing the context file for field discovery."""
    result: dict = {
        "@context": CONTEXT_FILENAME,
        "id": term_id,
        "type": descriptor,
    }
    context_file = universe_repo / descriptor / CONTEXT_FILENAME
    if not context_file.exists():
        return result

    try:
        data = json.loads(context_file.read_text())
    except json.JSONDecodeError:
        return result

    context = data.get("@context", {})
    for key, value in context.items():
        if key in _JSONLD_STRUCTURAL_KEYS:
            continue
        if key in result:
            continue
        # Reference fields (with @type: @id) default to empty list
        if isinstance(value, dict) and value.get("@type") == "@id":
            result[key] = []
        elif key in _STRING_DEFAULT_FIELDS:
            result[key] = ""
        else:
            result[key] = None

    return result


def _build_project_link(descriptor: str, term_id: str) -> dict:
    """Build the minimal project link JSON (always 3 fields)."""
    return {
        "@context": CONTEXT_FILENAME,
        "id": term_id,
        "type": descriptor,
    }


def generate_missing_terms(
    missing_terms: set[Term],
    universe_repo: Path,
    project_repo: Path,
) -> GenerationReport:
    """Plan the generation of JSON files for missing terms in both repos.

    Currently only supports dry-run (reporting what would be created).
    For each missing term:
    1. Check if it exists in universe. If not, plan a skeleton in universe repo.
    2. Plan a project link in project repo.

    Returns a report of what would be created.
    """
    mapping = detect_collection_mapping(project_repo)
    report = GenerationReport()

    # Collect known field values from field-level terms (e.g. EMD provides drs_name, description).
    # Also deduplicate: multiple (name, collection, field=X) entries represent the same term.
    known_values: dict[tuple[str, str], dict[str, str]] = {}
    unique_terms: dict[tuple[str, str], Term] = {}
    for term in sorted(missing_terms, key=lambda t: (t.collection, t.name)):
        key = (term.collection, term.name)
        if key not in unique_terms:
            unique_terms[key] = term
        if term.field and term.value is not None:
            known_values.setdefault(key, {})[term.field] = term.value

    for term in unique_terms.values():
        # Resolve universe descriptor
        descriptor = mapping.get(term.collection)
        if descriptor is None:
            report.skipped.append((term.collection, term.name, f"no mapping for collection '{term.collection}'"))
            continue

        # Check project collection directory exists
        project_collection_dir = project_repo / term.collection
        if not project_collection_dir.is_dir():
            report.skipped.append((term.collection, term.name, f"collection dir '{term.collection}' not found in project repo"))
            continue

        # Check universe
        universe_term_file = universe_repo / descriptor / f"{term.name}.json"
        if universe_term_file.exists():
            report.universe_existed.append((descriptor, term.name))
        else:
            universe_descriptor_dir = universe_repo / descriptor
            if not universe_descriptor_dir.is_dir():
                report.skipped.append((term.collection, term.name, f"universe descriptor dir '{descriptor}' not found"))
                continue
            skeleton = _build_universe_skeleton(universe_repo, descriptor, term.name)
            # Pre-populate with known values from field-level terms (e.g. EMD)
            field_values = known_values.get((term.collection, term.name), {})
            for field_name, value in field_values.items():
                if field_name in skeleton:
                    skeleton[field_name] = value
            report.universe_to_create.append((descriptor, term.name, skeleton))

        # Project link
        link = _build_project_link(descriptor, term.name)
        project_term_file = project_repo / term.collection / f"{term.name}.json"
        if project_term_file.exists():
            report.project_existed.append((term.collection, term.name))
        else:
            report.project_to_create.append((term.collection, term.name, link))

    return report


def write_report(report: GenerationReport, universe_repo: Path, project_repo: Path) -> None:
    """Write the JSON files planned in a GenerationReport to disk."""
    for descriptor, term_id, skeleton in report.universe_to_create:
        path = universe_repo / descriptor / f"{term_id}.json"
        path.write_text(json.dumps(skeleton, indent=2, ensure_ascii=False) + "\n")
    for collection, term_id, link in report.project_to_create:
        path = project_repo / collection / f"{term_id}.json"
        path.write_text(json.dumps(link, indent=4, ensure_ascii=False) + "\n")


@dataclass
class GenerationReport:
    """Report of what would be (or was) created by generate_missing_terms."""

    universe_to_create: list[tuple[str, str, dict]] = field(default_factory=list)
    project_to_create: list[tuple[str, str, dict]] = field(default_factory=list)
    universe_existed: list[tuple[str, str]] = field(default_factory=list)
    project_existed: list[tuple[str, str]] = field(default_factory=list)
    skipped: list[tuple[str, str, str]] = field(default_factory=list)

    def format_report(self) -> str:
        lines: list[str] = []

        if self.universe_to_create:
            lines.append(f"Universe terms to create ({len(self.universe_to_create)}):")
            for descriptor, term_id, skeleton in sorted(self.universe_to_create):
                lines.append(f"  {descriptor}/{term_id}.json:")
                lines.append(json.dumps(skeleton, indent=2, ensure_ascii=False))

        if self.project_to_create:
            lines.append(f"Project links to create ({len(self.project_to_create)}):")
            for collection, term_id, link in sorted(self.project_to_create):
                lines.append(f"  {collection}/{term_id}.json:")
                lines.append(json.dumps(link, indent=4, ensure_ascii=False))

        if self.universe_existed:
            lines.append(f"Already in universe ({len(self.universe_existed)}):")
            grouped_e: defaultdict[str, list[str]] = defaultdict(list)
            for descriptor, term_id in self.universe_existed:
                grouped_e[descriptor].append(term_id)
            for descriptor in sorted(grouped_e):
                lines.append(f"  {descriptor}/: {', '.join(sorted(grouped_e[descriptor]))}")

        if self.project_existed:
            lines.append(f"Already in project ({len(self.project_existed)}):")
            grouped_pe: defaultdict[str, list[str]] = defaultdict(list)
            for collection, term_id in self.project_existed:
                grouped_pe[collection].append(term_id)
            for collection in sorted(grouped_pe):
                lines.append(f"  {collection}/: {', '.join(sorted(grouped_pe[collection]))}")

        if self.skipped:
            lines.append(f"Skipped ({len(self.skipped)}):")
            for collection, term_id, reason in self.skipped:
                lines.append(f"  {collection}/{term_id}: {reason}")

        if not lines:
            lines.append("Nothing to generate.")

        return "\n".join(lines)
