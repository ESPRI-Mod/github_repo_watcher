"""Layer 5 — Generation: scaffold missing term JSON files in universe and project repos.

Pure file I/O. No network, no esgvoc, no Prefect.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from models import MultiStepConfig, Term, TermOverrideConfig

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


def _sample_existing_term(descriptor_dir: Path) -> dict | None:
    """Read the first existing term JSON in a universe descriptor directory."""
    for f in sorted(descriptor_dir.iterdir()):
        if f.name == CONTEXT_FILENAME or not f.suffix == ".json":
            continue
        try:
            return json.loads(f.read_text())
        except json.JSONDecodeError:
            continue
    return None


def _build_universe_skeleton(universe_repo: Path, descriptor: str, term_id: str) -> dict:
    """Build a skeleton universe term JSON by sampling an existing term for field discovery."""
    result: dict = {
        "@context": CONTEXT_FILENAME,
        "id": term_id,
        "type": descriptor,
    }
    descriptor_dir = universe_repo / descriptor

    # Sample an existing term to discover fields and their default types
    sample = _sample_existing_term(descriptor_dir)
    if sample is None:
        return result

    for key, value in sample.items():
        if key in result:
            continue
        if isinstance(value, list):
            result[key] = []
        elif key in _STRING_DEFAULT_FIELDS:
            result[key] = ""
        else:
            result[key] = None

    # drs_name defaults to term_id (standard esgvoc convention)
    if "drs_name" in result and not result["drs_name"]:
        result["drs_name"] = term_id

    return result


def _build_project_link(descriptor: str, term_id: str) -> dict:
    """Build the minimal project link JSON (always 3 fields)."""
    return {
        "@context": CONTEXT_FILENAME,
        "id": term_id,
        "type": descriptor,
    }


def _handle_multi_step_term(
    term: Term,
    term_id: str,
    cfg: MultiStepConfig,
    universe_repo: Path,
    project_repos: list[Path],
    report: GenerationReport,
    seen_members: set[tuple[str, str]],
) -> None:
    """Expand a term into member + group + project link artifacts.

    For example, institution_id "IUM-FDU" with separator "-" produces:
    - institution/ium.json + institution/fdu.json (members)
    - organisation/ium-fdu.json with members: ["ium", "fdu"] (group)
    - institution_id/ium-fdu.json project link -> organisation
    """
    separator = cfg.get("separator")
    member_descriptor = cfg["member_descriptor"]
    group_descriptor = cfg["group_descriptor"]

    # Split into atomic members
    if separator:
        members = [m.lower() for m in term_id.split(separator)]
    else:
        members = [term_id]

    # Step 1: member_descriptor — one file per atomic member
    member_dir = universe_repo / member_descriptor
    if member_dir.is_dir():
        for member_id in members:
            key = (member_descriptor, member_id)
            if key in seen_members:
                continue
            seen_members.add(key)
            member_file = member_dir / f"{member_id}.json"
            if member_file.exists():
                report.universe_existed.append(key)
            else:
                skeleton = _build_universe_skeleton(universe_repo, member_descriptor, member_id)
                report.universe_to_create.append((member_descriptor, member_id, skeleton))

    # Step 2: group_descriptor — one file for the full term
    group_dir = universe_repo / group_descriptor
    if group_dir.is_dir():
        group_file = group_dir / f"{term_id}.json"
        if group_file.exists():
            report.universe_existed.append((group_descriptor, term_id))
        else:
            skeleton = _build_universe_skeleton(universe_repo, group_descriptor, term_id)
            skeleton["members"] = members
            report.universe_to_create.append((group_descriptor, term_id, skeleton))

    # Step 3: project link -> group_descriptor
    found_in_project = False
    has_collection_dir = False
    for repo in project_repos:
        collection_dir = repo / term.collection
        if not collection_dir.is_dir():
            continue
        has_collection_dir = True
        if (collection_dir / f"{term_id}.json").exists():
            report.project_existed.append((term.collection, term_id))
            found_in_project = True
            break
    if not found_in_project and has_collection_dir:
        link = _build_project_link(group_descriptor, term_id)
        report.project_to_create.append((term.collection, term_id, link))


def generate_missing_terms(
    missing_terms: set[Term],
    universe_repo: Path,
    project_repos: list[Path],
    universe_type_overrides: dict[str, str] | None = None,
    generation_defaults: dict[str, dict[str, Any]] | None = None,
    term_overrides: dict[str, dict[str, TermOverrideConfig]] | None = None,
    multi_step_terms: dict[str, MultiStepConfig] | None = None,
    source_prefill: dict[str, dict[str, str]] | None = None,
    source_term_data: dict[str, dict[str, dict]] | None = None,
    extra_field_values: dict[tuple[str, str], dict[str, str]] | None = None,
) -> GenerationReport:
    """Plan the generation of JSON files for missing terms in both repos.

    Currently only supports dry-run (reporting what would be created).
    For each missing term:
    1. Check if it matches a term_override — if so, create a project-level
       override of an existing universe term (with field overrides).
    2. Otherwise check if it exists in universe. If not, plan a skeleton.
    3. Plan a project link — checks all project_repos for existence.

    Returns a report of what would be created.
    """
    mapping: dict[str, str] = {}
    for repo in project_repos:
        mapping.update(detect_collection_mapping(repo))
    if universe_type_overrides:
        mapping.update(universe_type_overrides)
    report = GenerationReport()

    # Collect known field values from field-level terms (e.g. EMD provides drs_name, description).
    # Also deduplicate: multiple (name, collection, field=X) entries represent the same term.
    # Start with extra_field_values (all source fields, not just missing) as baseline,
    # then overlay with values from missing terms (which take priority).
    known_values: dict[tuple[str, str], dict[str, str]] = {}
    if extra_field_values:
        for key, fields in extra_field_values.items():
            known_values.setdefault(key, {}).update(fields)
    unique_terms: dict[tuple[str, str], Term] = {}
    for term in sorted(missing_terms, key=lambda t: (t.collection, t.name)):
        key = (term.collection, term.name)
        if key not in unique_terms:
            unique_terms[key] = term
        if term.field and term.value is not None:
            known_values.setdefault(key, {})[term.field] = term.value

    seen_members: set[tuple[str, str]] = set()

    for term in unique_terms.values():
        # Resolve universe descriptor
        descriptor = mapping.get(term.collection)
        if descriptor is None:
            report.skipped.append((term.collection, term.name, f"no mapping for collection '{term.collection}'"))
            continue

        # esgvoc IDs are always lowercase
        term_id = term.name.lower()

        # Check for term overrides (e.g. tracking_id: reuse universe "prefuuid"
        # with project-specific regex, or variant_label with a config-defined ID)
        collection_overrides = term_overrides.get(term.collection) if term_overrides else None
        if collection_overrides and term.name in collection_overrides:
            override_cfg = collection_overrides[term.name]
            override_id = override_cfg["universe_term"]
            override_fields = override_cfg.get("fields", {})

            # Create universe term if requested and missing
            if override_cfg.get("create_universe"):
                universe_file = universe_repo / descriptor / f"{override_id}.json"
                if universe_file.exists():
                    report.universe_existed.append((descriptor, override_id))
                else:
                    universe_dir = universe_repo / descriptor
                    if universe_dir.is_dir():
                        skeleton = _build_universe_skeleton(universe_repo, descriptor, override_id)
                        skeleton.update(override_fields)
                        report.universe_to_create.append((descriptor, override_id, skeleton))

            # Build a project-level override (universe term + field overrides)
            override_json = {
                "@context": CONTEXT_FILENAME,
                "id": override_id,
                "type": descriptor,
            }
            override_json.update(override_fields)
            found = False
            for repo in project_repos:
                override_file = repo / term.collection / f"{override_id}.json"
                if override_file.exists():
                    report.project_existed.append((term.collection, override_id))
                    found = True
                    break
            if not found:
                # Use the first repo that has the collection directory
                for repo in project_repos:
                    if (repo / term.collection).is_dir():
                        report.project_to_create.append((term.collection, override_id, override_json))
                        break
            continue

        # Check for multi-step terms (e.g. institution_id: institution + organisation + project link)
        multi_step_cfg = multi_step_terms.get(term.collection) if multi_step_terms else None
        if multi_step_cfg is not None:
            _handle_multi_step_term(term, term_id, multi_step_cfg, universe_repo, project_repos, report, seen_members)
            continue

        # Check universe
        universe_term_file = universe_repo / descriptor / f"{term_id}.json"
        if universe_term_file.exists():
            report.universe_existed.append((descriptor, term_id))
        else:
            universe_descriptor_dir = universe_repo / descriptor
            if not universe_descriptor_dir.is_dir():
                report.skipped.append((term.collection, term_id, f"universe descriptor dir '{descriptor}' not found"))
                continue
            skeleton = _build_universe_skeleton(universe_repo, descriptor, term_id)
            # Pre-populate with known values from field-level terms (e.g. EMD)
            field_values = known_values.get((term.collection, term.name), {})
            for field_name, value in field_values.items():
                if field_name in skeleton:
                    skeleton[field_name] = value
            # Pre-populate from source community CV data (e.g. label, release_year)
            if source_prefill and source_term_data and term.collection in source_prefill:
                field_map = source_prefill[term.collection]
                cv_data = source_term_data.get(term.collection, {}).get(term.name, {})
                for src_field, target_field in field_map.items():
                    if src_field in cv_data and target_field in skeleton:
                        skeleton[target_field] = cv_data[src_field]
            # Apply generation defaults for fields still null/empty
            if generation_defaults and term.collection in generation_defaults:
                for field_name, default_value in generation_defaults[term.collection].items():
                    if field_name in skeleton and skeleton[field_name] is None:
                        skeleton[field_name] = default_value
            report.universe_to_create.append((descriptor, term_id, skeleton))

        # Project link — check across all project repos
        found_in_project = False
        has_collection_dir = False
        for repo in project_repos:
            project_collection_dir = repo / term.collection
            if not project_collection_dir.is_dir():
                continue
            has_collection_dir = True
            project_term_file = project_collection_dir / f"{term_id}.json"
            if project_term_file.exists():
                report.project_existed.append((term.collection, term_id))
                found_in_project = True
                break
        if not found_in_project and has_collection_dir:
            link = _build_project_link(descriptor, term_id)
            report.project_to_create.append((term.collection, term_id, link))

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

    @property
    def has_work(self) -> bool:
        return bool(self.universe_to_create or self.project_to_create)

    def format_report(self) -> str:
        lines: list[str] = []

        if not self.has_work:
            return "Nothing to generate — all terms are up to date."

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

        return "\n".join(lines)
