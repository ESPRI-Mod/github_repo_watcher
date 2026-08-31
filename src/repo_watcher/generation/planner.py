"""Core planning loop: decide what universe/project files to create, update, or skip."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from repo_watcher.config.schema import MultiStepConfig, TermOverrideConfig
from repo_watcher.domain import Term
from repo_watcher.generation.context import CONTEXT_FILENAME, detect_collection_mapping, detect_field_references
from repo_watcher.generation.multi_step import handle_multi_step_term
from repo_watcher.generation.prefill import prefill_from_cv_data
from repo_watcher.generation.report import (
    FieldChange,
    GenerationReport,
    TermFile,
    TermRef,
    TermSkip,
)
from repo_watcher.generation.skeleton import build_project_link, build_universe_skeleton


def _find_term_file_by_id(directory: Path, term_id: str) -> Path | None:
    """Search a directory for a JSON file whose ``"id"`` field matches *term_id*.

    Returns the path to the first matching file, or ``None`` if no match is found.
    """
    for f in sorted(directory.iterdir()):
        if f.name == CONTEXT_FILENAME or f.suffix != ".json":
            continue
        try:
            data = json.loads(f.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("id") == term_id:
            return f
    return None


def generate_missing_terms(
    missing_terms: set[Term],
    universe_repo: Path,
    project_repos: list[Path],
    project_name: str,
    universe_type_overrides: dict[str, str] | None = None,
    generation_defaults: dict[str, dict[str, Any]] | None = None,
    term_overrides: dict[str, dict[str, TermOverrideConfig]] | None = None,
    multi_step_terms: dict[str, MultiStepConfig] | None = None,
    source_prefill: dict[str, dict[str, str]] | None = None,
    source_term_data: dict[str, dict[str, dict]] | None = None,
    extra_field_values: dict[tuple[str, str], dict[str, str]] | None = None,
    update_terms: dict[tuple[str, str], dict[str, tuple[str, str]]] | None = None,
) -> GenerationReport:
    """Plan the generation of JSON files for missing terms in both repos.

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

    # Collect known field values and deduplicate terms
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
        descriptor = mapping.get(term.collection)
        if descriptor is None:
            report.skipped.append(
                TermSkip(term.collection, term.name, f"no mapping for collection '{term.collection}'")
            )
            continue

        term_id = term.name.lower()

        # Check for term overrides
        collection_overrides = term_overrides.get(term.collection) if term_overrides else None
        if collection_overrides and term.name in collection_overrides:
            _handle_term_override(term, term_id, descriptor, collection_overrides[term.name],
                                  universe_repo, project_repos, report)
            continue
        if "/" in term_id:
            report.skipped.append(
                TermSkip(term.collection, term.name, "term ID contains '/' (missing term_override?)")
            )
            continue

        # Check for multi-step terms
        multi_step_cfg = multi_step_terms.get(term.collection) if multi_step_terms else None
        if multi_step_cfg is not None:
            cv_desc = None
            if source_term_data and term.collection in source_term_data:
                cv_entry = source_term_data[term.collection].get(term.name, {})
                cv_desc = cv_entry.get("_value") if isinstance(cv_entry, dict) else None
            handle_multi_step_term(
                term, term_id, multi_step_cfg, universe_repo, project_repos,
                report, seen_members, cv_description=cv_desc,
            )
            continue

        # Extra fields from source_prefill that don't fit universe skeleton -> project link
        project_extra_fields: dict[str, object] = {}

        # Check universe
        universe_term_file = universe_repo / descriptor / f"{term_id}.json"
        if not universe_term_file.exists():
            universe_descriptor_dir = universe_repo / descriptor
            if universe_descriptor_dir.is_dir():
                universe_term_file = _find_term_file_by_id(universe_descriptor_dir, term_id) or universe_term_file
        term_update_key = (term.collection, term.name)
        if universe_term_file.exists():
            if update_terms and term_update_key in update_terms:
                report.universe_to_update.append(FieldChange(descriptor, term_id, update_terms[term_update_key]))
            else:
                report.universe_existed.append(TermRef(descriptor, term_id))
        else:
            universe_descriptor_dir = universe_repo / descriptor
            if not universe_descriptor_dir.is_dir():
                report.skipped.append(
                    TermSkip(term.collection, term_id, f"universe descriptor dir '{descriptor}' not found")
                )
                continue
            skeleton = build_universe_skeleton(universe_repo, descriptor, term_id)
            field_refs = detect_field_references(universe_repo, descriptor)
            # Pre-populate with known values from field-level terms
            field_values = known_values.get((term.collection, term.name), {})
            for field_name, value in field_values.items():
                if field_name in skeleton:
                    skeleton[field_name] = value
            # Pre-populate from source community CV data
            cv_collection = (source_term_data or {}).get(term.collection, {})
            cv_data = cv_collection.get(term.name, {})
            cv_original_key: str | None = term.name if cv_data else None
            # Case-insensitive fallback
            if not cv_data:
                for cv_key in cv_collection:
                    if cv_key.lower() == term.name.lower():
                        cv_data = cv_collection[cv_key]
                        cv_original_key = cv_key
                        break
            if cv_data:
                planned_ids = {tf.term_id for tf in report.universe_to_create}
                project_extra_fields = prefill_from_cv_data(
                    skeleton=skeleton,
                    term_name=term.name,
                    collection=term.collection,
                    descriptor=descriptor,
                    universe_repo=universe_repo,
                    project_name=project_name,
                    field_refs=field_refs,
                    collection_mapping=mapping,
                    cv_data=cv_data,
                    cv_original_key=cv_original_key,
                    source_prefill=source_prefill,
                    planned_ids=planned_ids,
                )
            # Apply generation defaults for fields still null/empty
            if generation_defaults and term.collection in generation_defaults:
                for field_name, default_value in generation_defaults[term.collection].items():
                    if field_name in skeleton and not skeleton[field_name]:
                        skeleton[field_name] = default_value
            report.universe_to_create.append(TermFile(descriptor, term_id, skeleton))

        # Project link — check across all project repos
        found_in_project = False
        has_collection_dir = False
        for repo in project_repos:
            project_collection_dir = repo / term.collection
            if not project_collection_dir.is_dir():
                continue
            has_collection_dir = True
            project_term_file = project_collection_dir / f"{term_id}.json"
            if not project_term_file.exists():
                project_term_file = _find_term_file_by_id(project_collection_dir, term_id) or project_term_file
            if project_term_file.exists():
                report.project_existed.append(TermRef(term.collection, term_id))
                found_in_project = True
                break
        if not found_in_project and has_collection_dir:
            link = build_project_link(descriptor, term_id)
            if project_extra_fields:
                link.update(project_extra_fields)
            report.project_to_create.append(TermFile(term.collection, term_id, link))

    return report


def _handle_term_override(
    term: Term,
    term_id: str,
    descriptor: str,
    override_cfg: TermOverrideConfig,
    universe_repo: Path,
    project_repos: list[Path],
    report: GenerationReport,
) -> None:
    """Handle a term that has an override config (e.g. tracking_id -> prefuuid)."""
    override_id = override_cfg["universe_term"]
    override_fields = override_cfg.get("fields", {})

    # Create universe term if requested and missing
    if override_cfg.get("create_universe"):
        universe_file = universe_repo / descriptor / f"{override_id}.json"
        if not universe_file.exists():
            universe_dir = universe_repo / descriptor
            if universe_dir.is_dir():
                universe_file = _find_term_file_by_id(universe_dir, override_id) or universe_file
        if universe_file.exists():
            report.universe_existed.append(TermRef(descriptor, override_id))
        else:
            universe_dir = universe_repo / descriptor
            if universe_dir.is_dir():
                skeleton = build_universe_skeleton(universe_repo, descriptor, override_id)
                skeleton.update(override_fields)
                report.universe_to_create.append(TermFile(descriptor, override_id, skeleton))

    # Build project link
    override_json = {
        "@context": CONTEXT_FILENAME,
        "id": override_id,
        "type": descriptor,
    }
    if not override_cfg.get("create_universe"):
        override_json.update(override_fields)
    found = False
    for repo in project_repos:
        collection_dir = repo / term.collection
        if not collection_dir.is_dir():
            continue
        override_file = collection_dir / f"{override_id}.json"
        if not override_file.exists():
            override_file = _find_term_file_by_id(collection_dir, override_id) or override_file
        if override_file.exists():
            report.project_existed.append(TermRef(term.collection, override_id))
            found = True
            break
    if not found:
        for repo in project_repos:
            if (repo / term.collection).is_dir():
                report.project_to_create.append(TermFile(term.collection, override_id, override_json))
                break
