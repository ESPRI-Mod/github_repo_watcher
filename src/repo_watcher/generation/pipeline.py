"""High-level generation pipeline: fetch, compare, merge configs, generate."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import esgvoc.api as ev

from repo_watcher.comparison import (
    _apply_exclusions,
    apply_term_id_transforms,
    compare_terms,
    identify_field_updates,
)
from repo_watcher.config.schema import MergedConfig
from repo_watcher.config.loader import merge_check_configs
from repo_watcher.domain import Term
from repo_watcher.extraction import extract_term_data
from repo_watcher.generation.context import detect_collection_mapping
from repo_watcher.generation.cordex_cmip6 import apply_cordex_cmip6_profile
from repo_watcher.generation.planner import generate_missing_terms
from repo_watcher.generation.report import GenerationReport
from repo_watcher.sources import build_source


def collect_and_generate(
    checks: list,
    universe_repo: Path,
    project_name: str,
    collections_filter: list[str] | None = None,
    on_progress: Any | None = None,
) -> GenerationReport:
    """Run all checks for a project, collect diffs, and generate missing term skeletons.

    This is the main orchestration function that ties together fetching, comparison,
    config merging, and generation.

    Parameters
    ----------
    checks : list[CheckConfig]
        All checks matching the target project.
    universe_repo : Path
        Path to the local WCRP-universe repo.
    project_name : str
        Project id (e.g. "cmip7").
    collections_filter : list[str] | None
        Only generate for these collections.
    on_progress : callable | None
        Optional callback for progress messages: on_progress(msg: str).
    """
    def _log(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    merged = merge_check_configs(checks)

    # Collect diffs across all checks
    all_missing: set[Term] = set()
    all_ref_not_source: set[Term] = set()
    all_source_field_values: dict[tuple[str, str], dict[str, str]] = {}
    source_repo_paths: list[Path] = []
    source_check_repos: list[tuple] = []

    for chk in checks:
        source = build_source(chk.source)
        ref = build_source(chk.reference)

        _log(f"[{chk.name}] Fetching {source.label()}...")
        source_terms = source.get_terms(
            project=chk.project,
            only_collections=chk.only_collections,
            skip_collections=chk.skip_collections,
            field_mappings=chk.field_mappings,
        )

        if chk.field_mappings:
            for t in source_terms:
                if t.field and t.value is not None:
                    all_source_field_values.setdefault((t.collection, t.name), {})[t.field] = t.value

        if chk.term_id_transforms:
            source_terms = apply_term_id_transforms(source_terms, chk.term_id_transforms)

        repo_path = source.repo_path()
        if repo_path is not None:
            source_repo_paths.append(repo_path)
            source_check_repos.append((chk, repo_path))

        _log(f"[{chk.name}] Fetching {ref.label()}...")
        ref_terms = ref.get_terms(
            project=chk.project,
            only_collections=chk.only_collections,
            skip_collections=chk.skip_collections,
            field_mappings=chk.field_mappings,
        )

        result = compare_terms(
            source_terms=source_terms,
            reference_terms=ref_terms,
            source_label=source.label(),
            reference_label=ref.label(),
            collection_mappings=chk.collection_mappings,
            exclude_terms=chk.exclude_terms,
        )
        all_missing |= result.in_source_not_reference
        all_ref_not_source |= result.in_reference_not_source

    # Build collection -> universe_type mapping
    type_mapping: dict[str, str] = {}
    _log(f"Detecting collection types from esgvoc/{project_name}...")
    for col in ev.get_all_collections_in_project(project_name):
        terms_list = list(ev.get_all_terms_in_collection(project_name, col))
        if terms_list:
            type_mapping[col] = terms_list[0].type

    for repo_path in source_repo_paths:
        context_mapping = detect_collection_mapping(repo_path)
        if context_mapping:
            type_mapping.update(context_mapping)
    type_mapping.update(merged.universe_type_mappings)

    # Find project repos for link checks
    project_repo_paths: list[Path] = [p for p in source_repo_paths if detect_collection_mapping(p)]
    if not project_repo_paths:
        project_repo_paths = list(source_repo_paths)

    if not project_repo_paths:
        msg = "No source repo path available for generation."
        raise ValueError(msg)

    if merged.excludes:
        all_missing = _apply_exclusions(all_missing, merged.excludes)

    if collections_filter:
        all_missing = {t for t in all_missing if t.collection in collections_filter}

    update_terms = identify_field_updates(all_missing, all_ref_not_source) or None

    if not all_missing and not update_terms and not merged.metadata_profile:
        return GenerationReport()

    # Extract source term data for prefill
    source_term_data: dict[str, dict[str, dict]] = {}
    extract_collections: set[str] = {t.collection for t in all_missing}
    extract_collections.update(merged.source_prefill.keys())
    extract_collections.update(merged.multi_step_terms.keys())
    if extract_collections:
        for chk, repo_path in source_check_repos:
            if chk.source.get("type") != "git":
                continue
            cv_subdir = chk.source.get("cv_subdir")
            cv_dir = repo_path / cv_subdir if cv_subdir else repo_path
            cv_format = chk.source.get("cv_format")
            data = extract_term_data(cv_dir, chk.project, collections=list(extract_collections), cv_format=cv_format)
            for col, terms in data.items():
                source_term_data.setdefault(col, {}).update(terms)

    report = generate_missing_terms(
        missing_terms=all_missing,
        universe_repo=universe_repo,
        project_repos=project_repo_paths,
        project_name=project_name,
        universe_type_overrides=type_mapping or None,
        generation_defaults=merged.generation_defaults or None,
        term_overrides=merged.term_overrides or None,
        multi_step_terms=merged.multi_step_terms or None,
        source_prefill=merged.source_prefill or None,
        source_term_data=source_term_data or None,
        extra_field_values=all_source_field_values or None,
        update_terms=update_terms,
    )
    if report.universe_to_create or report.universe_to_update:
        report.changed_by["universe"].add("generic")
    if report.project_to_create:
        report.changed_by["project"].add("generic")

    if merged.metadata_profile == "cordex_cmip6":
        apply_cordex_cmip6_profile(source_check_repos, universe_repo, report)
    elif merged.metadata_profile:
        msg = f"Unknown metadata profile: {merged.metadata_profile!r}"
        raise ValueError(msg)
    return report
