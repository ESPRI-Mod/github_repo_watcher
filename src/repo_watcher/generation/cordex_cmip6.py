"""CORDEX-CMIP6 metadata profile: sync CV metadata the generic term comparison ignores.

Values come from the conventional CV (``main`` branch, ``CORDEX-CMIP6_<collection>.json``)
and are planned into the esgvoc-format project branch and the Universe:

- Project-only fields are always kept in the project term: ``source``, ``source_type``,
  ``license``, ``driving_source``, ``institution`` and ``driving_experiment_id``
  (the latter is removed when the CV does not restrict it).
- Fields shared with Universe (``activity_participation``, region ``description``,
  ``experiment``): Universe is filled where empty; ``activity_participation`` is appended
  to (union across projects), never reduced. The project term is only written when the
  CORDEX-CMIP6 value differs from Universe. Identical copies already in the project are kept.
- When Universe lists activities that CORDEX-CMIP6 does not, a notice asks for a manual check.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from repo_watcher.generation.context import CONTEXT_FILENAME
from repo_watcher.generation.planner import _find_term_file_by_id
from repo_watcher.generation.report import ContextChange, FieldChange, GenerationReport, TermFile

PROJECT_BRANCH = "esgvoc_dev"
PROPERTY_BASE = "https://esgvoc.ipsl.fr/property"

# Conventional-CV license URL -> license value of a project source term.
LICENSES = {
    "https://cordex.org/data-access/cordex-cmip6-data/cordex-cmip6-terms-of-use": {
        "id": "cordex-cmip6-terms-of-use",
        "exceptions-contact": None,
        "source-specific-info": "",
    },
}

# Project collection -> type of its terms (= Universe descriptor).
TERM_TYPES = {
    "source_id": "source",
    "driving_source_id": "source",
    "institution_id": "organisation",
    "domain_id": "region",
    "driving_experiment_id": "experiment",
}

# Project collection -> field that also exists on the Universe term.
SHARED_FIELDS = {
    "source_id": "activity_participation",
    "domain_id": "description",
    "driving_experiment_id": "experiment",
}

# JSON-LD context definitions for the fields written into project terms.
CONTEXT_TERMS = {
    "source": "https://esgvoc.ipsl.fr/resource/universe/source",
    "source_type": f"{PROPERTY_BASE}/source_type",
    "activity_participation": f"{PROPERTY_BASE}/activity_participation",
    "license": {"@id": "http://schema.org/license", "@type": "@json"},
    "driving_source": f"{PROPERTY_BASE}/driving_source",
    "driving_experiment_id": f"{PROPERTY_BASE}/driving_experiment_id",
    "institution": f"{PROPERTY_BASE}/institution",
    "description": "https://schema.org/description",
    "experiment": "https://esgvoc.ipsl.fr/resource/universe/experiment",
}


def apply_cordex_cmip6_profile(
    source_check_repos: list[tuple[Any, Path]],
    universe_repo: Path,
    report: GenerationReport,
) -> None:
    """Add the CORDEX-CMIP6 metadata changes to *report*."""
    conventional_repo = next((p for chk, p in source_check_repos if chk.source.get("type") == "git"), None)
    project_repo = next(
        (
            p
            for chk, p in source_check_repos
            if chk.source.get("type") == "git_esgvoc_format" and chk.source.get("branch") == PROJECT_BRANCH
        ),
        None,
    )
    if conventional_repo is None or project_repo is None:
        msg = f"The cordex_cmip6 profile needs a 'git' check and a 'git_esgvoc_format' check on '{PROJECT_BRANCH}'"
        raise ValueError(msg)
    reconcile(conventional_repo, universe_repo, project_repo, report)


def reconcile(conventional_repo: Path, universe_repo: Path, project_repo: Path, report: GenerationReport) -> None:
    """Plan Universe fills, project field changes and context additions."""
    for collection, terms in _desired_project_fields(conventional_repo).items():
        written: set[str] = set()
        for term_id, fields in terms.items():
            universe_values = {}
            if collection in SHARED_FIELDS:
                field = SHARED_FIELDS[collection]
                universe_values[field] = _plan_universe_field(
                    report,
                    universe_repo,
                    TERM_TYPES[collection],
                    term_id,
                    field,
                    fields[field],
                )
            changes, present = _plan_project_term(report, project_repo, collection, term_id, fields, universe_values)
            written |= present
            if "activity_participation" in changes:
                extra = [
                    a
                    for a in universe_values["activity_participation"] or []
                    if a not in changes["activity_participation"][1]
                ]
                if extra:
                    report.notices.append(
                        f"source/{term_id}: Universe activity_participation also lists {extra}, which "
                        "CORDEX-CMIP6 does not. Not removed from Universe; check manually whether still needed.",
                    )
        _plan_context(report, project_repo, collection, written)


def _desired_project_fields(conventional_repo: Path) -> dict[str, dict[str, dict[str, Any]]]:
    """Return {collection: {term_id: {field: value}}}; a None value means "field must be absent"."""

    def load(collection: str) -> Any:
        return json.loads((conventional_repo / f"CORDEX-CMIP6_{collection}.json").read_text())[collection]

    license_url = load("license")[0].rstrip("/")
    if license_url not in LICENSES:
        msg = f"Unknown CORDEX-CMIP6 license: {license_url!r}"
        raise ValueError(msg)

    return {
        "source_id": {
            key.lower(): {
                "source": [f"{v['label_extended']} ({v['release_year']})", v["label_extended"]],
                "source_type": v["source_type"],
                "activity_participation": v["activity_participation"],
                "license": LICENSES[license_url],
            }
            for key, v in load("source_id").items()
        },
        "driving_source_id": {
            key.lower(): {
                "driving_source": v["driving_source"],
                "driving_experiment_id": v.get("driving_experiment_id"),
            }
            for key, v in load("driving_source_id").items()
        },
        "institution_id": {key.lower(): {"institution": v} for key, v in load("institution_id").items()},
        "domain_id": {key.lower(): {"description": v["domain"]} for key, v in load("domain_id").items()},
        "driving_experiment_id": {key.lower(): {"experiment": v} for key, v in load("driving_experiment_id").items()},
    }


def _find_term_file(directory: Path, term_id: str) -> Path | None:
    path = directory / f"{term_id}.json"
    if path.is_file():
        return path
    return _find_term_file_by_id(directory, term_id) if directory.is_dir() else None


def _find_planned(items: list, descriptor: str, term_id: str) -> Any:
    return next((item for item in items if (item.descriptor, item.term_id) == (descriptor, term_id)), None)


def _plan_universe_field(
    report: GenerationReport,
    universe_repo: Path,
    descriptor: str,
    term_id: str,
    field: str,
    project_value: Any,
) -> Any:
    """Fill (or, for activity_participation, union) a Universe field; return its resulting value.

    A Universe term that neither exists nor is planned must be created before the project term
    can resolve, so it is assumed to get the CORDEX-CMIP6 value (same rule as the project script).
    """
    planned = _find_planned(report.universe_to_create, descriptor, term_id)
    update = on_disk = None
    if planned is not None:
        current = planned.content.get(field)
    else:
        path = _find_term_file(universe_repo / descriptor, term_id)
        if path is None:
            return project_value
        on_disk = json.loads(path.read_text()).get(field)
        update = _find_planned(report.universe_to_update, descriptor, term_id)
        current = update.changes[field][1] if update is not None and field in update.changes else on_disk

    if field == "activity_participation":
        current = current or []
        new = [*current, *(v for v in project_value if v not in current)]
    else:
        new = current or project_value

    if new != current:
        report.changed_by["universe"].add("metadata_profile")
        if planned is not None:
            planned.content[field] = new
        elif update is not None:
            update.changes[field] = (on_disk, new)
        else:
            report.universe_to_update.append(FieldChange(descriptor, term_id, {field: (on_disk, new)}))
    return new


def _plan_project_term(
    report: GenerationReport,
    project_repo: Path,
    collection: str,
    term_id: str,
    fields: dict[str, Any],
    universe_values: dict[str, Any],
) -> tuple[dict[str, tuple[Any, Any]], set[str]]:
    """Plan the project term's field changes; return them and the fields the term will contain."""
    planned = _find_planned(report.project_to_create, collection, term_id)
    if planned is not None:
        content = planned.content
    else:
        path = _find_term_file(project_repo / collection, term_id)
        content = json.loads(path.read_text()) if path is not None else None

    current = content if content is not None else {}
    changes: dict[str, tuple[Any, Any]] = {}
    for field, value in fields.items():
        # Shared fields are only written to the project when they differ from Universe.
        if field in universe_values and universe_values[field] == value and field not in current:
            continue
        if current.get(field) != value:
            changes[field] = (current.get(field), value)

    if changes:
        report.changed_by["project"].add("metadata_profile")
    if changes and planned is None and content is not None:
        # Existing file: record field changes; the writer merges them (None removes a field).
        report.project_to_update.append(FieldChange(collection, term_id, changes))
    elif changes:
        if content is None:
            content = {"@context": CONTEXT_FILENAME, "id": term_id, "type": TERM_TYPES[collection]}
            report.project_to_create.append(TermFile(collection, term_id, content))
        for field, (_old, new) in changes.items():
            if new is None:
                content.pop(field, None)
            else:
                content[field] = new
    present = {f for f, v in fields.items() if v is not None and (f in current or f in changes)}
    return changes, present


def _plan_context(report: GenerationReport, project_repo: Path, collection: str, fields: set[str]) -> None:
    """Plan context definitions for written fields that the collection context lacks."""
    path = project_repo / collection / CONTEXT_FILENAME
    context = json.loads(path.read_text())["@context"]
    missing = {f: CONTEXT_TERMS[f] for f in sorted(fields) if f not in context}
    if missing:
        report.changed_by["project"].add("metadata_profile")
        report.project_context_to_update.append(ContextChange(collection, missing))
