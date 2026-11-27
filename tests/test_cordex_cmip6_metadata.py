"""Tests for the CORDEX-CMIP6 metadata profile."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from repo_watcher.generation.cordex_cmip6 import reconcile
from repo_watcher.generation.report import GenerationReport, TermFile
from repo_watcher.generation.writer import write_report

LICENSE_URL = "https://cordex.org/data-access/cordex-cmip6-data/cordex-cmip6-terms-of-use"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")


def _read(path: Path) -> dict:
    return json.loads(path.read_text())


def _term(term_id: str, term_type: str, **fields) -> dict:
    return {"@context": "000_context.jsonld", "id": term_id, "type": term_type, **fields}


@pytest.fixture
def repos(tmp_path: Path) -> tuple[Path, Path, Path]:
    conventional = tmp_path / "conventional"
    cv = {
        "source_id": {
            "MODEL": {
                "label_extended": "Model",
                "release_year": "2026",
                "source_type": "ARCM",
                "activity_participation": ["DD"],
            },
            "SHARED": {
                "label_extended": "Shared",
                "release_year": "2025",
                "source_type": "ARCM",
                "activity_participation": ["DD"],
            },
        },
        "driving_source_id": {
            "ERA5": {"driving_source": "ERA5 description", "driving_experiment_id": ["evaluation"]},
            "GCM": {"driving_source": "GCM description"},
        },
        "institution_id": {"DKRZ": "German Climate Computing Center"},
        "domain_id": {"EUR-12": {"domain": "Europe"}, "AFR-12": {"domain": "Africa (CORDEX)"}},
        "driving_experiment_id": {"evaluation": "reanalysis simulation"},
        "license": [LICENSE_URL],
    }
    for collection, values in cv.items():
        _write(conventional / f"CORDEX-CMIP6_{collection}.json", {collection: values})

    universe = tmp_path / "universe"
    _write(universe / "source/model.json", _term("model", "source", activity_participation=["CMIP"], license={}))
    _write(universe / "source/shared.json", _term("shared", "source", activity_participation=["DD"]))
    _write(universe / "region/eur-12.json", _term("eur-12", "region", description=""))
    _write(universe / "region/afr-12.json", _term("afr-12", "region", description="Africa"))
    _write(universe / "experiment/evaluation.json", _term("evaluation", "experiment"))

    project = tmp_path / "project"
    for term_id, term_type, collection, fields in [
        ("model", "source", "source_id", {"unrelated": "keep"}),
        ("shared", "source", "source_id", {}),
        ("era5", "source", "driving_source_id", {}),
        ("gcm", "source", "driving_source_id", {"driving_experiment_id": ["stale"]}),
        ("eur-12", "region", "domain_id", {}),
        ("afr-12", "region", "domain_id", {}),
        ("evaluation", "experiment", "driving_experiment_id", {"experiment": "reanalysis simulation"}),
    ]:
        _write(project / collection / f"{term_id}.json", _term(term_id, term_type, **fields))
    for collection in ("source_id", "driving_source_id", "institution_id", "domain_id", "driving_experiment_id"):
        _write(project / collection / "000_context.jsonld", {"@context": {"custom": "https://example.org/keep"}})
    return conventional, universe, project


def test_universe_is_filled_and_unioned_but_gets_no_project_only_fields(repos) -> None:
    conventional, universe, project = repos
    report = GenerationReport()
    reconcile(conventional, universe, project, report)

    updates = {(u.descriptor, u.term_id): u.changes for u in report.universe_to_update}
    assert updates == {
        ("source", "model"): {"activity_participation": (["CMIP"], ["CMIP", "DD"])},
        ("region", "eur-12"): {"description": ("", "Europe")},
        ("experiment", "evaluation"): {"experiment": (None, "reanalysis simulation")},
    }


def test_project_only_overrides_shared_fields_that_differ_from_universe(repos) -> None:
    conventional, universe, project = repos
    report = GenerationReport()
    reconcile(conventional, universe, project, report)

    updates = {(u.descriptor, u.term_id): u.changes for u in report.project_to_update}
    # Universe union ["CMIP", "DD"] differs from CORDEX ["DD"] -> override, plus a notice
    assert updates[("source_id", "model")]["activity_participation"] == (None, ["DD"])
    assert report.notices == [
        "source/model: Universe activity_participation also lists ['CMIP'], which CORDEX-CMIP6 does not. "
        "Not removed from Universe; check manually whether still needed."
    ]
    assert updates[("source_id", "model")]["source"] == (None, ["Model (2026)", "Model"])
    assert updates[("source_id", "model")]["license"][1]["id"] == "cordex-cmip6-terms-of-use"
    # Universe equals CORDEX (after filling) -> no override
    assert "activity_participation" not in updates[("source_id", "shared")]
    assert ("domain_id", "eur-12") not in updates
    assert ("driving_experiment_id", "evaluation") not in updates  # identical copy is kept
    # Universe differs from CORDEX -> override
    assert updates[("domain_id", "afr-12")] == {"description": (None, "Africa (CORDEX)")}
    assert report.changed_by == {"universe": {"metadata_profile"}, "project": {"metadata_profile"}}
    # Restriction added where given, removed where unrestricted
    assert updates[("driving_source_id", "era5")]["driving_experiment_id"] == (None, ["evaluation"])
    assert updates[("driving_source_id", "gcm")]["driving_experiment_id"] == (["stale"], None)

    created = {(t.descriptor, t.term_id): t.content for t in report.project_to_create}
    assert created == {
        ("institution_id", "dkrz"): _term("dkrz", "organisation", institution="German Climate Computing Center")
    }

    contexts = {c.collection: set(c.terms) for c in report.project_context_to_update}
    assert contexts == {
        "source_id": {"source", "source_type", "activity_participation", "license"},
        "driving_source_id": {"driving_source", "driving_experiment_id"},
        "institution_id": {"institution"},
        "domain_id": {"description"},
        "driving_experiment_id": {"experiment"},
    }


def test_planned_project_links_are_augmented_instead_of_duplicated(repos) -> None:
    conventional, universe, project = repos
    (project / "source_id/model.json").unlink()
    report = GenerationReport(project_to_create=[TermFile("source_id", "model", _term("model", "source"))])
    reconcile(conventional, universe, project, report)

    model = [t for t in report.project_to_create if t.term_id == "model"]
    assert len(model) == 1
    assert model[0].content["source_type"] == "ARCM"


def test_writer_merges_fields_into_existing_project_files(repos) -> None:
    conventional, universe, project = repos
    report = GenerationReport()
    reconcile(conventional, universe, project, report)
    write_report(report, universe_repo=universe, project_repo=project)

    model = _read(project / "source_id/model.json")
    assert model["unrelated"] == "keep"
    assert model["source_type"] == "ARCM"
    assert "driving_experiment_id" not in _read(project / "driving_source_id/gcm.json")
    context = _read(project / "source_id/000_context.jsonld")["@context"]
    assert context["custom"] == "https://example.org/keep"
    assert context["license"] == {"@id": "http://schema.org/license", "@type": "@json"}
    assert _read(universe / "region/eur-12.json")["description"] == "Europe"

    # A second run finds nothing left to do.
    rerun = GenerationReport()
    reconcile(conventional, universe, project, rerun)
    assert not rerun.has_work


def test_unknown_license_is_rejected(repos) -> None:
    conventional, universe, project = repos
    _write(conventional / "CORDEX-CMIP6_license.json", {"license": ["https://example.org/other"]})
    with pytest.raises(ValueError, match="Unknown CORDEX-CMIP6 license"):
        reconcile(conventional, universe, project, GenerationReport())
