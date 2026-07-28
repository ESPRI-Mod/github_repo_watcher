"""Tests for generation.py — skeleton building, reporting, and file writing."""

from __future__ import annotations

import json
from pathlib import Path

from repo_watcher.generation import (
    FieldChange,
    GenerationReport,
    TermFile,
    TermRef,
    TermSkip,
    _build_project_link,
    _build_universe_skeleton,
    detect_collection_mapping,
    generate_missing_terms,
    write_report,
)
from repo_watcher.models import Term


def _make_universe(tmp_path: Path, descriptor: str, terms: dict[str, dict] | None = None) -> Path:
    """Create a minimal universe repo structure for testing."""
    desc_dir = tmp_path / "universe" / descriptor
    desc_dir.mkdir(parents=True)
    if terms:
        for term_id, content in terms.items():
            (desc_dir / f"{term_id}.json").write_text(json.dumps(content))
    return tmp_path / "universe"


def _make_project_repo(tmp_path: Path, collection: str, terms: list[str] | None = None) -> Path:
    """Create a minimal project repo structure for testing."""
    col_dir = tmp_path / "project" / collection
    col_dir.mkdir(parents=True)
    context = {"@context": {"@base": f"https://esgvoc.ipsl.fr/resource/universe/{collection}/"}}
    (col_dir / "000_context.jsonld").write_text(json.dumps(context))
    if terms:
        for tid in terms:
            content = {"@context": "000_context.jsonld", "id": tid, "type": collection}
            (col_dir / f"{tid}.json").write_text(json.dumps(content))
    return tmp_path / "project"


# ── _build_project_link ────────────────────────────────────


def test_build_project_link_has_three_fields():
    link = _build_project_link("source", "my_model")
    assert link == {"@context": "000_context.jsonld", "id": "my_model", "type": "source"}


# ── _build_universe_skeleton ───────────────────────────────


def test_build_universe_skeleton_no_existing_terms(tmp_path):
    universe = _make_universe(tmp_path, "model")
    skeleton = _build_universe_skeleton(universe, "model", "test_model")
    assert skeleton["id"] == "test_model"
    assert skeleton["type"] == "model"
    assert skeleton["@context"] == "000_context.jsonld"


def test_build_universe_skeleton_samples_existing_term(tmp_path):
    existing = {
        "@context": "000_context.jsonld",
        "id": "existing",
        "type": "model",
        "description": "A model",
        "drs_name": "existing",
        "labels": ["existing"],
    }
    universe = _make_universe(tmp_path, "model", {"existing": existing})
    skeleton = _build_universe_skeleton(universe, "model", "new_model")
    assert skeleton["id"] == "new_model"
    assert skeleton["description"] == ""
    assert skeleton["drs_name"] == "new_model"
    assert skeleton["labels"] == []


# ── detect_collection_mapping ──────────────────────────────


def test_detect_collection_mapping(tmp_path):
    project_repo = _make_project_repo(tmp_path, "source")
    mapping = detect_collection_mapping(project_repo)
    assert mapping["source"] == "source"


def test_detect_collection_mapping_extracts_descriptor_from_base_url(tmp_path):
    col_dir = tmp_path / "repo" / "grid_label"
    col_dir.mkdir(parents=True)
    context = {"@context": {"@base": "https://esgvoc.ipsl.fr/resource/universe/horizontal_grid_cell/"}}
    (col_dir / "000_context.jsonld").write_text(json.dumps(context))
    mapping = detect_collection_mapping(tmp_path / "repo")
    assert mapping["grid_label"] == "horizontal_grid_cell"


# ── generate_missing_terms ─────────────────────────────────


def test_generate_creates_new_term(tmp_path):
    universe = _make_universe(tmp_path, "model")
    project_repo = _make_project_repo(tmp_path, "source")
    missing = {Term(name="new_model", collection="source")}
    report = generate_missing_terms(
        missing_terms=missing,
        universe_repo=universe,
        project_repos=[project_repo],
        project_name="cmip7",
        universe_type_overrides={"source": "model"},
    )
    assert len(report.universe_to_create) == 1
    assert report.universe_to_create[0].term_id == "new_model"
    assert len(report.project_to_create) == 1


def test_generate_skips_existing_term(tmp_path):
    existing = {"@context": "000_context.jsonld", "id": "old_model", "type": "model"}
    universe = _make_universe(tmp_path, "model", {"old_model": existing})
    project_repo = _make_project_repo(tmp_path, "source", ["old_model"])
    missing = {Term(name="old_model", collection="source")}
    report = generate_missing_terms(
        missing_terms=missing,
        universe_repo=universe,
        project_repos=[project_repo],
        project_name="cmip7",
        universe_type_overrides={"source": "model"},
    )
    assert len(report.universe_to_create) == 0
    assert len(report.universe_existed) == 1
    assert len(report.project_existed) == 1


def test_generate_detects_updates(tmp_path):
    existing = {"@context": "000_context.jsonld", "id": "gn", "type": "model", "description": "old"}
    universe = _make_universe(tmp_path, "model", {"gn": existing})
    project_repo = _make_project_repo(tmp_path, "source", ["gn"])
    missing = {Term(name="gn", collection="source", field="description", value="new")}
    update_terms = {("source", "gn"): {"description": ("old", "new")}}
    report = generate_missing_terms(
        missing_terms=missing,
        universe_repo=universe,
        project_repos=[project_repo],
        project_name="cmip7",
        universe_type_overrides={"source": "model"},
        update_terms=update_terms,
    )
    assert len(report.universe_to_update) == 1
    assert report.universe_to_update[0].changes["description"] == ("old", "new")
    assert len(report.universe_existed) == 0


# ── write_report ───────────────────────────────────────────


def test_write_report_creates_files(tmp_path):
    universe = _make_universe(tmp_path, "model")
    content = {"@context": "000_context.jsonld", "id": "new", "type": "model"}
    report = GenerationReport(
        universe_to_create=[TermFile("model", "new", content)],
    )
    write_report(report, universe_repo=universe)
    written = json.loads((universe / "model" / "new.json").read_text())
    assert written["id"] == "new"


def test_write_report_updates_preserve_other_fields(tmp_path):
    existing = {"@context": "000_context.jsonld", "id": "gn", "type": "model", "description": "old", "extra": "keep"}
    universe = _make_universe(tmp_path, "model", {"gn": existing})
    report = GenerationReport(
        universe_to_update=[FieldChange("model", "gn", {"description": ("old", "new")})],
    )
    write_report(report, universe_repo=universe)
    updated = json.loads((universe / "model" / "gn.json").read_text())
    assert updated["description"] == "new"
    assert updated["extra"] == "keep"


# ── format_report ──────────────────────────────────────────


def test_format_report_no_work():
    report = GenerationReport()
    assert "up to date" in report.format_report()


def test_format_report_shows_creates():
    report = GenerationReport(
        universe_to_create=[TermFile("model", "new", {"id": "new"})],
    )
    text = report.format_report()
    assert "Universe terms to create (1)" in text
    assert "model/new.json" in text


def test_format_report_shows_updates():
    report = GenerationReport(
        universe_to_update=[FieldChange("model", "gn", {"description": ("old", "new")})],
    )
    text = report.format_report()
    assert "Universe terms to update (1)" in text
    assert "'old' -> 'new'" in text


def test_format_report_shows_skipped():
    report = GenerationReport(
        universe_to_create=[TermFile("model", "new", {"id": "new"})],
        skipped=[TermSkip("unknown_col", "term1", "no mapping")],
    )
    text = report.format_report()
    assert "Skipped (1)" in text
    assert "unknown_col/term1: no mapping" in text
