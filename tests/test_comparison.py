"""Tests for comparison.py — pure set operations, no network."""

from repo_watcher.comparison import compare_terms, identify_field_updates
from repo_watcher.domain import ReportMode, Term


def test_identical_sets():
    terms = {Term(name="day", collection="frequency")}
    result = compare_terms(terms, terms, "source", "ref")
    assert result.is_consistent
    assert len(result.common) == 1


def test_source_has_extra():
    source = {
        Term(name="day", collection="frequency"),
        Term(name="FAKE", collection="activity_id"),
    }
    reference = {Term(name="day", collection="frequency")}
    result = compare_terms(source, reference, "source", "ref")
    assert not result.is_consistent
    assert Term(name="FAKE", collection="activity_id") in result.in_source_not_reference


def test_reference_has_extra():
    source = {Term(name="day", collection="frequency")}
    reference = {
        Term(name="day", collection="frequency"),
        Term(name="mon", collection="frequency"),
    }
    result = compare_terms(source, reference, "source", "ref")
    assert result.is_consistent  # is_consistent only checks source extras
    assert Term(name="mon", collection="frequency") in result.in_reference_not_source


def test_case_insensitive_matching():
    source = {Term(name="AerChemMIP", collection="activity_id")}
    reference = {Term(name="aerchemmip", collection="activity_id")}
    result = compare_terms(source, reference, "source", "ref")
    assert result.is_consistent


def test_empty_sets():
    result = compare_terms(set(), set(), "source", "ref")
    assert result.is_consistent
    assert len(result.common) == 0


def test_collection_mappings():
    source = {Term(name="day", collection="table_id")}
    reference = {Term(name="day", collection="frequency")}
    result = compare_terms(
        source,
        reference,
        "source",
        "ref",
        collection_mappings={"table_id": "frequency"},
    )
    assert result.is_consistent


def test_format_report_groups_by_collection():
    source = {
        Term(name="FAKE1", collection="activity_id"),
        Term(name="FAKE2", collection="activity_id"),
        Term(name="NOPE", collection="frequency"),
    }
    result = compare_terms(source, set(), "source", "ref")
    report = result.format_report(mode=ReportMode.missing_in_ref)
    assert "activity_id:" in report
    assert "frequency:" in report
    assert "FAKE1" in report
    assert "NOPE" in report


def test_format_report_consistent():
    terms = {Term(name="day", collection="frequency")}
    result = compare_terms(terms, terms, "source", "ref")
    report = result.format_report()
    assert "consistent" in report


def test_field_level_terms_different_values():
    source = {Term(name="model1", collection="source", field="resolution", value="100km")}
    reference = {Term(name="model1", collection="source", field="resolution", value="250km")}
    result = compare_terms(source, reference, "source", "ref")
    assert not result.is_consistent
    assert len(result.in_source_not_reference) == 1
    assert len(result.in_reference_not_source) == 1


# ── identify_field_updates ──────────────────────────────────


def test_identify_field_updates_detects_value_change():
    in_src = {Term(name="gn", collection="grid_label", field="description", value="new desc")}
    in_ref = {Term(name="gn", collection="grid_label", field="description", value="old desc")}
    updates = identify_field_updates(in_src, in_ref)
    assert ("grid_label", "gn") in updates
    assert updates[("grid_label", "gn")]["description"] == ("old desc", "new desc")


def test_identify_field_updates_ignores_genuinely_missing():
    """A term only in source (no counterpart in ref) is not an update."""
    in_src = {Term(name="gn", collection="grid_label", field="description", value="some desc")}
    in_ref: set[Term] = set()
    updates = identify_field_updates(in_src, in_ref)
    assert len(updates) == 0


def test_identify_field_updates_ignores_non_field_terms():
    """Regular terms (field=None) are never updates."""
    in_src = {Term(name="gn", collection="grid_label")}
    in_ref = {Term(name="gn", collection="grid_label")}
    updates = identify_field_updates(in_src, in_ref)
    assert len(updates) == 0


def test_identify_field_updates_multiple_fields():
    """Multiple fields changed on the same term."""
    in_src = {
        Term(name="gn", collection="grid_label", field="description", value="new desc"),
        Term(name="gn", collection="grid_label", field="drs_name", value="new_drs"),
    }
    in_ref = {
        Term(name="gn", collection="grid_label", field="description", value="old desc"),
        Term(name="gn", collection="grid_label", field="drs_name", value="old_drs"),
    }
    updates = identify_field_updates(in_src, in_ref)
    assert ("grid_label", "gn") in updates
    changes = updates[("grid_label", "gn")]
    assert changes["description"] == ("old desc", "new desc")
    assert changes["drs_name"] == ("old_drs", "new_drs")


def test_identify_field_updates_same_value_not_reported():
    """If field appears in both sets with the same value, no update."""
    in_src = {Term(name="gn", collection="grid_label", field="description", value="same")}
    in_ref = {Term(name="gn", collection="grid_label", field="description", value="same")}
    updates = identify_field_updates(in_src, in_ref)
    assert len(updates) == 0
