"""Tests for comparison.py — pure set operations, no network."""

from comparison import compare_terms
from models import ReportMode, Term


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
        source, reference, "source", "ref",
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
