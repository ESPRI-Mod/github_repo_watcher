"""Tests for extraction.py — pure parsing, no network."""

from pathlib import Path

import pytest

from extraction import extract_terms, validate_typing
from models import Term

FIXTURES = Path(__file__).parent / "fixtures"


# --- validate_typing ---


def test_validate_typing_cordex_dict_of_dicts():
    data = '{"activity_id": {"CORDEX": "desc"}}'
    result = validate_typing(data, "cordex-cmip6")
    assert "activity_id" in result


def test_validate_typing_cordex_dict_of_lists():
    data = '{"frequency": ["day", "mon"]}'
    result = validate_typing(data, "cordex-cmip6")
    assert result["frequency"] == ["day", "mon"]


def test_validate_typing_cordex_rejects_bare_list():
    with pytest.raises(ValueError):
        validate_typing('["term"]', "cordex-cmip6")


def test_validate_typing_cmip6plus_allows_str_value():
    data = '{"activity_id": "CMIP"}'
    result = validate_typing(data, "cmip6plus")
    assert result["activity_id"] == "CMIP"


def test_validate_typing_input4mip_accepts_list():
    data = '["input4MIPs", "CMIP"]'
    result = validate_typing(data, "input4mip")
    assert "input4MIPs" in result


def test_validate_typing_unknown_project():
    with pytest.raises(KeyError):
        validate_typing("{}", "nope")


# --- extract_terms ---


def test_cordex_extracts_known_terms():
    terms = extract_terms(FIXTURES / "cordex-cmip6", project="cordex-cmip6")
    assert Term(name="day", collection="frequency") in terms
    assert Term(name="CORDEX", collection="activity_id") in terms


def test_cordex_skips_driving_variant_label():
    terms = extract_terms(
        FIXTURES / "cordex-cmip6",
        project="cordex-cmip6",
        skip_collections=["driving_variant_label"],
    )
    assert all(t.collection != "driving_variant_label" for t in terms)


def test_cmip6_extracts_terms():
    terms = extract_terms(FIXTURES / "cmip6", project="cmip6")
    assert Term(name="AerChemMIP", collection="activity_id") in terms
    assert Term(name="CMIP", collection="activity_id") in terms


def test_cmip6_skips_version_metadata():
    terms = extract_terms(
        FIXTURES / "cmip6",
        project="cmip6",
        skip_collections=["version_metadata"],
    )
    assert all(t.collection != "version_metadata" for t in terms)


def test_cmip6plus_extracts_dict_and_str_terms():
    terms = extract_terms(FIXTURES / "cmip6plus", project="cmip6plus")
    assert Term(name="CMIP", collection="activity_id") in terms
    assert Term(name="LESFMIP", collection="activity_id") in terms


def test_input4mip_extracts_from_cvs_subdir():
    terms = extract_terms(FIXTURES / "input4mip" / "CVs", project="input4mip")
    assert Term(name="input4MIPs", collection="activity_id") in terms
    assert Term(name="CMIP", collection="activity_id") in terms


def test_input4mip_target_mip():
    terms = extract_terms(FIXTURES / "input4mip" / "CVs", project="input4mip")
    assert Term(name="AerChemMIP", collection="target_mip") in terms
    assert Term(name="CMIP", collection="target_mip") in terms


def test_only_collections_filter():
    terms = extract_terms(
        FIXTURES / "cordex-cmip6",
        project="cordex-cmip6",
        only_collections=["activity_id"],
    )
    assert all(t.collection == "activity_id" for t in terms)
    assert len(terms) > 0
