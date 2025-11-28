from esgvoc.core.exceptions import EsgvocNotFoundError
from branch_consistency import Term, config_esgvoc, validate_typing_cmip6, validate_typing_cmip6plus, validate_typing_input4mip, validate_typing_cordex_cmip6, validate_legacy_terms
import pytest

config_esgvoc()

content_nln = """[
    "term_key"
]"""

content_dds = """{
    "collection_key": {
        "term_key": "param_key"
    }
}"""

content_dln = """{
    "collection_key": [
        "term_key"
    ]
}"""

content_ddl = """{
    "collection_key": {
        "term_key": [
            "param_key"
        ]
    }
}"""

content_ddd = """{
    "collection_key": {
        "term_key": {
            "param_key": "param_value"
        }
    }
}"""

term_invalid_collection = Term(name="term_key",collection="collection_key")
term_invalid_name = Term(name="term_key",collection="activity_id")
term_valid_cordexcmip6 = Term(name="day",collection="table_id")
term_valid_cmip6 = Term(name="AerChemMIP", collection="activity_id")
term_valid_cmip6plus = Term(name="CMIP", collection="activity_id")
term_valid_input4mip = Term(name="input4MIPs", collection="activity_id")

# CORDEX-CMIP6

def test_cordexcmip6_nln():
    with pytest.raises(ValueError):
        validate_typing_cordex_cmip6(content_nln)

def test_cordexcmip6_dds():
    validate_typing_cordex_cmip6(content_dds)

def test_cordexcmip6_dls():
    validate_typing_cordex_cmip6(content_dln)

def test_cordexcmip6_ddl():
    with pytest.raises(ValueError):
        validate_typing_cordex_cmip6(content_ddl)

def test_cordexcmip6_ddd():
    validate_typing_cordex_cmip6(content_ddd)

def test_cordexcmip6_esgvoc_invalid_collection():
    with pytest.raises(EsgvocNotFoundError):
       invalid_terms, valid_terms = validate_legacy_terms(set([term_invalid_collection]), "cordex-cmip6")

def test_cordexcmip6_esgvoc_invalid_name():
    invalid_terms, valid_terms = validate_legacy_terms(set([term_invalid_name]), "cordex-cmip6")
    assert not valid_terms and invalid_terms

def test_cordexcmip6_esgvoc_empty():
    invalid_terms, valid_terms = validate_legacy_terms(set([]), "cordex-cmip6")
    assert not valid_terms and not invalid_terms

def test_cordexcmip6_esgvoc_valid():
    invalid_terms, valid_terms = validate_legacy_terms(set([term_valid_cordexcmip6]), "cordex-cmip6")
    assert valid_terms and not invalid_terms

# CMIP6

def test_cmip6_nln():
    with pytest.raises(ValueError):
        validate_typing_cmip6(content_nln)

def test_cmip6_dds():
    validate_typing_cmip6(content_dds)

def test_cmip6_dls():
    validate_typing_cmip6(content_dln)

def test_cmip6_ddl():
    with pytest.raises(ValueError):
        validate_typing_cmip6(content_ddl)

def test_cmip6_ddd():
    validate_typing_cmip6(content_ddd)

def test_cmip6_esgvoc_invalid_collection():
    with pytest.raises(EsgvocNotFoundError):
       invalid_terms, valid_terms = validate_legacy_terms(set([term_invalid_collection]), "cmip6")

def test_cmip6_esgvoc_invalid_name():
    invalid_terms, valid_terms = validate_legacy_terms(set([term_invalid_name]), "cmip6")
    assert not valid_terms and invalid_terms

def test_cmip6_esgvoc_empty():
    invalid_terms, valid_terms = validate_legacy_terms(set([]), "cmip6")
    assert not valid_terms and not invalid_terms

def test_cmip6_esgvoc_valid():
    invalid_terms, valid_terms = validate_legacy_terms(set([term_valid_cmip6]), "cmip6")
    assert valid_terms and not invalid_terms

# CMIP6Plus

def test_cmip6plus_nln():
    with pytest.raises(ValueError):
        validate_typing_cmip6plus(content_nln)

def test_cmip6plus_dds():
    validate_typing_cmip6plus(content_dds)

def test_cmip6plus_dls():
    validate_typing_cmip6plus(content_dln)

def test_cmip6plus_ddl():
    with pytest.raises(ValueError):
        validate_typing_cmip6plus(content_ddl)

def test_cmip6plus_ddd():
    validate_typing_cmip6plus(content_ddd)

def test_cmip6plus_esgvoc_invalid_collection():
    with pytest.raises(EsgvocNotFoundError):
       invalid_terms, valid_terms = validate_legacy_terms(set([term_invalid_collection]), "cmip6plus")

def test_cmip6plus_esgvoc_invalid_name():
    invalid_terms, valid_terms = validate_legacy_terms(set([term_invalid_name]), "cmip6plus")
    assert not valid_terms and invalid_terms

def test_cmip6plus_esgvoc_empty():
    invalid_terms, valid_terms = validate_legacy_terms(set([]), "cmip6plus")
    assert not valid_terms and not invalid_terms

def test_cmip6plus_esgvoc_valid():
    invalid_terms, valid_terms = validate_legacy_terms(set([term_valid_cmip6plus]), "cmip6plus")
    assert valid_terms and not invalid_terms

# Input4MIPs

def test_input4mip_nln():
    validate_typing_input4mip(content_nln)

def test_input4mip_dds():
    validate_typing_input4mip(content_dds)

def test_input4mip_dls():
    with pytest.raises(ValueError):
        validate_typing_input4mip(content_dln)

def test_input4mip_ddl():
    validate_typing_input4mip(content_ddl)

def test_input4mip_ddd():
    validate_typing_input4mip(content_ddd)

def test_input4mip_esgvoc_invalid_collection():
    with pytest.raises(EsgvocNotFoundError):
       invalid_terms, valid_terms = validate_legacy_terms(set([term_invalid_collection]), "input4mip")

def test_input4mip_esgvoc_invalid_name():
    invalid_terms, valid_terms = validate_legacy_terms(set([term_invalid_name]), "input4mip")
    assert not valid_terms and invalid_terms

def test_input4mip_esgvoc_empty():
    invalid_terms, valid_terms = validate_legacy_terms(set([]), "input4mip")
    assert not valid_terms and not invalid_terms

def test_input4mip_esgvoc_valid():
    invalid_terms, valid_terms = validate_legacy_terms(set([term_valid_input4mip]), "input4mip")
    assert valid_terms and not invalid_terms