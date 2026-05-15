"""Layer 3 — Comparison: two sets of terms -> ConsistencyResult.

Pure set operations. No network, no esgvoc, no Prefect.
"""

from __future__ import annotations

import re
import sys

from models import ConsistencyResult, Term


def _apply_exclusions(terms: set[Term], exclude_terms: dict[str, list[str]]) -> set[Term]:
    """Remove terms whose name matches any regex pattern for their collection."""
    compiled: dict[str, list[re.Pattern]] = {
        col: [re.compile(p) for p in patterns]
        for col, patterns in exclude_terms.items()
    }
    for col, patterns in exclude_terms.items():
        for pattern in patterns:
            print(f"Excluding terms matching /{pattern}/ from collection '{col}'", file=sys.stderr)
    return {
        t for t in terms
        if t.collection not in compiled
        or not any(p.search(t.name) for p in compiled[t.collection])
    }


def compare_terms(
    source_terms: set[Term],
    reference_terms: set[Term],
    source_label: str,
    reference_label: str,
    collection_mappings: dict[str, str] | None = None,
    exclude_terms: dict[str, list[str]] | None = None,
) -> ConsistencyResult:
    """Compare two sets of terms and return the diff.

    Parameters
    ----------
    source_terms : set[Term]
        Terms from the source being checked.
    reference_terms : set[Term]
        Terms from the reference.
    source_label : str
        Human-readable label for the source.
    reference_label : str
        Human-readable label for the reference.
    collection_mappings : dict[str, str] | None
        Renames source collections before comparing
        (e.g. {"table_id": "frequency"} for CORDEX).
    exclude_terms : dict[str, list[str]] | None
        Regex patterns per collection to exclude from the diff
        (e.g. {"grid_label": ["^temp_grid"]}).
    """
    if collection_mappings:
        source_terms = {
            Term(
                name=t.name,
                collection=collection_mappings.get(t.collection, t.collection),
                field=t.field,
                value=t.value,
            )
            for t in source_terms
        }

    in_source_not_ref = source_terms - reference_terms
    in_ref_not_source = reference_terms - source_terms

    if exclude_terms:
        in_source_not_ref = _apply_exclusions(in_source_not_ref, exclude_terms)
        in_ref_not_source = _apply_exclusions(in_ref_not_source, exclude_terms)

    return ConsistencyResult(
        source_label=source_label,
        reference_label=reference_label,
        in_source_not_reference=in_source_not_ref,
        in_reference_not_source=in_ref_not_source,
        common=source_terms & reference_terms,
    )
