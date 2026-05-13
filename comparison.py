"""Layer 3 — Comparison: two sets of terms -> ConsistencyResult.

Pure set operations. No network, no esgvoc, no Prefect.
"""

from __future__ import annotations

from models import ConsistencyResult, Term


def compare_terms(
    source_terms: set[Term],
    reference_terms: set[Term],
    source_label: str,
    reference_label: str,
    collection_mappings: dict[str, str] | None = None,
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

    return ConsistencyResult(
        source_label=source_label,
        reference_label=reference_label,
        in_source_not_reference=source_terms - reference_terms,
        in_reference_not_source=reference_terms - source_terms,
        common=source_terms & reference_terms,
    )
