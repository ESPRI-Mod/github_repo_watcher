"""Layer 3 — Comparison: two sets of terms -> ConsistencyResult.

Pure set operations. No network, no esgvoc, no Prefect.
"""

from __future__ import annotations

import logging
import re

from collections import defaultdict

from repo_watcher.models import ConsistencyResult, Term, TermIdTransform, TermOverrideConfig

logger = logging.getLogger(__name__)


def apply_term_id_transforms(terms: set[Term], transforms: dict[str, list[TermIdTransform]]) -> set[Term]:
    """Apply string replacements to term names for specified collections.

    transforms maps collection -> list of {pattern, replace} dicts.
    """
    result: set[Term] = set()
    for t in terms:
        if t.collection in transforms:
            name = t.name
            for rule in transforms[t.collection]:
                name = name.replace(rule["pattern"], rule["replace"])
            result.add(Term(name=name, collection=t.collection, field=t.field, value=t.value))
        else:
            result.add(t)
    return result


def apply_term_overrides(
    terms: set[Term],
    overrides: dict[str, dict[str, TermOverrideConfig]],
) -> set[Term]:
    """Remap source term names using term_overrides config.

    For each collection with overrides, if a term's name matches an override
    key (exact string match), replace it with the override's universe_term.
    This allows the comparison to recognise e.g. ``hdl:21.14100/.*`` as
    the existing registry term ``prefuuid``.
    """
    result: set[Term] = set()
    for t in terms:
        if t.collection in overrides and t.name in overrides[t.collection]:
            replacement = overrides[t.collection][t.name]["universe_term"]
            result.add(Term(name=replacement, collection=t.collection, field=t.field, value=t.value))
        else:
            result.add(t)
    return result


def _apply_exclusions(terms: set[Term], exclude_terms: dict[str, list[str]]) -> set[Term]:
    """Remove terms whose name matches any regex pattern for their collection."""
    compiled: dict[str, list[re.Pattern]] = {}
    for col, patterns in exclude_terms.items():
        compiled[col] = []
        for p in patterns:
            logger.debug("Excluding terms matching /%s/ from collection '%s'", p, col)
            compiled[col].append(re.compile(p))
    return {
        t for t in terms if t.collection not in compiled or not any(p.search(t.name) for p in compiled[t.collection])
    }


def identify_field_updates(
    in_source_not_ref: set[Term],
    in_ref_not_source: set[Term],
) -> dict[tuple[str, str], dict[str, tuple[str, str]]]:
    """Find field-level value changes by cross-referencing two diff sets.

    For each (collection, name, field) that appears in both sets with different
    values, records it as an update.

    Returns {(collection, name): {field: (old_value, new_value)}}.
    """
    # Index ref-side field-level terms by (collection, name, field)
    ref_index: dict[tuple[str, str, str], str] = {}
    for t in in_ref_not_source:
        if t.field is not None and t.value is not None:
            ref_index[(t.collection, t.name, t.field)] = t.value

    # Match source-side terms against the ref index
    updates: dict[tuple[str, str], dict[str, tuple[str, str]]] = defaultdict(dict)
    for t in in_source_not_ref:
        if t.field is None or t.value is None:
            continue
        key = (t.collection, t.name, t.field)
        if key in ref_index:
            old_value = ref_index[key]
            if old_value != t.value:
                updates[(t.collection, t.name)][t.field] = (old_value, t.value)

    return dict(updates)


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
