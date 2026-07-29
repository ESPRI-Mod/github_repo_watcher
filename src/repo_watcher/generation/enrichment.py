"""ROR API enrichment for institution skeletons."""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# Pattern: "ACRO [description]; ACRO2 [description2]"
_BRACKET_PATTERN = re.compile(r"([A-Za-z0-9]+)\s*\[([^\]]+)\]")

# Pattern: "Community (ACRO) partner: description, City, Country"
_PARTNER_PATTERN = re.compile(r"partner:\s*(.+)", re.IGNORECASE)


def parse_member_descriptions(
    term_name: str,
    composite_description: str,
    separator: str,
) -> dict[str, str]:
    """Extract per-member descriptions from a composite CV description.

    Handles three patterns:
      1. "ACRO1 [desc1]; ACRO2 [desc2]"          -> {acro1: desc1, acro2: desc2}
      2. "Community (X) partner: desc, City, Ctry" -> {member: desc, City, Ctry}
      3. Plain description (single org with "-")   -> {full_term: description}

    Returns {lowercase_member_id: description_string}.
    """
    result: dict[str, str] = {}

    # Pattern 1: bracket notation
    matches = _BRACKET_PATTERN.findall(composite_description)
    if matches:
        for acro, desc in matches:
            result[acro.lower()] = desc.strip().rstrip(";").strip()
        return result

    # Pattern 2: "partner: description"
    parts = term_name.split(separator) if separator else [term_name]
    partner_match = _PARTNER_PATTERN.search(composite_description)
    if partner_match and len(parts) == 2:
        # The partner description applies to the second member
        result[parts[1].lower()] = partner_match.group(1).strip()
        return result

    # Pattern 3: plain description — could be a single org with "-" in the name
    # Return it keyed to the full term_id so it can be used as-is
    result[term_name.lower()] = composite_description
    return result


def extract_country_hint(description: str) -> str | None:
    """Extract a country name from the end of a CV description.

    CV descriptions typically end with "City, Country".
    """
    parts = [p.strip() for p in description.split(",")]
    if len(parts) >= 2:
        return parts[-1]
    return None


def country_matches(ror_country: str, hint: str | None) -> bool:
    """Check if a ROR country matches the hint from the CV description."""
    if not hint:
        return True  # no hint = can't disqualify
    return ror_country.lower().strip() == hint.lower().strip()


def ror_enrich_institution(
    skeleton: dict,
    member_id: str,
    description: str | None,
    drs_name: str,
    fallback_country_desc: str | None = None,
) -> None:
    """Try to enrich an institution skeleton with ROR data.

    Searches the ROR API using the description (preferred) or acronym,
    cross-checks country, and only applies high-confidence matches.
    Modifies skeleton in-place. Leaves fields empty if no confident match.
    """
    from repo_watcher.ror import RORRecord, search_affiliation, search_query

    country_hint = extract_country_hint(description) if description else None
    if not country_hint and fallback_country_desc:
        country_hint = extract_country_hint(fallback_country_desc)
    score_threshold = 0.90
    match: RORRecord | None = None

    # Strategy 1: affiliation search with full description
    if description:
        for r in search_affiliation(description):
            if r.score >= score_threshold:
                acronym_ok = drs_name.upper() in [a.upper() for a in r.acronyms]
                country_ok = country_matches(r.country, country_hint)
                if (r.chosen or acronym_ok) and country_ok:
                    match = r
                    break

    # Strategy 2: query search by acronym
    if match is None:
        acronym_matches = [
            r for r in search_query(drs_name)[:5]
            if drs_name.upper() in [a.upper() for a in r.acronyms]
        ]
        if country_hint:
            for r in acronym_matches:
                if country_matches(r.country, country_hint):
                    match = r
                    break
        elif len(acronym_matches) == 1:
            match = acronym_matches[0]

    # Strategy 3: affiliation search with composite description
    if match is None and not description and fallback_country_desc:
        for r in search_affiliation(fallback_country_desc):
            if r.score >= score_threshold:
                acronym_ok = drs_name.upper() in [a.upper() for a in r.acronyms]
                if acronym_ok:
                    match = r
                    break

    # Strategy 4: affiliation search with just the acronym as last resort
    if match is None and not description:
        for r in search_affiliation(drs_name):
            if r.score >= score_threshold and r.chosen:
                acronym_ok = drs_name.upper() in [a.upper() for a in r.acronyms]
                if acronym_ok:
                    match = r
                    break

    if match is None:
        logger.info("ROR: no confident match for %s", drs_name)
        return

    logger.info("ROR: %s -> %s (%s)", drs_name, match.ror_id, match.display_name)

    # Apply ROR data to skeleton
    skeleton["ror"] = match.ror_id
    skeleton["acronyms"] = match.acronyms or [drs_name]
    skeleton["labels"] = match.labels[:3]
    skeleton["description"] = match.display_name
    skeleton["urls"] = match.website_urls
    if match.locations:
        geo = match.locations[0].get("geonames_details", {})
        if geo:
            skeleton["location"] = [
                {
                    "city": geo.get("name", ""),
                    "country": geo.get("country_name", ""),
                    "lat": geo.get("lat", 0.0),
                    "lon": geo.get("lng", 0.0),
                }
            ]
