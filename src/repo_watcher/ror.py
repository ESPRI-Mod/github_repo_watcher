"""ROR (Research Organization Registry) API client.

Pure API layer — search, fetch, parse. No business logic about how to
interpret results for esgvoc.

API docs: https://ror.readme.io/v2/docs
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

ROR_API_BASE = "https://api.ror.org/v2/organizations"
REQUEST_DELAY = 0.3  # seconds between requests, be polite


@dataclass
class RORRecord:
    """A single organisation record from the ROR API."""

    ror_id: str  # e.g. "03qn8fb07" (without URL prefix)
    score: float  # affiliation match score (0.0–1.0), or 1.0 for query results
    chosen: bool  # ROR's "best match" flag (affiliation endpoint only)
    names: list[dict[str, Any]] = field(default_factory=list)
    locations: list[dict[str, Any]] = field(default_factory=list)
    links: list[dict[str, Any]] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)

    # -- Name helpers --

    @property
    def acronyms(self) -> list[str]:
        return [n["value"] for n in self.names if "acronym" in n.get("types", [])]

    @property
    def labels(self) -> list[str]:
        """All non-acronym names (ror_display, label, alias)."""
        return [n["value"] for n in self.names if "acronym" not in n.get("types", [])]

    @property
    def display_name(self) -> str:
        for n in self.names:
            if "ror_display" in n.get("types", []):
                return n["value"]
        return self.labels[0] if self.labels else ""

    # -- Location helpers --

    @property
    def country(self) -> str:
        if self.locations:
            return self.locations[0].get("geonames_details", {}).get("country_name", "")
        return ""

    @property
    def city(self) -> str:
        if self.locations:
            return self.locations[0].get("geonames_details", {}).get("name", "")
        return ""

    @property
    def lat(self) -> float | None:
        if self.locations:
            return self.locations[0].get("geonames_details", {}).get("lat")
        return None

    @property
    def lng(self) -> float | None:
        if self.locations:
            return self.locations[0].get("geonames_details", {}).get("lng")
        return None

    # -- Link helpers --

    @property
    def website_urls(self) -> list[str]:
        return [lnk["value"] for lnk in self.links if lnk.get("type") == "website"]


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------


def _parse_items(items: list[dict], from_affiliation: bool) -> list[RORRecord]:
    """Parse raw API response items into RORRecord objects."""
    results = []
    for item in items:
        if from_affiliation:
            org = item.get("organization", {})
            score = item.get("score", 0.0)
            chosen = item.get("chosen", False)
        else:
            org = item
            score = 1.0
            chosen = False

        ror_url = org.get("id", "")
        ror_id = ror_url.rsplit("/", 1)[-1] if ror_url else ""
        results.append(
            RORRecord(
                ror_id=ror_id,
                score=score,
                chosen=chosen,
                names=org.get("names", []),
                locations=org.get("locations", []),
                links=org.get("links", []),
                domains=org.get("domains", []),
            )
        )
    return results


def search_affiliation(query: str, delay: float = REQUEST_DELAY) -> list[RORRecord]:
    """Search ROR using the affiliation endpoint (scored, ranked results).

    Best for matching full institution descriptions like
    "Commonwealth Scientific and Industrial Research Organisation, Canberra, Australia".
    """
    if delay > 0:
        time.sleep(delay)
    try:
        resp = requests.get(ROR_API_BASE, params={"affiliation": query}, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ROR affiliation search error for %r: %s", query, exc)
        return []
    return _parse_items(resp.json().get("items", []), from_affiliation=True)


def search_query(query: str, delay: float = REQUEST_DELAY) -> list[RORRecord]:
    """Search ROR using the query endpoint (keyword search on names/acronyms).

    Good for acronym-based lookups. Results are not scored.
    """
    if delay > 0:
        time.sleep(delay)
    try:
        resp = requests.get(ROR_API_BASE, params={"query": query}, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ROR query search error for %r: %s", query, exc)
        return []
    return _parse_items(resp.json().get("items", []), from_affiliation=False)


def fetch_by_id(ror_id: str, delay: float = REQUEST_DELAY) -> RORRecord | None:
    """Fetch a single ROR record by ID (e.g. "03qn8fb07")."""
    if delay > 0:
        time.sleep(delay)
    url = f"{ROR_API_BASE}/{ror_id}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ROR fetch error for %r: %s", ror_id, exc)
        return None
    org = resp.json()
    ror_url = org.get("id", "")
    return RORRecord(
        ror_id=ror_url.rsplit("/", 1)[-1] if ror_url else ror_id,
        score=1.0,
        chosen=True,
        names=org.get("names", []),
        locations=org.get("locations", []),
        links=org.get("links", []),
        domains=org.get("domains", []),
    )
