"""Layer 5 — Generation: scaffold missing term JSON files in universe and project repos.

Pure file I/O + optional ROR API enrichment for institutions.
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from repo_watcher.models import MultiStepConfig, Term, TermOverrideConfig

logger = logging.getLogger(__name__)

CONTEXT_FILENAME = "000_context.jsonld"


@dataclass(frozen=True)
class TermFile:
    """A planned file operation (create or update)."""

    descriptor: str
    term_id: str
    content: dict


@dataclass(frozen=True)
class FieldChange:
    """A field value update on an existing universe term."""

    descriptor: str
    term_id: str
    changes: dict[str, tuple[str, str]]


@dataclass(frozen=True)
class TermRef:
    """A reference to an existing term (descriptor/collection + id)."""

    descriptor: str
    term_id: str


@dataclass(frozen=True)
class TermSkip:
    """A term skipped during generation with a reason."""

    collection: str
    term_id: str
    reason: str


# JSON-LD structural keys to skip when discovering term fields from context
_JSONLD_STRUCTURAL_KEYS = {"@base", "@vocab", "id", "type"}

# Fields that get empty string defaults instead of null
_STRING_DEFAULT_FIELDS = {"description", "drs_name", "name"}


def detect_collection_mapping(project_repo: Path) -> dict[str, str]:
    """Parse @base URLs from project 000_context.jsonld files to build collection->universe mapping.

    Returns e.g. {"source": "model", "institution": "organisation", "grid_label": "horizontal_grid_cell"}.
    Collections where the name matches the universe descriptor are included too.
    """
    mapping: dict[str, str] = {}
    for collection_dir in sorted(project_repo.iterdir()):
        if not collection_dir.is_dir():
            continue
        context_file = collection_dir / CONTEXT_FILENAME
        if not context_file.exists():
            continue
        try:
            data = json.loads(context_file.read_text())
        except json.JSONDecodeError:
            continue
        context = data.get("@context", {})
        base_url = context.get("@base", "")
        if not base_url:
            continue
        # Extract universe descriptor from @base URL like
        # "https://esgvoc.ipsl.fr/resource/universe/model/"
        descriptor = base_url.rstrip("/").rsplit("/", 1)[-1]
        mapping[collection_dir.name] = descriptor
    return mapping


def _sample_existing_term(descriptor_dir: Path) -> dict | None:
    """Read the first existing term JSON in a universe descriptor directory."""
    for f in sorted(descriptor_dir.iterdir()):
        if f.name == CONTEXT_FILENAME or not f.suffix == ".json":
            continue
        try:
            return json.loads(f.read_text())
        except json.JSONDecodeError:
            continue
    return None


def _build_universe_skeleton(universe_repo: Path, descriptor: str, term_id: str) -> dict:
    """Build a skeleton universe term JSON by sampling an existing term for field discovery."""
    result: dict = {
        "@context": CONTEXT_FILENAME,
        "id": term_id,
        "type": descriptor,
    }
    descriptor_dir = universe_repo / descriptor

    # Sample an existing term to discover fields and their default types
    sample = _sample_existing_term(descriptor_dir)
    if sample is None:
        return result

    for key, value in sample.items():
        if key in result:
            continue
        if isinstance(value, list):
            result[key] = []
        elif isinstance(value, dict):
            result[key] = {}
        elif key in _STRING_DEFAULT_FIELDS:
            result[key] = ""
        else:
            result[key] = None

    # drs_name defaults to term_id (standard esgvoc convention)
    if "drs_name" in result and not result["drs_name"]:
        result["drs_name"] = term_id

    return result


def _build_project_link(descriptor: str, term_id: str) -> dict:
    """Build the minimal project link JSON (always 3 fields)."""
    return {
        "@context": CONTEXT_FILENAME,
        "id": term_id,
        "type": descriptor,
    }


# ---------------------------------------------------------------------------
# ROR enrichment for institution skeletons
# ---------------------------------------------------------------------------

# Pattern: "ACRO [description]; ACRO2 [description2]"
_BRACKET_PATTERN = re.compile(r"([A-Za-z0-9]+)\s*\[([^\]]+)\]")

# Pattern: "Community (ACRO) partner: description, City, Country"
_PARTNER_PATTERN = re.compile(r"partner:\s*(.+)", re.IGNORECASE)


def _parse_member_descriptions(
    term_name: str,
    composite_description: str,
    separator: str,
) -> dict[str, str]:
    """Extract per-member descriptions from a composite CV description.

    Handles three patterns:
      1. "ACRO1 [desc1]; ACRO2 [desc2]"          → {acro1: desc1, acro2: desc2}
      2. "Community (X) partner: desc, City, Ctry" → {member: desc, City, Ctry}
      3. Plain description (single org with "-")   → {full_term: description}

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


def _extract_country_hint(description: str) -> str | None:
    """Extract a country name from the end of a CV description.

    CV descriptions typically end with "City, Country".
    """
    parts = [p.strip() for p in description.split(",")]
    if len(parts) >= 2:
        return parts[-1]
    return None


def _ror_enrich_institution(
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

    country_hint = _extract_country_hint(description) if description else None
    if not country_hint and fallback_country_desc:
        country_hint = _extract_country_hint(fallback_country_desc)
    score_threshold = 0.90
    match: RORRecord | None = None

    # Strategy 1: affiliation search with full description
    if description:
        for r in search_affiliation(description):
            if r.score >= score_threshold:
                acronym_ok = drs_name.upper() in [a.upper() for a in r.acronyms]
                country_ok = _country_matches(r.country, country_hint)
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
            # With country hint: require country confirmation
            for r in acronym_matches:
                if _country_matches(r.country, country_hint):
                    match = r
                    break
        elif len(acronym_matches) == 1:
            # Without country hint: accept only if the acronym is unambiguous
            match = acronym_matches[0]

    # Strategy 3: affiliation search with composite description (when no per-member description)
    # The full org description (e.g. "Italian National Research Council, ..., Lecce, Italy")
    # often contains enough info for ROR to identify individual members by acronym.
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

    logger.info("ROR: %s → %s (%s)", drs_name, match.ror_id, match.display_name)

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


def _country_matches(ror_country: str, hint: str | None) -> bool:
    """Check if a ROR country matches the hint from the CV description."""
    if not hint:
        return True  # no hint = can't disqualify
    return ror_country.lower().strip() == hint.lower().strip()


def _handle_multi_step_term(
    term: Term,
    term_id: str,
    cfg: MultiStepConfig,
    universe_repo: Path,
    project_repos: list[Path],
    report: GenerationReport,
    seen_members: set[tuple[str, str]],
    cv_description: str | None = None,
) -> None:
    """Expand a term into member + group + project link artifacts.

    For example, institution_id "IUM-FDU" with separator "-" produces:
    - institution/ium.json + institution/fdu.json (members)
    - organisation/ium-fdu.json with members: ["ium", "fdu"] (group)
    - institution_id/ium-fdu.json project link -> organisation
    """
    separator = cfg.get("separator")
    member_descriptor = cfg["member_descriptor"]
    group_descriptor = cfg["group_descriptor"]

    # Split into atomic members
    if separator:
        members = [m.lower() for m in term_id.split(separator)]
    else:
        members = [term_id]

    # Parse per-member descriptions from composite CV description
    member_descriptions: dict[str, str] = {}
    if cv_description:
        member_descriptions = _parse_member_descriptions(term.name, cv_description, separator or "")

    # Step 1: member_descriptor — one file per atomic member
    # Track which members are valid (exist or created as member_descriptor type).
    # Members that exist as a different type (e.g. "clmcom" as organisation/)
    # are excluded from the group's members list.
    valid_members: list[str] = []
    member_dir = universe_repo / member_descriptor
    if member_dir.is_dir():
        for member_id in members:
            key = (member_descriptor, member_id)
            if key in seen_members:
                valid_members.append(member_id)
                continue
            seen_members.add(key)
            member_file = member_dir / f"{member_id}.json"
            if member_file.exists():
                report.universe_existed.append(TermRef(member_descriptor, member_id))
                valid_members.append(member_id)
            else:
                # Check if the member already exists under a different universe type
                # (e.g. "clmcom" exists as organisation/, not institution/)
                found_elsewhere = False
                for other_dir in universe_repo.iterdir():
                    if not other_dir.is_dir() or other_dir.name == member_descriptor:
                        continue
                    if (other_dir / f"{member_id}.json").exists():
                        report.universe_existed.append(TermRef(other_dir.name, member_id))
                        found_elsewhere = True
                        break
                if found_elsewhere:
                    # Not a valid member_descriptor entity — skip for members list
                    continue
                skeleton = _build_universe_skeleton(universe_repo, member_descriptor, member_id)
                # ROR enrichment for institution members
                if member_descriptor == "institution":
                    member_desc = member_descriptions.get(member_id)
                    drs_name = member_id.upper()
                    # Find the original-case acronym from the term name
                    if separator:
                        for part in term.name.split(separator):
                            if part.lower() == member_id:
                                drs_name = part
                                break
                    _ror_enrich_institution(
                        skeleton, member_id, member_desc, drs_name, fallback_country_desc=cv_description
                    )
                report.universe_to_create.append(TermFile(member_descriptor, member_id, skeleton))
                valid_members.append(member_id)

    # Step 2: group_descriptor — one file for the full term
    group_dir = universe_repo / group_descriptor
    if group_dir.is_dir():
        group_file = group_dir / f"{term_id}.json"
        if group_file.exists():
            # Check if existing organisation needs new members added
            existing = json.loads(group_file.read_text())
            existing_members = set(existing.get("members", []))
            new_members = [m for m in valid_members if m not in existing_members]
            if new_members:
                merged = sorted(existing_members | set(valid_members))
                report.universe_to_update.append(
                    FieldChange(group_descriptor, term_id, {"members": (list(existing_members), merged)})
                )
            else:
                report.universe_existed.append(TermRef(group_descriptor, term_id))
        else:
            skeleton = _build_universe_skeleton(universe_repo, group_descriptor, term_id)
            skeleton["members"] = valid_members
            if cv_description and "description" in skeleton:
                skeleton["description"] = cv_description
            if "drs_name" in skeleton:
                skeleton["drs_name"] = term.name
            report.universe_to_create.append(TermFile(group_descriptor, term_id, skeleton))

    # Step 3: project link -> group_descriptor
    found_in_project = False
    has_collection_dir = False
    for repo in project_repos:
        collection_dir = repo / term.collection
        if not collection_dir.is_dir():
            continue
        has_collection_dir = True
        if (collection_dir / f"{term_id}.json").exists():
            report.project_existed.append(TermRef(term.collection, term_id))
            found_in_project = True
            break
    if not found_in_project and has_collection_dir:
        link = _build_project_link(group_descriptor, term_id)
        report.project_to_create.append(TermFile(term.collection, term_id, link))


def generate_missing_terms(
    missing_terms: set[Term],
    universe_repo: Path,
    project_repos: list[Path],
    universe_type_overrides: dict[str, str] | None = None,
    generation_defaults: dict[str, dict[str, Any]] | None = None,
    term_overrides: dict[str, dict[str, TermOverrideConfig]] | None = None,
    multi_step_terms: dict[str, MultiStepConfig] | None = None,
    source_prefill: dict[str, dict[str, str]] | None = None,
    source_term_data: dict[str, dict[str, dict]] | None = None,
    extra_field_values: dict[tuple[str, str], dict[str, str]] | None = None,
    update_terms: dict[tuple[str, str], dict[str, tuple[str, str]]] | None = None,
) -> GenerationReport:
    """Plan the generation of JSON files for missing terms in both repos.

    Currently only supports dry-run (reporting what would be created).
    For each missing term:
    1. Check if it matches a term_override — if so, create a project-level
       override of an existing universe term (with field overrides).
    2. Otherwise check if it exists in universe. If not, plan a skeleton.
    3. Plan a project link — checks all project_repos for existence.

    Returns a report of what would be created.
    """
    mapping: dict[str, str] = {}
    for repo in project_repos:
        mapping.update(detect_collection_mapping(repo))
    if universe_type_overrides:
        mapping.update(universe_type_overrides)
    report = GenerationReport()

    # Collect known field values from field-level terms (e.g. EMD provides drs_name, description).
    # Also deduplicate: multiple (name, collection, field=X) entries represent the same term.
    # Start with extra_field_values (all source fields, not just missing) as baseline,
    # then overlay with values from missing terms (which take priority).
    known_values: dict[tuple[str, str], dict[str, str]] = {}
    if extra_field_values:
        for key, fields in extra_field_values.items():
            known_values.setdefault(key, {}).update(fields)
    unique_terms: dict[tuple[str, str], Term] = {}
    for term in sorted(missing_terms, key=lambda t: (t.collection, t.name)):
        key = (term.collection, term.name)
        if key not in unique_terms:
            unique_terms[key] = term
        if term.field and term.value is not None:
            known_values.setdefault(key, {})[term.field] = term.value

    seen_members: set[tuple[str, str]] = set()

    for term in unique_terms.values():
        # Resolve universe descriptor
        descriptor = mapping.get(term.collection)
        if descriptor is None:
            report.skipped.append(
                TermSkip(term.collection, term.name, f"no mapping for collection '{term.collection}'")
            )
            continue

        # esgvoc IDs are always lowercase
        term_id = term.name.lower()

        # Check for term overrides (e.g. tracking_id: reuse universe "prefuuid"
        # with project-specific regex, or variant_label with a config-defined ID)
        collection_overrides = term_overrides.get(term.collection) if term_overrides else None
        if collection_overrides and term.name in collection_overrides:
            override_cfg = collection_overrides[term.name]
            override_id = override_cfg["universe_term"]
            override_fields = override_cfg.get("fields", {})

            # Create universe term if requested and missing
            if override_cfg.get("create_universe"):
                universe_file = universe_repo / descriptor / f"{override_id}.json"
                if universe_file.exists():
                    report.universe_existed.append(TermRef(descriptor, override_id))
                else:
                    universe_dir = universe_repo / descriptor
                    if universe_dir.is_dir():
                        skeleton = _build_universe_skeleton(universe_repo, descriptor, override_id)
                        skeleton.update(override_fields)
                        report.universe_to_create.append(TermFile(descriptor, override_id, skeleton))

            # Build project link — only include field overrides when the universe
            # term already exists (i.e. we're overriding it per-project).
            # When create_universe is true, fields belong in the universe term only.
            override_json = {
                "@context": CONTEXT_FILENAME,
                "id": override_id,
                "type": descriptor,
            }
            if not override_cfg.get("create_universe"):
                override_json.update(override_fields)
            found = False
            for repo in project_repos:
                override_file = repo / term.collection / f"{override_id}.json"
                if override_file.exists():
                    report.project_existed.append(TermRef(term.collection, override_id))
                    found = True
                    break
            if not found:
                # Use the first repo that has the collection directory
                for repo in project_repos:
                    if (repo / term.collection).is_dir():
                        report.project_to_create.append(TermFile(term.collection, override_id, override_json))
                        break
            continue

        # Check for multi-step terms (e.g. institution_id: institution + organisation + project link)
        multi_step_cfg = multi_step_terms.get(term.collection) if multi_step_terms else None
        if multi_step_cfg is not None:
            # Get CV description for ROR enrichment
            cv_desc = None
            if source_term_data and term.collection in source_term_data:
                cv_entry = source_term_data[term.collection].get(term.name, {})
                cv_desc = cv_entry.get("_value") if isinstance(cv_entry, dict) else None
            _handle_multi_step_term(
                term,
                term_id,
                multi_step_cfg,
                universe_repo,
                project_repos,
                report,
                seen_members,
                cv_description=cv_desc,
            )
            continue

        # Check universe
        universe_term_file = universe_repo / descriptor / f"{term_id}.json"
        term_update_key = (term.collection, term.name)
        if universe_term_file.exists():
            if update_terms and term_update_key in update_terms:
                report.universe_to_update.append(FieldChange(descriptor, term_id, update_terms[term_update_key]))
            else:
                report.universe_existed.append(TermRef(descriptor, term_id))
        else:
            universe_descriptor_dir = universe_repo / descriptor
            if not universe_descriptor_dir.is_dir():
                report.skipped.append(
                    TermSkip(term.collection, term_id, f"universe descriptor dir '{descriptor}' not found")
                )
                continue
            skeleton = _build_universe_skeleton(universe_repo, descriptor, term_id)
            # Pre-populate with known values from field-level terms (e.g. EMD)
            field_values = known_values.get((term.collection, term.name), {})
            for field_name, value in field_values.items():
                if field_name in skeleton:
                    skeleton[field_name] = value
            # Pre-populate from source community CV data (e.g. label, release_year)
            if source_prefill and source_term_data and term.collection in source_prefill:
                field_map = source_prefill[term.collection]
                cv_data = source_term_data.get(term.collection, {}).get(term.name, {})
                for src_field, target_field in field_map.items():
                    if src_field in cv_data and target_field in skeleton:
                        skeleton[target_field] = cv_data[src_field]
            # Apply generation defaults for fields still null/empty
            if generation_defaults and term.collection in generation_defaults:
                for field_name, default_value in generation_defaults[term.collection].items():
                    if field_name in skeleton and skeleton[field_name] is None:
                        skeleton[field_name] = default_value
            report.universe_to_create.append(TermFile(descriptor, term_id, skeleton))

        # Project link — check across all project repos
        found_in_project = False
        has_collection_dir = False
        for repo in project_repos:
            project_collection_dir = repo / term.collection
            if not project_collection_dir.is_dir():
                continue
            has_collection_dir = True
            project_term_file = project_collection_dir / f"{term_id}.json"
            if project_term_file.exists():
                report.project_existed.append(TermRef(term.collection, term_id))
                found_in_project = True
                break
        if not found_in_project and has_collection_dir:
            link = _build_project_link(descriptor, term_id)
            report.project_to_create.append(TermFile(term.collection, term_id, link))

    return report


def write_report(report: GenerationReport, universe_repo: Path | None = None, project_repo: Path | None = None) -> None:
    """Write the JSON files planned in a GenerationReport to disk."""
    if universe_repo is not None:
        for tf in report.universe_to_create:
            path = universe_repo / tf.descriptor / f"{tf.term_id}.json"
            path.write_text(json.dumps(tf.content, indent=2, ensure_ascii=False) + "\n")
        for fc in report.universe_to_update:
            path = universe_repo / fc.descriptor / f"{fc.term_id}.json"
            existing = json.loads(path.read_text())
            existing.update({f: new for f, (_old, new) in fc.changes.items()})
            path.write_text(json.dumps(existing, indent=2, ensure_ascii=False) + "\n")
    if project_repo is not None:
        for tf in report.project_to_create:
            path = project_repo / tf.descriptor / f"{tf.term_id}.json"
            path.write_text(json.dumps(tf.content, indent=2, ensure_ascii=False) + "\n")


@dataclass
class GenerationReport:
    """Report of what would be (or was) created by generate_missing_terms."""

    universe_to_create: list[TermFile] = field(default_factory=list)
    project_to_create: list[TermFile] = field(default_factory=list)
    universe_to_update: list[FieldChange] = field(default_factory=list)
    universe_existed: list[TermRef] = field(default_factory=list)
    project_existed: list[TermRef] = field(default_factory=list)
    skipped: list[TermSkip] = field(default_factory=list)

    @property
    def has_work(self) -> bool:
        return bool(self.universe_to_create or self.project_to_create or self.universe_to_update)

    def format_report(self) -> str:
        lines: list[str] = []

        if not self.has_work:
            return "Nothing to generate — all terms are up to date."

        if self.universe_to_create:
            lines.append(f"Universe terms to create ({len(self.universe_to_create)}):")
            for tf in sorted(self.universe_to_create, key=lambda t: (t.descriptor, t.term_id)):
                lines.append(f"  {tf.descriptor}/{tf.term_id}.json:")
                lines.append(json.dumps(tf.content, indent=2, ensure_ascii=False))

        if self.project_to_create:
            lines.append(f"Project links to create ({len(self.project_to_create)}):")
            for tf in sorted(self.project_to_create, key=lambda t: (t.descriptor, t.term_id)):
                lines.append(f"  {tf.descriptor}/{tf.term_id}.json:")
                lines.append(json.dumps(tf.content, indent=2, ensure_ascii=False))

        if self.universe_to_update:
            lines.append(f"Universe terms to update ({len(self.universe_to_update)}):")
            for fc in sorted(self.universe_to_update, key=lambda t: (t.descriptor, t.term_id)):
                lines.append(f"  {fc.descriptor}/{fc.term_id}.json:")
                for field_name in sorted(fc.changes):
                    old_val, new_val = fc.changes[field_name]
                    lines.append(f"    {field_name}: {old_val!r} -> {new_val!r}")

        if self.universe_existed:
            lines.append(f"Already in universe ({len(self.universe_existed)}):")
            grouped_e: defaultdict[str, list[str]] = defaultdict(list)
            for ref in self.universe_existed:
                grouped_e[ref.descriptor].append(ref.term_id)
            for descriptor in sorted(grouped_e):
                lines.append(f"  {descriptor}/: {', '.join(sorted(grouped_e[descriptor]))}")

        if self.project_existed:
            lines.append(f"Already in project ({len(self.project_existed)}):")
            grouped_pe: defaultdict[str, list[str]] = defaultdict(list)
            for ref in self.project_existed:
                grouped_pe[ref.descriptor].append(ref.term_id)
            for descriptor in sorted(grouped_pe):
                lines.append(f"  {descriptor}/: {', '.join(sorted(grouped_pe[descriptor]))}")

        if self.skipped:
            lines.append(f"Skipped ({len(self.skipped)}):")
            for skip in self.skipped:
                lines.append(f"  {skip.collection}/{skip.term_id}: {skip.reason}")

        return "\n".join(lines)


def collect_and_generate(
    checks: list,
    universe_repo: Path,
    project_name: str,
    collections_filter: list[str] | None = None,
    on_progress: Any | None = None,
) -> GenerationReport:
    """Run all checks for a project, collect diffs, and generate missing term skeletons.

    This is the main orchestration function that ties together fetching, comparison,
    config merging, and generation. Extracted from the CLI command to keep it testable.

    Parameters
    ----------
    checks : list[CheckConfig]
        All checks matching the target project.
    universe_repo : Path
        Path to the local WCRP-universe repo.
    project_name : str
        Project id (e.g. "cmip7").
    collections_filter : list[str] | None
        Only generate for these collections.
    on_progress : callable | None
        Optional callback for progress messages: on_progress(msg: str).
    """
    from repo_watcher.comparison import (
        _apply_exclusions,
        apply_term_id_transforms,
        compare_terms,
        identify_field_updates,
    )
    from repo_watcher.extraction import extract_term_data
    from repo_watcher.sources import build_source

    def _log(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    # Merge config options across checks
    merged_excludes: dict[str, list[str]] = {}
    merged_universe_type_mappings: dict[str, str] = {}
    merged_generation_defaults: dict[str, dict[str, Any]] = {}
    merged_term_id_transforms: dict[str, list] = {}
    merged_term_overrides: dict[str, dict] = {}
    merged_multi_step_terms: dict[str, dict] = {}
    merged_source_prefill: dict[str, dict[str, str]] = {}
    for chk in checks:
        if chk.exclude_terms:
            for col, patterns in chk.exclude_terms.items():
                merged_excludes.setdefault(col, []).extend(patterns)
        if chk.universe_type_mappings:
            merged_universe_type_mappings.update(chk.universe_type_mappings)
        if chk.generation_defaults:
            for col, defaults in chk.generation_defaults.items():
                merged_generation_defaults.setdefault(col, {}).update(defaults)
        if chk.term_id_transforms:
            for col, rules in chk.term_id_transforms.items():
                merged_term_id_transforms.setdefault(col, []).extend(rules)
        if chk.term_overrides:
            for col, overrides in chk.term_overrides.items():
                merged_term_overrides.setdefault(col, {}).update(overrides)
        if chk.multi_step_terms:
            merged_multi_step_terms.update(chk.multi_step_terms)
        if chk.source_prefill:
            merged_source_prefill.update(chk.source_prefill)

    # Collect diffs across all checks
    all_missing: set[Term] = set()
    all_ref_not_source: set[Term] = set()
    all_source_field_values: dict[tuple[str, str], dict[str, str]] = {}
    source_repo_paths: list[Path] = []
    source_check_repos: list[tuple] = []

    for chk in checks:
        source = build_source(chk.source)
        ref = build_source(chk.reference)

        _log(f"[{chk.name}] Fetching {source.label()}...")
        source_terms = source.get_terms(
            project=chk.project,
            only_collections=chk.only_collections,
            skip_collections=chk.skip_collections,
            field_mappings=chk.field_mappings,
        )

        if chk.field_mappings:
            for t in source_terms:
                if t.field and t.value is not None:
                    all_source_field_values.setdefault((t.collection, t.name), {})[t.field] = t.value

        if chk.term_id_transforms:
            source_terms = apply_term_id_transforms(source_terms, chk.term_id_transforms)

        repo_path = source.repo_path()
        if repo_path is not None:
            source_repo_paths.append(repo_path)
            source_check_repos.append((chk, repo_path))

        _log(f"[{chk.name}] Fetching {ref.label()}...")
        ref_terms = ref.get_terms(
            project=chk.project,
            only_collections=chk.only_collections,
            skip_collections=chk.skip_collections,
            field_mappings=chk.field_mappings,
        )

        result = compare_terms(
            source_terms=source_terms,
            reference_terms=ref_terms,
            source_label=source.label(),
            reference_label=ref.label(),
            collection_mappings=chk.collection_mappings,
            exclude_terms=chk.exclude_terms,
        )
        all_missing |= result.in_source_not_reference
        all_ref_not_source |= result.in_reference_not_source

    # Build collection -> universe_type mapping
    import esgvoc.api as ev

    type_mapping: dict[str, str] = {}
    _log(f"Detecting collection types from esgvoc/{project_name}...")
    for col in ev.get_all_collections_in_project(project_name):
        terms_list = list(ev.get_all_terms_in_collection(project_name, col))
        if terms_list:
            type_mapping[col] = terms_list[0].type

    for repo_path in source_repo_paths:
        context_mapping = detect_collection_mapping(repo_path)
        if context_mapping:
            type_mapping.update(context_mapping)
    type_mapping.update(merged_universe_type_mappings)

    # Find project repos for link checks
    project_repo_paths: list[Path] = [p for p in source_repo_paths if detect_collection_mapping(p)]
    if not project_repo_paths:
        project_repo_paths = list(source_repo_paths)

    if not project_repo_paths:
        msg = "No source repo path available for generation."
        raise ValueError(msg)

    if merged_excludes:
        all_missing = _apply_exclusions(all_missing, merged_excludes)

    if collections_filter:
        all_missing = {t for t in all_missing if t.collection in collections_filter}

    update_terms = identify_field_updates(all_missing, all_ref_not_source) or None

    if not all_missing and not update_terms:
        return GenerationReport()

    # Extract source term data for prefill
    source_term_data: dict[str, dict[str, dict]] = {}
    extract_collections: set[str] = set(merged_source_prefill.keys())
    extract_collections.update(merged_multi_step_terms.keys())
    if extract_collections:
        for chk, repo_path in source_check_repos:
            if chk.source.get("type") != "git":
                continue
            cv_subdir = chk.source.get("cv_subdir")
            cv_dir = repo_path / cv_subdir if cv_subdir else repo_path
            cv_format = chk.source.get("cv_format")
            data = extract_term_data(cv_dir, chk.project, collections=list(extract_collections), cv_format=cv_format)
            for col, terms in data.items():
                source_term_data.setdefault(col, {}).update(terms)

    return generate_missing_terms(
        missing_terms=all_missing,
        universe_repo=universe_repo,
        project_repos=project_repo_paths,
        universe_type_overrides=type_mapping or None,
        generation_defaults=merged_generation_defaults or None,
        term_overrides=merged_term_overrides or None,
        multi_step_terms=merged_multi_step_terms or None,
        source_prefill=merged_source_prefill or None,
        source_term_data=source_term_data or None,
        extra_field_values=all_source_field_values or None,
        update_terms=update_terms,
    )
