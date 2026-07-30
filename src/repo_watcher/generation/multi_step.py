"""Multi-step term expansion (e.g. institution_id -> institution + organisation + project link)."""

from __future__ import annotations

import json
from pathlib import Path

from repo_watcher.config.schema import MultiStepConfig
from repo_watcher.domain import Term
from repo_watcher.generation.enrichment import parse_member_descriptions, ror_enrich_institution
from repo_watcher.generation.report import FieldChange, GenerationReport, TermFile, TermRef
from repo_watcher.generation.skeleton import build_project_link, build_universe_skeleton


def handle_multi_step_term(
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
        member_descriptions = parse_member_descriptions(term.name, cv_description, separator or "")

    # Step 1: member_descriptor — one file per atomic member
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
                found_elsewhere = False
                for other_dir in universe_repo.iterdir():
                    if not other_dir.is_dir() or other_dir.name == member_descriptor:
                        continue
                    if (other_dir / f"{member_id}.json").exists():
                        report.universe_existed.append(TermRef(other_dir.name, member_id))
                        found_elsewhere = True
                        break
                if found_elsewhere:
                    continue
                skeleton = build_universe_skeleton(universe_repo, member_descriptor, member_id)
                # ROR enrichment for institution members
                if member_descriptor == "institution":
                    member_desc = member_descriptions.get(member_id)
                    drs_name = member_id.upper()
                    if separator:
                        for part in term.name.split(separator):
                            if part.lower() == member_id:
                                drs_name = part
                                break
                    ror_enrich_institution(
                        skeleton, member_id, member_desc, drs_name, fallback_country_desc=cv_description
                    )
                report.universe_to_create.append(TermFile(member_descriptor, member_id, skeleton))
                valid_members.append(member_id)

    # Step 2: group_descriptor — one file for the full term
    group_dir = universe_repo / group_descriptor
    if group_dir.is_dir():
        group_file = group_dir / f"{term_id}.json"
        if group_file.exists():
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
            skeleton = build_universe_skeleton(universe_repo, group_descriptor, term_id)
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
        link = build_project_link(group_descriptor, term_id)
        report.project_to_create.append(TermFile(term.collection, term_id, link))
