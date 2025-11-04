import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any
import requests
import yaml
from collections import defaultdict
from git import Repo, GitCommandError

from prefect.artifacts import create_markdown_artifact
from prefect import flow, task, runtime
from prefect.client import get_client
from prefect.states import Cancelled
from prefect.client.schemas.objects import Variable


GITHUB_API_BASE = "https://api.github.com"
# GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
# HEADERS = {"Accept": "application/vnd.github+json"}
# if GITHUB_TOKEN:
#     HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"
# HTTP_TIMEOUT = 30
REPOS_BASE = Path("repos_cache")
STATE_VARIABLE_NAME = "repo_watcher_state"


def load_state_variable() -> dict[str, Any]:
    """
    """
    try:
        var = Variable.get(STATE_VARIABLE_NAME, default=None)
        if isinstance(var, str):
            return yaml.safe_load(var)
        elif isinstance(var, dict):
            return var
        else:
            return {}
    except Exception:
        return {}

def save_state_variable(state: dict[str, Any]):
    """
    """
    try:
        Variable.set(
            STATE_VARIABLE_NAME,
            yaml.safe_dump(state, sort_keys=True, allow_unicode=True),
        )
    except Exception as exc:
        print(f"⚠️ Failed to save state variable: {exc}")


def read_yaml(path: Path) -> dict | None:
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


def _repo_path(owner: str, repo: str) -> Path:
    """Construit le chemin local du dépôt."""
    return REPOS_BASE / f"{owner}__{repo}"


def _fetch(repo: Repo) -> None:
    """Met à jour le dépôt local depuis le remote origin."""
    try:
        repo.remotes.origin.fetch(prune=True)
    except GitCommandError:
        repo.remotes.origin.fetch()


def get_or_clone_repo(owner: str, repo: str, remote_url: str | None = None) -> Repo:
    """
    Ouvre un dépôt local s'il existe, sinon le clone depuis GitHub.
    """
    repo_path = _repo_path(owner, repo)
    repo_path.parent.mkdir(parents=True, exist_ok=True)

    if not repo_path.exists():
        if remote_url is None:
            remote_url = f"https://github.com/{owner}/{repo}.git"
        Repo.clone_from(remote_url, repo_path)

    _fetch(repo_path)

    return Repo(repo_path)





# --------------------------------------------------------------------
# Fonctions principales
# --------------------------------------------------------------------

def get_latest_commit(owner: str, repo: str, branch: str) -> dict[str, Any]:
    """Retourne le SHA et la date du dernier commit d'une branche donnée."""
    r = get_or_clone_repo(owner, repo)
    try:
        commit = r.commit(f"origin/{branch}")
    except Exception:
        commit = r.commit(branch)

    sha = commit.hexsha
    dt = commit.committed_datetime
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return {"sha": sha, "date": dt.isoformat(), "raw": commit}


def get_tree_index(owner: str, repo: str, sha: str) -> dict[str, list[str]]:
    """Retourne un index des fichiers présents dans un commit donné."""
    r = get_or_clone_repo(owner, repo)
    r.commit(sha)

    out = r.git.ls_tree("-r", "--name-only", sha)
    files_index = {}

    for line in out.splitlines():
        if not line:
            continue
        path = Path(line)
        dirname = path.parent.as_posix() or "."
        filename = path.name
        files_index.setdefault(dirname, []).append(filename)

    return files_index


def get_compare(owner: str, repo: str, base: str, head: str) -> dict[str, Any]:
    """Retourne la liste des fichiers modifiés, ajoutés, supprimés ou renommés entre deux commits."""
    r = get_or_clone_repo(owner, repo)
    out = r.git.diff("--name-status", "-M", base, head)
    files = []

    for line in out.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        status = parts[0]
        if status.startswith("R"):
            files.append({
                "status": "renamed",
                "filename": parts[2],
                "previous_filename": parts[1],
            })
        else:
            mapping = {"A": "added", "M": "modified", "D": "removed", "T": "modified"}
            files.append({
                "status": mapping.get(status, status.lower()),
                "filename": parts[1],
            })

    return {"files": files}


def get_contents(owner: str, repo: str, file: str, ref: str) -> str | None:
    """Retourne le contenu d’un fichier à une révision donnée (commit, tag ou branche)."""
    r = get_or_clone_repo(owner, repo)
    try:
        return r.git.show(f"{ref}:{file}")
    except GitCommandError:
        return None


def classify_files_events(
    files_changed: list[dict[str, Any]],
    prev_index: dict[str, list[str]],
    new_index: dict[str, list[str]],
) -> dict[str, list[dict[str, str]]]:
    """
    ...

    Parameters
    ----------
    files_changed : list[dict[str, Any]]
        List of files changed between the previously stored commit version and the most recent one found, as returned by GitHub compare API.
        Example:
        [
            {"sha": "...", "filename": "...", "status": "added", ...},
            ...
        ]

    prev_index : dict[str, list[str]]
        Tree index for the previously recorded commit/branch. Mapping directory -> list of
        filenames found in that directory. Top-level files are under the "." key.
        Example:
        {
            ".": ["README.md", "project_specs.json"],
            "activity_id": ["xxx.json", "yyy.json"],
            ...
        }

    new_index : dict[str, list[str]]
        Tree index for the new/latest commit/branch.
        Example:
        {
            ".": ["README.md", "project_specs.json"],
            "activity_id": ["xxx.json", "yyy.json"],
            ...
        }

    Returns
    -------
    events : dict[str, list[dict[str, str]]]
        Dictionary containing, for each event registered, a list of dictionaries with all important details about the object concerned by the event.
        Example:
        {
            "collection_removed": [{"collection": "old_collection"}],
            "term_created": [
                {"collection": "activity_id", "term": "cmip.json", "path": "activity_id/cmip.json"}
            ],
            ...
        }
    """
    events = {
        "collection_created": [],
        "collection_removed": [],
        "term_created": [],
        "term_modified": [],
        "term_removed": [],
    }

    prev_dirs = {d for d in (prev_index or {}) if d != "."}
    new_dirs = {d for d in (new_index or {}) if d != "."}
    seen_collections = set()

    for f in files_changed or []:
        path = f.get("filename")
        if not path or "/" not in path:
            continue
        collection, term = path.split("/", 1)
        status = f.get("status")

        if status == "added":
            if collection not in prev_dirs and collection not in seen_collections:
                events["collection_created"].append({"collection": collection})
                seen_collections.add(collection)
            events["term_created"].append(
                {"collection": collection, "term": term, "path": path}
            )

        elif status == "modified":
            events["term_modified"].append(
                {"collection": collection, "term": term, "path": path}
            )

        elif status == "removed":
            events["term_removed"].append(
                {"collection": collection, "term": term, "path": path}
            )

        elif status == "renamed":
            prev = f.get("previous_filename")
            if prev and "/" in prev:
                pc, pt = prev.split("/", 1)
                events["term_removed"].append(
                    {"collection": pc, "term": pt, "path": prev}
                )
            events["term_created"].append(
                {"collection": collection, "term": term, "path": path}
            )

    # collections removed (present before, absent now)
    for col in sorted(prev_dirs - new_dirs):
        if col not in seen_collections:
            events["collection_removed"].append({"collection": col})

    # deduplicate preserving order
    def _dedup(lst, key):
        seen = set()
        out = []
        for item in lst:
            k = item.get(key)
            if k not in seen:
                out.append(item)
                seen.add(k)
        return out

    events["collection_created"] = _dedup(events["collection_created"], "collection")
    events["collection_removed"] = _dedup(events["collection_removed"], "collection")
    events["term_created"] = _dedup(events["term_created"], "path")
    events["term_modified"] = _dedup(events["term_modified"], "path")
    events["term_removed"] = _dedup(events["term_removed"], "path")

    return events


def collect_templates_for_event(
    recos: dict, owner: str, repo_name: str, branch: str, event_type: str
) -> dict:
    """
    Return list of templates defined for event_type at repo->branch->events.<event_type>
    If none found, return [] (caller may fallback to globals).
    """
    recos_conf = recos.get(owner, {}).get(repo_name, {}).get(branch, {}) or {}

    return recos_conf


@task(log_prints=True)
def repo_check_single(
    owner: str,
    repo_name: str,
    working_branch: str,
    reference_branch: str | None,
    project: str,
    state_path: Path,
    reco_path: Path,
) -> dict:
    """
    Check a single repository for updates based on the provided configuration.

    Args:
        owner (str): The owner of the repository.
        repo_name (str): The name of the repository.
        working_branch (str): The branch to check for updates.
        reference_branch (str | None): The branch to compare against, if any.
        project (str): The project name associated with the repository.
        state_path (Path): Path to the state file.
        reco_path (Path): Path to the recommendations file.

    Returns:
        dict: A dictionary containing the results of the check.
    """

    state = load_state_variable()
    repo_key = f"{owner}/{repo_name}@{working_branch}"
    repo_state = state.get(repo_key, None)

    reco = (read_yaml(reco_path) or {}).get("globals", {}) or {}

    try:
        latest = get_latest_commit(owner, repo_name, working_branch)
    except Exception as exc:
        print("⚠️ Failed to fetch latest commit for {owner}/{repo_name}@{working_branch}: {exc}")
        return {
            "repo": f"{owner}/{repo_name}@{working_branch}",
            "error": f"Failed to fetch commits: {exc}",
        }

    sha = latest.get("sha")
    prev_sha = repo_state.get("sha") if repo_state else None
    date = latest.get("date")
    date_now = datetime.now(timezone.utc)

    changed = False
    events = {}

    # No new commit detected
    if sha == prev_sha:
        print(
            f"✅ No updates for {owner}/{repo_name} on branch {working_branch}. Latest commit: {sha} at {date}."
        )

        artifact_message = (
            f"✅ No new commits since last check. Latest commit: {sha} at {date}."
        )
    
    # No information stored about this repo/branch yet
    elif repo_state is None:
        print(f"🟣 Initial check for {owner}/{repo_name} on branch {working_branch}.")
        artifact_message = f"🟣 Initial check completed. Latest commit: {sha} at {date}."

    # A new commit has been detected
    else:
        comparison = get_compare(owner, repo_name, prev_sha, sha)
        files_changed = comparison.get("files", [])
        prev_index = get_tree_index(owner, repo_name, prev_sha)
        new_index= get_tree_index(owner, repo_name, sha)

        events = classify_files_events(files_changed, prev_index, new_index)

        if project == "universe":
            recommendation_type = "universe"
        else:
            recommendation_type = "vocab"
        
        rec_lines = []
        for event_type, event_list in events.items():
            if not event_list:
                continue
            templates = (reco.get(recommendation_type) or {}).get(event_type, [])
            for occ in event_list:
                context_ev = {
                    "repo": repo_name,
                    "owner": owner,
                    "repo_name": repo_name,
                    "branch": working_branch,
                    "sha": sha or "",
                    "date": date or "",
                    "collection": occ.get("collection", ""),
                    "term": occ.get("term", ""),
                    "path": occ.get("path", ""),
                }
                for template in templates:
                    try:
                        rendered = template.format_map(context_ev)
                    except Exception:
                        rendered = template
                    if rendered and rendered not in rec_lines:
                        rec_lines.append(rendered)

        artifact_message = []
        if rec_lines:
            artifact_message.append("### Recommendations")
            for rec in rec_lines:
                artifact_message.append(f"- {rec}")
            artifact_message.append("")
        if events:
            artifact_message.append("### Events detected")
            for event_type, items in events.items():
                if not items:
                    continue
                artifact_message.append(f"- {event_type}:")
                for item in items:
                    if isinstance(item, dict):
                        collection = item.get("collection")
                        path = item.get("path")
                        if event_type in ("collection_created", "collection_removed"):
                            artifact_message.append(f"  - {collection}")
                        else:
                            artifact_message.append(f"  - {path}")
                    else:
                        artifact_message.append(f"  - {item}")
            artifact_message.append("")

        artifact_message = "\n".join(artifact_message)

        changed = True

    artifact_name = f"repo-{owner}-{repo_name}-{working_branch}"
    artifact_name_safe = re.sub(r"[^a-z0-9-]", "-", artifact_name.lower())

    artifact_content = "\n".join(
        [
            f"# Check: {owner}/{repo_name}@{working_branch}",
            f"- Latest Commit: {sha}",
            f"- Date: {date}",
            "",
            f"{artifact_message}",
        ]
    )

    create_markdown_artifact(
        markdown=artifact_content,
        key=artifact_name_safe,
        description=f"Report for {owner}/{repo_name}@{working_branch}",
    )

    return {
        "owner": owner,
        "repo": repo_name,
        "working_branch": working_branch,
        "reference_branch": reference_branch,
        "project": project,
        "sha": sha,
        "date": date,
        "changed": changed,
        "task_run_id": runtime.task_run.id,
        "events": events,
        "artifact_name": artifact_name_safe,
        "artifact_content": artifact_content,
        "artifact_message": artifact_message,
    }


def compare_reference(
    owner: str,
    repo_name: str,
    working_branch: str,
    reference_branch: str,
    project: str,
):
    """
    """
    events = {
        "reference_term_missing": [],
        "reference_term_mismatch": [],
        "reference_file_parse_error": [],
    }

    working_sha = get_latest_commit(owner, repo_name, working_branch).get("sha")
    working_tree = get_tree_index(owner, repo_name, working_sha)
    working_root_files = working_tree.get(".", []) or []

    ref_sha = get_latest_commit(owner, repo_name, reference_branch).get("sha")
    ref_tree = get_tree_index(owner, repo_name, ref_sha)
    ref_root_files = ref_tree.get(".", []) or []

    dir_mapping = {}
    dir_mapping_lower = {}
    for dir_name, files in (working_root_files or {}).items():
        dir_mapping[dir_name] = set(files or [])
        dir_name_lower = dir_name.lower()
        dir_mapping_lower[dir_name_lower] = set()
        for f in (files or []):
            dir_mapping_lower[dir_name_lower].add(f.lower())

    skipped_collections = {"drs", "license", "fixed", "required_global_attributes", "table_id"}

    for file in ref_root_files:
        if "_" not in file:
            continue
        project_prefix, _, remainder = file.partition("_")
        if not remainder.lower().endswith(".json"):
            continue
        collection = remainder[:-5]

        if collection.lower() in skipped_collections:
            continue

        if project and project_prefix.lower() != project.lower():
            continue

        ref_content = get_contents(owner, repo_name, file, reference_branch)
        if not ref_content:
            events["reference_file_parse_error"].append({
                "file": file, "reason": "could not fetch content", "collection": collection
            })
            continue

        try:
            ref_json = json.loads(ref_content)
        except Exception as exc:
            events["reference_file_parse_error"].append({
                "file": file, "reason": f"json parse error: {exc}", "collection": collection
            })
            continue

        collection_root = None
        if isinstance(ref_json, dict):
            if collection in ref_json:
                collection_root = ref_json[collection]
            else:
                for k, v in ref_json.items():
                    if k.lower() == collection.lower():
                        collection_root = v
                        break

        if not isinstance(collection_root, dict):
            available = ", ".join(list(ref_json.keys())[:10]) if isinstance(ref_json, dict) else None
            events["reference_file_parse_error"].append({
                "file": file,
                "reason": f"no '{collection}' dict found. Available: {available}",
                "collection": collection,
            })
            continue

        matched_dir = None
        collection_lower = collection.lower()
        for d in dir_mapping.keys():
            if d.lower() == collection_lower:
                matched_dir = d
                break

        for term_key in collection_root.keys():
            expected_lower = f"{term_key}.json".lower()
            found_path = None
            found_fname = None

            if matched_dir:
                matched_dir_lower = matched_dir.lower()
                filenames_lower = dir_mapping_lower.get(matched_dir_lower, set())
                if expected_lower in filenames_lower:
                    for fname in dir_mapping.get(matched_dir, set()):
                        if fname.lower() == expected_lower:
                            found_fname = fname
                            found_path = f"{matched_dir}/{fname}"
                            break

            if not found_path:
                events["reference_term_missing"].append({
                    "file": file,
                    "expected": f"{collection}/{term_key.lower()}.json",
                    "key": term_key,
                    "collection": collection
                })
                continue

            working_content = get_contents(owner, repo_name, found_path, working_branch)
            if not working_content:
                events["reference_term_mismatch"].append({
                    "file": found_path,
                    "reason": "could not fetch working file",
                    "key": term_key,
                    "collection": collection
                })
                continue

            try:
                wj = json.loads(working_content)
            except Exception as exc:
                events["reference_term_mismatch"].append({
                    "file": found_path,
                    "reason": f"json parse error: {exc}",
                    "key": term_key,
                    "collection": collection
                })
                continue

            expected_id = term_key.lower()
            actual_id = wj.get("id") if isinstance(wj, dict) else None
            if not isinstance(actual_id, str) or actual_id.lower() != expected_id:
                events["reference_term_mismatch"].append({
                    "file": found_path,
                    "key": term_key,
                    "expected_id": expected_id,
                    "actual_id": actual_id,
                    "collection": collection
                })
                continue

            actual_type = wj.get("type")
            if not isinstance(actual_type, str) or not actual_type:
                events["reference_term_mismatch"].append({
                    "file": found_path,
                    "key": term_key,
                    "reason": "missing or invalid 'type' field",
                    "collection": collection
                })

    return events


@task(log_prints=True)
def check_reference_consistency(
    owner: str,
    repo_name: str,
    working_branch: str,
    reference_branch: str | None,
    project: str,
    reco_path: Path,
):
    reco = (read_yaml(reco_path) or {}).get("globals", {}) or {}

    ref_events = compare_reference(
        owner,
        repo_name,
        working_branch,
        reference_branch,
        project,
    )

    if project == "universe":
        recommendation_type = "universe"
    else:
        recommendation_type = "vocab"
    rec_lines = []
    for event_type, event_list in ref_events.items():
        if not event_list:
            continue
        templates = (reco.get(recommendation_type) or {}).get(event_type, [])
        for occ in event_list:
            context_ev = {
                "repo": repo_name,
                "owner": owner,
                "repo_name": repo_name,
                "branch": working_branch,
                **(occ if isinstance(occ, dict) else {"item": occ}),
            }
            for template in templates:
                try:
                    rendered = template.format_map(context_ev)
                except Exception:
                    rendered = template
                if rendered and rendered not in rec_lines:
                    rec_lines.append(rendered)
    
    safe_branch = re.sub(r"[^a-z0-9-]", "-", (reference_branch or "").lower())
    artifact_name = f"repo-{owner}-{repo_name}-{working_branch}-refcheck-{safe_branch}"
    artifact_name_safe = re.sub(r"[^a-z0-9-]", "-", artifact_name.lower())
    artifact_message = []

    grouped_missing = defaultdict(list)
    grouped_mismatch = defaultdict(list)
    grouped_parse = defaultdict(list)

    for event in ref_events.get("reference_term_missing", []):
        grouped_missing[event.get("collection") or "root"].append(event)
    for event in ref_events.get("reference_term_mismatch", []):
        grouped_mismatch[event.get("collection") or "root"].append(event)
    for event in ref_events.get("reference_file_parse_error", []):
        grouped_parse[event.get("collection") or "root"].append(event)

    if grouped_missing:
        artifact_message.append("## 🔎❓ Missing expected files in working branch")
        for coll, items in grouped_missing.items():
            artifact_message.append(f"### {coll}")
            for item in items:
                artifact_message.append(f"⚠️ expected {item.get('expected')} (from reference file {item.get('file')}, key={item.get('key')})")
        artifact_message.append("")

    if grouped_mismatch:
        artifact_message.append("## 📄❗ Mismatched or invalid files in working branch")
        for coll, items in grouped_mismatch.items():
            artifact_message.append(f"### {coll}")
            for item in items:
                if item.get("reason"):
                    artifact_message.append(f"⚠️ {item.get('file')}: {item.get('reason')} (key={item.get('key')})")
                else:
                    artifact_message.append(f"⚠️ {item.get('file')}: expected id {item.get('expected_id')} got {item.get('actual_id')} (key={item.get('key')})")
        artifact_message.append("")

    if grouped_parse:
        artifact_message.append("## 📄❌ Reference file parse errors")
        for coll, items in grouped_parse.items():
            artifact_message.append(f"### {coll}")
            for item in items:
                artifact_message.append(f"⚠️ {item.get('file')}: {item.get('reason')}")
        artifact_message.append("")

    if artifact_message:
        artifact_message = "\n".join(artifact_message)
        cancelled = False
    else:
        artifact_message = "✅ No issues detected."
        cancelled = True

    artifact_content = "\n".join(
    [
            f"# Reference coherence check — {owner}/{repo_name}",
            "",
            f"- working branch: {working_branch}",
            f"- reference branch: {reference_branch}",
            f"- checked at: {datetime.now(timezone.utc).isoformat()}",
            "",
            f"{artifact_message}",
        ]
    )

    create_markdown_artifact(
        markdown=artifact_content,
        key=artifact_name_safe,
        description=f"Reference check for {owner}/{repo_name} between working branch {working_branch} and reference branch {reference_branch}",
    )

    return {
        "owner": owner,
        "repo": repo_name,
        "working_branch": working_branch,
        "reference_branch": reference_branch,
        "reference_events": ref_events,
        "reference_recommendations": rec_lines,
        "artifact_name": artifact_name_safe,
        "artifact_content": artifact_content,
        "artifact_message": artifact_message,
        "cancelled": cancelled,
        "task_run_id": runtime.task_run.id,
    }


@task(log_prints=True)
def build_check_report(results):
    parts = []
    for result in results.values():
        artifact_content = result.get("artifact_content")
        if artifact_content:
            parts.append(artifact_content.strip())

    md_content = "\n\n---\n\n".join(parts).strip() + "\n"

    try:
        create_markdown_artifact(
            markdown=md_content,
            key="repo-check-recap",
            description="Concatenated repo check artifacts",
        )
    except Exception:
        pass


@task(log_prints=True)
def build_consistency_report(results):
    parts = []
    for result in results:
        artifact_content = result.get("artifact_content")
        if artifact_content:
            parts.append(artifact_content.strip())

    md_content = "\n\n---\n\n".join(parts).strip() + "\n"

    try:
        create_markdown_artifact(
            markdown=md_content,
            key="repo-consistency-recap",
            description="Concatenated repo consistency artifacts",
        )
    except Exception:
        pass


@task(log_prints=True)
def build_final_report(check_results, concistency_results):
    parts = []
    for key in check_results:
        check_result = check_results[key]
        artifact_content = "\n".join(
        [
                f"# Repo report — {check_result['owner']}/{check_result['repo']}",
                "",
                f"- working branch: {check_result['working_branch']}",
                f"- reference branch: {check_result['reference_branch']}",
                f"- checked at: {datetime.now(timezone.utc).isoformat()}",
                "",
                "## Repo check",
                f"{check_result['artifact_message']}",
                "",
            ]
        )
        if key in concistency_results:
            concistency_result = concistency_results[key]
            artifact_content += "\n".join(
                [
                    "## Reference consistency check",
                    f"{concistency_result['artifact_message']}",
                    "",
                ]
            )

        parts.append(artifact_content.strip() + "\n")

    md_content = "\n\n---\n\n".join(parts).strip() + "\n"

    try:
        create_markdown_artifact(
            markdown=md_content,
            key="repo-check-recap",
            description="Concatenated repo artifacts",
        )
    except Exception:
        pass


@flow(log_prints=True)
def repos_check_flow(
    repo_config_list: list[dict[str, Any]] | None = None,
    state_file: str = "repo_watcher_state.yml",
    reco_file: str = "repo_recommendations.yml",
):
    state_path = Path(state_file)
    if not state_path.exists():
        print("❌ Recommendations file '{state_file}' not found at path {state_path}.")
        raise SystemExit(0)

    reco_path = Path(reco_file)
    if not reco_path.exists():
        print("❌ Recommendations file '{reco_file}' not found at path {reco_path}.")
        raise SystemExit(0)

    check_results = {}
    for repo_config in repo_config_list:
        owner: str = repo_config.get("owner")
        repo_name: str = repo_config.get("repo")
        working_branch: str = repo_config.get("working_branch")
        reference_branch: str | None = repo_config.get("reference_branch")
        project: str = repo_config.get("project")

        check_result = repo_check_single(
            owner,
            repo_name,
            working_branch,
            reference_branch,
            project,
            state_path,
            reco_path,
        )

        if check_result and check_result.get("cancelled"):
            with get_client(sync_client=True) as client:
                client.set_task_run_state(
                    task_run_id=check_result.get("task_run_id"),
                    state=Cancelled(),
                    force=True,
                )

        check_results[f"{owner}-{repo_name}-{working_branch}"] = check_result
    
    build_check_report(check_results)

    consistency_results = []
    for key, check_result in check_results.items():
        if check_result["reference_branch"]:
            consistency_result = check_reference_consistency(
                check_result["owner"],
                check_result["repo"],
                check_result["working_branch"],
                check_result["reference_branch"],
                check_result["project"],
                reco_path,
            )

            if consistency_result and consistency_result.get("cancelled"):
                with get_client(sync_client=True) as client:
                    client.set_task_run_state(
                        task_run_id=consistency_result.get("task_run_id"),
                        state=Cancelled(),
                        force=True,
                    )

            consistency_results.append(consistency_result)
    
    build_consistency_report(consistency_results)

    build_final_report(check_results, consistency_results)


if __name__ == "__main__":
    repo_config_path = Path("repos_config.yml")
    if not repo_config_path.exists():
        print(
            "Configuration file 'repos_config.yml' not found at path {repo_config_path}."
        )
        raise SystemExit(0)
    repo_config_list = read_yaml(repo_config_path).get("repos")

    out = repos_check_flow(repo_config_list)
