from datetime import datetime, timezone
import os
from pathlib import Path
import re
from typing import Any, Dict, List
from prefect import flow, task
import requests
import yaml

from prefect.artifacts import create_markdown_artifact


GITHUB_API_BASE = "https://api.github.com"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"
HTTP_TIMEOUT = 30


def read_yaml(path: Path) -> dict | None:
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def github_get(url: str, params: dict = None) -> requests.Response:
    try:
        r = requests.get(
            url, headers=HEADERS, params=params or {}, timeout=HTTP_TIMEOUT
        )
    except Exception as e:
        print("Github API request error : {e}")
        raise


def get_latest_commit(owner: str, repo: str, branch: str) -> str:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/commits/{branch}"
    repo_json = github_get(url).json()
    sha = repo_json.get("sha")
    date = repo_json.get("commit", {}).get("committer", {}).get(
        "date"
    ) or repo_json.get("commit", {}).get("author", {}).get("date")
    return {"sha": sha, "date": date, "raw": repo_json}


def get_tree_index(owner: str, repo: str, sha: str) -> dict:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/trees/{sha}"
    tree_json = github_get(url, params={"recursive": "1"}).json()
    files_index = {}
    for entry in tree_json.get("tree", []):
        if entry.get("type") == "blob":
            path = entry.get("path", "")
            dirname = os.path.dirname(path)
            if dirname == "":
                dirname = "."
            filename = os.path.basename(path)
            files_index.setdefault(dirname, []).append(filename)

    return files_index


def get_compare(owner: str, repo: str, base: str, head: str) -> dict:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}"
    return github_get(url).json()


def classify_files_events(
    files_changed: List[Dict[str, Any]],
    prev_index: Dict[str, List[str]],
    new_index: Dict[str, List[str]],
) -> Dict[str, List[Dict[str, str]]]:
    events = {
        "collection_created": [],
        "collection_removed": [],
        "term_created": [],
        "term_modified": [],
        "term_removed": [],
    }

    def _add_collection_created(col):
        if col not in seen_collections:
            events["collection_created"].append({"collection": col})
            seen_collections.add(col)

    prev_dirs = set(prev_index.keys() if prev_index else [])
    new_dirs = set(new_index.keys() if new_index else [])
    seen_collections = set()

    for file in files_changed:
        path = file.get("filename")
        status = file.get("status")
        prev_filename = file.get("previous_filename")

        if not path:
            continue

        if "/" not in path:
            # ignore top-level files for this classification
            continue

        collection, term = path.split("/", 1)

        if status == "added":
            if collection not in prev_dirs:
                _add_collection_created(collection)
            events["term_created"].append({"collection": collection, "term": term, "path": path})
        elif status == "modified":
            events["term_modified"].append({"collection": collection, "term": term, "path": path})
        elif status == "removed":
            events["term_removed"].append({"collection": collection, "term": term, "path": path})
        elif status == "renamed":
            if prev_filename and "/" in prev_filename:
                prev_collection, prev_term = prev_filename.split("/", 1)
                events["term_removed"].append({"collection": prev_collection, "term": prev_term, "path": prev_filename})
            events["term_created"].append({"collection": collection, "term": term, "path": path})
        else:
            continue

    # Now detect collections that existed before but no longer exist -> collection_removed
    # ignore the top-level "." directory when detecting collection removal
    prev_dirs_filtered = {d for d in prev_dirs if d != "."}
    new_dirs_filtered = {d for d in new_dirs if d != "."}
    removed_dirs = sorted(prev_dirs_filtered - new_dirs_filtered)
    for col in removed_dirs:
        if col not in seen_collections:
            events["collection_removed"].append({"collection": col})

    # Deduplicate lists preserving order (helper for dict lists)
    def _dedup_dicts(lst, key):
        seen = set()
        out = []
        for d in lst:
            k = d.get(key)
            if k not in seen:
                out.append(d)
                seen.add(k)
        return out

    events["collection_created"] = _dedup_dicts(events["collection_created"], "collection")
    events["collection_removed"] = _dedup_dicts(events["collection_removed"], "collection")
    events["term_created"] = _dedup_dicts(events["term_created"], "path")
    events["term_modified"] = _dedup_dicts(events["term_modified"], "path")
    events["term_removed"] = _dedup_dicts(events["term_removed"], "path")

    return events


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

    state = read_yaml(state_path) or {}
    repo_state = state.get(owner, {}).get(repo_name, {}).get(working_branch, {}) or None
    reco = read_yaml(reco_path).get("repos", {}) or {}

    try:
        latest = get_latest_commit(owner, repo_name, working_branch)
    except Exception as exc:
        return {
            "repo": {repo_name} / {working_branch},
            "error": f"Failed to fetch commits: {exc}",
        }

    sha = latest.get("sha")
    prev_sha = repo_state.get("sha")
    date = latest.get("date")
    date_now = datetime.now(timezone.utc)

    changed = False
    recommendations = []
    events = {}
    # No new commit
    if sha == prev_sha:
        print(
            f"No updates for {owner}/{repo_name} on branch {working_branch}. Latest commit: {sha} at {date}."
        )

        artifact_message = (
            f"_No new commits since last check. Latest commit: {sha} at {date}._"
        )
    # Initial commit
    elif repo_state is None:
        print(f"Initial check for {owner}/{repo_name} on branch {working_branch}.")

        new_index = get_tree_index(owner, repo_name, sha)
        state.setdefault(owner, {}).setdefault(repo_name, {})[working_branch] = {
            "sha": sha,
            "date": date,
            "checked_at": date_now.isoformat(),
            "files_index": new_index,
        }

        print(
            f"Initial state recorded for {owner}/{repo_name} on branch {working_branch} at {date_now}. Commit SHA: {sha} published at {date_now.strftime("%Y-%m-%d %H:%m:%s")}."
        )

        artifact_message = f"_Initial check completed. Latest commit: {sha} at {date}._"
    # New commit
    else:
        comparison = get_compare(owner, repo_name, prev_sha, sha)
        files_changed = comparison.get("files", [])
        prev_index = repo_state.get("files_index", {})
        new_index = get_tree_index(owner, repo_name, sha)
        
        events = classify_files_events(files_changed, prev_index, new_index)

        


    artifact_name = (
        f"repo-{owner}-{repo_name}-{working_branch}-{date_now.strftime("%Y%m%d")}"
    )
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
        "branch": working_branch,
        "sha": sha,
        "date": date,
        "changed": changed,
        "state": repo_state,
        "recommendations": recommendations,
        "events": events,
        "artifact_name": artifact_name_safe,
        "files_index": repo_state.get("files_index", {}),
    }


@flow(log_prints=True)
def repos_check_flow(
    repo_config_list: List[Dict[str, Any]] = None,
    state_file: str = "repo_watcher_state.yml",
    reco_file: str = "repo_recommendations.yml",
    artifacts_dir: str = "artifacts",
    notify_slack: bool = False,
):
    state_path = Path(state_file)
    if not state_path.exists():
        print("Recommendations file '{state_file}' not found at path {state_path}.")
        raise SystemExit(0)

    reco_path = Path(reco_file)
    if not reco_path.exists():
        print("Recommendations file '{reco_file}' not found at path {reco_path}.")
        raise SystemExit(0)

    futures = []
    results = []
    for repo_config in repo_config_list:
        owner: str = repo_config.get("owner")
        repo_name: str = repo_config.get("repo")
        working_branch: str = repo_config.get("working_branch")
        reference_branch: str | None = repo_config.get("reference_branch")
        project: str = repo_config.get("project")

        check_future = repo_check_single(
            owner,
            repo_name,
            working_branch,
            reference_branch,
            project,
            state_path,
            reco_path,
        )

        futures.append(check_future)


if __name__ == "__main__":
    repo_config_path = Path("repos_config.yml")
    if not repo_config_path.exists():
        print(
            "Configuration file 'repos_config.yml' not found at path {repo_config_path}."
        )
        raise SystemExit(0)
    repo_config_list = read_yaml(repo_config_path).get("repos")

    out = repos_check_flow(repo_config_list, notify_slack=True)
