from datetime import datetime, timezone
import os
from pathlib import Path
import re
from typing import Any
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
        r = requests.get(url, headers=HEADERS, params=params or {}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()  # raise for non-2xx status codes so caller can catch
        return r
    except requests.HTTPError as http_err:
        # optionally log more info
        print(f"GitHub API HTTP error: {http_err} (url={url})")
        raise
    except Exception as e:
        print(f"GitHub API request error: {e} (url={url})")
        raise


def get_latest_commit(owner: str, repo: str, branch: str) -> str:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/commits/{branch}"
    repo_json = github_get(url).json()
    sha = repo_json.get("sha")
    date = repo_json.get("commit", {}).get("committer", {}).get(
        "date"
    ) or repo_json.get("commit", {}).get("author", {}).get("date")
    return {"sha": sha, "date": date, "raw": repo_json}


def get_tree_index(owner: str, repo: str, sha: str) -> dict[str, list[str]]:
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
            events["term_created"].append({"collection": collection, "term": term, "path": path})

        elif status == "modified":
            events["term_modified"].append({"collection": collection, "term": term, "path": path})

        elif status == "removed":
            events["term_removed"].append({"collection": collection, "term": term, "path": path})

        elif status == "renamed":
            prev = f.get("previous_filename")
            if prev and "/" in prev:
                pc, pt = prev.split("/", 1)
                events["term_removed"].append({"collection": pc, "term": pt, "path": prev})
            events["term_created"].append({"collection": collection, "term": term, "path": path})

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


@task
def build_report(results):
    parts = []
    for result in results:
        artifact_content = result.get("artifact_content")
        if artifact_content:
            parts.append(artifact_content.strip())

    # séparer chaque bloc par une ligne horizontale pour lisibilité
    md_content = "\n\n---\n\n".join(parts).strip() + "\n"

    try:
        create_markdown_artifact(markdown=md_content, key="repo-check-recap", description="Concatenated repo artifacts")
    except Exception:
        pass


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
    repo_state = state.get(owner, {}).get(repo_name, {}).get(working_branch) or None
    reco = (read_yaml(reco_path) or {}).get("repos", {}) or {}

    try:
        latest = get_latest_commit(owner, repo_name, working_branch)
    except Exception as exc:
        # return a string, not a set / division
        return {
            "repo": f"{owner}/{repo_name}@{working_branch}",
            "error": f"Failed to fetch commits: {exc}",
        }

    sha = latest.get("sha")
    prev_sha = repo_state.get("sha") if repo_state else None
    date = latest.get("date")
    date_now = datetime.now(timezone.utc)

    changed = False
    recommendations = []
    events = {}
    # => No new commit detected
    if sha == prev_sha:
        print(
            f"No updates for {owner}/{repo_name} on branch {working_branch}. Latest commit: {sha} at {date}."
        )

        artifact_message = (
            f"_No new commits since last check. Latest commit: {sha} at {date}._"
        )
    # => No information stored about this repo/branch yet
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
    # => A new commit has been detected
    else:
        comparison: dict[str, Any] = get_compare(owner, repo_name, prev_sha, sha)
        files_changed: dict[str, Any] = comparison.get("files", [])
        prev_index: dict[str, list[str]] = repo_state.get("files_index", {})
        new_index: dict[str, list[str]] = get_tree_index(owner, repo_name, sha)

        events = classify_files_events(files_changed, prev_index, new_index)

        rec_lines = []
        for event_type, event_list in events.items():
            if not event_list:
                continue
            templates = collect_templates_for_event(reco.get("repos", {}), owner, repo_name, working_branch, event_type)
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
                for t in templates:
                    try:
                        rendered = t.format_map(context_ev)
                    except Exception:
                        rendered = t
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
                for it in items:
                    if isinstance(it, dict):
                        col = it.get("collection")
                        term = it.get("term")
                        if event_type in ("collection_created", "collection_removed"):
                            artifact_message.append(f"  - {col}")
                        else:
                            artifact_message.append(f"  - {term}")
                    else:
                        artifact_message.append(f"  - {it}")
            artifact_message.append("")

        artifact_message = "\n".join(artifact_message)

    artifact_name = (
        f"repo-{owner}-{repo_name}-{working_branch}"
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
        "artifact_content": artifact_content,
        "files_index": repo_state.get("files_index", {}),
    }


@flow(log_prints=True)
def repos_check_flow(
    repo_config_list: list[dict[str, Any]] = None,
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
    
    for future in futures:
        results.append(future.result())

    build_report(results)


if __name__ == "__main__":
    repo_config_path = Path("repos_config.yml")
    if not repo_config_path.exists():
        print(
            "Configuration file 'repos_config.yml' not found at path {repo_config_path}."
        )
        raise SystemExit(0)
    repo_config_list = read_yaml(repo_config_path).get("repos")

    out = repos_check_flow(repo_config_list, notify_slack=True)
