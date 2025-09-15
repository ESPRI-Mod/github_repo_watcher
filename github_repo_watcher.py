# prefect_github_repo_watcher_full.py
"""
Functions:
 - Vérifies a list of GitHub repos (owner/repo[@branch])
 - Persists state into a YAML file (sha, date, files_index)
 - Uses the GitHub compare API to detect added/modified/removed/renamed files
 - Classifies changes into events: collection_created, term_created, term_modified, term_removed, collection_removed
 - Applies recommendation templates defined in repo_recommendations.yml
 - Writes a Markdown report into artifacts/, returns the path in the flow
 - Optionally: send a Slack notification if changes are detected (disabled by default)

Dependencies:
 pip install prefect requests pyyaml
"""

import os
import time
import re
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Union
from pathlib import Path

import requests
import yaml

import asyncio
import inspect
import anyio

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import flow_run as _flow_run
from prefect.client.orchestration import get_client
from prefect.blocks.notifications import SlackWebhook

# ---------------------------
# Configuration
# ---------------------------
GITHUB_API_BASE = "https://api.github.com"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

HTTP_TIMEOUT = 30
LOG = logging.getLogger("github_api")


def run_coro_sync(coro):
    """
    Run coroutine 'coro' and return its result from synchronous code.
    Works even if an event loop is already running (applies nest_asyncio).
    """
    if not inspect.isawaitable(coro):
        return coro
    try:
        return asyncio.run(coro)
    except RuntimeError:
        # event loop already running (e.g. Jupyter). Try nest_asyncio fallback.
        try:
            import nest_asyncio
        except Exception:
            raise
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)


# ---------------------------
# YAML helpers
# ---------------------------
def read_yaml(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def write_yaml(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(obj, fh, sort_keys=False, allow_unicode=True)


# ---------------------------
# Template rendering helper
# ---------------------------
class DefaultDict(dict):
    def __missing__(self, key):
        return ""


def render_templates(lines: List[str], context: Dict) -> List[str]:
    out = []
    seen = set()
    for l in lines:
        if not l:
            continue
        try:
            rendered = l.format_map(DefaultDict(context))
        except Exception:
            rendered = l
        if rendered and rendered not in seen:
            out.append(rendered)
            seen.add(rendered)
    return out


# ---------------------------
# Recommendation config helpers
# ---------------------------
def load_recommendations_conf(path: Path) -> Dict:
    conf = read_yaml(path) or {}
    return {
        "globals": conf.get("globals", []),
        "repos": conf.get("repos", {}),
    }


def branch_matches(candidate_key: str, branch: str) -> bool:
    if candidate_key == branch:
        return True
    if candidate_key.lower() == branch.lower():
        return True
    if candidate_key.replace("-", "_") == branch:
        return True
    if candidate_key.replace("_", "-") == branch:
        return True
    return False


def collect_repo_and_branch_rules(conf_repos: Dict, owner: str, repo_name: str, branch: str) -> List[str]:
    """
    Return ordered list: repo default rules then branch-specific rules.
    If none present, returns [] (caller may fall back to globals).
    """
    full = f"{owner}/{repo_name}"
    selected: List[str] = []

    repo_conf = conf_repos.get(full)
    if not repo_conf:
        import re
        for key, val in conf_repos.items():
            if key in ("*", ".*"):
                continue
            try:
                if re.fullmatch(key, full):
                    repo_conf = val
                    break
            except re.error:
                continue

    if not repo_conf:
        return []

    if isinstance(repo_conf, dict):
        repo_default = repo_conf.get("default", [])
        if repo_default:
            selected.extend(repo_default)
        branches = repo_conf.get("branches", {}) or {}
        for candidate_key, candidate_val in branches.items():
            if branch_matches(candidate_key, branch):
                if isinstance(candidate_val, list):
                    selected.extend(candidate_val)
                elif isinstance(candidate_val, dict):
                    # if branch maps to dict (with events), ignore here
                    pass
                elif candidate_val is None:
                    pass
                else:
                    selected.append(str(candidate_val))
                break
    else:
        if isinstance(repo_conf, list):
            selected.extend(repo_conf)
        elif repo_conf is None:
            pass
        else:
            selected.append(str(repo_conf))

    return selected


def collect_templates_for_event(conf_repos: Dict, owner: str, repo_name: str, branch: str, event_type: str) -> List[str]:
    """
    Return list of templates defined for event_type at repo->branch->events.<event_type>
    If none found, return [] (caller may fallback to globals).
    """
    full = f"{owner}/{repo_name}"
    repo_conf = conf_repos.get(full)
    if not repo_conf:
        import re
        for key, val in conf_repos.items():
            if key in ("*", ".*"):
                continue
            try:
                if re.fullmatch(key, full):
                    repo_conf = val
                    break
            except re.error:
                continue

    if not repo_conf:
        return []

    templates = []
    if isinstance(repo_conf, dict):
        root_events = repo_conf.get("events", {})
        if isinstance(root_events, dict):
            templates.extend(root_events.get(event_type, []) or [])

        branches = repo_conf.get("branches", {}) or {}
        for candidate_key, candidate_val in branches.items():
            if branch_matches(candidate_key, branch):
                if isinstance(candidate_val, dict):
                    evts = candidate_val.get("events", {}) or {}
                    templates.extend(evts.get(event_type, []) or [])
                elif isinstance(candidate_val, list):
                    # list at branch level: treat as general templates
                    templates.extend(candidate_val)
                break
    elif isinstance(repo_conf, list):
        templates.extend(repo_conf)

    return templates


# ---------------------------
# GitHub helpers (compare / trees)
# ---------------------------
def gh_get(url: str, params: Dict = None) -> requests.Response:
    """
    Helper GET with basic retry and GitHub rate-limit handling.
    - short exponential backoff for transient errors
    - tries to detect X-RateLimit-Remaining and X-RateLimit-Reset and act accordingly
    Raises on non-recoverable errors.
    """
    import time, logging
    LOG = logging.getLogger("github_api")
    max_attempts = 6
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params or {}, timeout=HTTP_TIMEOUT)
        except Exception as exc:
            LOG.warning("Request error %s -> attempt %d/%d: %s", url, attempt, max_attempts, exc)
            time.sleep(backoff)
            backoff *= 2
            continue

        # log rate-limit headers if present
        rem = r.headers.get("X-RateLimit-Remaining")
        reset = r.headers.get("X-RateLimit-Reset")
        if rem is not None:
            try:
                LOG.debug("GitHub rate: remaining=%s reset=%s for %s", rem, reset, url)
            except Exception:
                pass

        # handle explicit rate limit response
        if r.status_code == 403:
            body = (r.text or "").lower()
            if rem == "0" or "rate limit exceeded" in body:
                wait = None
                try:
                    reset_ts = int(reset)
                    now_ts = int(time.time())
                    wait = max(0, reset_ts - now_ts)
                except Exception:
                    wait = None

                # if reset is soon (< 120s), wait explicitly; otherwise do short backoff retries
                if wait is not None and wait <= 120:
                    LOG.warning("Rate limit hit; sleeping %s seconds until reset for %s", wait, url)
                    time.sleep(wait + 1)
                    continue
                else:
                    LOG.warning("Rate limit hit; attempt %d/%d on %s (backoff)", attempt, max_attempts, url)
                    time.sleep(backoff)
                    backoff *= 2
                    continue

        # if server error, allow retries
        if 500 <= r.status_code < 600 and attempt < max_attempts:
            LOG.warning("Server error %s for %s -> retrying %d/%d", r.status_code, url, attempt, max_attempts)
            time.sleep(backoff)
            backoff *= 2
            continue

        try:
            r.raise_for_status()
            return r
        except requests.HTTPError:
            # non-retryable client error (other than rate-limit handled above)
            raise

    raise RuntimeError(f"Failed to GET {url} after {max_attempts} attempts (rate-limited or server error)")


def get_default_branch(owner: str, repo: str) -> str:
    """
    Robust default branch fetch: if key missing/falsy, fallback to 'main'.
    """
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}"
    j = gh_get(url).json()
    return j.get("default_branch") or "main"


def get_latest_commit_on_branch(owner: str, repo: str, branch: str) -> Dict:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/commits/{branch}"
    j = gh_get(url).json()
    sha = j.get("sha")
    date = j.get("commit", {}).get("committer", {}).get("date") or j.get("commit", {}).get("author", {}).get("date")
    return {"sha": sha, "date": date, "raw": j}


def get_compare(owner: str, repo: str, base: str, head: str) -> Dict:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}"
    return gh_get(url).json()


def get_tree_index(owner: str, repo: str, sha_or_ref: str) -> Dict[str, List[str]]:
    """
    Returns map dir -> [filenames] for the tree at sha_or_ref (recursive).
    Top-level files are keyed under ".".

    sha_or_ref can be:
      - a full 40-character commit SHA
      - a branch name (e.g. "main") — in that case we resolve the latest commit SHA first.
    """
    # if sha_or_ref does not look like a commit SHA, resolve it
    resolved_sha = sha_or_ref
    if not isinstance(sha_or_ref, str) or len(sha_or_ref) != 40 or not re.fullmatch(r"[0-9a-fA-F]{40}", sha_or_ref):
        # attempt to resolve branch name to latest commit sha
        try:
            latest = get_latest_commit_on_branch(owner, repo, sha_or_ref)
            resolved_sha = latest.get("sha") or sha_or_ref
        except Exception:
            # let the downstream gh_get raise a clear error if resolution fails
            resolved_sha = sha_or_ref

    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/trees/{resolved_sha}"
    j = gh_get(url, params={"recursive": "1"}).json()
    index: Dict[str, List[str]] = {}
    for entry in j.get("tree", []) or []:
        if entry.get("type") != "blob":
            continue
        path = entry.get("path", "")
        dirpath = "/".join(path.split("/")[:-1])
        if dirpath == "":
            dirpath = "."
        filename = path.split("/")[-1]
        index.setdefault(dirpath, []).append(filename)
    return index


# ---------------------------
# Classification (updated signature)
# ---------------------------
def classify_files_events(files_changed: List[Dict[str, Any]], prev_index: Dict[str, List[str]], new_index: Dict[str, List[str]]) -> Dict[str, List[Dict[str, str]]]:
    """
    Updated: adds detection of 'collection_removed'.
    - files_changed : list of dicts coming from the compare API
    - prev_index / new_index : map dir -> [filenames] (tree index)
    Returns dict of events.
    """
    events = {
        "collection_created": [],
        "collection_removed": [],
        "term_created": [],
        "term_modified": [],
        "term_removed": [],
    }

    prev_dirs = set(prev_index.keys() if prev_index else [])
    new_dirs = set(new_index.keys() if new_index else [])
    seen_collections = set()

    def _add_collection_created(col):
        if col not in seen_collections:
            events["collection_created"].append({"collection": col})
            seen_collections.add(col)

    # Process file-level changes first (added/modified/removed/renamed)
    for f in files_changed:
        path = f.get("filename")
        status = f.get("status")
        prev_filename = f.get("previous_filename")

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


# ---------------------------
# Notifications (Slack)
# ---------------------------
def send_slack_message(webhook_url: str, text: str) -> bool:
    if not webhook_url:
        return False
    payload = {"text": text}
    try:
        r = requests.post(webhook_url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception:
        return False


def check_reference_consistency_minimal(
    owner: str,
    repo_name: str,
    working_branch: str,
    reference_branch: str,
    new_index: Dict[str, List[str]],
    project: str | None = None,
) -> Dict[str, List[Dict[str, str]]]:
    """
    Generic consistency check between reference_branch and working_branch.

    - Scan the reference branch root for files matching "PROJECT_<collection>.json".
      If `project` is provided, only files whose PROJECT matches (case-insensitive)
      are considered. If project is None, accept any PROJECT.
    - For each such file, read the JSON and find the dict named after the collection key
      (case-insensitive lookup). For each key inside that dict, expect working branch file:
        "<collection>/<term_filename>"
      where term_filename is matched case-insensitively against files present.
    - Special-case: skip checking collections named "DRS" and "license".
    - Accept ID matches case-insensitively (actual_id.lower() == expected.lower()) to tolerate
      mixed-case ids (e.g. "1hrCM" vs "1hrcm").
    - Returns events dict with:
        - reference_term_missing
        - reference_term_mismatch
        - reference_file_parse_error
    """
    events = {
        "reference_term_missing": [],
        "reference_term_mismatch": [],
        "reference_file_parse_error": [],
    }

    print(
"owner=", owner,
"repo_name=", repo_name,
"working_branch=", working_branch,
"reference_branch=", reference_branch,
"new_index=", new_index,
"project=", project,
)

    # fetch reference root files (fail-fast on error so user gets alerted)
    try:
        # get_tree_index now resolves branch->sha internally, but pass the branch name
        ref_tree = get_tree_index(owner, repo_name, reference_branch)
        ref_root_files = ref_tree.get(".", []) or []
    except Exception as exc:
        # fail fast so the Prefect task fails and you are alerted
        raise RuntimeError(f"Could not fetch reference tree for {owner}/{repo_name}@{reference_branch}: {exc}")

    # prepare working index mapping (dir -> set(files))
    working_index = new_index or {}
    dir_map: Dict[str, set] = {}
    for d, files in working_index.items():
        dir_map[d] = set(files or [])

    # pattern PROJECT_collection.json -> capture project and collection
    pattern = re.compile(r"^(.+?)_([A-Za-z0-9_]+)\.json$", flags=re.IGNORECASE)

    # helper to fetch contents of a file at a given ref (branch or sha)
    def _fetch_contents(path: str, ref: str):
        try:
            url = f"{GITHUB_API_BASE}/repos/{owner}/{repo_name}/contents/{path}"
            r = requests.get(url, headers=HEADERS, params={"ref": ref}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            j = r.json()
            content = j.get("content")
            encoding = j.get("encoding", "base64")
            if content and encoding == "base64":
                import base64 as _b64
                return _b64.b64decode(content).decode("utf-8")
            return content
        except Exception:
            return None

    # collections to skip (case-insensitive)
    skip_collections = {"drs", "license"}

    # iterate reference root files that match PROJECT_collection.json
    for af in ref_root_files:
        m: re.Match[str] | None = pattern.match(af)
        if not m:
            continue
        ref_project = m.group(1)
        collection_key_from_filename = m.group(2)  # e.g. "activity_id" or "sub_experiment_id"

        # skip known special files
        if collection_key_from_filename.lower() in skip_collections:
            continue

        # if a project is provided, skip files whose project doesn't match (case-insensitive)
        if project and ref_project.lower() != project.lower():
            continue

        # fetch and parse the reference file
        content = _fetch_contents(af, reference_branch)
        if not content:
            events["reference_file_parse_error"].append({
                "file": af,
                "reason": "could not fetch content",
                "collection": collection_key_from_filename,
            })
            continue
        try:
            ref_json = json.loads(content)
        except Exception as exc:
            events["reference_file_parse_error"].append({
                "file": af,
                "reason": f"json parse error: {exc}",
                "collection": collection_key_from_filename,
            })
            continue

        # case-insensitive lookup for the collection key inside the reference JSON
        collection_root = None
        collection_key_used = None
        if isinstance(ref_json, dict):
            # exact match first
            if collection_key_from_filename in ref_json:
                collection_root = ref_json[collection_key_from_filename]
                collection_key_used = collection_key_from_filename
            else:
                for k in ref_json.keys():
                    if k.lower() == collection_key_from_filename.lower():
                        collection_root = ref_json[k]
                        collection_key_used = k
                        break

        if not collection_root or not isinstance(collection_root, dict):
            # provide helpful diagnostic: show available keys (case-insensitive lookup performed)
            available = None
            if isinstance(ref_json, dict):
                available = ", ".join(list(ref_json.keys())[:10])  # limit length
            events["reference_file_parse_error"].append({
                "file": af,
                "reason": f"no '{collection_key_from_filename}' dict found (case-insensitive lookup performed). Available top-level keys: {available}",
                "collection": collection_key_from_filename,
            })
            continue
        
        # for each term in the reference collection, expect working file at: collection_key/<term_lower>.json
        for term_key in collection_root.keys():
            # expected filename (we will match filenames case-insensitively)
            expected_filename = f"{term_key}.json"
            expected_filename_lower = expected_filename.lower()
            expected_path_log = f"{collection_key_from_filename}/{term_key.lower()}.json"

            # find matching directory in working branch (case-insensitive)
            matched_dir = None
            for d in dir_map.keys():
                if d.lower() == collection_key_from_filename.lower():
                    matched_dir = d
                    break

            found_path = None
            found_filename_actual = None
            if matched_dir:
                # search for filename case-insensitively in the matched dir
                for fname in dir_map.get(matched_dir, set()):
                    if fname.lower() == expected_filename_lower:
                        found_filename_actual = fname
                        found_path = f"{matched_dir}/{fname}"
                        break

            if not found_path:
                # not found -> register missing
                events["reference_term_missing"].append({
                    "file": af,
                    "expected": expected_path_log,
                    "key": term_key,
                    "collection": collection_key_from_filename,
                })
                continue

            # fetch and validate working file content
            working_content = _fetch_contents(found_path, working_branch)
            if not working_content:
                events["reference_term_mismatch"].append({
                    "file": found_path,
                    "reason": "could not fetch working file",
                    "key": term_key,
                    "collection": collection_key_from_filename,
                })
                continue

            try:
                wj = json.loads(working_content)
            except Exception as exc:
                events["reference_term_mismatch"].append({
                    "file": found_path,
                    "reason": f"json parse error: {exc}",
                    "key": term_key,
                    "collection": collection_key_from_filename,
                })
                continue

            # ID check: accept case-insensitive match between expected and actual
            expected_id = term_key.lower()
            actual_id = wj.get("id") if isinstance(wj, dict) else None
            if not isinstance(actual_id, str) or actual_id.lower() != expected_id:
                events["reference_term_mismatch"].append({
                    "file": found_path,
                    "key": term_key,
                    "expected_id": expected_id,
                    "actual_id": actual_id,
                    "collection": collection_key_from_filename,
                })
                continue

            # TYPE presence check (still strict)
            actual_type = wj.get("type")
            if not isinstance(actual_type, str) or not actual_type:
                events["reference_term_mismatch"].append({
                    "file": found_path,
                    "key": term_key,
                    "reason": "missing or invalid 'type' field",
                    "collection": collection_key_from_filename,
                })

    return events


# ---------------------------
# Prefect tasks & flow
# ---------------------------

@task(retries=1, retry_delay_seconds=5)
def check_single_repo_task(repo_with_branch: str, state_path: str, reco_path: str) -> Dict:
    """
    Task: checks a repo@branch, builds recommendations/events,
    creates a per-repo Markdown artifact and returns the result including:
      - 'artifact_key'
      - 'files_index'  <-- NEW: the tree index used for this run (to be reused)
    """
    logger = get_run_logger()

    # parse input
    if "@" in repo_with_branch:
        repo_key, branch = repo_with_branch.split("@", 1)
    else:
        repo_key, branch = repo_with_branch, None

    if "/" not in repo_key:
        return {"repo": repo_with_branch, "error": "Expected owner/repo[@branch]"}

    owner, repo_name = repo_key.split("/", 1)
    logger.info(f"Checking {repo_key} (branch={branch or 'DEFAULT'})")

    state_file = Path(state_path)
    reco_file = Path(reco_path)
    state = read_yaml(state_file) or {}
    reco_conf = load_recommendations_conf(reco_file)

    # branch fallback
    if not branch:
        try:
            branch = get_default_branch(owner, repo_name)
            logger.info(f"No branch provided — using default branch '{branch}'")
        except Exception as exc:
            return {"repo": repo_with_branch, "error": f"Failed to get default branch: {exc}"}

    # fetch latest commit
    try:
        latest = get_latest_commit_on_branch(owner, repo_name, branch)
    except Exception as exc:
        return {"repo": repo_with_branch, "error": f"Failed to fetch commits: {exc}"}

    sha = latest.get("sha")
    date = latest.get("date")

    # recorded state (robust)
    repo_state = state.get(repo_key)
    recorded = None
    if isinstance(repo_state, dict):
        recorded = repo_state.get(branch)

    # FIRST RUN: initialize index and do not emit recommendations
    if recorded is None:
        try:
            new_index = get_tree_index(owner, repo_name, sha)
        except Exception:
            new_index = {}
        state.setdefault(repo_key, {})[branch] = {
            "sha": sha,
            "date": date,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "files_index": new_index,
        }
        write_yaml(state_file, state)
        logger.info(f"Initial state recorded for {repo_key}@{branch} (no recommendations on first run).")

        # Artifact Prefect (no local file)
        ts_short = datetime.now(timezone.utc).strftime("%Y%m%d")
        safe_branch = re.sub(r"[^a-z0-9-]", "-", branch.lower())
        artifact_key_raw = f"repo-{owner}-{repo_name}-{safe_branch}"
        artifact_key = re.sub(r"[^a-z0-9-]", "-", artifact_key_raw.lower())
        md_content = "\n".join([
            f"# Repo check — {repo_key}@{branch}",
            "",
            f"- sha: {sha}",
            f"- date: {date}",
            "",
            "_Initial scan — state recorded. No recommendations produced on first run._",
        ])
        try:
            create_markdown_artifact(markdown=md_content, key=artifact_key, description=f"Repo report for {repo_key}@{branch}")
        except Exception as exc:
            logger.warning(f"Failed to create Prefect artifact for {repo_key}: {exc}")

        return {
            "repo": repo_key,
            "branch": branch,
            "sha": sha,
            "date": date,
            "changed": False,
            "recorded": None,
            "recommendations": [],
            "events": {},
            "artifact_key": artifact_key,
            "files_index": new_index,
        }

    # if sha unchanged -> nothing to do
    prev_sha = recorded.get("sha")
    if prev_sha == sha:
        logger.info(f"No new commits for {repo_key}@{branch}")

        # use previously recorded index (avoid extra API calls)
        new_index = recorded.get("files_index") or {}

        # Artifact Prefect minimal (no local file)
        ts_short = datetime.now(timezone.utc).strftime("%Y%m%d")
        safe_branch = re.sub(r"[^a-z0-9-]", "-", branch.lower())
        artifact_key_raw = f"repo-{owner}-{repo_name}-{safe_branch}"
        artifact_key = re.sub(r"[^a-z0-9-]", "-", artifact_key_raw.lower())
        md_content = "\n".join([
            f"# Repo check — {repo_key}@{branch}",
            "",
            f"- sha: {sha}",
            f"- date: {date}",
            f"- previously recorded sha: {prev_sha}",
            "",
            "_No new commits since last check. No recommendations._",
        ])
        try:
            create_markdown_artifact(markdown=md_content, key=artifact_key, description=f"No-change report for {repo_key}@{branch}")
        except Exception as exc:
            logger.warning(f"Failed to create Prefect artifact for {repo_key}: {exc}")

        return {
            "repo": repo_key,
            "branch": branch,
            "sha": sha,
            "date": date,
            "changed": False,
            "recorded": recorded,
            "recommendations": [],
            "events": {},
            "artifact_key": artifact_key,
            "files_index": new_index,
        }

    # CHANGES: try compare API
    try:
        cmp = get_compare(owner, repo_name, prev_sha, sha)
        files_changed = cmp.get("files", []) or []
    except Exception as exc:
        logger.warning(f"Compare API failed: {exc} — falling back to tree diff.")
        prev_index = recorded.get("files_index") or {}
        try:
            new_index = get_tree_index(owner, repo_name, sha)
        except Exception:
            new_index = {}
        prev_files = set()
        for dir_, files in (prev_index or {}).items():
            for f in files:
                prev_files.add(f"{dir_}/{f}" if dir_ != "." else f)
        new_files = set()
        for dir_, files in (new_index or {}).items():
            for f in files:
                new_files.add(f"{dir_}/{f}" if dir_ != "." else f)
        files_changed = []
        for p in new_files - prev_files:
            files_changed.append({"filename": p, "status": "added"})
        for p in prev_files - new_files:
            files_changed.append({"filename": p, "status": "removed"})

    # load indices for classification
    prev_index = recorded.get("files_index") or {}
    try:
        # new_index might already be set in the except/fallback above; otherwise fetch it
        if "new_index" not in locals():
            new_index = get_tree_index(owner, repo_name, sha)
    except Exception:
        new_index = {}

    events = classify_files_events(files_changed, prev_index, new_index)

    # build recommendations from events (only for events that occurred)
    rec_lines: List[str] = []
    for ev_type, ev_list in events.items():
        if not ev_list:
            continue
        templates = collect_templates_for_event(reco_conf.get("repos", {}), owner, repo_name, branch, ev_type)
        if not templates:
            # fallback to globals if present
            templates = reco_conf.get("globals", [])
        for occ in ev_list:
            context_ev = {
                "repo": repo_key,
                "owner": owner,
                "repo_name": repo_name,
                "branch": branch,
                "sha": sha or "",
                "date": date or "",
                "collection": occ.get("collection", ""),
                "term": occ.get("term", ""),
                "path": occ.get("path", ""),
            }
            for t in templates:
                try:
                    rendered = t.format_map(DefaultDict(context_ev))
                except Exception:
                    rendered = t
                if rendered and rendered not in rec_lines:
                    rec_lines.append(rendered)

    # append auto hints
    rec_lines.append(f"Auto: change(s) detected between {prev_sha[:8]} and {sha[:8]} on branch {branch}.")
    rec_lines.append("Auto: Steps recommended: pull, rebuild, run tests, check CI.")

    # update state
    state.setdefault(repo_key, {})[branch] = {
        "sha": sha,
        "date": date,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "files_index": new_index,
    }
    write_yaml(state_file, state)
    logger.info(f"State updated for {repo_key}@{branch}: {sha[:8] if sha else 'None'}")

    # Artifact Prefect (no local file)
    ts_short = datetime.now(timezone.utc).strftime("%Y%m%d")
    safe_branch = re.sub(r"[^a-z0-9-]", "-", branch.lower())
    artifact_key_raw = f"repo-{owner}-{repo_name}-{safe_branch}"
    artifact_key = re.sub(r"[^a-z0-9-]", "-", artifact_key_raw.lower())

    md_lines = [
        f"# Repo check — {repo_key}@{branch}",
        "",
        f"- sha: {sha}",
        f"- date: {date}",
        f"- previously recorded sha: {prev_sha}",
        "",
    ]
    if rec_lines:
        md_lines.append("### Recommendations")
        for rec in rec_lines:
            md_lines.append(f"- {rec}")
        md_lines.append("")
    if events:
        md_lines.append("### Events detected")
        for etype, items in events.items():
            if not items:
                continue
            md_lines.append(f"- {etype}:")
            for it in items:
                if isinstance(it, dict):
                    col = it.get("collection")
                    term = it.get("term")
                    if etype in ("collection_created", "collection_removed"):
                        md_lines.append(f"  - {col}")
                    else:
                        md_lines.append(f"  - {term}")
                else:
                    md_lines.append(f"  - {it}")
        md_lines.append("")

    md_content = "\n".join(md_lines)

    try:
        create_markdown_artifact(markdown=md_content, key=artifact_key, description=f"Repo report for {repo_key}@{branch}")
    except Exception as exc:
        logger.warning(f"Failed to create Prefect artifact for {repo_key}: {exc}")

    result = {
        "repo": repo_key,
        "branch": branch,
        "sha": sha,
        "date": date,
        "changed": True,
        "recorded": recorded,
        "recommendations": rec_lines,
        "events": events,
        "artifact_key": artifact_key,
        "files_index": new_index,
    }
    return result


# ---------------------------
# --- ADDED / UPDATED: compare reference task (creates artifact) ---
# ---------------------------
@task(retries=0)
def compare_reference_task(repo_spec: Union[str, Dict], check_result: Dict, state_path: str, reco_path: str) -> Dict:
    """
    Compare working_branch vs reference_branch for a repo and create a markdown artifact describing results.
    repo_spec should be a dict with keys 'owner', 'repo', 'working_branch', 'reference_branch', 'project' (optional).
    check_result is the result from check_single_repo_task (used for context).
    Returns dict with 'repo', 'reference_events', 'reference_recommendations', 'artifact_key'.
    """
    logger = get_run_logger()
    reco_conf = load_recommendations_conf(Path(reco_path))

    # parse repo_spec
    if isinstance(repo_spec, dict):
        owner = repo_spec.get("owner")
        repo_name = repo_spec.get("repo")
        working_branch = repo_spec.get("working_branch") or check_result.get("branch")
        reference_branch = repo_spec.get("reference_branch")
        project = repo_spec.get("project")
    else:
        # cannot do reference compare if repo_spec is a simple string (no reference info)
        return {"repo": str(repo_spec), "reference_events": {}, "reference_recommendations": [], "artifact_key": None}

    repo_key = f"{owner}/{repo_name}"
    if not owner or not repo_name or not reference_branch:
        logger.info(f"Skipping reference compare for {repo_key}: missing information")
        return {"repo": repo_key, "reference_events": {}, "reference_recommendations": [], "artifact_key": None}

    logger.info(f"Compare reference for {repo_key}: working={working_branch}, reference={reference_branch}")

    # Prefer using files_index produced by check_single_repo_task if present (minimize API calls)
    if check_result and isinstance(check_result, dict) and check_result.get("files_index"):
        new_index = check_result.get("files_index") or {}
    else:
        # fetch tree index for working branch (if fails, we continue but will log/record issues)
        try:
            latest = get_latest_commit_on_branch(owner, repo_name, working_branch)
            working_sha = latest.get("sha")
            new_index = get_tree_index(owner, repo_name, working_sha)
        except Exception as exc:
            logger.warning(f"Could not fetch working tree for {repo_key}@{working_branch}: {exc}")
            new_index = {}

    # run minimal consistency check; this will raise if reference tree fetch fails (fail-fast)
    ref_events = check_reference_consistency_minimal(owner, repo_name, working_branch, reference_branch, new_index, project=project)

    # build recommendations from ref_events using templates
    rec_lines: List[str] = []
    for ev_type, ev_list in ref_events.items():
        if not ev_list:
            continue
        templates = collect_templates_for_event(reco_conf.get("repos", {}), owner, repo_name, working_branch, ev_type)
        if not templates:
            templates = reco_conf.get("globals", [])
        for occ in ev_list:
            context_ev = {
                "repo": repo_key,
                "owner": owner,
                "repo_name": repo_name,
                "branch": working_branch,
                **(occ if isinstance(occ, dict) else {"item": occ}),
            }
            for t in templates:
                try:
                    rendered = t.format_map(DefaultDict(context_ev))
                except Exception:
                    rendered = t
                if rendered and rendered not in rec_lines:
                    rec_lines.append(rendered)

    # auto hint if issues found
    any_issues = any(len(v) > 0 for v in ref_events.values())
    if any_issues:
        rec_lines.append("Auto: Reference consistency issues detected between working and reference branches.")

    # build markdown artifact for this reference check (group by collection)
    ts_short = datetime.now(timezone.utc).strftime("%Y%m%d")
    safe_branch = re.sub(r"[^a-z0-9-]", "-", (working_branch or "").lower())
    artifact_key_raw = f"repo-{owner}-{repo_name}-refcheck-{safe_branch}"
    artifact_key = re.sub(r"[^a-z0-9-]", "-", artifact_key_raw.lower())

    md_lines = [
        f"# Reference coherence check — {owner}/{repo_name}",
        "",
        f"- working branch: {working_branch}",
        f"- reference branch: {reference_branch}",
        f"- checked at: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]

    issues = any_issues
    md_lines.append(f"- issues found: {bool(issues)}")
    md_lines.append("")

    # group and print per-collection for readability (collection heading uses the collection value from events)
    from collections import defaultdict
    grouped_missing = defaultdict(list)
    grouped_mismatch = defaultdict(list)
    grouped_parse = defaultdict(list)
    for it in ref_events.get("reference_term_missing", []):
        grouped_missing[it.get("collection") or "root"].append(it)
    for it in ref_events.get("reference_term_mismatch", []):
        grouped_mismatch[it.get("collection") or "root"].append(it)
    for it in ref_events.get("reference_file_parse_error", []):
        grouped_parse[it.get("collection") or "root"].append(it)

    if grouped_missing:
        md_lines.append("## Missing expected files in working branch")
        for coll, items in grouped_missing.items():
            md_lines.append(f"### {coll}")
            for it in items:
                md_lines.append(f"- expected {it.get('expected')} (from reference file {it.get('file')}, key={it.get('key')})")
        md_lines.append("")

    if grouped_mismatch:
        md_lines.append("## Mismatched or invalid files in working branch")
        for coll, items in grouped_mismatch.items():
            md_lines.append(f"### {coll}")
            for it in items:
                if it.get("reason"):
                    md_lines.append(f"- {it.get('file')}: {it.get('reason')} (key={it.get('key')})")
                else:
                    md_lines.append(f"- {it.get('file')}: expected id {it.get('expected_id')} got {it.get('actual_id')} (key={it.get('key')})")
        md_lines.append("")

    if grouped_parse:
        md_lines.append("## Reference file parse errors")
        for coll, items in grouped_parse.items():
            md_lines.append(f"### {coll}")
            for it in items:
                md_lines.append(f"- {it.get('file')}: {it.get('reason')}")
        md_lines.append("")

    md_content = "\n".join(md_lines)

    try:
        create_markdown_artifact(markdown=md_content, key=artifact_key, description=f"Reference check for {owner}/{repo_name}")
        # also write a local copy for convenience
        ref_report_path = Path("artifacts") / f"refcheck_{owner}_{repo_name}_{ts_short}.md"
        ref_report_path.parent.mkdir(parents=True, exist_ok=True)
        ref_report_path.write_text(md_content, encoding="utf-8")
    except Exception as exc:
        logger.warning(f"Could not create reference check artifact: {exc}")
        artifact_key = None

    return {"repo": repo_key, "reference_events": ref_events, "reference_recommendations": rec_lines, "artifact_key": artifact_key}


@task
def build_and_save_report(results: List[Dict], artifacts_dir: str = "artifacts") -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    Path(artifacts_dir).mkdir(parents=True, exist_ok=True)
    md_path = Path(artifacts_dir) / f"repo_check_{ts}.md"

    lines = [f"# Repo check report — {ts}", ""]
    for r in results:
        if r.get("error"):
            lines.append(f"## {r['repo']} ({r.get('branch','')}) — ERROR")
            lines.append(f"- {r['error']}")
            lines.append("")
            continue

        status = "CHANGED" if r.get("changed") else "no change"
        lines.append(f"## {r['repo']}@{r.get('branch','')} — {status}")
        lines.append(f"- sha: {r.get('sha')}")
        lines.append(f"- date: {r.get('date')}")
        recorded = r.get("recorded")
        if recorded:
            lines.append(f"- previously recorded sha: {recorded.get('sha')}")
        if r.get("changed") and r.get("recommendations"):
            lines.append("")
            lines.append("### Recommendations")
            for rec in r["recommendations"]:
                lines.append(f"- {rec}")
        if r.get("changed") and r.get("events"):
            lines.append("")
            lines.append("### Events detected")
            for etype, items in r["events"].items():
                if not items:
                    continue
                lines.append(f"- {etype}:")
                for it in items:
                    if isinstance(it, dict):
                        if "collection" in it and not it.get("term"):
                            lines.append(f"  - {it.get('collection')}")
                        elif "expected" in it:
                            lines.append(f"  - expected file missing: {it.get('expected')} (from {it.get('file')})")
                        elif "file" in it and "reason" in it:
                            lines.append(f"  - {it.get('file')}: {it.get('reason')}")
                        elif "path" in it:
                            if it.get("term"):
                                lines.append(f"  - {it.get('path')}")
                            else:
                                lines.append(f"  - {it}")
                        else:
                            lines.append(f"  - {it}")
                    else:
                        lines.append(f"  - {it}")
        # if compare_reference_task added extra reference_recommendations, include them
        if r.get("reference_recommendations"):
            lines.append("")
            lines.append("### Reference recommendations")
            for rec in r["reference_recommendations"]:
                lines.append(f"- {rec}")
        lines.append("")

    lines.append("---")
    lines.append(f"Generated at {ts} UTC")

    md_content = "\n".join(lines)
    md_path.write_text(md_content, encoding="utf-8")

    create_markdown_artifact(markdown=md_content, key="repo-check-recap", description="Repo recap")

    return str(md_path)


@task(retries=0)
def notify_on_changes_task(results: List[Dict], slack_block_name: str = "slack-webhook-for-repo-checker") -> Dict:
    logger = get_run_logger()
    changed_items = [r for r in results if r.get("changed")]
    summary = {"changed_count": len(changed_items), "sent": False}

    if not changed_items:
        logger.info("No changes detected — no notification sent.")
        return summary

    text_lines = ["GitHub Watcher — changes detected:"]
    for r in changed_items:
        repo = r["repo"]
        branch = r["branch"]
        sha = (r.get("sha") or "")[:8]
        text_lines.append(f"- {repo}@{branch} — {sha}")
    text_lines.append("")
    text_lines.append("See the report generated in the run artifacts.")
    text = "\n".join(text_lines)

    try:
        slack_block_coro = SlackWebhook.load(slack_block_name)
        slack_block = run_coro_sync(slack_block_coro)

        notify_coro = slack_block.notify(text)
        run_coro_sync(notify_coro)

        summary["sent"] = True
        logger.info("Slack notification sent via Prefect Block.")
    except Exception as exc:
        logger.warning(f"Slack notification failed: {exc}")

    return summary


@flow
def github_repos_check_flow(
    repos: List[Dict[str, Any]] = None,
    state_path: str = "repo_watcher_state.yml",
    reco_path: str = "repo_recommendations.yml",
    artifacts_dir: str = "artifacts",
    notify_slack: bool = False,
):
    """
    Main flow: run per-repo checks (step 1) then reference comparisons (step 2 for repos that declare a reference_branch).
    repos must be a list of dicts (owner, repo, working_branch, reference_branch, project optional).
    """
    logger = get_run_logger()

    repos_list = repos or []
    if not isinstance(repos_list, list):
        raise ValueError("repos parameter must be a list of dicts")

    logger.info("Starting GitHub repos checker flow (full)")

    results = []
    # We'll keep mapping of check futures to their repo_spec so we can submit compare task when needed
    futures = []  # list of tuples (check_future, repo_spec)
    for repo_spec in repos_list:
        # determine a sensible task name
        if isinstance(repo_spec, dict):
            owner = repo_spec.get("owner")
            repo_name = repo_spec.get("repo")
            branch = repo_spec.get("working_branch") or ""
            task_name = f"{owner}/{repo_name}" + (f"@{branch}" if branch else "")
            check_future = check_single_repo_task.with_options(name=task_name).submit(f"{owner}/{repo_name}@{branch}" if branch else f"{owner}/{repo_name}", state_path, reco_path)
        else:
            # string: pass-through
            repo_spec_str = str(repo_spec)
            if "@" in repo_spec_str:
                task_name = repo_spec_str
            else:
                task_name = repo_spec_str
            check_future = check_single_repo_task.with_options(name=task_name).submit(repo_spec_str, state_path, reco_path)
        futures.append((check_future, repo_spec))
    # now optionally submit compare_reference_task for those repo_specs that include a reference_branch (keep dependency)
    compare_futures = []
    for check_fut, repo_spec in futures:
        # determine if repo_spec has a reference_branch
        has_ref = False
        if isinstance(repo_spec, dict) and repo_spec.get("reference_branch"):
            has_ref = True
        # also allow string format "owner/repo@working" with no reference -> skip
        if has_ref:
            comp_fut = compare_reference_task.submit(repo_spec, check_fut, state_path, reco_path)
        else:
            comp_fut = None
        compare_futures.append((check_fut, comp_fut, repo_spec))
    # collect results and merge compare results where present
    for check_fut, comp_fut, repo_spec in compare_futures:
        check_res = check_fut.result()
        # if compare future exists, merge its results
        if comp_fut is not None:
            comp_res = comp_fut.result()
            # merge events
            if comp_res.get("reference_events"):
                check_res.setdefault("events", {})
                for k, v in comp_res.get("reference_events").items():
                    if v:
                        existing = check_res["events"].get(k) or []
                        existing_keys = {json.dumps(e, sort_keys=True) for e in existing if isinstance(e, dict)}
                        for item in v:
                            j = json.dumps(item, sort_keys=True) if isinstance(item, dict) else str(item)
                            if j not in existing_keys:
                                existing.append(item)
                                existing_keys.add(j)
                        check_res["events"][k] = existing
            # merge recommendations
            if comp_res.get("reference_recommendations"):
                check_res.setdefault("recommendations", [])
                for r in comp_res.get("reference_recommendations"):
                    if r not in check_res["recommendations"]:
                        check_res["recommendations"].append(r)
                # if reference produced items but original check had no changes, mark changed True
                if comp_res.get("reference_recommendations") and not check_res.get("changed"):
                    check_res["changed"] = True
            # attach reference_recommendations and artifact_key for reporting
            if comp_res.get("reference_recommendations"):
                check_res["reference_recommendations"] = comp_res.get("reference_recommendations")
            if comp_res.get("artifact_key"):
                check_res["reference_artifact_key"] = comp_res.get("artifact_key")
        results.append(check_res)

    report_path = build_and_save_report(results, artifacts_dir=artifacts_dir)

    notify_summary = {}
    if notify_slack:
        notify_summary = notify_on_changes_task(results)

    changed = [r for r in results if r.get("changed")]
    changed_count = len(changed)

    logger.info(f"Checked {len(results)} repos — changes: {changed_count}")
    logger.info(f"Report written to: {report_path}")

    return {
        "results": results,
        "report_path": report_path,
        "notify": notify_summary,
        "changed_count": changed_count,          # <- useful for Automations
        "changed_repos": [f"{r['repo']}@{r.get('branch')}" for r in changed],
    }


# ---------------------------
# Example minimal recommendations YAML (for reference)
# ---------------------------
EXAMPLE_REC_YAML = """
globals:
  - "Global note: check downstream modules if interface changed."
repos:
  alaupin-ipsl/WCRP-universe:
    branches:
      main:
        events:
          collection_created:
            - "New DataDescriptor (collection) detected: {collection} in {repo}@{branch}."
            - "Action: check metadata and trigger ingestion if needed."
          term_created:
            - "New term '{term}' added in {collection} — verify JSON schema and mapping."
          term_modified:
            - "Term '{term}' modified in {collection} — check breaking changes & changelog."
  alaupin-ipsl/CORDEX-CMIP6_CVs:
    branches:
      main:
        events:
          term_created:
            - "New CV term: {term} → update index & catalogue."
          term_modified:
            - "CV term modified: {term} → check query impact."
"""


if __name__ == "__main__":
    # Main entry: read repos_config.yml (mandatory) and run the flow with the list of dicts.
    repo_config_path = Path("repos_config.yml")
    if not repo_config_path.exists():
        # Write an example config and exit so the user edits it.
        example = {
            "repos": [
                {"owner": "alaupin-ipsl", "repo": "CORDEX-CMIP6_CVs", "working_branch": "main", "reference_branch": None, "project": "CORDEX-CMIP6"},
                {"owner": "WCRP-CMIP", "repo": "WCRP-universe", "working_branch": "esgvoc_dev", "reference_branch": None, "project": None},
                {"owner": "ltroussellier", "repo": "test_new_CV", "working_branch": "esgvoc", "reference_branch": "master", "project": "input4MIPs"},
                {"owner": "ltroussellier", "repo": "obs4MIPs_CVs", "working_branch": "REF", "reference_branch": "main", "project": "obs4MIPs"},
                {"owner": "WCRP-CMIP", "repo": "CMIP6Plus_CVs", "working_branch": "esgvoc", "reference_branch": "main", "project": "CMIP6Plus"},
                {"owner": "WCRP-CMIP", "repo": "CMIP6_CVs", "working_branch": "esgvoc", "reference_branch": "main", "project": "CMIP6"},
                # {"owner": "WCRP-CMIP", "repo": "CMIP7_CVs", "working_branch": "esgvoc", "reference_branch": "main", "project": "CMIP7"},
            ]
        }
        write_yaml(repo_config_path, example)
        print(f"Wrote example repo config to {repo_config_path}. Please edit it and re-run.")
        raise SystemExit(0)

    cfg = read_yaml(repo_config_path) or {}
    repos_cfg = cfg.get("repos")
    if not isinstance(repos_cfg, list) or len(repos_cfg) == 0:
        raise SystemExit(f"{repo_config_path} must contain a top-level 'repos' list of dicts")

    # load recommendations example if missing
    rec_path = Path("repo_recommendations.yml")
    if not rec_path.exists():
        write_yaml(rec_path, yaml.safe_load(EXAMPLE_REC_YAML))
        print(f"Wrote example recommendations to {rec_path}")

    state_path = "repo_watcher_state.yml"

    # run flow with the list of dicts read from repos_config.yml
    out = github_repos_check_flow(repos_cfg, state_path=state_path, reco_path=str(rec_path), notify_slack=True)
    # print(json.dumps(out, indent=2, ensure_ascii=False))
