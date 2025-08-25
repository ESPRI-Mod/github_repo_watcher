"""
prefect_github_repo_watcher_full.py

Fonctions:
 - Vérifie une liste de repos GitHub (owner/repo[@branch])
 - Conserve l'état dans un fichier YAML (sha, date, files_index)
 - Utilise l'API compare pour détecter added/modified/removed/renamed files
 - Classifie les changements en events: collection_created, term_created, term_modified, term_removed, collection_removed
 - Applique des templates de recommandations définis dans repo_recommendations.yml
 - Écrit un rapport Markdown dans artifacts/, renvoie le path dans le flow
 - Optionnel: envoie une notification Slack si des changements ont été détectés (désactivé par défaut)

Dépendances:
 pip install prefect requests pyyaml
"""

import os
import re
import json
from datetime import datetime, timezone
from typing import List, Dict, Any
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
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r


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
    """
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/trees/{sha_or_ref}"
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
    Mise à jour : ajoute la détection 'collection_removed'.
    - files_changed : list de dicts venant de l'API compare
    - prev_index / new_index : map dir -> [filenames] (tree index)
    Retourne dict:
      {
        "collection_created": [{"collection": ...}, ...],
        "collection_removed": [{"collection": ...}, ...],
        "term_created": [{"collection":..., "term":..., "path":...}, ...],
        "term_modified": [...],
        "term_removed": [...]
      }
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
    removed_dirs = sorted(prev_dirs - new_dirs)
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


# ---------------------------
# Prefect tasks & flow
# ---------------------------
@task(retries=1, retry_delay_seconds=5)
def check_single_repo_task(repo_with_branch: str, state_path: str, reco_path: str) -> Dict:
    """
    Task : vérifie un repo@branch, construit recommandations/events,
    crée un artifact Markdown spécifique à ce repo, et retourne le résultat
    incluant 'artifact_key'.
    """
    logger = get_run_logger()

    # parse input
    if "@" in repo_with_branch:
        repo_key, branch = repo_with_branch.split("@", 1)
    else:
        repo_key, branch = repo_with_branch, None

    if "/" not in repo_key:
        return {"repo": repo_with_branch, "error": "Format attendu owner/repo[@branch]"}

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

        # Artifact Prefect (pas de fichier local)
        ts_short = datetime.now(timezone.utc).strftime("%Y%m%d")
        safe_branch = re.sub(r"[^a-z0-9-]", "-", branch.lower())
        artifact_key_raw = f"repo-{owner}-{repo_name}-{safe_branch}-{ts_short}"
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
            logger.warning(f"Impossible de créer l'artifact Prefect pour {repo_key}: {exc}")

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
        }

    # if sha unchanged -> nothing to do
    prev_sha = recorded.get("sha")
    if prev_sha == sha:
        logger.info(f"No new commits for {repo_key}@{branch}")

        # Artifact Prefect minimal (pas de fichier local)
        ts_short = datetime.now(timezone.utc).strftime("%Y%m%d")
        safe_branch = re.sub(r"[^a-z0-9-]", "-", branch.lower())
        artifact_key_raw = f"repo-{owner}-{repo_name}-{safe_branch}-{ts_short}"
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
            logger.warning(f"Impossible de créer l'artifact Prefect pour {repo_key}: {exc}")

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
    rec_lines.append("Auto: Steps recommandés: pull, rebuild, run tests, check CI.")

    # update state
    state.setdefault(repo_key, {})[branch] = {
        "sha": sha,
        "date": date,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "files_index": new_index,
    }
    write_yaml(state_file, state)
    logger.info(f"State updated for {repo_key}@{branch}: {sha[:8] if sha else 'None'}")

    # Artifact Prefect (pas de fichier local)
    ts_short = datetime.now(timezone.utc).strftime("%Y%m%d")
    safe_branch = re.sub(r"[^a-z0-9-]", "-", branch.lower())
    artifact_key_raw = f"repo-{owner}-{repo_name}-{safe_branch}-{ts_short}"
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
        logger.warning(f"Impossible de créer l'artifact Prefect pour {repo_key}: {exc}")

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
    }
    return result


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
        lines.append(f"## {r['repo']}@{r['branch']} — {status}")
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
                        elif "term" in it:
                            lines.append(f"  - {it.get('term')}")
                        else:
                            lines.append(f"  - {it}")
                    else:
                        lines.append(f"  - {it}")
        lines.append("")

    lines.append("---")
    lines.append(f"Generated at {ts} UTC")

    md_content = "\n".join(lines)
    md_path.write_text(md_content, encoding="utf-8")

    return str(md_path)


@task(retries=0)
def notify_on_changes_task(results: List[Dict], slack_block_name: str = "slack-webhook-for-repo-checker") -> Dict:
    """
    Envoie une notification Slack via un Prefect Block si des changements sont détectés.
    """
    logger = get_run_logger()
    changed_items = [r for r in results if r.get("changed")]
    summary = {"changed_count": len(changed_items), "sent": False}

    if not changed_items:
        logger.info("No changes detected — no Slack notification sent.")
        return summary

    text_lines = ["🔔 GitHub Watcher — changements détectés:"]
    for r in changed_items:
        repo = r["repo"]
        branch = r["branch"]
        sha = (r.get("sha") or "")[:8]
        text_lines.append(f"- {repo}@{branch} — {sha}")
    text_lines.append("")
    text_lines.append("📄 Voir le rapport généré dans les artifacts du flow run.")
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
    repos: List[str],
    state_path: str = "github_repos_state.yml",
    reco_path: str = "repo_recommendations.yml",
    parallel: bool = True,
    artifacts_dir: str = "artifacts",
    notify_slack: bool = True,  # DEFAULT CHANGED: pas de Slack par défaut
) -> Dict:
    logger = get_run_logger()

    # NEW: renommer le flow-run dès le début
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_name = f"github-repos-check-{ts}"
    try:
        async def _rename_start():
            async with get_client() as client:
                await client.update_flow_run(_flow_run.id, name=base_name)
        anyio.run(_rename_start)
    except Exception as exc:
        logger.warning(f"Flow run rename (start) failed: {exc}")

    logger.info("Starting GitHub repos checker flow (full)")

    results = []
    if parallel:
        futures = []
        for repo_spec in repos:
            # NOM DE TASK = "owner/repo@branch"
            if "@" in repo_spec:
                repo_part = repo_spec
            else:
                # Résoudre la branche par défaut pour le nom de task
                try:
                    owner, repo_name = repo_spec.split("/", 1)
                    default_branch = get_default_branch(owner, repo_name)
                    repo_part = f"{owner}/{repo_name}@{default_branch}"
                except Exception:
                    repo_part = repo_spec
            task_runner = check_single_repo_task.with_options(name=repo_part)
            futures.append(task_runner.submit(repo_spec, state_path, reco_path))
        for f in futures:
            results.append(f.result())
    else:
        for repo_spec in repos:
            if "@" in repo_spec:
                repo_part = repo_spec
            else:
                try:
                    owner, repo_name = repo_spec.split("/", 1)
                    default_branch = get_default_branch(owner, repo_name)
                    repo_part = f"{owner}/{repo_name}@{default_branch}"
                except Exception:
                    repo_part = repo_spec
            task_runner = check_single_repo_task.with_options(name=repo_part)
            r = task_runner.submit(repo_spec, state_path, reco_path)
            results.append(r.result())

    report_path = build_and_save_report(results, artifacts_dir=artifacts_dir)

    # OPTIONNEL (désactivé par défaut)
    notify_summary = {}
    if notify_slack:
        notify_summary = notify_on_changes_task(results)

    changed = [r for r in results if r.get("changed")]
    changed_count = len(changed)

    logger.info(f"Checked {len(results)} repos — changes: {changed_count}")
    logger.info(f"Report written to: {report_path}")

    # NEW: renommer le flow-run en fin d’exécution pour faciliter une Automation
    try:
        suffix = f"changes-{changed_count}" if changed_count > 0 else "nochange"
        final_name = f"{base_name}-{suffix}"
        async def _rename_end():
            async with get_client() as client:
                await client.update_flow_run(_flow_run.id, name=final_name)
        anyio.run(_rename_end)
    except Exception as exc:
        logger.warning(f"Flow run rename (end) failed: {exc}")

    return {
        "results": results,
        "report_path": report_path,
        "notify": notify_summary,
        "changed_count": changed_count,          # <- utile pour Automation basée sur le résultat
        "changed_repos": [f"{r['repo']}@{r['branch']}" for r in changed],
    }


# ---------------------------
# Example minimal recommendations YAML (for reference)
# ---------------------------
EXAMPLE_REC_YAML = """
globals:
  - "Note globale: vérifier les modules dépendants si l'interface a changé."
repos:
  alaupin-ipsl/WCRP-universe:
    branches:
      main:
        events:
          collection_created:
            - "Nouvelle DataDescriptor (collection) détectée : {collection} dans {repo}@{branch}."
            - "Action recommandée : vérifier métadonnées et lancer ingestion si nécessaire."
          term_created:
            - "Nouveau terme '{term}' ajouté dans {collection} — vérifier schéma JSON et mapping."
          term_modified:
            - "Terme '{term}' modifié dans {collection} — vérifier breaking changes et changelog."
  alaupin-ipsl/CORDEX-CMIP6_CVs:
    branches:
      main:
        events:
          term_created:
            - "Nouveau terme CV : {term} → mettre à jour index & catalogue."
          term_modified:
            - "Modification de terme CV : {term} → vérifier impact sur les requêtes."
"""


if __name__ == "__main__":
    # Demo (modifie la liste ci-dessous pour tester localement)
    demo_repos = [
        # format: owner/repo[@branch]
        "alaupin-ipsl/CORDEX-CMIP6_CVs@main",
        "WCRP-CMIP/WCRP-universe@esgvoc_dev",
        "ltroussellier/test_new_CV@esgvoc",
        "ltroussellier/obs4MIPs_CVs@REF",
        "WCRP-CMIP/CMIP6Plus_CVs@esgvoc",
        "WCRP-CMIP/CMIP6_CVs@esgvoc",
        "WCRP-CMIP/CMIP7_CVs@esgvoc"
    ]

    rec_path = Path("repo_recommendations.yml")
    if not rec_path.exists():
        write_yaml(rec_path, yaml.safe_load(EXAMPLE_REC_YAML))
        print(f"Wrote example recommendations to {rec_path}")

    state_path = "repo_watcher_state.yml"
    out = github_repos_check_flow(demo_repos, state_path=state_path, reco_path=str(rec_path), parallel=True, notify_slack=True)
    print(json.dumps(out, indent=2, ensure_ascii=False))
