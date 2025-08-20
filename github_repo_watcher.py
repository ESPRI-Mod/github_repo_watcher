"""
prefect_github_repo_watcher_yaml_v2.py

- Reco only when change detected
- Repo-level recommendations first, then branch-level
- No noisy 'default' fallbacks when no change
- Write a Markdown artifact for Prefect and optionally send Slack notifications

Dépendances:
  pip install prefect requests pyyaml
"""

import os
import json
from datetime import datetime, timezone
from typing import List, Dict, Any
from pathlib import Path

import requests
import yaml
from prefect import flow, task, get_run_logger

GITHUB_API_BASE = "https://api.github.com"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

HTTP_TIMEOUT = 30


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
# Recommendation config helpers
# ---------------------------
def load_recommendations_conf(path: Path) -> Dict:
    conf = read_yaml(path) or {}
    return {
        "globals": conf.get("globals", []),
        "repos": conf.get("repos", {}),
    }


def branch_matches(candidate_key: str, branch: str) -> bool:
    """
    branch matching tolerant:
      - exact match
      - lowercase match
      - '-' <-> '_' substitution
    """
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
    Return ordered list: first repo default rules (if any), then branch-specific rules (if any).
    Do NOT include regex/global fallbacks automatically to avoid noisy defaults.
    If no repo-specific rules are present, we return [] (caller may use globals as fallback).
    """
    full = f"{owner}/{repo_name}"
    selected: List[str] = []

    repo_conf = conf_repos.get(full)
    if not repo_conf:
        # try regex keys that exactly match the repo (rare) but avoid '*' '.*' here
        import re

        for key, val in conf_repos.items():
            if key in ("*", ".*"):
                continue
            if key == full:
                repo_conf = val
                break
            try:
                if re.fullmatch(key, full):
                    repo_conf = val
                    break
            except re.error:
                continue

    if not repo_conf:
        return []

    # repo default (if present)
    if isinstance(repo_conf, dict):
        repo_default = repo_conf.get("default", [])
        if repo_default:
            selected.extend(repo_default)

        # branch-specific, but appended AFTER repo default (per request: repo > branche)
        branches = repo_conf.get("branches", {}) or {}
        # find matching branch keys with tolerant comparator
        for candidate_key, candidate_val in branches.items():
            if branch_matches(candidate_key, branch):
                if isinstance(candidate_val, list):
                    selected.extend(candidate_val)
                elif candidate_val is None:
                    continue
                else:
                    selected.append(candidate_val)
                break  # stop on first matching branch key
    else:
        # repo_conf might be a list directly
        if isinstance(repo_conf, list):
            selected.extend(repo_conf)
        elif repo_conf is None:
            pass
        else:
            selected.append(str(repo_conf))

    return selected


def render_templates(lines: List[str], context: Dict) -> List[str]:
    out = []
    seen = set()
    for l in lines:
        if not l:
            continue  # skip empty lines from YAML
        try:
            rendered = l.format_map(DefaultDict(context))
        except Exception:
            rendered = l
        if rendered not in seen:
            out.append(rendered)
            seen.add(rendered)
    return out


class DefaultDict(dict):
    def __missing__(self, key):
        return ""


# ---------------------------
# GitHub helpers
# ---------------------------
def gh_get(url: str, params: Dict = None) -> requests.Response:
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r


def get_default_branch(owner: str, repo: str) -> str:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}"
    j = gh_get(url).json()
    # use 'or "main"' for robustness if key exists but is falsy
    return j.get("default_branch") or "main"


def get_latest_commit_on_branch(owner: str, repo: str, branch: str) -> Dict:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/commits/{branch}"
    j = gh_get(url).json()
    sha = j.get("sha")
    date = j.get("commit", {}).get("committer", {}).get("date") or j.get("commit", {}).get("author", {}).get("date")
    return {"sha": sha, "date": date, "raw": j}


# ---------------------------
# Notifications (Slack example)
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
    logger = get_run_logger()

    # parse
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

    # branch fallback to default branch if not provided
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

    # determine change
    recorded_sha = (recorded or {}).get("sha")
    changed = (recorded is None) or (sha and recorded_sha != sha)

    # if changed: collect repo-level and branch-level recos only for this repo
    rec_lines: List[str] = []
    if changed:
        # collect repo+branch rules (repo default then branch)
        repo_rules = collect_repo_and_branch_rules(reco_conf.get("repos", {}), owner, repo_name, branch)

        if repo_rules:
            rec_lines.extend(repo_rules)
        else:
            # if no repo-specific rules, use globals as fallback (optional)
            rec_lines.extend(reco_conf.get("globals", []))

        # auto hints appended last
        rec_lines.append("Auto: Detected change on {repo}@{branch}: {sha[:8]} at {date}.")
        rec_lines.append("Auto: Steps recommandés: pull, rebuild, run tests, check CI.")
    else:
        # no change -> do not include recommendations
        logger.info(f"No change for {repo_key}@{branch}; skipping recommendations.")

    # render templates (remove duplicates & empty)
    context = {
        "repo": repo_key,
        "owner": owner,
        "repo_name": repo_name,
        "branch": branch,
        "sha": sha or "",
        "date": date or "",
    }
    recommendations = render_templates(rec_lines, context)

    # update state only if changed (or if first time)
    if changed:
        state.setdefault(repo_key, {})[branch] = {
            "sha": sha,
            "date": date,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        write_yaml(state_file, state)
        logger.info(f"State updated for {repo_key}@{branch}: {sha[:8] if sha else 'None'}")

    result = {
        "repo": repo_key,
        "branch": branch,
        "sha": sha,
        "date": date,
        "changed": bool(changed),
        "recorded": recorded,
        "recommendations": recommendations,
    }
    return result


@task
def build_and_save_report(results: List[Dict], artifacts_dir: str = "artifacts") -> str:
    """Build a Markdown report and save it to artifacts/ with timestamp. Returns path."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    Path(artifacts_dir).mkdir(parents=True, exist_ok=True)
    md_path = Path(artifacts_dir) / f"repo_check_{ts}.md"

    lines = [f"# Repo check report — {ts}", ""]
    changed_any = False
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
            changed_any = True
            lines.append("")
            lines.append("### Recommendations")
            for rec in r["recommendations"]:
                lines.append(f"- {rec}")
        lines.append("")

    lines.append("---")
    lines.append(f"Generated at {ts} UTC")

    md_content = "\n".join(lines)
    md_path.write_text(md_content, encoding="utf-8")
    return str(md_path)


@task(retries=0)
def notify_on_changes_task(results: List[Dict], slack_webhook_env: str = "SLACK_WEBHOOK_URL") -> Dict:
    """
    If any result has changed==True, send a Slack notification (simple text).
    Returns a dict summarizing notifications sent.
    """
    logger = get_run_logger()
    webhook = os.environ.get(slack_webhook_env)
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

    text_lines.append("")  # newline
    text_lines.append("Voir le rapport généré dans les artifacts du run.")

    text = "\n".join(text_lines)
    if webhook:
        ok = send_slack_message(webhook, text)
        summary["sent"] = ok
        logger.info(f"Slack notification sent: {ok}")
    else:
        logger.info("No SLACK_WEBHOOK_URL defined — skipping Slack notify. Summary:")
        logger.info(text)

    return summary


@flow
def github_repos_check_flow(
    repos: List[str],
    state_path: str = "repo_watcher_state.yml",
    reco_path: str = "repo_recommendations.yml",
    parallel: bool = True,
    artifacts_dir: str = "artifacts",
    notify_slack: bool = True,
) -> Dict:
    """
    Main Prefect 3 flow.
    - repos: list of "owner/repo@branch"
    - returns a dict with keys: results (list), report_path, notify_summary
    """
    logger = get_run_logger()
    logger.info("Starting GitHub repos checker flow (v2)")

    # run checks
    results = []
    if parallel:
        futures = [check_single_repo_task.submit(r, state_path, reco_path) for r in repos]
        for f in futures:
            results.append(f.result())
    else:
        for r in repos:
            results.append(check_single_repo_task(r, state_path, reco_path))

    # build + save report (artifact)
    report_path = build_and_save_report(results, artifacts_dir=artifacts_dir)

    notify_summary = {}
    if notify_slack:
        notify_summary = notify_on_changes_task(results)

    # print/log summary for Prefect UI
    changed = [r for r in results if r.get("changed")]
    logger.info(f"Checked {len(results)} repos — changes: {len(changed)}")
    logger.info(f"Report written to: {report_path}")

    return {"results": results, "report_path": report_path, "notify": notify_summary}


# ---------------------------
# Example YAML & demo
# ---------------------------
EXAMPLE_REC_YAML = """
globals:
  - "Note globale: vérifier les modules dépendants si interface modifiée."
repos:
  alaupin-ipsl/WCRP-universe:
    default:
      - "Le dépot Universe a été mis à jour."
    branches:
      main:
        - "Contactez Dan pour plus d'informations"
      esgvoc-dev:
        - "Contactez Laurent pour plus d'informations"
  alaupin-ipsl/CORDEX-CMIP6_CVs:
    default:
      - "Le dépôt des CVs de CORDEX-CMIP6 a été mis à jour"
    branches:
      main:
        - "Vérifier integrity of CVs"
"""

if __name__ == "__main__":
    demo_repos = [
        "alaupin-ipsl/CORDEX-CMIP6_CVs@main",
        "alaupin-ipsl/WCRP-universe@esgvoc_dev",
    ]

    rec_path = Path("repo_recommendations.yml")
    if not rec_path.exists():
        write_yaml(rec_path, yaml.safe_load(EXAMPLE_REC_YAML))
        print(f"Wrote example recommendations to {rec_path}")

    state_path = "repo_watcher_state.yml"
    out = github_repos_check_flow(demo_repos, state_path=state_path, reco_path=str(rec_path), parallel=True, notify_slack=False)
    print(json.dumps(out, indent=2, ensure_ascii=False))
