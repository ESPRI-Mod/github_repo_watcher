"""
prefect_github_repo_watcher_yaml.py

But :
 - Vérifier des dépôts GitHub sur une branche donnée (owner/repo@branch).
 - Conserver l'état observé (sha/date) dans un fichier YAML (pas de DB).
 - Charger des recommandations depuis un fichier YAML (templating simple).
 - Fournir un flow Prefect 3 prêt à être déployé/exécuté via Automation.

Usage :
 - Définir GITHUB_TOKEN dans l'environnement si tu veux accéder aux dépôts privés ou éviter les limites.
 - Préparer un fichier de recommandations (voir example ci-dessous).
 - Appeler `github_repos_check_flow(repos=[...], ...)` depuis Prefect.
"""

import os
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
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
# Helpers : YAML state + config
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
# Config format (recommendations)
# ---------------------------
# Exemple minimal de recommendations.yml :
# globals:
#   - "A vérifier : nouvelle commit sur {repo}@{branch} ({sha[:8]}) à {date}."
# repos:
#   "ipsl/climatsuds":
#     default:
#       - "Run deploy script: ./deploy.sh {repo} {branch}"
#     branches:
#       "main":
#         - "Attention: main déclenche déploiement. Vérifier staging d'abord."
#   ".*":   # pattern regex match (fallback)
#     default:
#       - "Run tests, rebuild image, redeploy if needed."
#
# Les chaînes supportent les placeholders : {repo}, {owner}, {repo_name}, {branch}, {sha}, {date}

def load_recommendations_conf(path: Path) -> Dict:
    conf = read_yaml(path) or {}
    # normalize structure
    return {
        "globals": conf.get("globals", []),
        "repos": conf.get("repos", {}),
    }


def match_repo_rules(conf_repos: Dict, owner: str, repo_name: str, branch: str) -> List[str]:
    """
    Récupère les règles applicables : priorité
    1) exact owner/repo + branch override
    2) exact owner/repo default
    3) regex patterns in keys (applied in insertion order)
    4) global default rules in conf_repos.get('*') or conf_repos.get('.*')
    """
    import re

    selected: List[str] = []

    full = f"{owner}/{repo_name}"

    # 1) exact match
    repo_conf = conf_repos.get(full)
    if repo_conf:
        # branch-specific
        branches = repo_conf.get("branches", {})
        if branch in branches:
            selected.extend(branches[branch])
        # default for that repo
        selected.extend(repo_conf.get("default", []))

    # 2) regex keys & wildcard keys
    for key, val in conf_repos.items():
        if key == full:
            continue
        try:
            # treat key as regex
            if re.fullmatch(key, full):
                selected.extend(val.get("default", []) if isinstance(val, dict) else ([] if val is None else val))
                # branch override
                if isinstance(val, dict):
                    branches = val.get("branches", {})
                    if branch in branches:
                        selected.extend(branches[branch])
        except re.error:
            # ignore invalid regex keys
            continue

    # 3) fallback star keys
    for star in ("*", ".*"):
        if star in conf_repos:
            v = conf_repos[star]
            if isinstance(v, dict):
                selected.extend(v.get("default", []))
                if branch in v.get("branches", {}):
                    selected.extend(v["branches"][branch])
            else:
                selected.extend(v if isinstance(v, list) else [v])

    return selected


def render_templates(lines: List[str], context: Dict) -> List[str]:
    out = []
    for l in lines:
        try:
            out.append(l.format_map(DefaultDict(context)))
        except Exception:
            # en cas d'erreur de templating, renvoyer la ligne brute
            out.append(l)
    return out


class DefaultDict(dict):
    """dict qui retourne une chaîne vide pour les clés manquantes (utilisé pour format_map)."""

    def __missing__(self, key):
        return ""


# ---------------------------
# GitHub minimal helpers
# ---------------------------
def gh_get(url: str, params: Dict = None) -> requests.Response:
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r


def get_default_branch(owner: str, repo: str) -> str:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}"
    j = gh_get(url).json()
    return j.get("default_branch", "main")


def get_latest_commit_on_branch(owner: str, repo: str, branch: str) -> Dict:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/commits/{branch}"
    j = gh_get(url).json()
    sha = j.get("sha")
    date = j.get("commit", {}).get("committer", {}).get("date") or j.get("commit", {}).get("author", {}).get("date")
    return {"sha": sha, "date": date, "raw": j}


# ---------------------------
# Prefect tasks & flow
# ---------------------------
@task(retries=1, retry_delay_seconds=10)
def check_single_repo_task(repo_with_branch: str, state_path: str, reco_path: str) -> Dict:
    """
    repo_with_branch: "owner/repo@branch" (branch optionnelle; si absente on récupère default branch)
    state_path: chemin vers YAML d'état
    reco_path: chemin vers YAML des recommandations
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

    # load state and config
    state_file = Path(state_path)
    reco_file = Path(reco_path)
    state = read_yaml(state_file) or {}
    reco_conf = load_recommendations_conf(reco_file)

    # determine branch if not provided
    if not branch:
        try:
            branch = get_default_branch(owner, repo_name)
            logger.info(f"No branch provided — using default branch '{branch}'")
        except Exception as exc:
            return {"repo": repo_with_branch, "error": f"Failed to get default branch: {exc}"}

    # fetch latest commit on that branch
    try:
        latest = get_latest_commit_on_branch(owner, repo_name, branch)
    except Exception as exc:
        return {"repo": repo_with_branch, "error": f"Failed to fetch commits: {exc}"}

    sha = latest.get("sha")
    date = latest.get("date")

    # read recorded state for this repo+branch
    # structure stored: state[repo_key][branch] = {"sha":..., "date":...}
    recorded = state.get(repo_key, {}).get(branch)

    changed = (not recorded) or (sha and recorded.get("sha") != sha)

    # Build recommendations
    rec_lines = []

    # global recommendations first
    rec_lines.extend(reco_conf.get("globals", []))

    # repo-specific rules
    repo_rules = match_repo_rules(reco_conf.get("repos", {}), owner, repo_name, branch)
    if repo_rules:
        rec_lines.extend(repo_rules)

    # always add a few auto-generated hints
    if changed:
        rec_lines.append("Auto: Detected change on {repo}@{branch}: {sha[:8]} at {date}.")
        rec_lines.append("Auto: Steps recommandés: pull, rebuild, run tests, check CI.")
    else:
        rec_lines.append("Auto: Pas de nouvelle commit depuis la dernière vérification enregistrée.")

    # context for templating
    context = {
        "repo": repo_key,
        "owner": owner,
        "repo_name": repo_name,
        "branch": branch,
        "sha": sha or "",
        "date": date or "",
    }

    recommendations = render_templates(rec_lines, context)

    # update state if changed (or if not recorded)
    if changed:
        state.setdefault(repo_key, {})[branch] = {"sha": sha, "date": date, "checked_at": datetime.now(timezone.utc).isoformat()}
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


@flow
def github_repos_check_flow(
    repos: List[str],
    state_path: str = "repo_watcher_state.yml",
    reco_path: str = "repo_recommendations.yml",
    parallel: bool = True,
) -> List[Dict]:
    """
    Flow principal Prefect 3.
    - repos: liste de "owner/repo@branch" (branch optionnelle -> utilisera default)
    - state_path: fichier YAML qui enregistre les sha/dates observés
    - reco_path: fichier YAML de recommandations
    - parallel: si True, on lance les tâches en parallèle via task.submit()
    """
    logger = get_run_logger()
    logger.info("Starting GitHub repos checker flow (YAML state + conf)")
    results = []

    if parallel:
        # submit tasks concurrently
        futures = [check_single_repo_task.submit(r, state_path, reco_path) for r in repos]
        for f in futures:
            results.append(f.result())
    else:
        for r in repos:
            results.append(check_single_repo_task(r, state_path, reco_path))

    logger.info("Flow terminé")
    return results


# ---------------------------
# Example: fichier recommendations.yml exemple et usage
# ---------------------------
EXAMPLE_REC_YAML = """
globals:
  - "Note globale: vérifier les modules dépendants si interface modifiée."
repos:
  "ipsl/climatsuds":
    default:
      - "Si commit sur {repo}@{branch}, lancer: ./deploy_climatsuds.sh {branch}"
    branches:
      "main":
        - "Main branch: vérifier les tests de non-régression avant déploiement."
  ".*":
    default:
      - "Standard: pull -> build -> smoke-tests -> redeploy"
"""

EXAMPLE_STATE_YAML = """
# Exemple de structure d'état :
# ipsl/climatsuds:
#   main:
#     sha: "abcd1234..."
#     date: "2025-08-19T12:34:56Z"
#     checked_at: "2025-08-19T12:35:00+00:00"
"""

if __name__ == "__main__":
    # Démo locale (ne pas oublier GITHUB_TOKEN si privé)
    demo_repos = [
        "alaupin-ipsl/CORDEX-CMIP6_CVs",
        "alaupin-ipsl/WCRP-universe@esgvoc_dev"
    ]
    # write example config if not exists
    rec_path = Path("repo_recommendations.yml")
    if not rec_path.exists():
        write_yaml(rec_path, yaml.safe_load(EXAMPLE_REC_YAML))
        print(f"Wrote example recommendations to {rec_path}")

    state_path = "repo_watcher_state.yml"
    out = github_repos_check_flow(demo_repos, state_path=state_path, reco_path=str(rec_path), parallel=True)
    print(json.dumps(out, indent=2, ensure_ascii=False))
