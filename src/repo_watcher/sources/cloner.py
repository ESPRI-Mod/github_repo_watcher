"""Git repo cloning with per-session cache."""

from __future__ import annotations

import shutil
from pathlib import Path

from git import Repo

# Session-level cache: avoids re-cloning the same repo+branch within one run.
_cache: dict[str, Path] = {}


def clone_repo(owner: str, repo: str, branch: str) -> Path:
    """Clone a GitHub repo to /tmp and return the local path.

    If the same (owner, repo, branch) was already cloned this session,
    returns the cached path instead of re-cloning.
    """
    cache_key = f"{owner}/{repo}@{branch}"
    if cache_key in _cache:
        return _cache[cache_key]

    url = f"https://github.com/{owner}/{repo}.git"
    dest = Path("/tmp") / f"{repo}-{branch}"
    if dest.is_dir():
        shutil.rmtree(dest)
    Repo.clone_from(url, dest, branch=branch, depth=1)

    _cache[cache_key] = dest
    return dest


def clear_cache() -> None:
    """Clear the clone cache (useful for testing)."""
    _cache.clear()
