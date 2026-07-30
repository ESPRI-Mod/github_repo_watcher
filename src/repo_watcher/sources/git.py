"""Git-based term sources (all variants unified into one class)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from repo_watcher.config.schema import FieldMappingConfig
from repo_watcher.domain import Term
from repo_watcher.sources.base import TermSource
from repo_watcher.sources.cloner import clone_repo


class GitSource(TermSource):
    """Clone a repo at a given branch, extract terms using a provided function.

    Unifies GitRepoSource, GitRepoEsgvocFormatSource, and GitRepoEmdSource
    into a single class parameterized by the extraction function.
    """

    def __init__(
        self,
        owner: str,
        repo: str,
        branch: str,
        extract_fn: Callable[..., set[Term]],
        cv_subdir: str | None = None,
        cv_format: str | None = None,
        requires_field_mappings: bool = False,
    ) -> None:
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self.extract_fn = extract_fn
        self.cv_subdir = cv_subdir
        self.cv_format = cv_format
        self.requires_field_mappings = requires_field_mappings
        self._dest: Path | None = None

    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, FieldMappingConfig] | None = None,
    ) -> set[Term]:
        self._dest = clone_repo(self.owner, self.repo, self.branch)
        cv_dir = self._dest / self.cv_subdir if self.cv_subdir else self._dest

        if self.requires_field_mappings:
            if not field_mappings:
                msg = f"GitSource({self.repo}) requires field_mappings"
                raise ValueError(msg)
            return self.extract_fn(repo_directory=cv_dir, field_mappings=field_mappings)

        # Build kwargs based on what the extract function accepts
        kwargs: dict[str, Any] = {}
        import inspect
        sig = inspect.signature(self.extract_fn)
        params = sig.parameters

        if "cv_directory" in params:
            kwargs["cv_directory"] = cv_dir
        elif "repo_directory" in params:
            kwargs["repo_directory"] = cv_dir

        if "project" in params:
            kwargs["project"] = project
        if "cv_format" in params:
            kwargs["cv_format"] = self.cv_format
        if "only_collections" in params:
            kwargs["only_collections"] = only_collections
        if "skip_collections" in params:
            kwargs["skip_collections"] = skip_collections
        if "field_mappings" in params:
            kwargs["field_mappings"] = field_mappings

        return self.extract_fn(**kwargs)

    def label(self) -> str:
        return f"{self.owner}/{self.repo}@{self.branch}"

    def repo_path(self) -> Path | None:
        return self._dest
