"""Layer 1 — Sources: "get me terms from somewhere."

Each source knows how to fetch data and produce a set[Term].
Handles all network I/O: git cloning, esgvoc registry queries.
"""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import esgvoc.api as ev
from prefect.runner.storage import GitRepository
from prefect.utilities.asyncutils import run_coro_as_sync

from extraction import extract_terms, extract_terms_emd, extract_terms_esgvoc_format
from models import Term


class TermSource(ABC):
    """Something that can produce a set[Term]."""

    @abstractmethod
    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, dict[str, Any]] | None = None,
    ) -> set[Term]: ...

    @abstractmethod
    def label(self) -> str:
        """Human-readable label for reports."""
        ...


class GitRepoSource(TermSource):
    """Clone a repo at a given branch, extract terms from JSON CVs."""

    def __init__(self, owner: str, repo: str, branch: str) -> None:
        self.owner = owner
        self.repo = repo
        self.branch = branch

    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, dict[str, Any]] | None = None,
    ) -> set[Term]:
        repo_dir = self._clone()
        cv_dir = repo_dir / "CVs" if project == "input4mip" else repo_dir
        return extract_terms(
            cv_directory=cv_dir,
            project=project,
            only_collections=only_collections,
            skip_collections=skip_collections,
            field_mappings=field_mappings,
        )

    def label(self) -> str:
        return f"{self.owner}/{self.repo}@{self.branch}"

    def _clone(self) -> Path:
        url = f"https://github.com/{self.owner}/{self.repo}.git"
        git_repo = GitRepository(url=url, branch=self.branch)
        git_repo._storage_base_path = Path("/tmp")  # noqa: SLF001
        dest = git_repo.destination
        if dest.is_dir():
            shutil.rmtree(dest)
        run_coro_as_sync(git_repo.pull_code())
        return dest


class GitRepoEsgvocFormatSource(TermSource):
    """Clone a repo that uses the esgvoc directory format (one dir/collection, one JSON/term)."""

    def __init__(self, owner: str, repo: str, branch: str) -> None:
        self.owner = owner
        self.repo = repo
        self.branch = branch

    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, dict[str, Any]] | None = None,
    ) -> set[Term]:
        repo_dir = self._clone()
        return extract_terms_esgvoc_format(
            repo_directory=repo_dir,
            only_collections=only_collections,
            skip_collections=skip_collections,
        )

    def label(self) -> str:
        return f"{self.owner}/{self.repo}@{self.branch}"

    def _clone(self) -> Path:
        url = f"https://github.com/{self.owner}/{self.repo}.git"
        git_repo = GitRepository(url=url, branch=self.branch)
        git_repo._storage_base_path = Path("/tmp")  # noqa: SLF001
        dest = git_repo.destination
        if dest.is_dir():
            shutil.rmtree(dest)
        run_coro_as_sync(git_repo.pull_code())
        return dest


class GitRepoEmdSource(TermSource):
    """Clone the EMD repo and extract field-level terms from src-data."""

    def __init__(self, owner: str, repo: str, branch: str) -> None:
        self.owner = owner
        self.repo = repo
        self.branch = branch

    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, dict[str, Any]] | None = None,
    ) -> set[Term]:
        if not field_mappings:
            msg = "GitRepoEmdSource requires field_mappings"
            raise ValueError(msg)
        repo_dir = self._clone()
        return extract_terms_emd(
            repo_directory=repo_dir,
            field_mappings=field_mappings,
        )

    def label(self) -> str:
        return f"{self.owner}/{self.repo}@{self.branch}"

    def _clone(self) -> Path:
        url = f"https://github.com/{self.owner}/{self.repo}.git"
        git_repo = GitRepository(url=url, branch=self.branch)
        git_repo._storage_base_path = Path("/tmp")  # noqa: SLF001
        dest = git_repo.destination
        if dest.is_dir():
            shutil.rmtree(dest)
        run_coro_as_sync(git_repo.pull_code())
        return dest


class EsgvocRegistrySource(TermSource):
    """Query esgvoc registry for all terms in a project."""

    def __init__(self, project: str, version: str | None = None) -> None:
        self.project = project
        self.version = version

    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, dict[str, Any]] | None = None,
    ) -> set[Term]:
        collections = ev.get_all_collections_in_project(
            self.project, version=self.version
        )
        terms: set[Term] = set()

        # Build a set of collections that need field-level extraction
        field_target_collections: dict[str, dict[str, str]] = {}
        if field_mappings:
            for mapping in field_mappings.values():
                target_col = mapping["target_collection"]
                field_target_collections[target_col] = mapping["fields"]

        for collection in collections:
            if only_collections and collection not in only_collections:
                continue
            if skip_collections and collection in skip_collections:
                continue

            if collection in field_target_collections:
                # Field-level extraction: produce Term per field/value
                target_fields = field_target_collections[collection]
                for term_obj in ev.get_all_terms_in_collection(
                    self.project, collection, version=self.version
                ):
                    for target_field in target_fields.values():
                        value = getattr(term_obj, target_field, None)
                        if value is not None:
                            terms.add(
                                Term(
                                    name=term_obj.id,
                                    collection=collection,
                                    field=target_field,
                                    value=str(value),
                                ),
                            )
            else:
                for term_obj in ev.get_all_terms_in_collection(
                    self.project, collection, version=self.version
                ):
                    terms.add(Term(name=term_obj.id, collection=collection))
        return terms

    def label(self) -> str:
        v = self.version or "latest"
        return f"esgvoc/{self.project}@{v}"
