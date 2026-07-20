"""Layer 1 — Sources: "get me terms from somewhere."

Each source knows how to fetch data and produce a set[Term].
Handles all network I/O: git cloning, esgvoc registry queries.
"""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from pathlib import Path

import esgvoc.api as ev
from git import Repo

from repo_watcher.extraction import extract_terms, extract_terms_emd, extract_terms_esgvoc_format
from repo_watcher.models import FieldMappingConfig, Term


class TermSource(ABC):
    """Something that can produce a set[Term]."""

    @abstractmethod
    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, FieldMappingConfig] | None = None,
    ) -> set[Term]: ...

    @abstractmethod
    def label(self) -> str:
        """Human-readable label for reports."""
        ...

    def repo_path(self) -> Path | None:
        """Return the local path of the cloned repo, if available."""
        return None


def _clone_repo(owner: str, repo: str, branch: str) -> Path:
    """Clone a GitHub repo to /tmp and return the local path."""
    url = f"https://github.com/{owner}/{repo}.git"
    dest = Path("/tmp") / f"{repo}-{branch}"
    if dest.is_dir():
        shutil.rmtree(dest)
    Repo.clone_from(url, dest, branch=branch, depth=1)
    return dest


class GitRepoSource(TermSource):
    """Clone a repo at a given branch, extract terms from JSON CVs."""

    def __init__(self, owner: str, repo: str, branch: str,
                 cv_subdir: str | None = None, cv_format: str | None = None) -> None:
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self.cv_subdir = cv_subdir
        self.cv_format = cv_format
        self._dest: Path | None = None

    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, FieldMappingConfig] | None = None,
    ) -> set[Term]:
        self._dest = _clone_repo(self.owner, self.repo, self.branch)
        cv_dir = self._dest / self.cv_subdir if self.cv_subdir else self._dest
        return extract_terms(
            cv_directory=cv_dir,
            project=project,
            cv_format=self.cv_format,
            only_collections=only_collections,
            skip_collections=skip_collections,
            field_mappings=field_mappings,
        )

    def label(self) -> str:
        return f"{self.owner}/{self.repo}@{self.branch}"

    def repo_path(self) -> Path | None:
        return self._dest


class GitRepoEsgvocFormatSource(TermSource):
    """Clone a repo that uses the esgvoc directory format (one dir/collection, one JSON/term)."""

    def __init__(self, owner: str, repo: str, branch: str) -> None:
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self._dest: Path | None = None

    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, FieldMappingConfig] | None = None,
    ) -> set[Term]:
        self._dest = _clone_repo(self.owner, self.repo, self.branch)
        return extract_terms_esgvoc_format(
            repo_directory=self._dest,
            only_collections=only_collections,
            skip_collections=skip_collections,
        )

    def label(self) -> str:
        return f"{self.owner}/{self.repo}@{self.branch}"

    def repo_path(self) -> Path | None:
        return self._dest


class GitRepoEmdSource(TermSource):
    """Clone the EMD repo and extract field-level terms from src-data."""

    def __init__(self, owner: str, repo: str, branch: str) -> None:
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self._dest: Path | None = None

    def get_terms(
        self,
        project: str,
        only_collections: list[str] | None = None,
        skip_collections: list[str] | None = None,
        field_mappings: dict[str, FieldMappingConfig] | None = None,
    ) -> set[Term]:
        if not field_mappings:
            msg = "GitRepoEmdSource requires field_mappings"
            raise ValueError(msg)
        self._dest = _clone_repo(self.owner, self.repo, self.branch)
        return extract_terms_emd(
            repo_directory=self._dest,
            field_mappings=field_mappings,
        )

    def label(self) -> str:
        return f"{self.owner}/{self.repo}@{self.branch}"

    def repo_path(self) -> Path | None:
        return self._dest


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
        field_mappings: dict[str, FieldMappingConfig] | None = None,
    ) -> set[Term]:
        collections = ev.get_all_collections_in_project(self.project, version=self.version)
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
                for term_obj in ev.get_all_terms_in_collection(self.project, collection, version=self.version):
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
                for term_obj in ev.get_all_terms_in_collection(self.project, collection, version=self.version):
                    terms.add(Term(name=term_obj.id, collection=collection))
        return terms

    def label(self) -> str:
        v = self.version or "latest"
        return f"esgvoc/{self.project}@{v}"


def build_source(source_config: dict) -> TermSource:
    """Factory: config dict -> TermSource instance."""
    source_type = source_config["type"]
    if source_type == "git":
        return GitRepoSource(
            owner=source_config["owner"], repo=source_config["repo"], branch=source_config["branch"],
            cv_subdir=source_config.get("cv_subdir"), cv_format=source_config.get("cv_format"),
        )
    if source_type == "git_esgvoc_format":
        return GitRepoEsgvocFormatSource(
            owner=source_config["owner"], repo=source_config["repo"], branch=source_config["branch"]
        )
    if source_type == "git_emd":
        return GitRepoEmdSource(
            owner=source_config["owner"], repo=source_config["repo"], branch=source_config["branch"]
        )
    if source_type == "registry":
        return EsgvocRegistrySource(project=source_config["project"], version=source_config.get("version"))
    msg = f"Unknown source type: {source_type}"
    raise ValueError(msg)
