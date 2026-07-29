"""TermSource ABC: something that can produce a set of Terms."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from repo_watcher.config.schema import FieldMappingConfig
from repo_watcher.domain import Term


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
