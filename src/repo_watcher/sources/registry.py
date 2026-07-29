"""Esgvoc registry term source."""

from __future__ import annotations

import esgvoc.api as ev

from repo_watcher.config.schema import FieldMappingConfig
from repo_watcher.domain import Term
from repo_watcher.sources.base import TermSource


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
