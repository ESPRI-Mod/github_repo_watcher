"""Sources: fetch terms from git repos or the esgvoc registry."""

from repo_watcher.sources.base import TermSource
from repo_watcher.sources.git import GitSource
from repo_watcher.sources.registry import EsgvocRegistrySource

from repo_watcher.config.schema import FieldMappingConfig
from repo_watcher.domain import Term


def build_source(source_config: dict) -> TermSource:
    """Factory: config dict -> TermSource instance."""
    from repo_watcher.extraction import extract_terms, extract_terms_emd, extract_terms_esgvoc_format

    source_type = source_config["type"]
    if source_type == "git":
        return GitSource(
            owner=source_config["owner"],
            repo=source_config["repo"],
            branch=source_config["branch"],
            extract_fn=extract_terms,
            cv_subdir=source_config.get("cv_subdir"),
            cv_format=source_config.get("cv_format"),
        )
    if source_type == "git_esgvoc_format":
        return GitSource(
            owner=source_config["owner"],
            repo=source_config["repo"],
            branch=source_config["branch"],
            extract_fn=extract_terms_esgvoc_format,
        )
    if source_type == "git_emd":
        return GitSource(
            owner=source_config["owner"],
            repo=source_config["repo"],
            branch=source_config["branch"],
            extract_fn=extract_terms_emd,
            requires_field_mappings=True,
        )
    if source_type == "registry":
        return EsgvocRegistrySource(project=source_config["project"], version=source_config.get("version"))
    msg = f"Unknown source type: {source_type}"
    raise ValueError(msg)


__all__ = [
    "EsgvocRegistrySource",
    "GitSource",
    "TermSource",
    "build_source",
]
