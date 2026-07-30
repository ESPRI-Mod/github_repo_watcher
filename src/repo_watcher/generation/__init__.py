"""Generation: scaffold missing term JSON files in universe and project repos."""

from repo_watcher.generation.context import detect_collection_mapping, detect_field_references
from repo_watcher.generation.pipeline import collect_and_generate
from repo_watcher.generation.planner import generate_missing_terms
from repo_watcher.generation.report import (
    FieldChange,
    GenerationReport,
    TermFile,
    TermRef,
    TermSkip,
)
from repo_watcher.generation.skeleton import build_project_link, build_universe_skeleton
from repo_watcher.generation.writer import write_report

# Backward-compatible aliases (tests reference the underscore-prefixed names)
_build_project_link = build_project_link
_build_universe_skeleton = build_universe_skeleton

__all__ = [
    "FieldChange",
    "GenerationReport",
    "TermFile",
    "TermRef",
    "TermSkip",
    "build_project_link",
    "build_universe_skeleton",
    "collect_and_generate",
    "detect_collection_mapping",
    "detect_field_references",
    "generate_missing_terms",
    "write_report",
]
