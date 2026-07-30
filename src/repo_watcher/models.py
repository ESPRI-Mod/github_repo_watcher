"""Backward-compatible re-exports.

All types have moved to repo_watcher.config and repo_watcher.domain.
This module re-exports them so existing imports keep working.
"""

from repo_watcher.config.schema import (
    CheckConfig,
    FieldMappingConfig,
    MultiStepConfig,
    SourceConfig,
    TermIdTransform,
    TermOverrideConfig,
)
from repo_watcher.domain import (
    ConsistencyResult,
    ProjectResult,
    ReportMode,
    Term,
)

__all__ = [
    "CheckConfig",
    "ConsistencyResult",
    "FieldMappingConfig",
    "MultiStepConfig",
    "ProjectResult",
    "ReportMode",
    "SourceConfig",
    "Term",
    "TermIdTransform",
    "TermOverrideConfig",
]
