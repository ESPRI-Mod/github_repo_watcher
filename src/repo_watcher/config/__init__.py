"""Configuration: schema definitions and YAML loading."""

from repo_watcher.config.loader import load_checks, merge_check_configs
from repo_watcher.config.schema import (
    CheckConfig,
    FieldMappingConfig,
    MergedConfig,
    MultiStepConfig,
    SourceConfig,
    TermIdTransform,
    TermOverrideConfig,
)

__all__ = [
    "CheckConfig",
    "FieldMappingConfig",
    "MergedConfig",
    "MultiStepConfig",
    "SourceConfig",
    "TermIdTransform",
    "TermOverrideConfig",
    "load_checks",
    "merge_check_configs",
]
