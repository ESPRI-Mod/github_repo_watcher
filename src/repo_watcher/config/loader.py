"""YAML loading and config merging."""

from __future__ import annotations

from pathlib import Path

import yaml

from repo_watcher.config.schema import CheckConfig, MergedConfig


def load_checks(config_path: Path) -> list[CheckConfig]:
    """Load all check definitions from a repos_config.yml file."""
    with config_path.open() as f:
        raw = yaml.safe_load(f)
    return [CheckConfig(**entry) for entry in raw["checks"]]


def merge_check_configs(checks: list[CheckConfig]) -> MergedConfig:
    """Merge configuration options across multiple checks for the same project.

    Combines excludes, type mappings, generation defaults, transforms, overrides,
    multi-step configs, and source prefill into a single MergedConfig.
    """
    merged = MergedConfig()
    for chk in checks:
        if chk.exclude_terms:
            for col, patterns in chk.exclude_terms.items():
                merged.excludes.setdefault(col, []).extend(patterns)
        if chk.universe_type_mappings:
            merged.universe_type_mappings.update(chk.universe_type_mappings)
        if chk.generation_defaults:
            for col, defaults in chk.generation_defaults.items():
                merged.generation_defaults.setdefault(col, {}).update(defaults)
        if chk.term_id_transforms:
            for col, rules in chk.term_id_transforms.items():
                merged.term_id_transforms.setdefault(col, []).extend(rules)
        if chk.term_overrides:
            for col, overrides in chk.term_overrides.items():
                merged.term_overrides.setdefault(col, {}).update(overrides)
        if chk.multi_step_terms:
            merged.multi_step_terms.update(chk.multi_step_terms)
        if chk.source_prefill:
            merged.source_prefill.update(chk.source_prefill)
        if chk.metadata_profile:
            merged.metadata_profile = chk.metadata_profile
    return merged
