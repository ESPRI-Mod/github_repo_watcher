"""Prefect flows for scheduled consistency checks."""

from __future__ import annotations

from prefect import flow, get_run_logger, task

from repo_watcher.comparison import compare_terms
from repo_watcher.config.schema import CheckConfig
from repo_watcher.domain import ConsistencyResult
from repo_watcher.orchestration.nocodb import send_skip_record_to_nocodb, send_to_nocodb
from repo_watcher.sources import TermSource, build_source


@task(retries=3, retry_delay_seconds=[30, 60, 120], log_prints=True)
def fetch_terms(source: TermSource, check: CheckConfig) -> set:
    """Fetch terms from a source, with Prefect retry logic."""
    return source.get_terms(
        project=check.project,
        only_collections=check.only_collections,
        skip_collections=check.skip_collections,
        field_mappings=check.field_mappings,
    )


@flow(log_prints=True)
def check_consistency(check: CheckConfig) -> ConsistencyResult | None:
    """Run a single consistency check: fetch terms from two sources, compare."""
    logger = get_run_logger()

    source = build_source(check.source)
    reference = build_source(check.reference)

    try:
        source_terms = fetch_terms(source, check)
        reference_terms = fetch_terms(reference, check)
    except Exception:
        logger.exception("%s: source unavailable", check.name)
        send_skip_record_to_nocodb(check, reason="source fetch failed")
        return None

    result = compare_terms(
        source_terms=source_terms,
        reference_terms=reference_terms,
        source_label=source.label(),
        reference_label=reference.label(),
        collection_mappings=check.collection_mappings,
        exclude_terms=check.exclude_terms,
    )

    if not result.is_consistent:
        logger.error(result.format_report())
        send_to_nocodb(check, result)
    else:
        logger.info("%s: consistent (%d terms)", check.name, len(result.common))

    return result
