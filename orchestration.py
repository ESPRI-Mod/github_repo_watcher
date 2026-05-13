"""Layer 4 — Orchestration: Prefect flows, NocoDB reporting, config wiring.

This is the only module that imports Prefect and NocoDB-related code.
"""

from __future__ import annotations

import requests
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.runtime import flow_run
from prefect.variables import Variable

from comparison import compare_terms
from models import CheckConfig, ConsistencyResult
from sources import (
    EsgvocRegistrySource,
    GitRepoEmdSource,
    GitRepoEsgvocFormatSource,
    GitRepoSource,
    TermSource,
)


def build_source(source_config: dict) -> TermSource:
    """Factory: config dict -> TermSource instance."""
    source_type = source_config["type"]
    if source_type == "git":
        return GitRepoSource(
            owner=source_config["owner"],
            repo=source_config["repo"],
            branch=source_config["branch"],
        )
    if source_type == "git_esgvoc_format":
        return GitRepoEsgvocFormatSource(
            owner=source_config["owner"],
            repo=source_config["repo"],
            branch=source_config["branch"],
        )
    if source_type == "git_emd":
        return GitRepoEmdSource(
            owner=source_config["owner"],
            repo=source_config["repo"],
            branch=source_config["branch"],
        )
    if source_type == "registry":
        return EsgvocRegistrySource(
            project=source_config["project"],
            version=source_config.get("version"),
        )
    msg = f"Unknown source type: {source_type}"
    raise ValueError(msg)


@task(retries=3, retry_delay_seconds=[30, 60, 120], log_prints=True)
def fetch_terms(source: TermSource, check: CheckConfig) -> set:
    """Fetch terms from a source, with Prefect retry logic."""
    return source.get_terms(
        project=check.project,
        only_collections=check.only_collections,
        skip_collections=check.skip_collections,
        field_mappings=check.field_mappings,
    )


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def send_to_nocodb(check: CheckConfig, result: ConsistencyResult) -> None:
    """Post inconsistency record to NocoDB."""
    base_url = Variable.get("nocodb_base_url")
    table_id = Variable.get("nocodb_repo_watcher_tables_ids")
    table_url = f"{base_url}/{table_id}/records"
    api_key = Secret.load("nocodb-consistency-api-key").get()

    headers = {"xc-token": api_key, "Content-Type": "application/json"}
    payload = {
        "check_name": check.name,
        "source": result.source_label,
        "reference": result.reference_label,
        "is_verified": False,
        "task_url": flow_run.get_flow_run_ui_url(),
    }
    r = requests.post(table_url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def send_skip_record_to_nocodb(check: CheckConfig, reason: str) -> None:
    """Post a 'check skipped' record when a source is unavailable."""
    base_url = Variable.get("nocodb_base_url")
    table_id = Variable.get("nocodb_repo_watcher_tables_ids")
    table_url = f"{base_url}/{table_id}/records"
    api_key = Secret.load("nocodb-consistency-api-key").get()

    headers = {"xc-token": api_key, "Content-Type": "application/json"}
    payload = {
        "check_name": check.name,
        "source": "unavailable",
        "reference": "unavailable",
        "is_verified": False,
        "task_url": flow_run.get_flow_run_ui_url(),
        "notes": f"Skipped: {reason}",
    }
    r = requests.post(table_url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()


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
    )

    if not result.is_consistent:
        logger.error(result.format_report())
        send_to_nocodb(check, result)
    else:
        logger.info("%s: consistent (%d terms)", check.name, len(result.common))

    return result
