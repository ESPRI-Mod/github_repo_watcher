"""NocoDB reporting for consistency check results."""

from __future__ import annotations

import requests
from prefect import task
from prefect.blocks.system import Secret
from prefect.runtime import flow_run
from prefect.variables import Variable

from repo_watcher.config.schema import CheckConfig
from repo_watcher.domain import ConsistencyResult


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
