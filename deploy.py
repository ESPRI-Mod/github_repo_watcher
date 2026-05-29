"""Deploy Prefect flows for repository consistency checks.

Reads repos_config.yml, creates one Prefect deployment per check.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, override

import yaml
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect_github import GitHubRepository

from repo_watcher.models import CheckConfig


class SpiritGitRepository(GitRepository):
    """Custom GitRepository for Prefect deployments on SLURM systems."""

    @override
    def to_pull_step(self) -> list[dict[str, Any]]:
        git_clone_step = super().to_pull_step()
        mkdir_step = {
            "prefect.deployments.steps.run_shell_script": {
                "script": "mkdir -p /tmp/$SLURM_JOBID/",
                "expand_env_vars": True,
            },
        }
        cd_tmp_jobid_step = {
            "prefect.deployments.steps.set_working_directory": {
                "directory": "/tmp/{{ $SLURM_JOBID }}/",
            },
        }
        return [mkdir_step, cd_tmp_jobid_step, git_clone_step]


if __name__ == "__main__":
    with Path("repos_config.yml").open() as f:
        raw_config = yaml.safe_load(f)

    checks = [CheckConfig(**entry) for entry in raw_config["checks"]]

    github_storage = GitHubRepository.load("repo-watcher")
    git_storage = SpiritGitRepository(
        url=github_storage.repository_url,
        branch=github_storage.reference,
        credentials=github_storage.credentials,
    )

    code = flow.from_source(
        git_storage,
        entrypoint="src/repo_watcher/orchestration.py:check_consistency",
    )

    for check in checks:
        deployment = code.deploy(
            check.name,
            work_pool_name="alaupin-spiritx",
            tags=["consistency", check.name],
            parameters={"check": check},
            cron="0 6 * * 1-5",
        )
        print(deployment)
