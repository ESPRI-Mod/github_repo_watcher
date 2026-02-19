"""
Deploy Prefect flows for repository consistency checks.

This module orchestrates deployments for monitoring branch consistency
across multiple repositories using Prefect workflows and GitHub integration.
"""

from pathlib import Path

import yaml
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect_github import GitHubRepository
from pydantic import BaseModel

from branch_consistency import EsgvocConfig, Repo


class Config(BaseModel):
    """Configuration for a repository consistency check."""

    owner: str
    repo: str
    reference_branch: str | None = None
    project: str


if __name__ == "__main__":
    with Path("repos_config.yml").open() as f:
        configs: list[Config] = [
            Config(**config) for config in yaml.safe_load(f)["repos"]
        ]

    github_storage = GitHubRepository.load("repo-watcher")
    git_storage = GitRepository(
        url=github_storage.repository_url,
        branch=github_storage.reference,
        credentials=github_storage.credentials,
    )

    code = flow.from_source(
        git_storage,
        entrypoint="branch_consistency.py:are_branch_consistent",
    )

    deployments = []
    for config in configs:
        if config.reference_branch is None:
            continue

        repo_reference = Repo(
            url=f"https://github.com/{config.owner}/{config.repo}",
            branch=config.reference_branch,
            project=config.project,
        )

        name = repo_reference.nicename.replace("/", "-")
        deployment = code.deploy(
            name,
            work_pool_name="alaupin-spiritx",
            tags=[
                "consistency",
                repo_reference.nicename,
                repo_reference.branch,
                repo_reference.nicename.split("@")[0],
            ],
            parameters={
                "project_repo": repo_reference,
                "esgvoc_config": EsgvocConfig.prod,
            },
            cron="0 6 * * 1-5",
        )
        print(deployment)
