from typing import Any, override
from prefect.runner.storage import GitRepository
import yaml
from prefect import flow
from prefect_github import GitHubRepository
from pydantic import BaseModel

from branch_consistency import Repo


class SpiritGitRepository(GitRepository):
    @override
    def to_pull_step(self) -> list[dict[str, Any]]:
        git_clone_step = super().to_pull_step()
        mkdir_step = {
            "prefect.deployments.steps.run_shell_script": {
                "script": "mkdir -p /tmp/$SLURM_JOBID/",
                "expand_env_vars": True,
            }
        }
        cd_tmp_jobid_step = {
            "prefect.deployments.steps.set_working_directory": {
                "directory": "/tmp/{{ $SLURM_JOBID }}/"
            }
        }
        return [
            mkdir_step,
            cd_tmp_jobid_step,
            git_clone_step,
        ]


class Config(BaseModel):
    owner: str
    repo: str
    reference_branch: str | None = None
    project: str


if __name__ == "__main__":
    with open("repos_config.yml") as f:
        configs: list[Config] = [Config(**config) for config in yaml.safe_load(f)["repos"]]

    github_storage = GitHubRepository.load("repo-watcher")
    git_storage = SpiritGitRepository(
        url=github_storage.repository_url,
        branch=github_storage.reference,
        credentials=github_storage.credentials,
    )

    code = flow.from_source(
        git_storage, entrypoint="branch_consistency.py:are_branch_consistent"
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
        code.deploy(
            name,
            work_pool_name="alaupin-spiritx",
            tags=[
                "consistency",
                repo_reference.nicename,
                repo_reference.branch,
                repo_reference.nicename.split("@")[0],
            ],
            parameters={"repo": repo_reference, "esgvoc_github_repo": "https://github.com/WCRP-CMIP/WCRP-universe", "esgvoc_branch": "esgvoc"},
            cron="0 6 * * 1-5",
        )
