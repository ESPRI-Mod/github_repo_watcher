from typing import Any, override
from prefect.runner.storage import GitRepository
import yaml
from datetime import timedelta
from prefect import flow
from prefect_github import GitHubCredentials, GitHubRepository

from branch_consistency import Form, Repo


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


if __name__ == "__main__":
    with open("repos_config.yml") as f:
        configs = yaml.safe_load(f)["repos"]

    github_credentials_block = GitHubCredentials.load("github-repoc-checker")
    github_storage = GitHubRepository.load("repo-watcher")
    git_storage = SpiritGitRepository(
        url=github_storage.repository_url,
        branch=github_storage.reference,
        credentials=github_credentials_block,
    )

    code = flow.from_source(
        git_storage, entrypoint="branch_consistency.py:are_branch_consistent"
    )

    deployments = []
    for config in configs:
        if config["reference_branch"] is None:
            continue
        if config["working_branch"] == "REF":
            main_form = Form.esgvoc
        else:
            main_form = Form.legacy

        repo_working = Repo(
            url=f"https://github.com/{config['owner']}/{config['repo']}",
            branch=config["working_branch"],
            form=Form.esgvoc,
        )
        repo_reference = Repo(
            url=f"https://github.com/{config['owner']}/{config['repo']}",
            branch=config["reference_branch"],
            form=main_form,
        )

        name = repo_working.nicename.replace("/", "-")
        code.deploy(
            name,
            work_pool_name="alaupin-spiritx",
            tags=[
                "consistency",
                repo_working.nicename,
                repo_reference.nicename,
                repo_working.branch,
                repo_reference.branch,
                repo_working.nicename.split("@")[0],
            ],
            parameters={"repo_working": repo_working, "repo_reference": repo_reference},
            cron="0 6 * * *",
        )
