import yaml
from prefect import flow
from prefect_github import GitHubCredentials, GitHubRepository

from branch_consistency import Form, Repo

if __name__ == "__main__":
    with open("repos_config.yml") as f:
        configs = yaml.safe_load(f)["repos"]

    github_credentials_block = GitHubCredentials.load("github-repoc-checker")
    github_storage = GitHubRepository.load("repo-watcher")
    github_storage.reference = "dev-simple"
    github_storage.save(overwrite=True)
    code = flow.from_source(github_storage, entrypoint="branch_consistency.py:are_branch_consistent")

    deployments = []
    for config in configs:
        if config["reference_branch"] is None:
            continue
        if config["working_branch"] == "REF":
            main_form = Form.esgvoc
        else:
            main_form = Form.legacy

        repo_a = Repo(
            url=f"https://github.com/{config['owner']}/{config['repo']}",
            branch=config["working_branch"],
            form=Form.esgvoc,
        )
        repo_b = Repo(
            url=f"https://github.com/{config['owner']}/{config['repo']}",
            branch=config["reference_branch"],
            form=main_form,
        )

        name = repo_a.nicename.replace("/", "-")
        code.deploy(
            name,
            work_pool_name="process",
            tags=[
                "consistency",
                repo_a.nicename,
                repo_b.nicename,
                repo_a.branch,
                repo_b.branch,
                repo_a.nicename.split("@")[0],
            ],
            parameters={"repo_a": repo_a, "repo_b": repo_b},
        )
