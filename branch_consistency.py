from collections import defaultdict
from unittest import result
from esgvoc.api.search import MatchingTerm
import json
import shutil
from enum import StrEnum, auto
from functools import cached_property
from pathlib import Path
from typing import Any, Self

from prefect import flow, get_run_logger, task
from prefect.variables import Variable
from prefect.runner.storage import GitRepository
from prefect.runtime import flow_run
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect.blocks.system import Secret
from pydantic import BaseModel
import requests

from esgvoc.core.service.state import ServiceSettings
from esgvoc.core.service import config_manager, current_state
import esgvoc.api as ev

# ESGVOC_EXCLUDED_COLLECTIONS = [
#     "variable_id"
# ]
LEGACY_EXCLUDED_COLLECTIONS = [
    "DRS",
    "required_global_attributes",
    "global_attrs",
]


class Status(StrEnum):
    inconsistent = auto()
    consistent = auto()


class Form(StrEnum):
    esgvoc = auto()
    legacy = auto()


class Term(BaseModel):
    name: str
    collection: str
    # path: Path
    # json_path: str = "" | ".institution_id.AWI"

    @property
    def key(self) -> str:
        return f"{self.name.lower()}/{self.collection.lower()}"

    def __eq__(self, other: Self) -> bool:
        return self.key == other.key

    def __hash__(self) -> int:
        return hash(self.key)


class Repo(BaseModel):
    url: str
    branch: str
    form: Form
    project: str
    # local_path: Path | None = None

    @property
    def git_repo(self) -> GitRepository:
        result = GitRepository(url=self.url, branch=self.branch)
        result._storage_base_path = Path("/tmp")
        return result

    @cached_property
    def terms(self) -> set[Term]:
        match self.form:
            case Form.esgvoc:
                return parse_esgvoc_terms(self)
            case Form.legacy:
                return parse_legacy_terms(self)

    @property
    def destination(self) -> Path:
        return self.git_repo.destination

    @property
    def cv_location(self) -> Path:
        if self.project == "input4mip":
            return self.destination / "CVs"
        else:
            return self.destination

    def pull(self) -> None:
        if self.destination.is_dir():
            shutil.rmtree(self.destination)
        run_coro_as_sync(self.git_repo.pull_code())

    @property
    def nicename(self) -> str:
        _, owner, repo = self.url.rsplit("/", 2)
        repo = repo.replace(".git", "")
        return f"{owner}/{repo}@{self.branch}"


@task
def parse_esgvoc_terms(repo: Repo) -> set[Term]:
    result: set[Term] = set()
    for term_path in repo.cv_location.glob("*/*.json"):
        if term_path.parent.name[0] in ["_", "."]:
            continue
        if "drs.json" in term_path.name.lower():
            continue
        contents = term_path.read_text()
        data = json.loads(contents)
        result.add(
            Term(
                name=data["id"],
                collection=term_path.parent.name,
            ),
        )
    return result


@task(log_prints=True)
def validate_legacy_terms(repo: Repo) -> set[Term]:
    valid_terms: set[Term] = set()
    invalid_terms: set[Term] = set()
    for term in repo.terms:
        if repo.project == "cordex-cmip6" and term.collection == "table_id":
            term.collection = "frequency"
        matching_term: list[MatchingTerm] = ev.valid_term_in_collection(term.name, repo.project, term.collection.lower())
        if matching_term:
            term.name = matching_term[0].term_id
            valid_terms.add(term)
        else:
            invalid_terms.add(term)
    repo.terms = valid_terms
    return invalid_terms


# def build_terms(collection_key: str, data: dict, repo) -> Term | None:
#     if collection_key in ["Header", "version_metadata", "license"]:
#         return None
#     if repo.project == "cordex-cmip6" and collection_key == "fixed" and isinstance(data, dict):
#         for collection_subkey in data.keys():
#             if collection_subkey == "license":
#                 return None
#             term_data = data[collection_subkey]
#             return Term(
#                 name=term_data,
#                 collection=collection_subkey,
#             )
#     else:
#         term_data = data[collection_key]
#         if isinstance(term_data, list):
#             term_keys = [term for term in term_data]
#         elif isinstance(term_data, dict):
#             term_keys = list(term for term in term_data.keys())
#         elif isinstance(term_data, str):
#             term_keys = [term_data]
#         else:
#             raise TypeError(term_data)
            
#         for term_key in term_keys:
#             return Term(
#                 name=term_key,
#                 collection=collection_key,
#             )


def prepare_json_cordex_cmip6(data: dict) -> dict[str, dict[str, dict[str, str] | None]]:
    result = {}
    for collection_key, terms in data.items():
        result[collection_key] = {}

        if isinstance(terms, dict):
            for term_key, term_content in terms.items():
                result[collection_key][term_key] = term_content
        elif isinstance(terms, list):
            for term_key in data[collection_key]:
                result[collection_key][term_key] = None
        else:
            raise TypeError(terms)
    return result


def prepare_json_cmip6(data: dict) -> dict[str, dict[str, dict[str, str] | None]]:
    result = {}
    for collection_key, terms in data.items():
        if collection_key == "version_metadata":
            continue
        else:
            result[collection_key] = {}

            if isinstance(terms, dict):
                for term_key, term_content in terms.items():
                    result[collection_key][term_key] = term_content
            elif isinstance(terms, list):
                for term_key in terms:
                    result[collection_key][term_key] = None
            else:
                raise TypeError(terms)
    return result


def prepare_json_cmip6plus(data: dict) -> dict[str, dict[str, dict[str, str] | None]]:
    result = {}
    for collection_key, terms in data.items():
        if collection_key == "Header":
            continue
        else:
            result[collection_key] = {}
            if isinstance(terms, dict):
                for term_key, term_content in terms.items():
                    result[collection_key][term_key] = term_content
            elif isinstance(terms, list):
                for term_key in terms:
                    result[collection_key][term_key] = None
            elif isinstance(terms, str):
                result[collection_key][terms] = None
            else:
                raise TypeError(terms)
    return result


def prepare_json_input4mip(data: dict | list[str], collection_path: Path) -> dict[str, dict[str, dict[str, str] | None]]:
    result = {}
    collection = collection_path.stem.removeprefix("input4MIPs_")
    result[collection] = {}
    if collection == "target_mip.json":
        for target_mips in data.values():
            for target_mip, target_mip_params in target_mips.items():
                result["target_mip"][target_mip] = target_mip_params
    else:
        if isinstance(data, dict):
            for term_key, term_content in data.items():
                result[collection][term_key] = term_content
        elif isinstance(data, list):
            for term_key in data:
                result[collection][term_key] = None
        else:
            raise TypeError(data)
    return result


@task(log_prints=True)
def parse_legacy_terms(repo: Repo) -> set[Term]:
    result: set[Term] = set()
    for collection_path in repo.cv_location.glob("*.json"):
        if any(
            collection in collection_path.name
            for collection in LEGACY_EXCLUDED_COLLECTIONS
        ):
            continue
        contents = collection_path.read_text()
        data = json.loads(contents)

        match repo.project:
            case "cordex-cmip6":
                prepared_data = prepare_json_cordex_cmip6(data)
            case "cmip6":
                prepared_data = prepare_json_cmip6(data)
            case "cmip6plus":
                prepared_data = prepare_json_cmip6plus(data)
            case "input4mip":
                prepared_data = prepare_json_input4mip(data, collection_path)
            case _:
                raise ValueError(repo.project)
        print(collection_path)
        print(prepared_data)
        # if isinstance(data, dict):
        #     for collection_key in data.keys():
        #         term_key = parse_terms_in_json(collection_key, data, repo)
        #         if term_key is not None:
        #             result.add(term_key)
        # elif isinstance(data, list):
        #     for collection_key in data:
        #         term_key = parse_terms_in_json(collection_key, data, repo)
        #         if term_key is not None:
        #             result.add(term_key)

        for collection_key, terms in prepared_data.items():
            for term_key in terms.keys():
                result.add(
                    Term(
                        name=term_key,
                        collection=collection_key,
                    )
                )

        else:
            raise TypeError(data)
    return result


@task(log_prints=True)
def send_to_nocodb(repo_working: Repo, repo_reference: Repo):
    API_KEY = Secret.load("nocodb-consistency-api-key").get()
    HEADERS = {"xc-token": API_KEY, "Content-Type": "application/json"}
    BASE_URL = Variable.get("nocodb_base_url")
    TABLE_ID = Variable.get("nocodb_repo_watcher_tables_ids")
    TABLE_URL = f"{BASE_URL}/{TABLE_ID}/records"

    payload = {
        "repository": repo_working.nicename.split("@")[0],
        "working_branch": repo_working.branch,
        "reference_branch": repo_reference.branch,
        "is_verified": False,
        "task_url": flow_run.get_flow_run_ui_url(),
    }

    r_post = requests.post(TABLE_URL, json=payload, headers=HEADERS)
    r_post.raise_for_status()


def format_output(repo1: Repo, repo2: Repo, invalid_legacy_terms: set[Term]) -> str:
    f"Terms in {repo1.nicename} not in {repo2.nicename}:"
    # missing_terms = repo1.terms - repo2.terms

    grouped_terms: defaultdict[Any, list[Any]] = defaultdict(list)
    for term in invalid_legacy_terms:
        grouped_terms[term.collection].append(term.name)

    lines = [f"Terms in {repo1.nicename} not in {repo2.nicename}:"]
    for collection in sorted(grouped_terms.keys()):
        terms = grouped_terms[collection]
        lines.append(f"{collection}:")
        for term_name in sorted(terms):
            lines.append(f"\t- {term_name}")

    return "\n".join(lines)


@task
def config_esgvoc():
    dico = {
        "universe": {
            "github_repo": "https://github.com/WCRP-CMIP/WCRP-universe",
            "branch": "esgvoc",
            "local_path": "repos/WCRP-universe",
            "db_path": "dbs/universe.sqlite",
            "offline_mode": False,
        },
        "projects": [
            ServiceSettings._get_default_project_configs()[proj_id]
            for proj_id in ServiceSettings._get_default_project_configs().keys()
        ],
    }

    config_manager.save_config(
        dico, "coherence"
    )  # save le dico dans la config (toml) puis switch sur cette config
    current_state.synchronize_all()


@flow(log_prints=True)
def are_branch_consistent(repo_working: Repo, repo_reference: Repo):
    logger = get_run_logger()

    config_esgvoc()
    repo_working.pull()
    repo_reference.pull()

    logger.error(repo_reference.cv_location)
    for collection_path in repo_reference.cv_location.glob("*"):
        logger.error(collection_path)
    # logger.error(repo_working)
    # logger.error(repo_reference)

    logger.error("Legacy terms :")
    logger.error("\n".join(f"{term.collection} | {term.name}" for term in repo_reference.terms))

    # logger.error("Esgvoc terms :")
    # logger.error("\n".join(f"{term.collection} | {term.name}" for term in repo_working.terms))

    invalid_legacy_terms = validate_legacy_terms(repo_reference)

    logger.error("Legacy missing terms :")
    logger.error("\n".join(f"{term.collection} | {term.name}" for term in invalid_legacy_terms))

    if repo_working.terms == repo_reference.terms and not invalid_legacy_terms:
        status = Status.consistent
    else:
        logger.error(format_output(repo_reference, repo_working, invalid_legacy_terms))
        status = Status.inconsistent
        send_to_nocodb(repo_working, repo_reference)
