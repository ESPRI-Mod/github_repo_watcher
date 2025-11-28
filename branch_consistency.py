from collections import defaultdict
from esgvoc.api.search import MatchingTerm
import json
import shutil
from functools import cached_property
from pathlib import Path
from typing import Any, Self, TypeAlias

from prefect import flow, get_run_logger, task
from prefect.variables import Variable
from prefect.runner.storage import GitRepository
from prefect.runtime import flow_run
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect.blocks.system import Secret
from pydantic import BaseModel, TypeAdapter
import requests

from esgvoc.core.service.state import ServiceSettings
from esgvoc.core.service import config_manager, current_state
import esgvoc.api as ev

LEGACY_EXCLUDED_COLLECTIONS = [
    "DRS",
    "required_global_attributes",
    "global_attrs",
]

ParamStr: TypeAlias = str
ParamDict: TypeAlias = dict[str, Any]
TermKey: TypeAlias = str
TermDict: TypeAlias = dict[TermKey, ParamStr | ParamDict]
CollectionKey: TypeAlias = str
NotCollectionKey: TypeAlias = str

CordexCMIP6InputDict: TypeAlias = dict[CollectionKey, list[TermKey] | TermDict]
CMIP6InputDict: TypeAlias = dict[CollectionKey, list[TermKey] | TermDict]
CMIP6PlusInputDict: TypeAlias = dict[CollectionKey, TermKey | list[TermKey] | TermDict]
Input4MipsInputDict: TypeAlias = list[TermKey] | TermDict | dict[NotCollectionKey, TermDict]

VocabularyDict: TypeAlias = dict[CollectionKey, TermDict]


class Term(BaseModel):
    name: str
    collection: str

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
    project: str

    @property
    def git_repo(self) -> GitRepository:
        result = GitRepository(url=self.url, branch=self.branch)
        result._storage_base_path = Path("/tmp")
        return result

    @cached_property
    def terms(self) -> set[Term]:
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


def validate_typing_cordex_cmip6(data: str) -> CordexCMIP6InputDict:
    return TypeAdapter(CordexCMIP6InputDict).validate_json(data)


def validate_typing_cmip6(data: str) -> CMIP6InputDict:
    return TypeAdapter(CMIP6InputDict).validate_json(data)


def validate_typing_cmip6plus(data: str) -> CMIP6PlusInputDict:
    return TypeAdapter(CMIP6PlusInputDict).validate_json(data)


def validate_typing_input4mip(data: str) -> Input4MipsInputDict:
    return TypeAdapter(Input4MipsInputDict).validate_json(data)


def prepare_json_cordex_cmip6(
    data: CordexCMIP6InputDict,
) -> VocabularyDict:
    result: VocabularyDict = {}
    skipped_collections = ["driving_variant_label"]
    
    for collection_key, terms in data.items():
        if collection_key in skipped_collections:
            continue
        result[collection_key] = {}

        match terms:
            case dict():
                for term_key, term_content in terms.items():
                    result[collection_key][term_key] = term_content
            case list():
                for term_key in terms:
                    result[collection_key][term_key] = {}

    return result


def prepare_json_cmip6(data: CMIP6InputDict) -> VocabularyDict:
    result: VocabularyDict = {}
    for collection_key, terms in data.items():
        if collection_key == "version_metadata":
            continue
        else:
            result[collection_key] = {}

            match terms:
                case dict():
                    for term_key, term_content in terms.items():
                        result[collection_key][term_key] = term_content
                case list():
                    for term_key in terms:
                        result[collection_key][term_key] = {}

    return result


def prepare_json_cmip6plus(data: CMIP6PlusInputDict) -> VocabularyDict:
    result: VocabularyDict = {}
    for collection_key, terms in data.items():
        if collection_key == "Header":
            continue
        else:
            result[collection_key] = {}

            match terms:
                case dict():
                    for term_key, term_content in terms.items():
                        result[collection_key][term_key] = term_content
                case list():
                    for term_key in terms:
                        result[collection_key][term_key] = {}
                case str():
                    result[collection_key][terms] = {}

    return result


def prepare_json_input4mip(
    data: Input4MipsInputDict, collection_path: Path
) -> VocabularyDict:
    result: VocabularyDict = {}
    collection = collection_path.stem.removeprefix("input4MIPs_")
    result[collection] = {}
    
    match data:
        case dict() if collection == "target_mip":
            for target_mips in data.values():
                for target_mip, target_mip_params in target_mips.items():
                    result["target_mip"][target_mip] = target_mip_params
        case dict():
            for term_key, term_content in data.items():
                result[collection][term_key] = term_content
        case list():
            for term_key in data:
                result[collection][term_key] = {}

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

        match repo.project:
            case "cordex-cmip6":
                prepared_data = prepare_json_cordex_cmip6(validate_typing_cordex_cmip6(contents))
            case "cmip6":
                prepared_data = prepare_json_cmip6(validate_typing_cmip6(contents))
            case "cmip6plus":
                prepared_data = prepare_json_cmip6plus(validate_typing_cmip6plus(contents))
            case "input4mip":
                prepared_data = prepare_json_input4mip(validate_typing_input4mip(contents), collection_path)
            case _:
                raise ValueError(repo.project)

        for collection_key, terms in prepared_data.items():
            for term_key in terms.keys():
                result.add(
                    Term(
                        name=term_key,
                        collection=collection_key,
                    )
                )

    return result


@task(log_prints=True)
def validate_legacy_terms(terms: set[Term], project: str) -> tuple[set[Term], set[Term]]:
    valid_terms: set[Term] = set()
    invalid_terms: set[Term] = set()
    for term in terms:
        if project == "cordex-cmip6" and term.collection == "table_id":
            term.collection = "frequency"
        matching_term: list[MatchingTerm] = ev.valid_term_in_collection(
            term.name, project, term.collection.lower()
        )
        if matching_term:
            term.name = matching_term[0].term_id
            valid_terms.add(term)
        else:
            invalid_terms.add(term)
    return invalid_terms, valid_terms


@task(log_prints=True)
def send_to_nocodb(repo: Repo):
    API_KEY = Secret.load("nocodb-consistency-api-key").get()
    HEADERS = {"xc-token": API_KEY, "Content-Type": "application/json"}
    BASE_URL = Variable.get("nocodb_base_url")
    TABLE_ID = Variable.get("nocodb_repo_watcher_tables_ids")
    TABLE_URL = f"{BASE_URL}/{TABLE_ID}/records"

    payload = {
        "repository": repo.nicename.split("@")[0],
        "reference_branch": repo.branch,
        "is_verified": False,
        "task_url": flow_run.get_flow_run_ui_url(),
    }

    r_post = requests.post(TABLE_URL, json=payload, headers=HEADERS)
    r_post.raise_for_status()


def format_output(repo: Repo, invalid_legacy_terms: set[Term]) -> str:
    f"Terms in {repo.nicename} not found in esgvoc:"

    grouped_terms: defaultdict[Any, list[Any]] = defaultdict(list)
    for term in invalid_legacy_terms:
        grouped_terms[term.collection].append(term.name)

    lines = [f"Terms in {repo.nicename} not found in esgvoc:"]
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
def are_branch_consistent(project_repo: Repo):
    logger = get_run_logger()
    config_esgvoc()
    
    project_repo.pull()

    invalid_legacy_terms, valid_terms = validate_legacy_terms(project_repo.terms, project_repo.project)

    if invalid_legacy_terms:
        logger.error(format_output(project_repo, invalid_legacy_terms))
        send_to_nocodb(project_repo)
