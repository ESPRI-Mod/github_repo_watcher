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
from esgvoc.core import service
import esgvoc.api as ev

LEGACY_EXCLUDED_COLLECTIONS = [
    "DRS",
    "required_global_attributes",
    "global_attrs",
]

# Type aliases for better readability
ParamStr: TypeAlias = str
ParamDict: TypeAlias = dict[str, Any]
TermKey: TypeAlias = str
TermDict: TypeAlias = dict[TermKey, ParamStr | ParamDict]
CollectionKey: TypeAlias = str
NotCollectionKey: TypeAlias = str

# Input types for validating the json files
CordexCMIP6InputDict: TypeAlias = dict[CollectionKey, list[TermKey] | TermDict]
CMIP6InputDict: TypeAlias = dict[CollectionKey, list[TermKey] | TermDict]
CMIP6PlusInputDict: TypeAlias = dict[CollectionKey, TermKey | list[TermKey] | TermDict]
Input4MipsInputDict: TypeAlias = list[TermKey] | TermDict | dict[NotCollectionKey, TermDict]

# Output type after preparing the json files
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
    """
    Validate the type of the input data for CORDEX-CMIP6.
    
    Parameters
    ----------
    data : str
        The input data as a JSON string.
    
    Returns
    -------
    CordexCMIP6InputDict
        The validated input data.
    """

    return TypeAdapter(CordexCMIP6InputDict).validate_json(data)


def validate_typing_cmip6(data: str) -> CMIP6InputDict:
    """
    Validate the type of the input data for CMIP6.
    
    Parameters
    ----------
    data : str
        The input data as a JSON string.
    
    Returns
    -------
    CMIP6InputDict
        The validated input data.
    """

    return TypeAdapter(CMIP6InputDict).validate_json(data)


def validate_typing_cmip6plus(data: str) -> CMIP6PlusInputDict:
    """
    Validate the type of the input data for CMIP6Plus.
    
    Parameters
    ----------
    data : str
        The input data as a JSON string.
    
    Returns
    -------
    CMIP6PlusInputDict
        The validated input data.
    """

    return TypeAdapter(CMIP6PlusInputDict).validate_json(data)


def validate_typing_input4mip(data: str) -> Input4MipsInputDict:
    """
    Validate the type of the input data for Input4Mips.
    
    Parameters
    ----------
    data : str
        The input data as a JSON string.
    
    Returns
    -------
    Input4MipsInputDict
        The validated input data.
    """

    return TypeAdapter(Input4MipsInputDict).validate_json(data)


def prepare_json_cordex_cmip6(
    data: CordexCMIP6InputDict,
) -> VocabularyDict:
    """
    Prepare the JSON data from CORDEX-CMIP6 for term extraction.

    Parameters
    ----------
    data : CordexCMIP6InputDict
        The input data to prepare.
    
    Returns
    -------
    VocabularyDict
        The prepared vocabulary dictionary.
    """

    result: VocabularyDict = {}
    skipped_collections = ["driving_variant_label"]
    
    for collection_key, terms in data.items():
        # Ensure the CV path doesn't contain excluded collections
        if collection_key in skipped_collections:
            continue
        result[collection_key] = {}

        # Handle both dict and list cases
        match terms:
            case dict():
                for term_key, term_content in terms.items():
                    result[collection_key][term_key] = term_content
            case list():
                for term_key in terms:
                    result[collection_key][term_key] = {}

    return result


def prepare_json_cmip6(data: CMIP6InputDict) -> VocabularyDict:
    """
    Prepare the JSON data from CMIP6 for term extraction.

    Parameters
    ----------
    data : CMIP6InputDict
        The input data to prepare.
    
    Returns
    -------
    VocabularyDict
        The prepared vocabulary dictionary.
    """

    result: VocabularyDict = {}
    skipped_collections = ["version_metadata"]
    for collection_key, terms in data.items():
        # Ensure the CV path doesn't contain excluded collections
        if collection_key in skipped_collections:
            continue
        result[collection_key] = {}

        # Handle both dict and list cases
        match terms:
            case dict():
                for term_key, term_content in terms.items():
                    result[collection_key][term_key] = term_content
            case list():
                for term_key in terms:
                    result[collection_key][term_key] = {}

    return result


def prepare_json_cmip6plus(data: CMIP6PlusInputDict) -> VocabularyDict:
    """
    Prepare the JSON data from CMIP6Plus for term extraction.

    Parameters
    ----------
    data : CMIP6PlusInputDict
        The input data to prepare.
    
    Returns
    -------
    VocabularyDict
        The prepared vocabulary dictionary.
    """
    
    result: VocabularyDict = {}
    skipped_collections = ["Header"]

    for collection_key, terms in data.items():
        # Ensure the CV path doesn't contain excluded collections
        if collection_key in skipped_collections:
            continue
        else:
            result[collection_key] = {}

            # Handle all dict, list and str cases
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
    """
    Prepare the JSON data from Input4Mips for term extraction.

    Parameters
    ----------
    data : Input4MipsInputDict
        The input data to prepare.
    
    Returns
    -------
    VocabularyDict
        The prepared vocabulary dictionary.
    """
    
    result: VocabularyDict = {}
    # Extract collection name from file path
    collection = collection_path.stem.removeprefix("input4MIPs_")
    result[collection] = {}
    
    # Handle dict, target_mip and list cases
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
    """
    Parse the legacy terms from the repository's CV location.

    Parameters
    ----------
    repo : Repo
        The repository to parse.
    
    Returns
    -------
    set[Term]
        A set of Terms found in the legacy branch.
    """

    result: set[Term] = set()

    for collection_path in repo.cv_location.glob("*.json"):
        # Ensure the CV path doesn't contain excluded collections
        if any(
            collection in collection_path.name
            for collection in LEGACY_EXCLUDED_COLLECTIONS
        ):
            continue

        # Extract the contents of the JSON file
        contents = collection_path.read_text()

        # Prepare the data based on the project type
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

        # Extract Terms from the prepared data
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
    """
    Validate terms from the legacy branch using esgvoc.

    Parameters
    ----------
    terms : set[Term]
        A set of Terms to validate.
    project : str
        The project name.
    
    Returns
    -------
    invalid_terms : set[Term]
        A set of Terms not found in esgvoc.
    valid_terms : set[Term]
        A set of Terms found in esgvoc.
    """

    valid_terms: set[Term] = set()
    invalid_terms: set[Term] = set()
    for term in terms:
        # Special case for CORDEX-CMIP6 table_id collection
        if project == "cordex-cmip6" and term.collection == "table_id":
            term.collection = "frequency"
        # Search for the term in esgvoc
        matching_term: list[MatchingTerm] = ev.valid_term_in_collection(
            term.name, project, term.collection.lower()
        )

        # Classify the term based on search results
        if matching_term:
            term.name = matching_term[0].term_id
            valid_terms.add(term)
        else:
            invalid_terms.add(term)
    return invalid_terms, valid_terms


@task(log_prints=True)
def send_to_nocodb(repo: Repo):
    """
    Send a record to NocoDB indicating that the repository has inconsistencies.

    Parameters
    ----------
    repo : Repo
        The repository with inconsistencies.
    """

    # Prepare NocoDB API request
    API_KEY = Secret.load("nocodb-consistency-api-key").get()
    HEADERS = {"xc-token": API_KEY, "Content-Type": "application/json"}
    BASE_URL = Variable.get("nocodb_base_url")
    TABLE_ID = Variable.get("nocodb_repo_watcher_tables_ids")
    TABLE_URL = f"{BASE_URL}/{TABLE_ID}/records"

    # Prepare the payload
    payload = {
        "repository": repo.nicename.split("@")[0],
        "reference_branch": repo.branch,
        "is_verified": False,
        "task_url": flow_run.get_flow_run_ui_url(),
    }

    # Send the POST request to NocoDB
    r_post = requests.post(TABLE_URL, json=payload, headers=HEADERS)
    r_post.raise_for_status()


def format_output(repo: Repo, invalid_legacy_terms: set[Term]) -> str:
    """
    Format the output message for invalid legacy terms.

    Parameters
    ----------
    repo : Repo
        The repository with invalid terms.
    invalid_legacy_terms : set[Term]
        A set of invalid Terms.
    
    Returns
    -------
    str
        The formatted output message.
    """

    # Group invalid terms by collection
    grouped_terms: defaultdict[Any, list[Any]] = defaultdict(list)
    for term in invalid_legacy_terms:
        grouped_terms[term.collection].append(term.name)

    # Prepare the output message
    lines = [f"Terms in {repo.nicename} not found in esgvoc:"]
    for collection in sorted(grouped_terms.keys()):
        terms = grouped_terms[collection]
        lines.append(f"{collection}:")
        for term_name in sorted(terms):
            lines.append(f"\t- {term_name}")

    return "\n".join(lines)


@task
def config_esgvoc():
    """
    Configure esgvoc with default project and universe repositories.
    """

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

    service.config_manager.save_config(
        dico, "coherence"
    )  # save le dico dans la config (toml) puis switch sur cette config
    service.current_state = service.get_state()
    service.current_state.synchronize_all()


@task
def config_custom_esgvoc():
    """
    Configure esgvoc with custom project and universe repositories for development.
    """
    
    dico = {
        "projects": [
            {
                "project_name": "cordex-cmip6",
                "github_repo": "https://github.com/WCRP-CORDEX/cordex-cmip6-cv",
                "branch": "esgvoc_dev",
                "local_path": "repos/cordex-cmip6-cv",
                "db_path": "dbs/cmip6.sqlite",
                "offline_mode": False,
            },
            {
                "project_name": "cmip6",
                "github_repo": "https://github.com/WCRP-CMIP/CMIP6_CVs",
                "branch": "esgvoc_dev",
                "local_path": "repos/CMIP6_CVs",
                "db_path": "dbs/cmip6.sqlite",
                "offline_mode": False,
            },
            {
                "project_name": "cmip6plus",
                "github_repo": "https://github.com/WCRP-CMIP/CMIP6Plus_CVs",
                "branch": "esgvoc_dev",
                "local_path": "repos/CMIP6Plus_CVs",
                "db_path": "dbs/cmip6plus.sqlite",
                "offline_mode": False,
            },
            {
                "project_name": "input4mip",
                "github_repo": "https://github.com/PCMDI/input4MIPs_CVs",
                "branch": "esgvoc_dev",
                "local_path": "repos/input4MIPs_CVs",
                "db_path": "dbs/input4mip.sqlite",
                "offline_mode": False,
            }
        ],
        "universe": {
            "github_repo": "https://github.com/WCRP-CMIP/WCRP-universe",
            "branch": "esgvoc_dev",
            "local_path": "repos/WCRP-universe",
            "db_path": "dbs/universe.sqlite",
        },
    }
    service.config_manager.save_config(
        dico, "coherence_dev"
    )  # save le dico dans la config (toml) puis switch sur cette config
    service.current_state = service.get_state()
    service.current_state.synchronize_all()



@flow(log_prints=True)
def are_branch_consistent(project_repo: Repo, dev: bool):
    """
    Check the consistency of legacy terms in a repository against esgvoc.

    Parameters
    ----------
    project_repo : Repo
        The repository to check.
    dev : bool
        Whether to use the development configuration for esgvoc.
    """

    logger = get_run_logger()

    # Ensure that esgvoc is configured and up to date
    if not dev:
        config_esgvoc()
    else:
        config_custom_esgvoc()
    
    # Pull the latest changes from the repository
    project_repo.pull()

    # Validate legacy terms using esgvoc
    invalid_legacy_terms, valid_terms = validate_legacy_terms(project_repo.terms, project_repo.project)

    # Log and report invalid terms in NocoDB if any were found
    if invalid_legacy_terms:
        logger.error(format_output(project_repo, invalid_legacy_terms))
        send_to_nocodb(project_repo)
