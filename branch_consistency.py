import json
import shutil
from enum import Enum, auto
from functools import cached_property
from pathlib import Path
from typing import Self

from prefect.runner.storage import GitRepository
from prefect.utilities.asyncutils import run_coro_as_sync
from pydantic import BaseModel


class Status(Enum):
    inconsistent = auto()
    consistent = auto()


class Form(Enum):
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

    def pull(self) -> None:
        if self.destination.is_dir():
            shutil.rmtree(self.destination)
        run_coro_as_sync(self.git_repo.pull_code())

    @property
    def nicename(self) -> str:
        return f"{self.url}@{self.branch}"


@task
def parse_esgvoc_terms(repo: Repo) -> set[Term]:
    result = set()
    for term_path in repo.destination.glob("*/*.json"):
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


@task
def parse_legacy_terms(repo: Repo) -> set[Term]:
    result = set()
    for collection_path in repo.destination.glob("*.json"):
        if "global_attrs" in collection_path.name:
            continue
        contents = collection_path.read_text()
        data = json.loads(contents)
        for collection in data.keys():
            if collection in ["Header", "version_metadata"]:
                continue
            term_data = data[collection]
            if isinstance(term_data, list):
                term_keys = [term.replace(" ", "") for term in term_data]
            elif isinstance(term_data, dict):
                term_keys = list(term_data.keys())
            elif isinstance(term_data, str):
                term_keys = [term_data]
            else:
                raise TypeError(term_data)
            for term_key in term_keys:
                result.add(
                    Term(
                        name=term_key,
                        collection=collection,
                    ),
                )
    return result


@flow
def are_branch_consistent(repoA: Repo, repoB: Repo) -> Status:
    repoA.pull()
    repoB.pull()
    if repoA.terms == repoB.terms:
        return Status.consistent
    logger.error(f"Terms in {repoA.nicename} not in {repoB.nicename}: {repoA.terms - repoB.terms}")
    logger.error(f"Terms in {repoB.nicename} not in {repoA.nicename}: {repoB.terms - repoA.terms}")
    return Status.inconsistent
