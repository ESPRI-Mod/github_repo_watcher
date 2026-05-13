"""CLI for the repo watcher consistency checker."""

from __future__ import annotations

import warnings

warnings.filterwarnings("ignore", message="Config key.*is set in model_config but will be ignored", category=UserWarning, module="pydantic_settings")
warnings.filterwarnings("ignore", message="EMD Conformance:", category=UserWarning, module="pydantic")

from pathlib import Path
from typing import Annotated, Optional

import typer
import yaml

from comparison import compare_terms
from models import CheckConfig
from sources import EsgvocRegistrySource, GitRepoEmdSource, GitRepoEsgvocFormatSource, GitRepoSource, TermSource

app = typer.Typer(
    name="repo-watcher",
    help="Check consistency between git CV repos and the esgvoc registry.",
)


def _source_label(cfg: dict) -> str:
    if "owner" in cfg:
        return f"{cfg['owner']}/{cfg['repo']}@{cfg['branch']}"
    return f"esgvoc/{cfg['project']}@{cfg.get('version', 'latest')}"


def _build_source(cfg: dict) -> TermSource:
    source_type = cfg["type"]
    if source_type == "git":
        return GitRepoSource(owner=cfg["owner"], repo=cfg["repo"], branch=cfg["branch"])
    if source_type == "git_esgvoc_format":
        return GitRepoEsgvocFormatSource(owner=cfg["owner"], repo=cfg["repo"], branch=cfg["branch"])
    if source_type == "git_emd":
        return GitRepoEmdSource(owner=cfg["owner"], repo=cfg["repo"], branch=cfg["branch"])
    if source_type == "registry":
        return EsgvocRegistrySource(project=cfg["project"], version=cfg.get("version"))
    msg = f"Unknown source type: {source_type}"
    raise typer.BadParameter(msg)


def _run_check(check: CheckConfig) -> None:
    source = _build_source(check.source)
    ref = _build_source(check.reference)

    typer.echo(f"Fetching terms from {source.label()}...")
    source_terms = source.get_terms(
        project=check.project,
        only_collections=check.only_collections,
        skip_collections=check.skip_collections,
        field_mappings=check.field_mappings,
    )
    typer.echo(f"  -> {len(source_terms)} terms")

    typer.echo(f"Fetching terms from {ref.label()}...")
    ref_terms = ref.get_terms(
        project=check.project,
        only_collections=check.only_collections,
        skip_collections=check.skip_collections,
        field_mappings=check.field_mappings,
    )
    typer.echo(f"  -> {len(ref_terms)} terms")

    result = compare_terms(
        source_terms=source_terms,
        reference_terms=ref_terms,
        source_label=source.label(),
        reference_label=ref.label(),
        collection_mappings=check.collection_mappings,
    )

    typer.echo()
    typer.echo(result.format_report())


def _load_checks(config_path: Path) -> list[CheckConfig]:
    with config_path.open() as f:
        raw = yaml.safe_load(f)
    return [CheckConfig(**entry) for entry in raw["checks"]]


@app.command()
def check(
    name: Annotated[str, typer.Argument(help="Name of the check to run from repos_config.yml.")],
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
) -> None:
    """Run a single consistency check by name."""
    checks = _load_checks(config)
    matching = [c for c in checks if c.name == name]
    if not matching:
        available = ", ".join(c.name for c in checks)
        typer.echo(f"Check '{name}' not found. Available: {available}", err=True)
        raise typer.Exit(code=1)
    _run_check(matching[0])


@app.command()
def check_all(
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
) -> None:
    """Run all consistency checks from the config file."""
    checks = _load_checks(config)
    for i, c in enumerate(checks):
        if i > 0:
            typer.echo("\n" + "=" * 60 + "\n")
        typer.echo(typer.style(f"[{c.name}]", bold=True))
        _run_check(c)


@app.command()
def list_checks(
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
) -> None:
    """List all configured checks."""
    checks = _load_checks(config)
    for c in checks:
        src_label = _source_label(c.source)
        ref_label = _source_label(c.reference)
        typer.echo(f"  {c.name}: {src_label} vs {ref_label}")


@app.command()
def git_vs_registry(
    owner: Annotated[str, typer.Argument(help="GitHub owner (e.g. WCRP-CMIP).")],
    repo: Annotated[str, typer.Argument(help="GitHub repo name (e.g. CMIP6_CVs).")],
    project: Annotated[str, typer.Argument(help="esgvoc project id (e.g. cmip6).")],
    branch: Annotated[str, typer.Option(help="Git branch.")] = "main",
    skip: Annotated[Optional[list[str]], typer.Option(help="Collections to skip.")] = None,
    only: Annotated[Optional[list[str]], typer.Option(help="Only check these collections.")] = None,
    collection_mapping: Annotated[Optional[list[str]], typer.Option(help="Collection mapping as key=value.")] = None,
) -> None:
    """Ad-hoc check: compare a git repo branch against the esgvoc registry."""
    mappings = None
    if collection_mapping:
        mappings = {}
        for m in collection_mapping:
            k, v = m.split("=", 1)
            mappings[k] = v

    check_cfg = CheckConfig(
        name="ad-hoc",
        source={"type": "git", "owner": owner, "repo": repo, "branch": branch},
        reference={"type": "registry", "project": project},
        project=project,
        skip_collections=skip,
        only_collections=only,
        collection_mappings=mappings,
    )
    _run_check(check_cfg)


if __name__ == "__main__":
    app()
