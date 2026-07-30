"""CLI for the repo watcher consistency checker."""

from __future__ import annotations

import warnings

warnings.filterwarnings(
    "ignore",
    message="Config key.*is set in model_config but will be ignored",
    category=UserWarning,
    module="pydantic_settings",
)
warnings.filterwarnings("ignore", message="EMD Conformance:", category=UserWarning, module="pydantic")

import json
from pathlib import Path
from typing import Annotated, Optional

import typer

from repo_watcher.comparison import apply_term_id_transforms, apply_term_overrides, compare_terms
from repo_watcher.config import CheckConfig, load_checks
from repo_watcher.domain import ConsistencyResult, ProjectResult, ReportMode
from repo_watcher.generation import collect_and_generate, write_report
from repo_watcher.sources import build_source

app = typer.Typer(
    name="repo-watcher",
    help="Check consistency between git CV repos and the esgvoc registry.",
)


def _source_label(cfg: dict) -> str:
    if "owner" in cfg:
        return f"{cfg['owner']}/{cfg['repo']}@{cfg['branch']}"
    return f"esgvoc/{cfg['project']}@{cfg.get('version', 'latest')}"


def _run_checks(
    checks: list[CheckConfig],
    mode: ReportMode = ReportMode.missing_in_ref,
) -> list[tuple[str, ConsistencyResult]]:
    """Shared fetch-transform-compare logic for all CLI commands.

    Returns a list of (source_label, ConsistencyResult) tuples.
    """
    results: list[tuple[str, ConsistencyResult]] = []
    for chk in checks:
        source = build_source(chk.source)
        ref = build_source(chk.reference)

        typer.echo(f"[{chk.name}] Fetching {source.label()}...")
        source_terms = source.get_terms(
            project=chk.project,
            only_collections=chk.only_collections,
            skip_collections=chk.skip_collections,
            field_mappings=chk.field_mappings,
        )
        typer.echo(f"  -> {len(source_terms)} terms")

        if chk.term_id_transforms:
            source_terms = apply_term_id_transforms(source_terms, chk.term_id_transforms)
        if chk.term_overrides:
            source_terms = apply_term_overrides(source_terms, chk.term_overrides)

        typer.echo(f"[{chk.name}] Fetching {ref.label()}...")
        ref_terms = ref.get_terms(
            project=chk.project,
            only_collections=chk.only_collections,
            skip_collections=chk.skip_collections,
            field_mappings=chk.field_mappings,
        )
        typer.echo(f"  -> {len(ref_terms)} terms")

        result = compare_terms(
            source_terms=source_terms,
            reference_terms=ref_terms,
            source_label=source.label(),
            reference_label=ref.label(),
            collection_mappings=chk.collection_mappings,
            exclude_terms=chk.exclude_terms,
        )
        results.append((source.label(), result))
    return results


@app.command()
def check(
    name: Annotated[str, typer.Argument(help="Name of the check to run from repos_config.yml.")],
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
    mode: Annotated[
        ReportMode, typer.Option(help="Report mode: missing_in_ref (default), missing_in_src, or both.")
    ] = ReportMode.missing_in_ref,
) -> None:
    """Run a single consistency check by name."""
    checks = load_checks(config)
    matching = [c for c in checks if c.name == name]
    if not matching:
        available = ", ".join(c.name for c in checks)
        typer.echo(f"Check '{name}' not found. Available: {available}", err=True)
        raise typer.Exit(code=1)
    results = _run_checks(matching, mode=mode)
    typer.echo()
    typer.echo(results[0][1].format_report(mode=mode))


@app.command()
def check_all(
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
    mode: Annotated[
        ReportMode, typer.Option(help="Report mode: missing_in_ref (default), missing_in_src, or both.")
    ] = ReportMode.missing_in_ref,
) -> None:
    """Run all consistency checks from the config file."""
    checks = load_checks(config)
    for i, c in enumerate(checks):
        if i > 0:
            typer.echo("\n" + "=" * 60 + "\n")
        typer.echo(typer.style(f"[{c.name}]", bold=True))
        results = _run_checks([c], mode=mode)
        typer.echo()
        typer.echo(results[0][1].format_report(mode=mode))


@app.command()
def list_checks(
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
) -> None:
    """List all configured checks."""
    checks = load_checks(config)
    for c in checks:
        src_label = _source_label(c.source)
        ref_label = _source_label(c.reference)
        typer.echo(f"  {c.name}: {src_label} vs {ref_label}")


@app.command()
def project(
    project_name: Annotated[
        str, typer.Argument(help="Project id to check (e.g. cmip7). Runs all checks matching this project.")
    ],
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
    mode: Annotated[
        ReportMode, typer.Option(help="Report mode: missing_in_ref (default), missing_in_src, or both.")
    ] = ReportMode.missing_in_ref,
    detail: Annotated[bool, typer.Option("--detail", help="Show per-source breakdown.")] = False,
) -> None:
    """Run all checks for a project and show a unified report."""
    checks = load_checks(config)
    matching = [c for c in checks if c.project == project_name]
    if not matching:
        available = sorted({c.project for c in checks})
        typer.echo(f"No checks for project '{project_name}'. Available: {', '.join(available)}", err=True)
        raise typer.Exit(code=1)

    ref_label = _source_label(matching[0].reference)
    proj_result = ProjectResult(project=project_name, reference_label=ref_label)
    proj_result.check_results = _run_checks(matching, mode=mode)

    typer.echo()
    typer.echo(proj_result.format_report(mode=mode, detail=detail))


@app.command()
def generate_missing(
    project_name: Annotated[str, typer.Argument(help="Project id (e.g. cmip7).")],
    universe_repo: Annotated[Path, typer.Option(help="Path to local WCRP-universe repo.")] = Path("../WCRP-universe"),
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
    collections: Annotated[Optional[list[str]], typer.Option(help="Only generate for these collections.")] = None,
    write_project: Annotated[
        Optional[Path],
        typer.Option("--write-project", help="Write project link files to this local project repo path."),
    ] = None,
    write_universe: Annotated[
        Optional[Path],
        typer.Option("--write-universe", help="Write universe term files to this local universe repo path."),
    ] = None,
) -> None:
    """Show what term files would need to be created in universe and project repos."""
    if not universe_repo.is_dir():
        typer.echo(f"Universe repo not found: {universe_repo}", err=True)
        raise typer.Exit(code=1)
    if write_project is not None and not write_project.is_dir():
        typer.echo(f"Project write target not found: {write_project}", err=True)
        raise typer.Exit(code=1)
    if write_universe is not None and not write_universe.is_dir():
        typer.echo(f"Universe write target not found: {write_universe}", err=True)
        raise typer.Exit(code=1)

    checks = load_checks(config)
    matching = [c for c in checks if c.project == project_name]
    if not matching:
        available = sorted({c.project for c in checks})
        typer.echo(f"No checks for project '{project_name}'. Available: {', '.join(available)}", err=True)
        raise typer.Exit(code=1)

    report = collect_and_generate(
        checks=matching,
        universe_repo=universe_repo,
        project_name=project_name,
        collections_filter=collections,
        on_progress=typer.echo,
    )

    typer.echo()
    if not report.has_work:
        typer.echo(typer.style(report.format_report(), fg=typer.colors.GREEN))
    else:
        typer.echo(report.format_report())

    if write_universe is not None or write_project is not None:
        write_report(
            report,
            universe_repo=write_universe,
            project_repo=write_project,
        )
        parts = []
        if write_universe is not None and report.universe_to_create:
            parts.append(f"{len(report.universe_to_create)} universe file(s) created in {write_universe.resolve()}")
        if write_universe is not None and report.universe_to_update:
            parts.append(f"{len(report.universe_to_update)} universe file(s) updated in {write_universe.resolve()}")
        if write_project is not None and report.project_to_create:
            parts.append(f"{len(report.project_to_create)} project file(s) created in {write_project.resolve()}")
        if parts:
            typer.echo(f"\nWritten: {', '.join(parts)}. Review the generated skeletons and fill in placeholder values.")


@app.command()
def git_vs_registry(
    owner: Annotated[str, typer.Argument(help="GitHub owner (e.g. WCRP-CMIP).")],
    repo: Annotated[str, typer.Argument(help="GitHub repo name (e.g. CMIP6_CVs).")],
    project_id: Annotated[str, typer.Argument(help="esgvoc project id (e.g. cmip6).")],
    branch: Annotated[str, typer.Option(help="Git branch.")] = "main",
    skip: Annotated[Optional[list[str]], typer.Option(help="Collections to skip.")] = None,
    only: Annotated[Optional[list[str]], typer.Option(help="Only check these collections.")] = None,
    collection_mapping: Annotated[Optional[list[str]], typer.Option(help="Collection mapping as key=value.")] = None,
    mode: Annotated[
        ReportMode, typer.Option(help="Report mode: missing_in_ref (default), missing_in_src, or both.")
    ] = ReportMode.missing_in_ref,
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
        reference={"type": "registry", "project": project_id},
        project=project_id,
        skip_collections=skip,
        only_collections=only,
        collection_mappings=mappings,
    )
    results = _run_checks([check_cfg], mode=mode)
    typer.echo()
    typer.echo(results[0][1].format_report(mode=mode))


@app.command()
def list_projects(
    config: Annotated[Path, typer.Option(help="Path to config file.")] = Path("repos_config.yml"),
) -> None:
    """List distinct projects with their repo info as JSON (for CI matrix)."""
    checks = load_checks(config)
    seen: dict[str, dict] = {}
    for c in checks:
        if c.project in seen:
            continue
        src = c.source
        if "owner" in src:
            seen[c.project] = {
                "project": c.project,
                "owner": src["owner"],
                "repo": src["repo"],
            }
    typer.echo(json.dumps(list(seen.values())))


if __name__ == "__main__":
    app()
