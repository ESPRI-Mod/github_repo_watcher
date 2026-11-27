# GitHub Repo Watcher

Consistency checker for WCRP climate vocabulary repositories against the [esgvoc](https://github.com/ESPRI-Mod/esgvoc) registry.

Automatically detects when a git branch of a controlled vocabulary (CV) repo diverges from the official esgvoc registry, reports the differences, and can scaffold missing term definitions.

## Supported Projects

| Project | Source Format | Description |
|---------|-------------|-------------|
| CORDEX-CMIP6 | Legacy JSON CVs | Regional climate model vocabularies |
| CMIP6 | Legacy JSON CVs | Climate Model Intercomparison Project Phase 6 |
| CMIP6Plus | Legacy JSON CVs | Extended CMIP6 vocabularies |
| CMIP7 | esgvoc directory format | Next-generation CMIP vocabularies |
| input4MIPs | Legacy JSON CVs | Forcing datasets for climate models |
| EMD | Field-level JSON | Essential Model Documentation (field-level checks) |
| Obs4REF | esgvoc directory format | Observations reference datasets |

## Architecture

The codebase is organized into self-documenting packages under `src/repo_watcher/`:

```
repo_watcher/
├── domain.py              Core data types (Term, ReportMode, ConsistencyResult)
├── comparison.py          Pure set operations to find diffs
├── config/                Configuration schema and YAML loading
│   ├── schema.py          Dataclass definitions (CheckConfig, SourceConfig, …)
│   └── loader.py          Load and merge checks from repos_config.yml
├── sources/               Fetch terms from git repos or esgvoc registry
│   ├── base.py            TermSource protocol
│   ├── cloner.py          Shallow-clone helper
│   ├── git.py             GitSource (legacy JSON, esgvoc-format, EMD)
│   └── registry.py        EsgvocRegistrySource (API queries)
├── extraction/            Parse CV files into Term objects
│   ├── standard.py        Legacy JSON CVs (CMIP6, CMIP6Plus, input4MIPs, CORDEX)
│   ├── esgvoc_format.py   esgvoc directory layout (one JSON per term)
│   ├── emd.py             EMD field-level extraction
│   ├── formats.py         Format detection and typing validation
│   └── term_data.py       Extract raw term data for generation prefill
├── generation/            Scaffold missing JSON files
│   ├── pipeline.py        Top-level collect_and_generate entry point
│   ├── planner.py         Decide what to generate (universe vs project)
│   ├── skeleton.py        Build universe and project-link JSON skeletons
│   ├── enrichment.py      Enrich skeletons with CV data and defaults
│   ├── prefill.py         Auto-prefill from source CV fields
│   ├── context.py         Detect collection mappings from 000_context.jsonld
│   ├── multi_step.py      Composite terms (e.g. institution_id → institution + organisation)
│   ├── report.py          GenerationReport data model
│   └── writer.py          Write report to disk as JSON
├── orchestration/         Prefect flows and NocoDB reporting
│   ├── flows.py           check_consistency Prefect flow
│   └── nocodb.py          NocoDB API client
├── cli/                   Typer CLI application
│   └── app.py             Commands: check, project, generate-missing, …
└── ror.py                 ROR API lookups for institution metadata
```

The core data model is a frozen `Term` dataclass (`domain.py`) with case-insensitive equality, enabling set-based comparison.

## Installation

Requires Python 3.12+.

```bash
uv sync
```

## CLI Usage

```bash
# List all configured checks
uv run check list-checks

# Run a single check by name
uv run check check cordex-cmip6-consistency

# Run all checks
uv run check check-all

# Run all checks for a project with unified report
uv run check project cordex-cmip6
uv run check project cordex-cmip6 --detail --mode both

# Ad-hoc comparison of any git repo branch against the registry
uv run check git-vs-registry WCRP-CMIP CMIP6_CVs cmip6

# Generate missing term skeletons
uv run check generate-missing cordex-cmip6 --universe-repo ../WCRP-universe
uv run check generate-missing cordex-cmip6 --universe-repo ../WCRP-universe --write ../cordex-cmip6-cv
```

### Common Options

| Option | Description |
|--------|-------------|
| `--config PATH` | Path to config file (default: `repos_config.yml`) |
| `--mode` | Report mode: `missing_in_ref` (default), `missing_in_src`, or `both` |
| `--detail` | Show per-source breakdown (project command only) |

## Configuration

All checks are defined in `repos_config.yml`. Each check compares a **source** against a **reference** and belongs to a **project**.

### Source Types

| Type | Description | Required Fields |
|------|-------------|-----------------|
| `git` | Clone a GitHub repo, parse legacy JSON CVs | `owner`, `repo`, `branch` |
| `git_esgvoc_format` | Clone a repo using esgvoc directory layout (one dir/collection, one JSON/term) | `owner`, `repo`, `branch` |
| `git_emd` | Clone the EMD repo, extract field-level terms | `owner`, `repo`, `branch` |
| `registry` | Query the esgvoc registry API | `project`, optionally `version` |

### Basic Check Options

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Unique check identifier |
| `source` | `SourceConfig` | Where to fetch terms from |
| `reference` | `SourceConfig` | What to compare against |
| `project` | `str` | esgvoc project id (e.g. `cordex-cmip6`, `cmip7`) |
| `skip_collections` | `list[str]` | Collections to exclude from the check |
| `only_collections` | `list[str]` | If set, only check these collections |
| `collection_mappings` | `dict[str, str]` | Rename source collections before comparing (e.g. `table_id: frequency`) |
| `exclude_terms` | `dict[str, list[str]]` | Regex patterns per collection to exclude from the diff |

### Field Mappings (EMD-style checks)

Used for field-level comparison where individual term fields (not just term existence) are checked.

```yaml
field_mappings:
  model:                          # source collection name
    target_collection: source     # registry collection to compare against
    fields:
      validation_key: drs_name    # source_field: target_field
      description: description
```

| Field | Type | Description |
|-------|------|-------------|
| `target_collection` | `str` | Registry collection to compare against |
| `fields` | `dict[str, str]` | Maps source field names to target field names |

### Term ID Transforms

String replacements applied to term names before comparison. Useful for normalizing formatting differences between source and registry.

```yaml
term_id_transforms:
  nominal_resolution:
    - pattern: " "          # find this substring
      replace: ""           # replace with this
```

| Field | Type | Description |
|-------|------|-------------|
| `pattern` | `str` | Substring to find in term IDs |
| `replace` | `str` | Replacement string |

## Generation-Specific Configuration

These options control how `generate-missing` scaffolds JSON files for terms missing from the registry. They are set per-check in `repos_config.yml` and merged across all checks for a given project.

### `universe_type_mappings`

Maps collection names to universe descriptor types. Used when the collection name doesn't match the universe descriptor (e.g. `source_id` maps to `source` in the universe). Overrides auto-detection from `000_context.jsonld` files and the esgvoc registry.

```yaml
universe_type_mappings:
  source_id: source                    # collection "source_id" uses universe type "source"
  version_realization: variant_label   # collection "version_realization" uses "variant_label"
```

### `generation_defaults`

Default field values applied to generated universe term skeletons when a field is `null`. Useful for setting common defaults across all terms in a collection.

```yaml
generation_defaults:
  grid_label:
    region: "global"     # all generated grid_label terms get region="global"
```

### `term_overrides`

Handles special terms that don't follow the standard 1:1 universe mapping. Instead of creating a new universe term, they reuse an existing one with project-specific field overrides (e.g. a regex pattern).

Each override is keyed by a pattern matching the term name in the source CV.

```yaml
term_overrides:
  tracking_id:                              # collection name
    "hdl:21.14103/.*":                      # term name pattern from source
      universe_term: prefuuid               # reuse this existing universe term
      fields:                               # project-level field overrides
        regex: "^hdl:21\\.14103/[a-fA-F0-9]{8}-..."
  version_realization:
    "v[1-9]...":
      universe_term: vr                     # target universe term id
      create_universe: true                 # create the universe term if missing
      fields:
        regex: "^v\\d+-r\\d+$"
        description: "CORDEX version-realization label"
```

| Field | Type | Description |
|-------|------|-------------|
| `universe_term` | `str` (required) | ID of the universe term to link to or create |
| `fields` | `dict[str, str]` | Field overrides for the project-level JSON (e.g. custom regex) |
| `create_universe` | `bool` | If `true`, scaffold a new universe term when it doesn't exist |

### `multi_step_terms`

Handles composite term IDs that expand into multiple universe entries. For example, `institution_id: "IUM-FDU"` becomes:

1. `institution/ium.json` + `institution/fdu.json` (member terms)
2. `organisation/ium-fdu.json` with `members: ["ium", "fdu"]` (group term)
3. `institution_id/ium-fdu.json` project link pointing to the organisation

```yaml
multi_step_terms:
  institution_id:
    separator: "-"                    # split term_id by this character
    member_descriptor: institution    # universe type for each atomic part
    group_descriptor: organisation    # universe type for the combined group
```

| Field | Type | Description |
|-------|------|-------------|
| `separator` | `str` | Character to split the term ID into atomic members |
| `member_descriptor` | `str` (required) | Universe type for each split part |
| `group_descriptor` | `str` (required) | Universe type for the wrapper/group term |

### `source_prefill`

Maps source CV fields to universe term fields for pre-filling generated skeletons with real data from the community CV. This avoids generating empty placeholders when the source already has the information.

```yaml
source_prefill:
  source_id:                         # collection name
    source_id: drs_name              # source_field: target_universe_field
    label: label
    label_extended: label_extended
    release_year: release_year
```

### `metadata_profile`

Runs a project-specific metadata sync after the generic generation step. It keeps metadata in sync that the generic term comparison does not cover, such as the exact global attribute strings of a project's conventional CV. Changes are merged field by field into existing project terms and contexts. Universe content is only filled or extended, never removed; cases needing a manual decision are listed as notices in the report. With `--summary-file`, `generate-missing` writes a JSON summary of which step changed which repo, plus the notices. The CI uses it for PR titles and bodies.

Currently only the `cordex_cmip6` profile is available.

```yaml
metadata_profile: cordex_cmip6
```

#### `cordex_cmip6`

Reads the conventional CORDEX-CMIP6 CV (the `git` check on `main`) and plans changes for the `esgvoc_dev` project branch and Universe. It follows the same rules as the project's own `_scripts/generate_cc6_cv_additional_collection_entries.py`, so both produce the same project changes.

| Project collection | Field | Rule |
|--------------------|-------|------|
| `source_id` | `source`, `source_type`, `license` | Project only |
| `source_id` | `activity_participation` | Appended to Universe (union across projects, never reduced); in the project only if it differs from Universe |
| `driving_source_id` | `driving_source` | Project only |
| `driving_source_id` | `driving_experiment_id` | Project only; omitted when unrestricted |
| `institution_id` | `institution` | Project only |
| `domain_id` | `description` | Filled in Universe where empty; in the project only if it differs from Universe |
| `driving_experiment_id` | `experiment` | Filled in Universe where empty; in the project only if it differs from Universe |

Copies of shared fields already in the project are kept when identical to Universe. When Universe lists activities that CORDEX-CMIP6 does not, a notice asks for a manual check. New context entries are added in alphabetical order.

## Prefect Deployment

The orchestration layer runs checks as Prefect flows, with results posted to NocoDB.

```bash
# Deploy all checks as scheduled Prefect flows (weekdays at 6 AM)
uv run python deploy.py
```

Required Prefect configuration:
- **Secret:** `nocodb-consistency-api-key`
- **Variables:** `nocodb_base_url`, `nocodb_repo_watcher_tables_ids`

## Tests

```bash
uv run pytest
```

Tests cover the extraction and comparison layers with fixture data for CORDEX-CMIP6, CMIP6, CMIP6Plus, and input4MIPs formats.
