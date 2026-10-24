# AGENTS.md — Weather Forecast Pipeline

## Project Overview

This is an **end-to-end weather forecast data pipeline** using **Medallion Architecture** (Bronze → Silver → Gold). It fetches weather data from the [Open-Meteo API](https://open-meteo.com/) and processes it through three layers of increasing refinement.

## Architecture

| Component | Technology |
|---|---|
| Orchestration | **Dagster** (software-defined assets, schedules, asset checks) |
| Storage | **Parquet** files (all layers) + **DuckDB** (Gold layer queries) |
| API Source | **Open-Meteo** (no API key required) |
| Validation | **Pydantic** v2 models |
| Language | **Python 3.10+** |

### Medallion Layers

| Layer | Purpose | Storage |
|---|---|---|
| **Bronze** | Raw API responses preserved as-is | `data/bronze/{location}/{date}.parquet` (hourly + daily), `data/bronze/current/{location}/{date}/{HHMM}.parquet` (snapshots) |
| **Silver** | Cleaned, typed, deduplicated records, one column per configured variable | `data/silver/{hourly,daily,current}/{date}.parquet` |
| **Gold** | Aggregated daily summaries + latest current conditions | `data/gold/{metric}/{date}.parquet`, `data/gold/current_conditions/latest.parquet` + `warehouse/weather.duckdb` |

## Project Structure

```
weather_forecast/
├── pyproject.toml                  # Dependencies and project metadata
├── dagster.yaml                    # Dagster instance configuration
├── config/
│   ├── locations.yaml              # Configurable list of cities to track
│   └── variables.yaml              # Configurable weather variables and API settings
├── weather_forecast/               # Main Python package
│   ├── definitions.py              # Dagster entry point — wires assets, resources, schedules
│   ├── assets/
│   │   ├── bronze.py               # Bronze layer: raw API ingestion
│   │   ├── silver.py               # Silver layer: config-driven parsing and validation (hourly, daily)
│   │   ├── current.py              # Current weather: snapshots → history → latest conditions
│   │   └── gold.py                 # Gold layer: daily aggregations + config requirements
│   ├── resources/
│   │   ├── api_client.py           # Open-Meteo API wrapper (ConfigurableResource)
│   │   └── duckdb_resource.py      # DuckDB connection manager (ConfigurableResource)
│   ├── models/
│   │   └── weather.py              # Pydantic schemas for API data and domain models
│   ├── partitions.py               # DailyPartitionsDefinition
│   ├── schedules.py                # Jobs + hourly and current-weather schedules
│   └── utils/
│       └── helpers.py              # YAML config loaders and shared utilities
├── data/                           # Local data lake (git-ignored)
├── warehouse/                      # DuckDB database (git-ignored)
├── tests/                          # Pytest test suite
└── docs/                           # Design proposal and implementation plan
```

## Key Files for New Contributors

1. **Start here:** `weather_forecast/definitions.py` — the Dagster entry point that wires everything together
2. **Configuration:** `config/locations.yaml` and `config/variables.yaml` — add cities or weather variables without code changes
3. **Asset pipeline:** `weather_forecast/assets/bronze.py` → `silver.py` → `gold.py` — follow the data flow
4. **Data models:** `weather_forecast/models/weather.py` — Pydantic schemas define the data contracts

## Development Workflow (mandatory)

All work, by humans or agents, follows the plan in `docs/implementation-plan.md` and is tracked on GitHub:

1. **Story.** Every phase or feature has a `[Story] Phase N: …` issue (label `enhancement`) with a user story, acceptance criteria, estimate and dependencies. Reuse the existing story; don't open a duplicate.
2. **Issues.** Concrete work and defects are `[Task] …` / `[Bug] …` issues. Each body has *Problem* and a *Tasks* checklist, and each is linked as a **sub-issue** of its story:
   ```bash
   id=$(gh api repos/datvuong/weather_forecast/issues/<issue> -q .id)
   gh api -X POST repos/datvuong/weather_forecast/issues/<story>/sub_issues -F sub_issue_id=$id
   ```
3. **Commits.** Make one commit per story/issue, as soon as that work is done; don't leave finished work uncommitted. Use a Conventional Commits subject (`feat|fix|test|docs|chore: Phase N — summary`). The body explains what changed and why, grouped by file or area, plus the tests and verification, and ends with `Closes #<story>` / `Closes #<issue>`. Every commit must pass `pytest` and `ruff check .` on its own.
4. **Definition of done.** New behaviour has tests, `pytest` and `ruff check .` pass, and README/AGENTS.md are updated when behaviour or conventions change.

## Development Setup

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install in development mode
pip install -e ".[dev]"

# Launch Dagster dev server (UI at http://localhost:3000)
dagster dev

# Run tests
pytest

# Lint
ruff check .
```

## Adding a New Location

Edit `config/locations.yaml`:

```yaml
locations:
  # ... existing locations ...
  - name: Sydney
    latitude: -33.8688
    longitude: 151.2093
    timezone: Australia/Sydney
```

No code changes needed. The pipeline will automatically include the new location on the next run.

## Adding a Weather Variable

Edit `config/variables.yaml` under the matching section: `hourly_variables`, `daily_variables` or `current_variables`.

```yaml
hourly_variables:
  # ... existing variables ...
  - name: cloud_cover
    description: "Total cloud cover"
    unit: "%"
    valid_range: [0, 100]   # optional: checked by the Silver quality checks
```

No code changes are needed: Bronze requests it, and Silver adds it as a column with a Pydantic model built from the config. Notes:
- The name must be a valid Open-Meteo parameter **for that section** (see https://open-meteo.com/en/docs). An invalid name fails the run with Open-Meteo's `reason`.
- Variables an endpoint doesn't provide (unit `"undefined"`, e.g. `precipitation_probability` from the archive) become null columns and are listed in the asset metadata.
- A `unit` of `iso8601`/`unixtime` marks a time-valued variable (e.g. `sunrise`), stored as a UTC timestamp. Everything else is `float64`.
- Variable names must be unique within a section and must not clash with Silver key columns (`time`, `location_name`, `timestamp_utc`, …).
- Other API parameters go in `settings.extra_params` (pipeline-controlled ones like `timezone` are rejected).
- Gold needs certain hourly variables and metric units (`REQUIRED_HOURLY_VARIABLES` / `REQUIRED_UNITS` in `assets/gold.py`). The definitions fail to load if the config removes them.

## Dagster Concepts Used

| Concept | Where | Purpose |
|---|---|---|
| **Software-Defined Assets** | `assets/*.py` | Each Medallion layer is a Dagster asset |
| **ConfigurableResource** | `resources/*.py` | API client and DuckDB connections are injected resources |
| **DailyPartitionsDefinition** | `partitions.py` | Each partition = one day of weather data |
| **Unpartitioned asset** | `assets/current.py` | `gold_current_conditions` is overwritten with the latest snapshot per location |
| **Asset jobs + schedules** | `schedules.py` | `weather_pipeline_job` (hourly, backfillable) and `current_weather_job` (every `current_interval_minutes`) both target today's partition |
| **Asset Checks** | `assets/*.py` | Data quality validation after each layer |

## API Details

### Request strategy
- `OpenMeteoClient.fetch(endpoint, OpenMeteoRequest)` is variable-agnostic: sections, window and params come from the request.
- Bronze requests each location's **own timezone** with `timeformat=unixtime`. Hourly `time` values are exact UTC epochs. Daily rows are stamped with local midnight **under the response's single fixed offset** (`utc_offset_seconds` = the offset at request time), even for dates across a DST change. Date them with `daily_epoch_to_date(epoch, utc_offset_seconds)`, never with zoneinfo. For such dates the API's daily aggregation window is also shifted by the DST hour.
- UTC partition `d` spans local dates `d-1..d+1`, so that window is requested (today: `past_days=1` + forecast days).
- Only transport errors, 429 and 5xx are retried. Other 4xx raise `OpenMeteoAPIError` with Open-Meteo's `reason`.

### Open-Meteo Forecast Endpoint
- URL: `https://api.open-meteo.com/v1/forecast`
- No authentication required
- Returns JSON with `hourly`/`daily` time arrays and `current` (a single 15-minute snapshot)

### Open-Meteo Archive Endpoint
- URL: `https://archive-api.open-meteo.com/v1/archive`
- Used for historical backfill (dates in the past)
- Unsupported variables (e.g. `precipitation_probability`, `uv_index`, `uv_index_max`) come back with unit `"undefined"` and null values; they stay null in Silver for archive days
- Ignores `current`; current weather can't be fetched for past times
- Lags real time: for days it doesn't have yet it returns HTTP 200 with null-filled arrays, so Bronze falls back to the forecast endpoint for that window

### Rate Limits (Free Tier)
- 600 calls/minute
- 10,000 calls/day: scheduled usage is `locations × (24 + 1440 / current_interval_minutes)` (~83 locations at 15 min). `warn_if_over_api_budget()` logs a warning above 80%

## Testing Strategy

- **Unit tests** mock HTTP responses and test each asset independently
- **Pydantic models** tested with valid/invalid data
- **Asset tests** use Dagster's `materialize()` test utility
- **Fixtures** in `tests/conftest.py` provide sample API responses, a fake API client and temp directories
- The `data_dir` fixture sets `WEATHER_FORECAST_DATA_DIR`, which `get_data_dir()` honours; never write to the real `data/` in tests
- The `variables_config` fixture writes a temp `variables.yaml` (via `WEATHER_FORECAST_CONFIG_DIR`) and clears the loader caches; use it to test config-driven behaviour
- Tests must not hit the network: mock HTTP with `respx` or pass `FakeOpenMeteoClient`

## Conventions

- All timestamps stored in **UTC** (with a separate `timestamp_local` column)
- Partitions are **UTC calendar days**. The API is queried in each location's timezone with `timeformat=unixtime`. Hourly rows are kept by UTC date, with `timestamp_local` from zoneinfo (DST-correct). Daily rows are kept by the date from `daily_epoch_to_date()` (fixed response offset, as Open-Meteo documents). Use `utc_today()`, never `date.today()`
- Don't add `from __future__ import annotations` to asset modules: Dagster rejects the stringified `context: AssetExecutionContext` hint
- Parquet files use **snappy** compression
- DuckDB Gold tables use **upsert** semantics (idempotent re-runs); `gold_current_conditions` is replaced on each run
- Never hard-code variable names outside Gold: Bronze/Silver read them from config
- Config changes go in **YAML files**, not Python code
- Weather variable nulls are **preserved** (e.g., UV index is null at night)
