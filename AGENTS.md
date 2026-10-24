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
| **Bronze** | Raw API responses preserved as-is | `data/bronze/{location}/{date}.parquet` |
| **Silver** | Cleaned, typed, deduplicated hourly records | `data/silver/{date}.parquet` |
| **Gold** | Aggregated daily summaries for analytics | `data/gold/{metric}/{date}.parquet` + `warehouse/weather.duckdb` |

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
│   │   ├── silver.py               # Silver layer: cleaning and validation
│   │   └── gold.py                 # Gold layer: daily aggregations
│   ├── resources/
│   │   ├── api_client.py           # Open-Meteo API wrapper (ConfigurableResource)
│   │   └── duckdb_resource.py      # DuckDB connection manager (ConfigurableResource)
│   ├── models/
│   │   └── weather.py              # Pydantic schemas for API data and domain models
│   ├── partitions.py               # DailyPartitionsDefinition
│   ├── schedules.py                # Hourly schedule definition
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

Edit `config/variables.yaml`:

```yaml
hourly_variables:
  # ... existing variables ...
  - name: cloud_cover
    description: "Total cloud cover"
    unit: "%"
```

> **Note:** The variable name must match an Open-Meteo API parameter. See https://open-meteo.com/en/docs for the full list. After adding a variable, the Silver layer Pydantic model (`models/weather.py`) may need updating to include the new field.

## Dagster Concepts Used

| Concept | Where | Purpose |
|---|---|---|
| **Software-Defined Assets** | `assets/*.py` | Each Medallion layer is a Dagster asset |
| **ConfigurableResource** | `resources/*.py` | API client and DuckDB connections are injected resources |
| **DailyPartitionsDefinition** | `partitions.py` | Each partition = one day of weather data |
| **ScheduleDefinition** | `schedules.py` | Hourly cron triggers today's partition |
| **Asset Checks** | `assets/*.py` | Data quality validation after each layer |

## API Details

### Open-Meteo Forecast Endpoint
- URL: `https://api.open-meteo.com/v1/forecast`
- No authentication required
- Returns JSON with `hourly.time[]` and variable arrays

### Open-Meteo Archive Endpoint
- URL: `https://archive-api.open-meteo.com/v1/archive`
- Used for historical backfill (dates in the past)
- `precipitation_probability` is NOT available in archive data (will be null)

### Rate Limits (Free Tier)
- 600 calls/minute
- 10,000 calls/day

## Testing Strategy

- **Unit tests** mock HTTP responses and test each asset independently
- **Pydantic models** tested with valid/invalid data
- **Asset tests** use Dagster's `materialize()` test utility
- **Fixtures** in `tests/conftest.py` provide sample API responses and temp directories

## Conventions

- All timestamps stored in **UTC** (with a separate `timestamp_local` column)
- Parquet files use **snappy** compression
- DuckDB Gold tables use **upsert** semantics (idempotent re-runs)
- Config changes go in **YAML files**, not Python code
- Weather variable nulls are **preserved** (e.g., UV index is null at night)
