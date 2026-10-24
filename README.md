# Weather Forecast Data Pipeline

End-to-end weather forecast data pipeline using **Medallion Architecture** built with **Dagster**, **DuckDB**, and **Parquet**.

## Features

- 🥉 **Bronze Layer** — Raw weather data ingested from Open-Meteo API
- 🥈 **Silver Layer** — Cleaned, validated, and typed hourly weather records
- 🥇 **Gold Layer** — Daily temperature trends and precipitation summaries
- ⏰ **Hourly scheduling** — Automatic data refresh via Dagster
- 📊 **DuckDB analytics** — Fast local queries on aggregated data
- ✅ **Data quality checks** — Built-in asset checks at every layer
- 🔧 **Configurable** — Add locations and variables via YAML files

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | [Dagster](https://dagster.io/) |
| Storage | [Apache Parquet](https://parquet.apache.org/) |
| Analytics DB | [DuckDB](https://duckdb.org/) |
| Data Source | [Open-Meteo API](https://open-meteo.com/) |
| Validation | [Pydantic](https://docs.pydantic.dev/) v2 |
| Language | Python 3.10+ |

## Quick Start

```bash
# Clone the repository
git clone https://github.com/datvuong/weather_forecast.git
cd weather_forecast

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Launch Dagster (UI available at http://localhost:3000)
dagster dev
```

## Configuration

### Locations (`config/locations.yaml`)

```yaml
locations:
  - name: Melbourne
    latitude: -37.8136
    longitude: 144.9631
    timezone: Australia/Melbourne
  - name: Adelaide
    latitude: -34.9285
    longitude: 138.6007
    timezone: Australia/Adelaide
```

### Weather Variables (`config/variables.yaml`)

Tracks: temperature, humidity, precipitation, precipitation probability, wind speed, wind direction, wind gusts, and UV index.

## Architecture

```
Open-Meteo API
      │
      ▼
┌──────────┐    ┌──────────┐    ┌──────────┐
│  Bronze   │───▶│  Silver   │───▶│   Gold   │
│ Raw JSON  │    │ Cleaned   │    │ Summaries│
│ Parquet   │    │ Parquet   │    │ Parquet  │
└──────────┘    └──────────┘    │ + DuckDB │
                                └──────────┘
```

## Scheduling

`hourly_weather_schedule` runs `weather_pipeline_job` (Bronze → Silver → Gold) every hour at `:00` UTC
for **today's UTC partition**, refreshing the latest forecast. Turn it on under
**Overview → Schedules** in the Dagster UI (schedules start stopped).

Partitions are **UTC calendar days**: in Adelaide (UTC+10:30) the `2026-10-24` partition covers
10:30 on the 24th to 09:30 on the 25th, local time. Each Silver row has both
`timestamp_utc` and `timestamp_local`.

## Historical Backfill

On initial setup, backfill the last 30 days (`backfill_days` in `config/variables.yaml`) through
every layer:

1. Open http://localhost:3000
2. Navigate to **Jobs** → `weather_pipeline_job`
3. Click **Materialize all** → **Select partitions** → choose the date range → **Launch backfill**
4. Each partition runs Bronze → Silver → Gold. Past days use the archive API; recent days the
   archive doesn't have yet fall back to the forecast API automatically

> Materializing only `bronze_forecast` does **not** update Silver/Gold. Select all assets (or
> use the job) so downstream layers run too.

## Testing

```bash
pytest                    # Run all tests (no network access needed)
pytest --cov              # Run with coverage for the weather_forecast package
ruff check .              # Lint
```

| Test file | Covers |
|---|---|
| `tests/test_models.py` | Pydantic config/domain models, YAML config loaders |
| `tests/test_api_client.py` | Open-Meteo request params, retries and timeouts (HTTP mocked with `respx`) |
| `tests/test_assets_bronze.py` | Endpoint selection, archive→forecast fallback, Bronze Parquet, file check |
| `tests/test_assets_silver.py` | JSON parsing, UTC/local timestamps, validation, dedup, quality check |
| `tests/test_assets_gold.py` | Daily aggregations, DuckDB upsert idempotency, temperature check |
| `tests/test_duckdb_resource.py` | DuckDB queries and writer-lock retries |
| `tests/test_schedules.py` | Hourly schedule targets the UTC partition; Definitions load |

Asset tests run through Dagster's `materialize()` with a fake API client. Shared fixtures live
in `tests/conftest.py`. The `data_dir` fixture points the pipeline at a temp directory via the
`WEATHER_FORECAST_DATA_DIR` environment variable, which you can also use to relocate the data
lake outside tests.

## Documentation

- [Design Proposal](docs/design-proposal.md) — Architecture options and trade-offs
- [Implementation Plan](docs/implementation-plan.md) — Detailed phased implementation plan
- [AGENTS.md](AGENTS.md) — Contributor and agent onboarding guide

## License

MIT
