# Weather Forecast Data Pipeline

End-to-end weather forecast data pipeline using **Medallion Architecture** built with **Dagster**, **DuckDB**, and **Parquet**.

## Features

- 🥉 **Bronze Layer** — Raw weather data ingested from Open-Meteo API
- 🥈 **Silver Layer** — Cleaned, validated, and typed hourly and daily records, with one column per configured variable
- 🥇 **Gold Layer** — Daily temperature and precipitation summaries per **local** calendar day
- 🧭 **Dimensions** — `dim_location` (region, country, elevation) and `dim_weather_code` (WMO code labels and categories), joined in ready-made DuckDB views
- 🌡️ **Current conditions** — 15-minute snapshot history plus a "latest conditions" table
- ⏰ **Scheduling** — Hourly pipeline refresh plus a 15-minute current-weather poll via Dagster
- 📊 **DuckDB analytics** — SQL over Gold, Silver and the dimensions (`v_hourly_weather`, `v_daily_weather`, `v_current_conditions`)
- ✅ **Data quality checks** — Built-in asset checks at every layer
- 🔧 **Configurable** — Add locations and any Open-Meteo hourly/daily/current variable via YAML, with no code changes

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

> New to the project? The **[User Guide](docs/user-guide.md)** walks through configuration, first run, backfill, schedules, checking results and troubleshooting.

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
  - id: melbourne              # optional stable key (defaults to a slug of name)
    name: Melbourne
    latitude: -37.8136
    longitude: 144.9631
    timezone: Australia/Melbourne
    country: Australia         # optional: country / region / tags for analytics
    region: Victoria
    tags: [capital]
```

Every table carries `location_id`, joined to the `dim_location` dimension. Removed locations stay there
as inactive, so their history still resolves.

### Weather codes (`config/weather_codes.yaml`)

The 29 WMO weather interpretation codes Open-Meteo uses, with descriptions and analytics categories
(`condition_group`, `precipitation_type`, `intensity`, flags, `severity_rank`). They seed
`dim_weather_code`, and you can edit labels without code changes.

### Weather Variables (`config/variables.yaml`)

Three request sections, each a list of [Open-Meteo variables](https://open-meteo.com/en/docs):

| Section | What it is | Silver table |
|---|---|---|
| `hourly_variables` | Hourly time series | `data/silver/hourly/{date}.parquet` |
| `daily_variables` | Daily aggregates over each location's **local** calendar day | `data/silver/daily/{date}.parquet` |
| `current_variables` | 15-minute "current conditions" snapshots (forecast API only) | `data/silver/current/{date}.parquet` |

```yaml
hourly_variables:
  - name: cloud_cover          # any Open-Meteo hourly variable
    description: "Total cloud cover"
    unit: "%"
    valid_range: [0, 100]      # optional, enforced by the Silver quality checks

daily_variables:
  - name: sunrise              # time-valued variables ("iso8601"/"unixtime") become UTC timestamps
    description: "Sunrise time"
    unit: "iso8601"

settings:
  temperature_unit: celsius    # celsius | fahrenheit   (Gold requires celsius)
  wind_speed_unit: kmh         # kmh | ms | mph | kn    (Gold requires kmh)
  precipitation_unit: mm       # mm | inch              (Gold requires mm)
  forecast_days: 7
  backfill_days: 30
  current_interval_minutes: 15 # 15 | 30 | 60
  extra_params: {}             # any other API parameter, e.g. {models: best_match}
```

Adding a variable needs no code change: it becomes a Silver column on the next run. Variables
an endpoint doesn't provide (e.g. `precipitation_probability` from the archive) are kept as
null columns and listed in the asset metadata. An invalid variable name fails the run with
Open-Meteo's error message. The Gold assets need `temperature_2m`, `relative_humidity_2m`,
`wind_speed_10m`, `uv_index`, `precipitation` and `precipitation_probability` with metric
units; Dagster refuses to load the definitions if they're missing.

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
                     dim_location · dim_weather_code
                     DuckDB views: silver_*, v_hourly_weather,
                     v_daily_weather, v_current_conditions
```

| Layer | Grain | Storage |
|---|---|---|
| Bronze | raw response per city per UTC day (+ current snapshots) | `data/bronze/` |
| Silver | city × hour (UTC day partitions), city × local day, city × 15-min snapshot | `data/silver/{hourly,daily,current}/` + DuckDB views |
| Gold | city × **local** day; latest current conditions | `data/gold/` + DuckDB tables |
| Dimensions | one row per location / per WMO code | `data/gold/dimensions/` + DuckDB tables |

## Scheduling

| Schedule | Job | Cadence | What it does |
|---|---|---|---|
| `hourly_weather_schedule` | `weather_pipeline_job` | every hour at `:00` UTC | Bronze → Silver (hourly + daily) → Gold for **today's UTC partition**, refreshing the latest forecast |
| `current_weather_schedule` | `current_weather_job` | every `current_interval_minutes` (default 15) | Snapshot of current conditions → Silver history → `gold_current_conditions` |
| — | `dimensions_job` | on demand | Rebuild `dim_location`, `dim_weather_code` and the DuckDB views (they also run at the end of every pipeline run) |

Each hourly tick also refreshes **yesterday's** Gold summaries, because Gold days are local days and
can include hours from today's UTC partition.

Turn them on in the Dagster UI under **Automation → Schedules and sensors**. Schedules start stopped. UI steps are for Dagster 1.13; see the [User Guide](docs/user-guide.md#finding-your-way-around-the-ui).

Bronze and Silver partitions are **UTC calendar days**: in Adelaide (UTC+10:30) the `2026-10-24`
partition covers 10:30 on the 24th to 09:30 on the 25th, local time. **Gold** partitions are
**local** calendar days: Gold `2026-10-24` is the 24th in each city's own timezone. Each hourly Silver row has both
`timestamp_utc` and `timestamp_local`. Hourly values are on the location's local whole hours,
so for half-hour-offset timezones like Adelaide they fall at `:30` UTC. Daily rows cover the
location's local calendar day equal to the partition date. (Open-Meteo builds daily values with the
offset in effect when the data was requested, so for days across a daylight-saving change the
day's window is shifted by an hour.)

## Current Weather

Open-Meteo's `current` values are model nowcasts for the current 15-minute slot, not station
observations, and they can't be requested for a past time. So:

- `bronze_current` stores one snapshot per poll at
  `data/bronze/current/{location}/{date}/{HHMM}.parquet`, named by observation time. A re-run in
  the same slot overwrites the same file. It only runs for today's UTC partition.
- `silver_current_weather` rebuilds the day's history from all snapshots into one file.
- `gold_current_conditions` (DuckDB table + `data/gold/current_conditions/latest.parquet`) holds
  the latest snapshot per location with its `age_minutes`.
- `gold_current_freshness_check` fails if a location's latest snapshot is older than twice the
  poll interval (e.g. the scheduler stopped).

Snapshots missed while Dagster isn't running are **permanent gaps**. For verified history, use
the archive-backed hourly data. API budget (free tier: 10,000 calls/day): each location needs 24
hourly-pipeline calls plus 96 current polls/day at 15 minutes, so ~83 locations fit. Dagster
logs a warning above 80% of the limit.

## Historical Backfill

On initial setup, backfill the last 30 days (`backfill_days` in `config/variables.yaml`) through
every layer:

1. Open http://localhost:3000
2. Navigate to **Jobs** → `weather_pipeline_job`
3. Click **Materialize all…** → under **Partition selection** choose **All** (or **Missing**) → **Launch backfill**
4. Each partition runs Bronze → Silver → Gold. Past days use the archive API; recent days the
   archive doesn't have yet fall back to the forecast API automatically

> Materializing only `bronze_forecast` does **not** update Silver/Gold. Select all assets (or
> use the job) so downstream layers run too. Current-weather assets aren't part of the job and
> can't be backfilled.

## Testing

```bash
pytest                    # Run all tests (no network access needed)
pytest --cov              # Run with coverage for the weather_forecast package
ruff check .              # Lint
```

| Test file | Covers |
|---|---|
| `tests/test_models.py` | Config schema (sections, `valid_range`, units, `extra_params`), runtime Silver record models, YAML loaders |
| `tests/test_api_client.py` | Request building per endpoint, unsupported-variable detection, fail-fast 4xx, retries (HTTP mocked with `respx`) |
| `tests/test_assets_bronze.py` | Endpoint selection, location-timezone unixtime window, archive→forecast fallback, file check |
| `tests/test_assets_silver.py` | Config-driven hourly/daily parsing, DST, new-variable-without-code-change, dedup, quality checks |
| `tests/test_assets_current.py` | Snapshot files, same-slot idempotency, history rebuild, latest conditions, freshness |
| `tests/test_assets_gold.py` | Local-day aggregations across UTC partitions (incl. DST), upsert idempotency, schema migration, temperature check, Gold config requirements |
| `tests/test_dimensions.py` | WMO code seed, `dim_weather_code`, `dim_location` (grid data, inactive locations), coverage checks |
| `tests/test_warehouse.py` | DuckDB views and joins, name fallback for old files, fan-out check, job wiring |
| `tests/test_duckdb_resource.py` | DuckDB queries and writer-lock retries |
| `tests/test_schedules.py` | Schedules target the UTC partition, job selections, current cadence, API budget, Definitions load |

Asset tests run through Dagster's `materialize()` with a fake API client. Shared fixtures live
in `tests/conftest.py`. The `data_dir` fixture points the pipeline at a temp directory via the
`WEATHER_FORECAST_DATA_DIR` environment variable, and `variables_config` writes a temporary
`variables.yaml` via `WEATHER_FORECAST_CONFIG_DIR`. Both variables also work outside tests to
relocate the data lake or config.

## Documentation

- [User Guide](docs/user-guide.md) — Configure, run and test the pipeline locally
- [Design Proposal](docs/design-proposal.md) — Architecture options and trade-offs
- [Implementation Plan](docs/implementation-plan.md) — Detailed phased implementation plan
- [AGENTS.md](AGENTS.md) — Contributor and agent onboarding guide

## License

MIT
