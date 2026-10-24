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

## Historical Backfill

On initial setup, backfill 30 days of historical data via the Dagster UI:

1. Open http://localhost:3000
2. Navigate to **Assets** → `bronze_forecast`
3. Click **Materialize** → select partition range (last 30 days)
4. Dagster processes all partitions through Bronze → Silver → Gold

## Testing

```bash
pytest                    # Run all tests
pytest --cov              # Run with coverage
ruff check .              # Lint
```

## Documentation

- [Design Proposal](docs/design-proposal.md) — Architecture options and trade-offs
- [Implementation Plan](docs/implementation-plan.md) — Detailed phased implementation plan
- [AGENTS.md](AGENTS.md) — Contributor and agent onboarding guide

## License

MIT
