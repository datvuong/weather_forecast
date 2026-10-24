"""Shared pytest fixtures: temp data directories, sample API responses, fake clients."""

from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from weather_forecast.resources.duckdb_resource import DuckDBResource
from weather_forecast.utils.helpers import load_locations_config, utc_today

VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "uv_index",
]


def make_api_response(
    start: date,
    days: int = 1,
    variables: list[str] | None = None,
    temperature_base: float = 15.0,
) -> dict:
    """Build an Open-Meteo-style hourly response with UTC timestamps.

    Temperature rises by 0.5 °C per hour from temperature_base, so the
    expected min/max/avg for a day are easy to compute in tests.
    """
    variables = VARIABLES if variables is None else variables
    hours = 24 * days
    times = [
        (datetime.combine(start, datetime.min.time()) + timedelta(hours=h)).strftime(
            "%Y-%m-%dT%H:%M"
        )
        for h in range(hours)
    ]
    generators = {
        "temperature_2m": lambda h: temperature_base + 0.5 * (h % 24),
        "relative_humidity_2m": lambda h: 60.0,
        "precipitation": lambda h: 1.0 if h % 24 < 3 else 0.0,
        "precipitation_probability": lambda h: 50.0,
        "wind_speed_10m": lambda h: 10.0 + (h % 24),
        "wind_direction_10m": lambda h: 180.0,
        "wind_gusts_10m": lambda h: 20.0,
        "uv_index": lambda h: None if h % 24 < 6 else 3.0,
    }
    hourly = {"time": times}
    for var in variables:
        hourly[var] = [generators[var](h) for h in range(hours)]

    return {
        "latitude": -34.9,
        "longitude": 138.6,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "hourly": hourly,
    }


class FakeOpenMeteoClient:
    """Stand-in for OpenMeteoClient that records calls and returns canned data."""

    def __init__(self, archive_response=None, forecast_response=None, archive_error=None):
        self.archive_response = archive_response
        self.forecast_response = forecast_response
        self.archive_error = archive_error
        self.calls: list[tuple[str, dict]] = []

    def fetch_forecast(self, **kwargs) -> dict:
        self.calls.append(("forecast", kwargs))
        return self.forecast_response

    def fetch_historical(self, **kwargs) -> dict:
        self.calls.append(("archive", kwargs))
        if self.archive_error is not None:
            raise self.archive_error
        return self.archive_response


def write_bronze_file(
    data_dir,
    location_name: str,
    partition_date: str,
    responses: list[tuple[dict, datetime]],
    api_endpoint: str = "archive",
) -> None:
    """Write a Bronze Parquet file with one row per (response, ingestion time)."""
    import json

    location_dir = data_dir / "bronze" / location_name
    location_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "raw_json": [json.dumps(r) for r, _ in responses],
            "location_name": [location_name] * len(responses),
            "latitude": [0.0] * len(responses),
            "longitude": [0.0] * len(responses),
            "partition_date": [partition_date] * len(responses),
            "ingestion_timestamp": [ts for _, ts in responses],
            "api_endpoint": [api_endpoint] * len(responses),
        }
    )
    pq.write_table(table, str(location_dir / f"{partition_date}.parquet"))


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Redirect the pipeline's data/ directory to a temp dir."""
    path = tmp_path / "data"
    path.mkdir()
    monkeypatch.setenv("WEATHER_FORECAST_DATA_DIR", str(path))
    return path


@pytest.fixture
def duckdb_resource(tmp_path):
    return DuckDBResource(database_path=str(tmp_path / "warehouse" / "weather.duckdb"))


@pytest.fixture
def locations():
    return load_locations_config()


@pytest.fixture
def past_date() -> date:
    """A partition date safely inside the partition range and in the past."""
    return utc_today() - timedelta(days=3)


@pytest.fixture
def ingestion_ts() -> datetime:
    return datetime(2026, 10, 24, 1, 0, tzinfo=timezone.utc)
