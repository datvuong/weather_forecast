"""Shared pytest fixtures: temp data directories, sample API responses, fake clients."""

import shutil
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from weather_forecast.resources.api_client import OpenMeteoRequest
from weather_forecast.resources.duckdb_resource import DuckDBResource
from weather_forecast.utils.helpers import (
    get_project_root,
    load_locations_config,
    load_variables_config,
    utc_today,
)

VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "uv_index",
    "weather_code",
    "is_day",
    "apparent_temperature",
    "cloud_cover",
]


def make_api_response(
    start: date,
    days: int = 1,
    variables: list[str] | None = None,
    temperature_base: float = 15.0,
    daily_variables: list[str] | tuple[str, ...] = (),
    tz: str = "Australia/Adelaide",
    unsupported: list[str] | tuple[str, ...] = (),
    utc_offset_seconds: int | None = None,
) -> dict:
    """Build an Open-Meteo-style response in unixtime format (as Bronze requests it).

    Hourly rows cover `days` UTC days from `start` 00:00 UTC. Temperature rises
    by 0.5 °C per hour from temperature_base, so the expected min/max/avg for a
    day are easy to compute. Like the real API, the response carries ONE fixed
    offset (`utc_offset_seconds`; default: tz's offset at `start`, standing in
    for "the offset at request time"), and daily rows are stamped with local
    midnight under that fixed offset, even for dates across a DST change.
    Variables listed in `unsupported` get unit "undefined" and null values.
    """
    variables = VARIABLES if variables is None else variables
    hours = 24 * days
    utc_start = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
    times = [int((utc_start + timedelta(hours=h)).timestamp()) for h in range(hours)]
    generators = {
        "temperature_2m": lambda h: temperature_base + 0.5 * (h % 24),
        "relative_humidity_2m": lambda h: 60.0,
        "precipitation": lambda h: 1.0 if h % 24 < 3 else 0.0,
        "precipitation_probability": lambda h: 50.0,
        "wind_speed_10m": lambda h: 10.0 + (h % 24),
        "wind_direction_10m": lambda h: 180.0,
        "wind_gusts_10m": lambda h: 20.0,
        "uv_index": lambda h: None if h % 24 < 6 else 3.0,
        "weather_code": lambda h: 61 if h % 24 < 3 else 2,
        "is_day": lambda h: 1 if 6 <= h % 24 < 18 else 0,
        "apparent_temperature": lambda h: temperature_base - 1.0 + 0.5 * (h % 24),
        "cloud_cover": lambda h: 40.0,
    }
    hourly = {"time": times}
    hourly_units = {"time": "unixtime"}
    for var in variables:
        hourly[var] = [
            None if var in unsupported else generators[var](h) for h in range(hours)
        ]
        hourly_units[var] = "undefined" if var in unsupported else "x"

    if utc_offset_seconds is None:
        utc_offset_seconds = int(ZoneInfo(tz).utcoffset(utc_start).total_seconds())
    response_tz = timezone(timedelta(seconds=utc_offset_seconds))
    response = {
        "latitude": -34.9,
        "longitude": 138.6,
        "utc_offset_seconds": utc_offset_seconds,
        "timezone": tz,
        "hourly_units": hourly_units,
        "hourly": hourly,
    }

    if daily_variables:
        local_midnights = [
            datetime.combine(start + timedelta(days=d), datetime.min.time(), tzinfo=response_tz)
            for d in range(days)
        ]
        daily_generators = {
            "temperature_2m_max": lambda d, m: temperature_base + 11.5 + d,
            "temperature_2m_min": lambda d, m: temperature_base + d,
            "precipitation_sum": lambda d, m: 3.0,
            "precipitation_probability_max": lambda d, m: 80.0,
            "uv_index_max": lambda d, m: 7.0,
            "wind_speed_10m_max": lambda d, m: 25.0,
            "weather_code": lambda d, m: 61,
            "daylight_duration": lambda d, m: 46800.0,
            "sunshine_duration": lambda d, m: 36000.0,
            "sunrise": lambda d, m: int((m + timedelta(hours=6, minutes=30)).timestamp()),
            "sunset": lambda d, m: int((m + timedelta(hours=19, minutes=30)).timestamp()),
        }
        time_valued = {"sunrise", "sunset"}
        daily = {"time": [int(m.timestamp()) for m in local_midnights]}
        daily_units = {"time": "unixtime"}
        for var in daily_variables:
            daily[var] = [
                None if var in unsupported else daily_generators[var](d, m)
                for d, m in enumerate(local_midnights)
            ]
            daily_units[var] = (
                "undefined" if var in unsupported else "unixtime" if var in time_valued else "x"
            )
        response["daily"] = daily
        response["daily_units"] = daily_units

    return response


CURRENT_VALUES = {
    "temperature_2m": 18.2,
    "relative_humidity_2m": 86.0,
    "apparent_temperature": 19.2,
    "precipitation": 0.1,
    "weather_code": 51,
    "wind_speed_10m": 6.3,
    "wind_direction_10m": 187,
    "is_day": 0,
}


def make_current_response(
    observed_at: datetime,
    tz: str = "Australia/Adelaide",
    unsupported: list[str] | tuple[str, ...] = (),
    **values,
) -> dict:
    """Build an Open-Meteo-style `current` snapshot in unixtime format.

    Values default to CURRENT_VALUES (one per configured current variable);
    keyword arguments override them.
    """
    snapshot = {**CURRENT_VALUES, **values}
    current = {"time": int(observed_at.timestamp()), "interval": 900}
    units = {"time": "unixtime", "interval": "seconds"}
    for var, value in snapshot.items():
        current[var] = None if var in unsupported else value
        units[var] = "undefined" if var in unsupported else "x"
    return {
        "latitude": -34.9,
        "longitude": 138.6,
        "utc_offset_seconds": int(ZoneInfo(tz).utcoffset(observed_at).total_seconds()),
        "timezone": tz,
        "current_units": units,
        "current": current,
    }


class FakeOpenMeteoClient:
    """Stand-in for OpenMeteoClient that records requests and returns canned data."""

    def __init__(self, archive_response=None, forecast_response=None, archive_error=None):
        self.archive_response = archive_response
        self.forecast_response = forecast_response
        self.archive_error = archive_error
        self.calls: list[tuple[str, OpenMeteoRequest]] = []

    def fetch(self, endpoint: str, request: OpenMeteoRequest) -> dict:
        self.calls.append((endpoint, request))
        if endpoint == "archive":
            if self.archive_error is not None:
                raise self.archive_error
            return self.archive_response
        return self.forecast_response


def write_bronze_rows(
    path,
    location_name: str,
    partition_date: str,
    responses: list[tuple[dict, datetime]],
    api_endpoint: str = "archive",
) -> None:
    """Write a Bronze Parquet file at `path` with one row per (response, ingestion time)."""
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
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
    pq.write_table(table, str(path))


def write_bronze_file(
    data_dir,
    location_name: str,
    partition_date: str,
    responses: list[tuple[dict, datetime]],
    api_endpoint: str = "archive",
) -> None:
    """Write an hourly/daily Bronze file (data/bronze/{location}/{date}.parquet)."""
    path = data_dir / "bronze" / location_name / f"{partition_date}.parquet"
    write_bronze_rows(path, location_name, partition_date, responses, api_endpoint)


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


@pytest.fixture
def variables_config(tmp_path, monkeypatch):
    """Factory pointing the config loaders at a temp variables.yaml.

    Call it with section overrides, e.g. variables_config(hourly_variables=[...]);
    unspecified sections/settings come from the project's config. Returns the
    loaded VariablesConfig. The project's locations.yaml is reused.
    """
    project_config = get_project_root() / "config"
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    shutil.copy(project_config / "locations.yaml", config_dir)
    monkeypatch.setenv("WEATHER_FORECAST_CONFIG_DIR", str(config_dir))

    def write(**overrides):
        raw = yaml.safe_load((project_config / "variables.yaml").read_text())
        raw.update(overrides)
        (config_dir / "variables.yaml").write_text(yaml.safe_dump(raw, allow_unicode=True))
        load_variables_config.cache_clear()
        load_locations_config.cache_clear()
        return load_variables_config()

    write()  # start from the project's config so the loaders work immediately
    yield write
    load_variables_config.cache_clear()
    load_locations_config.cache_clear()
