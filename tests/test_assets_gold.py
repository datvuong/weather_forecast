"""Tests for the Gold layer aggregations and DuckDB upserts."""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import dagster as dg
import pandas as pd
import pytest

from weather_forecast.assets.gold import (
    gold_daily_precipitation,
    gold_daily_temperature,
    gold_temperature_check,
)
from weather_forecast.assets.silver import section_columns, silver_hourly_weather, silver_path
from weather_forecast.utils.helpers import load_variables_config


def _write_silver(data_dir, locations, partition_date):
    """Write a Silver file where location i has temperature = 10*i + hour/2."""
    rows = []
    start = datetime.combine(partition_date, datetime.min.time(), tzinfo=timezone.utc)
    for i, loc in enumerate(locations):
        for h in range(24):
            ts_utc = start + timedelta(hours=h)
            rows.append(
                {
                    "location_name": loc.name,
                    "latitude": loc.latitude,
                    "longitude": loc.longitude,
                    "timestamp_utc": ts_utc,
                    "timestamp_local": ts_utc.astimezone(ZoneInfo(loc.timezone)).replace(
                        tzinfo=None
                    ),
                    "temperature_2m": 10.0 * i + h / 2,
                    "relative_humidity_2m": 50.0,
                    "precipitation": 2.0 if h < 6 else 0.0,
                    "precipitation_probability": None,
                    "wind_speed_10m": float(h),
                    "wind_direction_10m": 90.0,
                    "wind_gusts_10m": 30.0,
                    "uv_index": None if h > 12 else 5.0,
                    "partition_date": partition_date.isoformat(),
                    "ingestion_timestamp": start,
                }
            )
    df = pd.DataFrame(rows, columns=section_columns("hourly", load_variables_config().hourly_names))
    path = silver_path("hourly", partition_date.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return df


def _materialize(partition_key, duckdb_resource):
    return dg.materialize(
        [silver_hourly_weather, gold_daily_temperature, gold_daily_precipitation,
         gold_temperature_check],
        partition_key=partition_key,
        selection=dg.AssetSelection.groups("gold")
        | dg.AssetSelection.checks(gold_temperature_check),
        resources={"duckdb_resource": duckdb_resource},
    )


@pytest.fixture
def silver_df(data_dir, locations, past_date):
    return _write_silver(data_dir, locations, past_date)


def test_daily_temperature(data_dir, duckdb_resource, locations, past_date, silver_df):
    pk = past_date.isoformat()

    result = _materialize(pk, duckdb_resource)

    assert result.success
    df = duckdb_resource.query("SELECT * FROM gold_daily_temperature ORDER BY location_name")
    assert len(df) == len(locations)
    for loc in locations:
        expected = silver_df[silver_df["location_name"] == loc.name]
        row = df[df["location_name"] == loc.name].iloc[0]
        assert row["date"] == pk
        assert row["temp_min_c"] == expected["temperature_2m"].min()
        assert row["temp_max_c"] == expected["temperature_2m"].max()
        assert row["temp_avg_c"] == pytest.approx(expected["temperature_2m"].mean(), abs=0.01)
        daytime = expected[expected["timestamp_local"].dt.hour.between(6, 18)]
        assert row["daytime_temp_min_c"] == daytime["temperature_2m"].min()
        assert row["daytime_temp_max_c"] == daytime["temperature_2m"].max()
        assert row["wind_max_kmh"] == 23.0
        assert row["uv_max"] == 5.0
    parquet = pd.read_parquet(data_dir / "gold" / "daily_temperature" / f"{pk}.parquet")
    assert len(parquet) == len(locations)
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed


def test_daily_precipitation(data_dir, duckdb_resource, locations, past_date, silver_df):
    pk = past_date.isoformat()

    result = _materialize(pk, duckdb_resource)

    assert result.success
    df = duckdb_resource.query("SELECT * FROM gold_daily_precipitation")
    assert len(df) == len(locations)
    row = df.iloc[0]
    assert row["total_precipitation_mm"] == 12.0
    assert row["rainy_hours"] == 6
    assert row["total_hours"] == 24
    assert row["pct_hours_with_rain"] == 25.0
    # Archive-style data: probability is null, not zero
    assert pd.isna(row["max_precip_probability_pct"])
    assert (data_dir / "gold" / "daily_precipitation" / f"{pk}.parquet").exists()


def test_rerun_is_idempotent(data_dir, duckdb_resource, locations, past_date, silver_df):
    pk = past_date.isoformat()

    _materialize(pk, duckdb_resource)
    _materialize(pk, duckdb_resource)

    for table in ("gold_daily_temperature", "gold_daily_precipitation"):
        count = duckdb_resource.query(f"SELECT COUNT(*) AS n FROM {table}")["n"].iloc[0]
        assert count == len(locations)


def test_missing_silver_skips_without_failing(data_dir, duckdb_resource, past_date):
    result = _materialize(past_date.isoformat(), duckdb_resource)

    assert result.success
    assert not (data_dir / "gold" / "daily_temperature").exists()
    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
