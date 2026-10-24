"""Tests for the DuckDB warehouse views and the dimensions job wiring."""

import dagster as dg
import pandas as pd
import pytest

from tests.conftest import make_api_response, write_bronze_file
from weather_forecast.assets.bronze import bronze_forecast
from weather_forecast.assets.current import gold_current_conditions, silver_current_weather
from weather_forecast.assets.dimensions import dim_location, dim_weather_code
from weather_forecast.assets.silver import silver_daily_weather, silver_hourly_weather, silver_path
from weather_forecast.assets.warehouse import warehouse_views, warehouse_views_check
from weather_forecast.definitions import defs

INGESTED = pd.Timestamp("2026-10-24", tz="UTC").to_pydatetime()
ALL_ASSETS = [
    bronze_forecast,
    silver_hourly_weather,
    silver_daily_weather,
    silver_current_weather,
    gold_current_conditions,
    dim_location,
    dim_weather_code,
    warehouse_views,
    warehouse_views_check,
]


def _run(selection, duckdb_resource, partition_key=None):
    return dg.materialize(
        ALL_ASSETS,
        selection=selection,
        partition_key=partition_key,
        resources={"duckdb_resource": duckdb_resource, "open_meteo_client": None},
    )


def _build_dimensions_and_views(duckdb_resource):
    return _run(
        dg.AssetSelection.groups("dimensions") | dg.AssetSelection.checks(warehouse_views_check),
        duckdb_resource,
    )


@pytest.fixture
def silver(data_dir, duckdb_resource, locations, past_date):
    """Bronze + Silver hourly/daily for one partition, built by the real assets."""
    pk = past_date.isoformat()
    raw = make_api_response(
        past_date, daily_variables=["temperature_2m_max", "weather_code", "sunrise"]
    )
    for loc in locations:
        write_bronze_file(data_dir, loc.name, pk, [(raw, INGESTED)])
    dg.materialize(
        ALL_ASSETS,
        selection=dg.AssetSelection.assets(silver_hourly_weather, silver_daily_weather),
        partition_key=pk,
        resources={"open_meteo_client": None, "duckdb_resource": duckdb_resource},
    )
    return pk


def test_views_join_dimensions(data_dir, duckdb_resource, locations, silver):
    result = _build_dimensions_and_views(duckdb_resource)

    assert result.success
    views = result.asset_materializations_for_node("warehouse_views")[0].metadata["views"].value
    assert views["v_hourly_weather"] == views["v_daily_weather"] == "created"
    assert views["v_current_conditions"].startswith("skipped")  # no current weather yet

    hourly = duckdb_resource.query(
        "SELECT location_id, region, weather_code, weather_description, condition_group, "
        "is_precipitation FROM v_hourly_weather"
    )
    assert len(hourly) == 24 * len(locations)
    assert set(hourly["region"]) == {loc.region for loc in locations}
    rain = hourly[hourly["weather_code"] == 61].iloc[0]
    assert (rain["weather_description"], rain["condition_group"]) == ("Slight rain", "rain")
    assert bool(rain["is_precipitation"]) is True

    daily = duckdb_resource.query("SELECT * FROM v_daily_weather")
    assert len(daily) == len(locations)
    assert set(daily["weather_description"]) == {"Slight rain"}
    assert {"country", "elevation_m", "sunrise"} <= set(daily.columns)

    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed


def test_old_silver_without_location_id_joins_by_name(
    data_dir, duckdb_resource, locations, silver
):
    # Rewrite the Silver file as it looked before location_id existed
    path = silver_path("hourly", silver)
    pd.read_parquet(path).drop(columns=["location_id"]).to_parquet(path)

    result = _build_dimensions_and_views(duckdb_resource)

    assert result.success
    df = duckdb_resource.query(
        "SELECT DISTINCT location_id, location_name, region FROM v_hourly_weather"
    )
    assert dict(zip(df["location_name"], df["location_id"])) == {
        loc.name: loc.id for loc in locations
    }
    assert df["region"].notna().all()


def test_views_skipped_without_silver(data_dir, duckdb_resource):
    result = _build_dimensions_and_views(duckdb_resource)

    assert result.success
    views = result.asset_materializations_for_node("warehouse_views")[0].metadata["views"].value
    assert views["silver_hourly_weather"] == "skipped: no Silver hourly files"
    assert views["v_hourly_weather"].startswith("skipped")


def test_check_detects_join_fan_out(data_dir, duckdb_resource, silver):
    _build_dimensions_and_views(duckdb_resource)
    # A duplicated dimension key would multiply fact rows in the joins
    duckdb_resource.execute(
        "INSERT INTO dim_location SELECT * FROM dim_location WHERE location_id = 'adelaide'"
    )

    [evaluation] = _run(
        dg.AssetSelection.checks(warehouse_views_check), duckdb_resource
    ).get_asset_check_evaluations()

    assert not evaluation.passed
    assert "join fan-out" in evaluation.description


def _job_assets(name):
    job = defs.resolve_job_def(name)
    return {k.to_user_string() for k in job.asset_layer.executable_asset_keys}


def test_pipeline_job_ends_with_dimensions_and_views():
    assets = _job_assets("weather_pipeline_job")

    assert {"dim_location", "dim_weather_code", "warehouse_views"} <= assets
    assert "gold_daily_temperature" in assets
    assert "bronze_current" not in assets


def test_dimensions_job():
    assert _job_assets("dimensions_job") == {"dim_location", "dim_weather_code", "warehouse_views"}
