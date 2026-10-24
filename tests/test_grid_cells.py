"""Tests for Open-Meteo grid-cell coordinates (#41).

Open-Meteo snaps each request to a model grid cell and reports that cell's
coordinates and elevation. Silver keeps the API's grid coordinates; the
configured coordinates live in dim_location next to the grid ones.
"""

from datetime import timedelta

import dagster as dg
import pandas as pd

from tests.conftest import GRID_CELLS, FakeOpenMeteoClient, make_api_response, write_bronze_file
from weather_forecast.assets.bronze import bronze_forecast
from weather_forecast.assets.dimensions import dim_location
from weather_forecast.assets.gold import gold_daily_precipitation, gold_daily_temperature
from weather_forecast.assets.silver import silver_daily_weather, silver_hourly_weather, silver_path
from weather_forecast.utils.helpers import load_variables_config

PIPELINE = [
    bronze_forecast,
    silver_hourly_weather,
    silver_daily_weather,
    gold_daily_temperature,
    gold_daily_precipitation,
]


def _window(request, grid=None):
    """A realistic response for a Bronze request (d-1..d+1 in the location's timezone)."""
    return make_api_response(
        request.start_date,
        days=3,
        daily_variables=load_variables_config().daily_names,
        tz=request.timezone,
        grid=grid,
    )


def _run(client, duckdb_resource, partition_key):
    return dg.materialize(
        PIPELINE,
        partition_key=partition_key,
        resources={"open_meteo_client": client, "duckdb_resource": duckdb_resource},
    )


def test_silver_stores_api_grid_not_configured_coordinates(
    data_dir, duckdb_resource, locations, past_date
):
    client = FakeOpenMeteoClient(respond=lambda endpoint, request: _window(request))

    assert _run(client, duckdb_resource, past_date.isoformat()).success

    for section in ("hourly", "daily"):
        df = pd.read_parquet(silver_path(section, past_date.isoformat()))
        for loc in locations:
            rows = df[df["location_id"] == loc.id]
            grid_lat, grid_lon, _ = GRID_CELLS[loc.timezone]
            assert set(zip(rows["latitude"], rows["longitude"])) == {(grid_lat, grid_lon)}
            assert (grid_lat, grid_lon) != (loc.latitude, loc.longitude)


def test_cell_shift_between_endpoints_keeps_one_gold_row_per_location(
    data_dir, duckdb_resource, locations, past_date
):
    # d-1 and d+1 come from the archive; the archive fails for d, so it falls back to the
    # forecast endpoint, which reports a different cell for the same location
    def respond(endpoint, request):
        if endpoint == "archive" and request.start_date == past_date - timedelta(days=1):
            raise ConnectionError("archive down")
        lat, lon, elevation = GRID_CELLS[request.timezone]
        if endpoint == "forecast":
            lat, lon, elevation = lat + 0.05, lon + 0.05, elevation + 6
        return _window(request, grid=(lat, lon, elevation))

    client = FakeOpenMeteoClient(respond=respond)
    days = [past_date - timedelta(days=1), past_date, past_date + timedelta(days=1)]
    for day in days:
        assert _run(client, duckdb_resource, day.isoformat()).success

    silver = pd.concat(pd.read_parquet(silver_path("hourly", d.isoformat())) for d in days)
    assert silver.groupby("location_id")["latitude"].nunique().eq(2).all()  # the shift happened

    gold = duckdb_resource.query(
        f"SELECT location_id, hours_covered FROM gold_daily_temperature WHERE date = '{past_date}'"
    )
    assert sorted(gold["location_id"]) == sorted(loc.id for loc in locations)  # not per cell
    assert (gold["hours_covered"] == 24).all()


def test_dim_location_uses_latest_dated_bronze_file(data_dir, duckdb_resource, past_date):
    # Written newest-date first: the older partition is ingested LATER (e.g. a backfill),
    # but dim_location still takes the latest-dated file, independent of backfill order
    newer, older = past_date, past_date - timedelta(days=1)
    for day, elevation in ((newer, 46.0), (older, 40.0)):
        raw = make_api_response(day, grid=(-34.9, 138.5, elevation))
        ingested = pd.Timestamp.now(tz="UTC")
        write_bronze_file(data_dir, "Adelaide", day.isoformat(), [(raw, ingested)])

    dg.materialize(
        [bronze_forecast, dim_location],
        selection=dg.AssetSelection.assets(dim_location),
        resources={"duckdb_resource": duckdb_resource, "open_meteo_client": None},
    )

    elevation = duckdb_resource.query(
        "SELECT elevation_m FROM dim_location WHERE location_id = 'adelaide'"
    )["elevation_m"].item()
    assert elevation == 46.0


def test_extra_params_cell_selection_reaches_requests(
    data_dir, duckdb_resource, past_date, variables_config
):
    settings = load_variables_config().settings.model_dump()
    settings["extra_params"] = {"cell_selection": "land"}
    variables_config(settings=settings)
    client = FakeOpenMeteoClient(respond=lambda endpoint, request: _window(request))

    dg.materialize(
        [bronze_forecast],
        partition_key=past_date.isoformat(),
        resources={"open_meteo_client": client},
    )

    assert client.calls
    for endpoint, request in client.calls:
        assert request.params["cell_selection"] == "land"
        assert request.to_query(endpoint)["cell_selection"] == "land"
