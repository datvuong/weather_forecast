"""Tests for the Bronze layer asset and its asset check."""

import json

import dagster as dg
import httpx
import pyarrow.parquet as pq

from tests.conftest import FakeOpenMeteoClient, make_api_response
from weather_forecast.assets import bronze
from weather_forecast.assets.bronze import bronze_file_exists_check, bronze_forecast
from weather_forecast.models.weather import LocationConfig
from weather_forecast.utils.helpers import utc_today


def _materialize(client, partition_key):
    return dg.materialize(
        [bronze_forecast, bronze_file_exists_check],
        resources={"open_meteo_client": client},
        partition_key=partition_key,
    )


def test_today_uses_forecast_endpoint(data_dir, locations):
    today = utc_today()
    client = FakeOpenMeteoClient(forecast_response=make_api_response(today, days=7))

    result = _materialize(client, today.isoformat())

    assert result.success
    assert [c[0] for c in client.calls] == ["forecast"] * len(locations)
    assert all(c[1]["forecast_days"] == 7 for c in client.calls)
    for loc in locations:
        table = pq.read_table(data_dir / "bronze" / loc.name / f"{today}.parquet")
        row = table.to_pylist()[0]
        assert row["api_endpoint"] == "forecast"
        assert row["location_name"] == loc.name
        assert row["partition_date"] == today.isoformat()
        assert json.loads(row["raw_json"])["hourly"]["time"][0].startswith(today.isoformat())


def test_past_date_uses_archive_endpoint(data_dir, locations, past_date):
    client = FakeOpenMeteoClient(archive_response=make_api_response(past_date))

    result = _materialize(client, past_date.isoformat())

    assert result.success
    assert [c[0] for c in client.calls] == ["archive"] * len(locations)
    assert client.calls[0][1]["start_date"] == client.calls[0][1]["end_date"] == past_date
    row = pq.read_table(
        data_dir / "bronze" / locations[0].name / f"{past_date}.parquet"
    ).to_pylist()[0]
    assert row["api_endpoint"] == "archive"
    assert row["ingestion_timestamp"] is not None


def test_archive_without_data_falls_back_to_forecast_for_that_date(data_dir, past_date):
    empty = make_api_response(past_date)
    for key in empty["hourly"]:
        if key != "time":
            empty["hourly"][key] = [None] * 24
    client = FakeOpenMeteoClient(
        archive_response=empty, forecast_response=make_api_response(past_date)
    )

    result = _materialize(client, past_date.isoformat())

    assert result.success
    forecast_calls = [kw for kind, kw in client.calls if kind == "forecast"]
    assert forecast_calls
    assert all(kw["start_date"] == kw["end_date"] == past_date for kw in forecast_calls)


def test_archive_error_falls_back_to_forecast(data_dir, locations, past_date):
    client = FakeOpenMeteoClient(
        archive_error=httpx.ConnectError("down"),
        forecast_response=make_api_response(past_date),
    )

    result = _materialize(client, past_date.isoformat())

    assert result.success
    row = pq.read_table(
        data_dir / "bronze" / locations[0].name / f"{past_date}.parquet"
    ).to_pylist()[0]
    assert row["api_endpoint"] == "forecast"


def test_file_exists_check_passes(data_dir, past_date):
    client = FakeOpenMeteoClient(archive_response=make_api_response(past_date))

    result = _materialize(client, past_date.isoformat())

    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed


def test_file_exists_check_fails_when_file_missing(data_dir, locations, past_date, monkeypatch):
    # The asset sees the configured locations; the check (which runs after it)
    # additionally expects a location that was never written.
    extra = LocationConfig(name="Nowhere", latitude=0, longitude=0, timezone="UTC")
    configs = iter([locations, [*locations, extra]])
    monkeypatch.setattr(bronze, "load_locations_config", lambda: next(configs))
    client = FakeOpenMeteoClient(archive_response=make_api_response(past_date))

    result = _materialize(client, past_date.isoformat())

    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert evaluation.metadata["missing_locations"].value == ["Nowhere"]
