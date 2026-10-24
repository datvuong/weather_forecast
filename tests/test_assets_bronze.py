"""Tests for the Bronze layer asset and its asset check."""

import json
from datetime import date, timedelta

import dagster as dg
import httpx
import pyarrow.parquet as pq

from tests.conftest import FakeOpenMeteoClient, make_api_response
from weather_forecast.assets import bronze
from weather_forecast.assets.bronze import (
    _covers_partition,
    bronze_file_exists_check,
    bronze_forecast,
)
from weather_forecast.models.weather import LocationConfig
from weather_forecast.utils.helpers import load_variables_config, utc_today


def _materialize(client, partition_key):
    return dg.materialize(
        [bronze_forecast, bronze_file_exists_check],
        resources={"open_meteo_client": client},
        partition_key=partition_key,
    )


def _window_response(partition_date, **kwargs):
    """A response covering local dates d-1..d+1, like the real Bronze request."""
    return make_api_response(partition_date - timedelta(days=1), days=3, **kwargs)


def test_today_uses_forecast_endpoint(data_dir, locations):
    today = utc_today()
    client = FakeOpenMeteoClient(forecast_response=make_api_response(today, days=7))

    result = _materialize(client, today.isoformat())

    assert result.success
    assert [c[0] for c in client.calls] == ["forecast"] * len(locations)
    request = client.calls[0][1]
    assert (request.past_days, request.forecast_days) == (1, 7)
    for loc in locations:
        table = pq.read_table(data_dir / "bronze" / loc.name / f"{today}.parquet")
        row = table.to_pylist()[0]
        assert row["api_endpoint"] == "forecast"
        assert row["location_name"] == loc.name
        assert row["partition_date"] == today.isoformat()
        assert "hourly" in json.loads(row["raw_json"])


def test_requests_use_location_timezone_unixtime_and_config_sections(
    data_dir, locations, past_date
):
    config = load_variables_config()
    client = FakeOpenMeteoClient(archive_response=_window_response(past_date))

    _materialize(client, past_date.isoformat())

    for (_, request), loc in zip(client.calls, locations):
        assert request.timezone == loc.timezone
        assert request.timeformat == "unixtime"
        assert request.hourly == config.hourly_names
        assert request.daily == config.daily_names
        assert request.current == []
        assert request.params == config.settings.request_params


def test_past_date_uses_archive_with_surrounding_local_days(data_dir, locations, past_date):
    client = FakeOpenMeteoClient(archive_response=_window_response(past_date))

    result = _materialize(client, past_date.isoformat())

    assert result.success
    assert [c[0] for c in client.calls] == ["archive"] * len(locations)
    request = client.calls[0][1]
    assert request.start_date == past_date - timedelta(days=1)
    assert request.end_date == past_date + timedelta(days=1)
    row = pq.read_table(
        data_dir / "bronze" / locations[0].name / f"{past_date}.parquet"
    ).to_pylist()[0]
    assert row["api_endpoint"] == "archive"
    assert row["ingestion_timestamp"] is not None


def test_archive_without_partition_data_falls_back_to_forecast(data_dir, past_date):
    # Archive has the day before, but nothing for the partition day itself
    lagging = make_api_response(past_date - timedelta(days=1), days=1)
    client = FakeOpenMeteoClient(
        archive_response=lagging, forecast_response=_window_response(past_date)
    )

    result = _materialize(client, past_date.isoformat())

    assert result.success
    forecast_calls = [r for kind, r in client.calls if kind == "forecast"]
    assert forecast_calls
    assert all(r.start_date == past_date - timedelta(days=1) for r in forecast_calls)
    assert all(r.end_date == past_date + timedelta(days=1) for r in forecast_calls)


def test_archive_error_falls_back_to_forecast(data_dir, locations, past_date):
    client = FakeOpenMeteoClient(
        archive_error=httpx.ConnectError("down"),
        forecast_response=_window_response(past_date),
    )

    result = _materialize(client, past_date.isoformat())

    assert result.success
    row = pq.read_table(
        data_dir / "bronze" / locations[0].name / f"{past_date}.parquet"
    ).to_pylist()[0]
    assert row["api_endpoint"] == "forecast"


def test_unsupported_variables_reported_in_metadata(data_dir, locations, past_date):
    response = _window_response(past_date, unsupported=["precipitation_probability"])
    client = FakeOpenMeteoClient(archive_response=response)

    result = _materialize(client, past_date.isoformat())

    metadata = result.asset_materializations_for_node("bronze_forecast")[0].metadata
    assert metadata["unsupported_variables"].value == {
        loc.name: {"hourly": ["precipitation_probability"]} for loc in locations
    }


class TestCoversPartition:
    day = date(2026, 10, 21)

    def test_hourly_data_in_utc_day(self):
        assert _covers_partition(make_api_response(self.day), self.day, "Australia/Adelaide")

    def test_hourly_data_only_outside_utc_day(self):
        response = make_api_response(self.day - timedelta(days=1))
        assert not _covers_partition(response, self.day, "Australia/Adelaide")

    def test_all_null_hourly_values(self):
        response = make_api_response(self.day)
        for key in response["hourly"]:
            if key != "time":
                response["hourly"][key] = [None] * 24
        assert not _covers_partition(response, self.day, "Australia/Adelaide")

    def test_missing_daily_row_for_local_date(self):
        response = make_api_response(self.day, daily_variables=["temperature_2m_max"])
        response["daily"]["temperature_2m_max"] = [None]
        assert not _covers_partition(response, self.day, "Australia/Adelaide")

    def test_daily_row_for_local_date(self):
        response = make_api_response(self.day, daily_variables=["temperature_2m_max"])
        assert _covers_partition(response, self.day, "Australia/Adelaide")


def test_file_exists_check_passes(data_dir, past_date):
    client = FakeOpenMeteoClient(archive_response=_window_response(past_date))

    result = _materialize(client, past_date.isoformat())

    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed


def test_file_exists_check_fails_when_file_missing(data_dir, locations, past_date, monkeypatch):
    # The asset sees the configured locations; the check (which runs after it)
    # additionally expects a location that was never written.
    extra = LocationConfig(name="Nowhere", latitude=0, longitude=0, timezone="UTC")
    configs = iter([locations, [*locations, extra]])
    monkeypatch.setattr(bronze, "load_locations_config", lambda: next(configs))
    client = FakeOpenMeteoClient(archive_response=_window_response(past_date))

    result = _materialize(client, past_date.isoformat())

    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert evaluation.metadata["missing_locations"].value == ["Nowhere"]
