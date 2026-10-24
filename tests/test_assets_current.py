"""Tests for current-weather assets: snapshot history and latest conditions."""

from datetime import datetime, timedelta, timezone

import dagster as dg
import pandas as pd
import pytest

from tests.conftest import FakeOpenMeteoClient, make_current_response, write_bronze_rows
from weather_forecast.assets.current import (
    bronze_current,
    bronze_current_dir,
    current_conditions_path,
    gold_current_conditions,
    gold_current_freshness_check,
    silver_current_check,
    silver_current_weather,
    stale_locations,
)
from weather_forecast.assets.silver import section_columns, silver_path
from weather_forecast.utils.helpers import load_variables_config, utc_today


def _slot(minutes_ago: int = 0) -> datetime:
    """A 15-minute-aligned observation time today (UTC), never before midnight."""
    now = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    floored = now.replace(minute=now.minute - now.minute % 15, second=0, microsecond=0)
    midnight = datetime.combine(utc_today(), datetime.min.time(), tzinfo=timezone.utc)
    return max(floored, midnight)


def _run_bronze(client, partition_key):
    return dg.materialize(
        [bronze_current],
        resources={"open_meteo_client": client},
        partition_key=partition_key,
    )


def _run_silver(partition_key):
    return dg.materialize(
        [bronze_current, silver_current_weather, silver_current_check],
        selection=dg.AssetSelection.assets(silver_current_weather)
        | dg.AssetSelection.checks(silver_current_check),
        resources={"open_meteo_client": None},
        partition_key=partition_key,
    )


def _run_gold(duckdb_resource):
    return dg.materialize(
        [silver_current_weather, gold_current_conditions, gold_current_freshness_check],
        selection=dg.AssetSelection.assets(gold_current_conditions)
        | dg.AssetSelection.checks(gold_current_freshness_check),
        resources={"duckdb_resource": duckdb_resource},
    )


def _write_snapshots(data_dir, locations, partition_key, snapshots):
    """Write Bronze snapshot files: snapshots = [(observed_at, ingested_at, values)]."""
    for loc in locations:
        for observed_at, ingested_at, values in snapshots:
            raw = make_current_response(observed_at, **values)
            path = bronze_current_dir(loc.name, partition_key) / f"{observed_at:%H%M}.parquet"
            write_bronze_rows(path, loc.name, partition_key, [(raw, ingested_at)], "forecast")


class TestBronzeCurrent:
    def test_writes_snapshot_named_by_observation_time(self, data_dir, locations):
        observed = _slot()
        today = utc_today().isoformat()
        client = FakeOpenMeteoClient(forecast_response=make_current_response(observed))

        result = _run_bronze(client, today)

        assert result.success
        config = load_variables_config()
        for (endpoint, request), loc in zip(client.calls, locations):
            assert endpoint == "forecast"
            assert request.current == config.current_names
            assert request.hourly == [] and request.daily == []
            assert request.timezone == loc.timezone
            files = list(bronze_current_dir(loc.name, today).glob("*.parquet"))
            assert [f.name for f in files] == [f"{observed:%H%M}.parquet"]
        metadata = result.asset_materializations_for_node("bronze_current")[0].metadata
        assert metadata["snapshots_written"].value == len(locations)

    def test_rerun_in_same_slot_overwrites_new_slot_appends(self, data_dir, locations):
        today = utc_today().isoformat()
        first, second = _slot(15), _slot()
        client = FakeOpenMeteoClient(forecast_response=make_current_response(first))
        _run_bronze(client, today)
        _run_bronze(client, today)
        assert len(list(bronze_current_dir(locations[0].name, today).glob("*"))) == 1

        client.forecast_response = make_current_response(second)
        _run_bronze(client, today)

        expected = sorted({f"{first:%H%M}.parquet", f"{second:%H%M}.parquet"})
        files = sorted(f.name for f in bronze_current_dir(locations[0].name, today).glob("*"))
        assert files == expected

    def test_past_partition_is_skipped(self, data_dir, past_date):
        client = FakeOpenMeteoClient(forecast_response=make_current_response(_slot()))

        result = _run_bronze(client, past_date.isoformat())

        assert result.success
        assert client.calls == []
        assert not (data_dir / "bronze" / "current").exists()
        # No materialization: the past day stays "missing" instead of looking done (#35)
        assert result.asset_materializations_for_node("bronze_current") == []

    def test_skipped_without_current_variables(self, data_dir, variables_config):
        variables_config(current_variables=[])
        client = FakeOpenMeteoClient(forecast_response=make_current_response(_slot()))

        result = _run_bronze(client, utc_today().isoformat())

        assert client.calls == []
        assert result.asset_materializations_for_node("bronze_current") == []

    def test_snapshot_outside_partition_is_skipped(self, data_dir, locations):
        yesterday_late = datetime.combine(
            utc_today(), datetime.min.time(), tzinfo=timezone.utc
        ) - timedelta(minutes=15)
        client = FakeOpenMeteoClient(forecast_response=make_current_response(yesterday_late))

        result = _run_bronze(client, utc_today().isoformat())

        assert result.success
        assert not (data_dir / "bronze" / "current").exists()
        assert result.asset_materializations_for_node("bronze_current") == []

    def test_past_partition_skips_the_whole_current_chain(
        self, data_dir, duckdb_resource, past_date
    ):
        client = FakeOpenMeteoClient(forecast_response=make_current_response(_slot()))

        result = dg.materialize(
            [bronze_current, silver_current_weather, gold_current_conditions,
             silver_current_check, gold_current_freshness_check],
            partition_key=past_date.isoformat(),
            resources={"open_meteo_client": client, "duckdb_resource": duckdb_resource},
        )

        assert result.success
        assert result.get_asset_materialization_events() == []
        skipped = {e.step_key for e in result.all_events if e.event_type_value == "STEP_SKIPPED"}
        assert {"silver_current_weather", "gold_current_conditions"} <= skipped
        assert not (data_dir / "silver" / "current").exists()  # no empty history file


class TestSilverCurrent:
    def test_rebuilds_day_from_snapshots_with_dedup(self, data_dir, locations, past_date):
        pk = past_date.isoformat()
        day = datetime.combine(past_date, datetime.min.time(), tzinfo=timezone.utc)
        t1, t2 = day + timedelta(hours=1), day + timedelta(hours=1, minutes=15)
        ingested = day + timedelta(hours=2)
        _write_snapshots(
            data_dir,
            locations,
            pk,
            [
                (t1, ingested, {"temperature_2m": 10.0}),
                (t2, ingested, {"temperature_2m": 11.0}),
            ],
        )
        # The t1 slot re-polled later with a revised value
        repoll = make_current_response(t1, temperature_2m=12.0)
        for loc in locations:
            path = bronze_current_dir(loc.name, pk) / "0100-repoll.parquet"
            write_bronze_rows(path, loc.name, pk, [(repoll, ingested + timedelta(hours=1))])

        result = _run_silver(pk)

        assert result.success
        df = pd.read_parquet(silver_path("current", pk))
        assert list(df.columns) == section_columns("current", load_variables_config().current_names)
        assert len(df) == 2 * len(locations)
        one = df[df["location_name"] == locations[0].name].set_index("observed_at_utc")
        assert one.loc[pd.Timestamp(t1), "temperature_2m"] == 12.0
        assert one.loc[pd.Timestamp(t2), "temperature_2m"] == 11.0
        assert (df["interval_seconds"] == 900).all()
        adelaide = df[df["location_name"] == "Adelaide"].iloc[0]
        expected_local = pd.Timestamp(t1).tz_convert("Australia/Adelaide").tz_localize(None)
        assert adelaide["observed_at_local"] == expected_local
        [evaluation] = result.get_asset_check_evaluations()
        assert evaluation.passed

    def test_check_fails_on_out_of_range_values(self, data_dir, locations, past_date):
        pk = past_date.isoformat()
        day = datetime.combine(past_date, datetime.min.time(), tzinfo=timezone.utc)
        _write_snapshots(data_dir, locations, pk, [(day, day, {"relative_humidity_2m": 140.0})])

        result = _run_silver(pk)

        [evaluation] = result.get_asset_check_evaluations()
        assert not evaluation.passed
        assert "relative_humidity_2m out of range" in evaluation.description

    def test_no_snapshots_writes_empty_history(self, data_dir, past_date):
        pk = past_date.isoformat()

        result = _run_silver(pk)

        assert result.success
        assert pd.read_parquet(silver_path("current", pk)).empty


def _write_silver_current(day, rows):
    """Write a Silver current file from (location, observed_at, temperature) rows."""
    variables = load_variables_config().current_names
    df = pd.DataFrame(
        [
            {
                "location_name": loc,
                "latitude": 0.0,
                "longitude": 0.0,
                "observed_at_utc": observed,
                "observed_at_local": observed.replace(tzinfo=None),
                "interval_seconds": 900,
                "temperature_2m": temperature,
                "partition_date": day.isoformat(),
                "ingestion_timestamp": observed,
            }
            for loc, observed, temperature in rows
        ],
        columns=section_columns("current", variables),
    )
    path = silver_path("current", day.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


class TestGoldCurrentConditions:
    def test_latest_snapshot_per_location_across_midnight(
        self, data_dir, duckdb_resource, locations
    ):
        today, yesterday = utc_today(), utc_today() - timedelta(days=1)
        midnight = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)
        recent = _slot()
        first, second = locations[0].name, locations[1].name
        _write_silver_current(
            yesterday,
            [
                (first, midnight - timedelta(minutes=30), 1.0),
                (second, midnight - timedelta(minutes=15), 2.0),
            ],
        )
        _write_silver_current(today, [(first, recent, 3.0)])

        result = _run_gold(duckdb_resource)

        assert result.success
        df = duckdb_resource.query(
            "SELECT location_name, temperature_2m, age_minutes FROM gold_current_conditions "
            "ORDER BY location_name"
        ).set_index("location_name")
        assert df.loc[first, "temperature_2m"] == 3.0  # today's snapshot wins
        assert df.loc[second, "temperature_2m"] == 2.0  # yesterday's is the latest known
        assert df.loc[first, "age_minutes"] < 16
        assert len(pd.read_parquet(current_conditions_path())) == len(locations)

    def test_freshness_check_passes_when_recent(self, data_dir, duckdb_resource, locations):
        _write_silver_current(utc_today(), [(loc.name, _slot(), 20.0) for loc in locations])

        result = _run_gold(duckdb_resource)

        [evaluation] = result.get_asset_check_evaluations()
        assert evaluation.passed

    def test_freshness_check_fails_when_stale(self, data_dir, duckdb_resource, locations):
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        _write_silver_current(old.date(), [(loc.name, old, 20.0) for loc in locations])

        result = _run_gold(duckdb_resource)

        [evaluation] = result.get_asset_check_evaluations()
        assert not evaluation.passed
        assert set(evaluation.metadata["stale_locations"].value) == {loc.name for loc in locations}

    def test_no_snapshots_keeps_previous_and_check_fails(self, data_dir, duckdb_resource):
        result = _run_gold(duckdb_resource)

        assert result.success
        assert not current_conditions_path().exists()
        [evaluation] = result.get_asset_check_evaluations()
        assert not evaluation.passed


class TestStaleLocations:
    @pytest.fixture
    def now(self):
        return datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)

    def test_missing_and_old_snapshots(self, locations, now):
        df = pd.DataFrame(
            {
                "location_name": [locations[0].name],
                "observed_at_utc": [pd.Timestamp(now - timedelta(minutes=45))],
            }
        )

        stale = stale_locations(df, locations, timedelta(minutes=30), now)

        assert stale[locations[0].name].startswith("last snapshot")
        assert stale[locations[1].name] == "no snapshot"

    def test_within_max_age(self, locations, now):
        df = pd.DataFrame(
            {
                "location_name": [loc.name for loc in locations],
                "observed_at_utc": [pd.Timestamp(now - timedelta(minutes=30))] * len(locations),
            }
        )

        assert stale_locations(df, locations, timedelta(minutes=30), now) == {}
