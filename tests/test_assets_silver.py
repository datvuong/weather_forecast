"""Tests for the Silver layer: dynamic parsing, cleaning, dedup, and quality checks."""

from datetime import date, datetime, timedelta, timezone

import dagster as dg
import pandas as pd
import pytest

from tests.conftest import make_api_response, write_bronze_file
from weather_forecast.assets.bronze import bronze_forecast
from weather_forecast.assets.silver import (
    parse_section,
    section_columns,
    silver_daily_check,
    silver_daily_weather,
    silver_hourly_weather,
    silver_path,
    silver_quality_check,
)
from weather_forecast.models.weather import LocationConfig
from weather_forecast.utils.helpers import load_variables_config

ADELAIDE = LocationConfig(
    name="Adelaide", latitude=-34.9, longitude=138.6, timezone="Australia/Adelaide"
)
INGESTED = datetime(2026, 10, 24, tzinfo=timezone.utc)
DAILY = ["temperature_2m_max", "precipitation_sum", "sunrise"]


def _parse(raw, section="hourly", variables=None, time_vars=frozenset(), day=date(2026, 10, 21)):
    variables = variables if variables is not None else ["temperature_2m", "uv_index"]
    return parse_section(raw, section, variables, set(time_vars), ADELAIDE, day, INGESTED)


def _materialize(partition_key, assets, checks=()):
    selection = dg.AssetSelection.assets(*assets)
    if checks:
        selection = selection | dg.AssetSelection.checks(*checks)
    return dg.materialize(
        [bronze_forecast, *assets, *checks],
        partition_key=partition_key,
        selection=selection,
        resources={"open_meteo_client": None},
    )


def _write_bronze(data_dir, locations, partition_date, **response_kwargs):
    raw = make_api_response(partition_date - timedelta(days=1), days=3, **response_kwargs)
    for loc in locations:
        write_bronze_file(data_dir, loc.name, partition_date.isoformat(), [(raw, INGESTED)])
    return raw


class TestParseSection:
    def test_hourly_filters_to_utc_partition_date(self):
        records = _parse(make_api_response(date(2026, 10, 20), days=3))

        assert len(records) == 24
        assert {r["timestamp_utc"].date() for r in records} == {date(2026, 10, 21)}

    def test_hourly_timestamps_are_utc_with_local_conversion(self):
        first = _parse(make_api_response(date(2026, 10, 21)))[0]

        assert first["timestamp_utc"] == datetime(2026, 10, 21, 0, tzinfo=timezone.utc)
        # Adelaide is on daylight time (UTC+10:30) in late October
        assert first["timestamp_local"] == datetime(2026, 10, 21, 10, 30)
        assert first["timestamp_local"].tzinfo is None

    def test_local_time_across_dst_start(self):
        # Adelaide clocks go 02:00 ACST -> 03:00 ACDT on 2026-10-04 (16:30 UTC on the 3rd)
        records = _parse(make_api_response(date(2026, 10, 3)), day=date(2026, 10, 3))
        local = {r["timestamp_utc"].hour: r["timestamp_local"] for r in records}

        assert local[15] == datetime(2026, 10, 4, 0, 30)  # UTC+9:30
        assert local[16] == datetime(2026, 10, 4, 1, 30)
        assert local[17] == datetime(2026, 10, 4, 3, 30)  # UTC+10:30
        assert len(records) == 24

    def test_columns_follow_configured_variables(self):
        raw = make_api_response(date(2026, 10, 21), variables=["temperature_2m", "cloud_cover"])

        record = _parse(raw, variables=["cloud_cover"])[0]

        assert record["cloud_cover"] == 40.0
        assert "temperature_2m" not in record

    def test_missing_variable_becomes_none(self):
        raw = make_api_response(date(2026, 10, 21), variables=["temperature_2m"])

        record = _parse(raw, variables=["temperature_2m", "uv_index"])[0]

        assert record["temperature_2m"] == 15.0
        assert record["uv_index"] is None

    def test_daily_keeps_row_for_local_partition_date(self):
        raw = make_api_response(date(2026, 10, 20), days=3, daily_variables=DAILY)

        [record] = _parse(raw, "daily", DAILY, {"sunrise"})

        assert record["date_local"] == date(2026, 10, 21)
        assert record["temperature_2m_max"] == 15.0 + 11.5 + 1
        # 06:30 local (UTC+10:30) on the 21st is 20:00 UTC on the 20th
        assert record["sunrise"] == datetime(2026, 10, 20, 20, 0, tzinfo=timezone.utc)

    def test_daily_on_dst_start_day(self):
        raw = make_api_response(date(2026, 10, 3), days=3, daily_variables=DAILY)

        [record] = _parse(raw, "daily", DAILY, {"sunrise"}, day=date(2026, 10, 4))

        assert record["date_local"] == date(2026, 10, 4)

    def test_daily_dated_with_response_offset_real_capture(self):
        # Real archive response (#24): Melbourne 2026-09-24..26 requested in AEDT, so the
        # whole response uses +11:00 although September is AEST (+10:00)
        raw = {
            "utc_offset_seconds": 39600,
            "daily_units": {"time": "unixtime", "temperature_2m_max": "°C"},
            "daily": {
                "time": [1790168400, 1790254800, 1790341200],
                "temperature_2m_max": [25.5, 23.2, 14.7],
            },
        }

        [record] = _parse(raw, "daily", ["temperature_2m_max"], day=date(2026, 9, 25))

        assert record["date_local"] == date(2026, 9, 25)
        assert record["temperature_2m_max"] == 23.2

    @pytest.mark.parametrize(
        "start,request_offset",
        [
            (date(2026, 9, 28), 37800),  # requested after DST start (ACDT) for days before it
            (date(2027, 4, 1), 37800),  # requested before DST end (ACDT) for days after it
        ],
    )
    def test_daily_labels_correct_across_dst_change(self, start, request_offset):
        raw = make_api_response(
            start, days=10, daily_variables=DAILY, utc_offset_seconds=request_offset
        )

        for d in range(10):
            day = start + timedelta(days=d)
            [record] = _parse(raw, "daily", DAILY, {"sunrise"}, day=day)
            assert record["date_local"] == day
            assert record["temperature_2m_max"] == 15.0 + 11.5 + d

    def test_empty_response(self):
        assert _parse({"hourly": {}}) == []
        assert _parse({}, "daily", DAILY) == []

    def test_rejects_non_unixtime_response(self):
        raw = make_api_response(date(2026, 10, 21))
        raw["hourly_units"]["time"] = "iso8601"

        with pytest.raises(ValueError, match="unixtime"):
            _parse(raw)


class TestSilverHourly:
    def test_writes_config_driven_parquet(self, data_dir, locations, past_date):
        pk = past_date.isoformat()
        _write_bronze(data_dir, locations, past_date, unsupported=["precipitation_probability"])

        result = _materialize(pk, [silver_hourly_weather], [silver_quality_check])

        assert result.success
        df = pd.read_parquet(silver_path("hourly", pk))
        assert silver_path("hourly", pk) == data_dir / "silver" / "hourly" / f"{pk}.parquet"
        assert list(df.columns) == section_columns("hourly", load_variables_config().hourly_names)
        assert len(df) == 24 * len(locations)
        assert str(df["timestamp_utc"].dt.tz) == "UTC"
        assert all(df[c].dtype == "float64" for c in load_variables_config().hourly_names)
        # Unsupported on the endpoint: nulls are preserved, reported in metadata
        assert df["precipitation_probability"].isna().all()
        assert df["uv_index"].isna().sum() == 6 * len(locations)
        assert (df["ingestion_timestamp"] == pd.Timestamp(INGESTED)).all()
        metadata = result.asset_materializations_for_node("silver_hourly_weather")[0].metadata
        assert metadata["unsupported_variables"].value == {
            loc.name: ["precipitation_probability"] for loc in locations
        }
        [evaluation] = result.get_asset_check_evaluations()
        assert evaluation.passed

    def test_new_variable_needs_no_code_change(
        self, data_dir, locations, past_date, variables_config
    ):
        config = variables_config(
            hourly_variables=[
                {"name": "temperature_2m", "description": "t", "unit": "°C"},
                {"name": "cloud_cover", "description": "c", "unit": "%", "valid_range": [0, 100]},
            ]
        )
        pk = past_date.isoformat()
        _write_bronze(data_dir, locations, past_date, variables=["temperature_2m", "cloud_cover"])

        result = _materialize(pk, [silver_hourly_weather], [silver_quality_check])

        assert result.success
        df = pd.read_parquet(silver_path("hourly", pk))
        assert list(df.columns) == section_columns("hourly", config.hourly_names)
        assert "cloud_cover" in df.columns and "uv_index" not in df.columns
        assert (df["cloud_cover"] == 40.0).all()
        [evaluation] = result.get_asset_check_evaluations()
        assert evaluation.passed

    def test_configured_variable_absent_from_response_is_null(
        self, data_dir, locations, past_date, variables_config
    ):
        variables_config(
            hourly_variables=[
                {"name": "temperature_2m", "description": "t", "unit": "°C"},
                {"name": "cloud_cover", "description": "c", "unit": "%"},
            ]
        )
        pk = past_date.isoformat()
        _write_bronze(data_dir, locations, past_date, variables=["temperature_2m"])

        _materialize(pk, [silver_hourly_weather])

        df = pd.read_parquet(silver_path("hourly", pk))
        assert df["cloud_cover"].isna().all()
        assert df["cloud_cover"].dtype == "float64"

    def test_deduplicates_keeping_latest_ingestion(self, data_dir, locations, past_date):
        pk = past_date.isoformat()
        older = datetime(2026, 10, 24, 1, tzinfo=timezone.utc)
        newer = older + timedelta(hours=1)
        for loc in locations:
            write_bronze_file(
                data_dir,
                loc.name,
                pk,
                [
                    (make_api_response(past_date, temperature_base=30.0), newer),
                    (make_api_response(past_date, temperature_base=10.0), older),
                ],
            )

        result = _materialize(pk, [silver_hourly_weather])

        assert result.success
        df = pd.read_parquet(silver_path("hourly", pk))
        assert len(df) == 24 * len(locations)
        assert df["temperature_2m"].min() == 30.0
        metadata = result.asset_materializations_for_node("silver_hourly_weather")[0].metadata
        assert metadata["duplicates_removed"].value == 24 * len(locations)

    def test_invalid_records_are_skipped(self, data_dir, locations, past_date):
        pk = past_date.isoformat()
        raw = make_api_response(past_date)
        raw["hourly"]["temperature_2m"][5] = "not-a-number"
        for loc in locations:
            write_bronze_file(data_dir, loc.name, pk, [(raw, INGESTED)])

        result = _materialize(pk, [silver_hourly_weather])

        assert result.success
        assert len(pd.read_parquet(silver_path("hourly", pk))) == 23 * len(locations)

    def test_missing_bronze_writes_empty_file(self, data_dir, past_date):
        pk = past_date.isoformat()

        result = _materialize(pk, [silver_hourly_weather], [silver_quality_check])

        df = pd.read_parquet(silver_path("hourly", pk))
        assert df.empty
        assert list(df.columns) == section_columns("hourly", load_variables_config().hourly_names)
        [evaluation] = result.get_asset_check_evaluations()
        assert not evaluation.passed


class TestSilverDaily:
    def test_one_row_per_location_for_local_date(self, data_dir, locations, past_date):
        pk = past_date.isoformat()
        config = load_variables_config()
        _write_bronze(
            data_dir,
            locations,
            past_date,
            daily_variables=config.daily_names,
            unsupported=["precipitation_probability_max", "uv_index_max"],
        )

        result = _materialize(pk, [silver_daily_weather], [silver_daily_check])

        assert result.success
        df = pd.read_parquet(silver_path("daily", pk))
        assert list(df.columns) == section_columns("daily", config.daily_names)
        assert sorted(df["location_name"]) == sorted(loc.name for loc in locations)
        assert (df["date_local"] == past_date).all()
        assert str(df["sunrise"].dt.tz) == "UTC"
        assert df["temperature_2m_max"].dtype == "float64"
        assert df["uv_index_max"].isna().all()
        [evaluation] = result.get_asset_check_evaluations()
        assert evaluation.passed

    def test_check_fails_when_location_missing(self, data_dir, locations, past_date):
        pk = past_date.isoformat()
        _write_bronze(data_dir, locations[:1], past_date, daily_variables=DAILY)

        result = _materialize(pk, [silver_daily_weather], [silver_daily_check])

        [evaluation] = result.get_asset_check_evaluations()
        assert not evaluation.passed
        assert "one row per location" in evaluation.description


class TestSilverQualityCheck:
    def test_out_of_range_values_fail(self, data_dir, locations, past_date):
        pk = past_date.isoformat()
        raw = make_api_response(past_date)
        raw["hourly"]["relative_humidity_2m"][0] = 150.0
        for loc in locations:
            write_bronze_file(data_dir, loc.name, pk, [(raw, INGESTED)])

        result = _materialize(pk, [silver_hourly_weather], [silver_quality_check])

        [evaluation] = result.get_asset_check_evaluations()
        assert not evaluation.passed
        assert "relative_humidity_2m out of range" in evaluation.description

    def test_ranges_come_from_config(self, data_dir, locations, past_date, variables_config):
        variables_config(
            hourly_variables=[
                {"name": "temperature_2m", "description": "t", "unit": "°C",
                 "valid_range": [0, 20]},
            ]
        )
        pk = past_date.isoformat()
        _write_bronze(data_dir, locations, past_date, variables=["temperature_2m"])

        result = _materialize(pk, [silver_hourly_weather], [silver_quality_check])

        [evaluation] = result.get_asset_check_evaluations()
        # The fixture's temperatures reach 26.5 °C
        assert not evaluation.passed
        assert "temperature_2m out of range [0.0, 20.0]" in evaluation.description
