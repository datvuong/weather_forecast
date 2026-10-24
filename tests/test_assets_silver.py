"""Tests for the Silver layer: JSON parsing, cleaning, dedup, and quality check."""

from datetime import date, datetime, timedelta, timezone

import dagster as dg
import pandas as pd
import pytest

from tests.conftest import make_api_response, write_bronze_file
from weather_forecast.assets.bronze import bronze_forecast
from weather_forecast.assets.silver import (
    SILVER_COLUMNS,
    WEATHER_COLUMNS,
    _parse_hourly_data,
    silver_hourly_weather,
    silver_quality_check,
)


def _parse(raw, partition_date="2026-10-21", tz="Australia/Adelaide"):
    return _parse_hourly_data(
        raw_json=raw,
        location_name="Adelaide",
        latitude=-34.9,
        longitude=138.6,
        local_timezone=tz,
        partition_date=partition_date,
        ingestion_ts=datetime(2026, 10, 24, tzinfo=timezone.utc),
    )


def _materialize(partition_key, with_check=True):
    assets = [bronze_forecast, silver_hourly_weather]
    if with_check:
        assets.append(silver_quality_check)
    selection = [silver_hourly_weather]
    if with_check:
        selection = dg.AssetSelection.assets(silver_hourly_weather) | dg.AssetSelection.checks(
            silver_quality_check
        )
    return dg.materialize(
        assets,
        partition_key=partition_key,
        selection=selection,
        resources={"open_meteo_client": None},
    )


class TestParseHourlyData:
    def test_filters_to_partition_date(self):
        raw = make_api_response(date(2026, 10, 20), days=3)

        records = _parse(raw)

        assert len(records) == 24
        assert {r["timestamp_utc"].date() for r in records} == {date(2026, 10, 21)}

    def test_timestamps_are_utc_with_local_conversion(self):
        records = _parse(make_api_response(date(2026, 10, 21)))

        first = records[0]
        assert first["timestamp_utc"] == datetime(2026, 10, 21, 0, tzinfo=timezone.utc)
        # Adelaide is on daylight time (UTC+10:30) in late October
        assert first["timestamp_local"] == datetime(2026, 10, 21, 10, 30)
        assert first["timestamp_local"].tzinfo is None

    def test_missing_variable_becomes_none(self):
        raw = make_api_response(date(2026, 10, 21), variables=["temperature_2m"])

        records = _parse(raw)

        assert records[0]["temperature_2m"] == 15.0
        assert all(records[0][v] is None for v in WEATHER_COLUMNS if v != "temperature_2m")

    def test_empty_response(self):
        assert _parse({"hourly": {}}) == []

    def test_rejects_non_utc_response(self):
        raw = make_api_response(date(2026, 10, 21))
        raw["utc_offset_seconds"] = 37800

        with pytest.raises(ValueError, match="UTC"):
            _parse(raw)


class TestSilverAsset:
    def test_writes_clean_parquet(self, data_dir, locations, past_date, ingestion_ts):
        pk = past_date.isoformat()
        archive = make_api_response(
            past_date, variables=[v for v in WEATHER_COLUMNS if v != "precipitation_probability"]
        )
        for loc in locations:
            write_bronze_file(data_dir, loc.name, pk, [(archive, ingestion_ts)])

        result = _materialize(pk)

        assert result.success
        df = pd.read_parquet(data_dir / "silver" / f"{pk}.parquet")
        assert list(df.columns) == SILVER_COLUMNS
        assert len(df) == 24 * len(locations)
        assert str(df["timestamp_utc"].dt.tz) == "UTC"
        assert all(df[c].dtype == "float64" for c in WEATHER_COLUMNS)
        # Archive data has no precipitation_probability: nulls are preserved
        assert df["precipitation_probability"].isna().all()
        assert df["uv_index"].isna().sum() == 6 * len(locations)
        assert (df["ingestion_timestamp"] == pd.Timestamp(ingestion_ts)).all()
        [evaluation] = result.get_asset_check_evaluations()
        assert evaluation.passed

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

        result = _materialize(pk)

        assert result.success
        df = pd.read_parquet(data_dir / "silver" / f"{pk}.parquet")
        assert len(df) == 24 * len(locations)
        assert df["temperature_2m"].min() == 30.0
        assert result.asset_materializations_for_node("silver_hourly_weather")[0].metadata[
            "duplicates_removed"
        ].value == 24 * len(locations)

    def test_invalid_records_are_skipped(self, data_dir, locations, past_date, ingestion_ts):
        pk = past_date.isoformat()
        raw = make_api_response(past_date)
        raw["hourly"]["temperature_2m"][5] = "not-a-number"
        for loc in locations:
            write_bronze_file(data_dir, loc.name, pk, [(raw, ingestion_ts)])

        result = _materialize(pk, with_check=False)

        assert result.success
        df = pd.read_parquet(data_dir / "silver" / f"{pk}.parquet")
        assert len(df) == 23 * len(locations)

    def test_missing_bronze_writes_empty_file(self, data_dir, past_date):
        pk = past_date.isoformat()

        result = _materialize(pk)

        df = pd.read_parquet(data_dir / "silver" / f"{pk}.parquet")
        assert df.empty
        assert list(df.columns) == SILVER_COLUMNS
        [evaluation] = result.get_asset_check_evaluations()
        assert not evaluation.passed


class TestSilverQualityCheck:
    def test_out_of_range_values_fail(self, data_dir, locations, past_date, ingestion_ts):
        pk = past_date.isoformat()
        raw = make_api_response(past_date)
        raw["hourly"]["relative_humidity_2m"][0] = 150.0
        for loc in locations:
            write_bronze_file(data_dir, loc.name, pk, [(raw, ingestion_ts)])

        result = _materialize(pk)

        [evaluation] = result.get_asset_check_evaluations()
        assert not evaluation.passed
        assert "relative_humidity_2m out of range" in evaluation.description
