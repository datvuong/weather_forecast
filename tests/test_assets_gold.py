"""Tests for the Gold layer aggregations and DuckDB upserts."""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import dagster as dg
import pandas as pd
import pytest

from weather_forecast.assets.gold import (
    gold_daily_precipitation,
    gold_daily_temperature,
    gold_temperature_check,
    load_local_day_hourly,
    validate_gold_requirements,
)
from weather_forecast.assets.silver import section_columns, silver_hourly_weather, silver_path
from weather_forecast.utils.helpers import load_variables_config


def _utc_midnight(day):
    return datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)


def _write_silver(data_dir, locations, partition_date, with_location_id=True, days=(-1, 0, 1)):
    """Write Silver hourly files for the UTC days around partition_date.

    Location i has temperature = 10*i + n/2 for the n-th hour of the 3-day
    window, so local-day and UTC-day aggregates differ. Precipitation (2 mm),
    UV (None after noon) and wind (= local hour) follow the LOCAL clock.
    Returns all rows written.
    """
    frames = []
    columns = section_columns("hourly", load_variables_config().hourly_names)
    first = _utc_midnight(partition_date - timedelta(days=1))
    for offset in days:
        day = partition_date + timedelta(days=offset)
        rows = []
        for i, loc in enumerate(locations):
            for h in range(24):
                ts_utc = _utc_midnight(day) + timedelta(hours=h)
                local = ts_utc.astimezone(ZoneInfo(loc.timezone)).replace(tzinfo=None)
                n = int((ts_utc - first).total_seconds() // 3600)
                rows.append(
                    {
                        "location_id": loc.id if with_location_id else None,
                        "location_name": loc.name,
                        "latitude": loc.latitude,
                        "longitude": loc.longitude,
                        "timestamp_utc": ts_utc,
                        "timestamp_local": local,
                        "temperature_2m": 10.0 * i + n / 2,
                        "relative_humidity_2m": 50.0,
                        "precipitation": 2.0 if local.hour < 6 else 0.0,
                        "precipitation_probability": None,
                        "wind_speed_10m": float(local.hour),
                        "wind_direction_10m": 90.0,
                        "wind_gusts_10m": 30.0,
                        "uv_index": None if local.hour > 12 else 5.0,
                        "partition_date": day.isoformat(),
                        "ingestion_timestamp": first,
                    }
                )
        df = pd.DataFrame(rows, columns=columns)
        path = silver_path("hourly", day.isoformat())
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _local_day(df, loc, day):
    rows = df[df["location_name"] == loc.name]
    return rows[rows["timestamp_local"].dt.date == day]


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


def test_daily_temperature_over_local_day(
    data_dir, duckdb_resource, locations, past_date, silver_df
):
    pk = past_date.isoformat()

    result = _materialize(pk, duckdb_resource)

    assert result.success
    df = duckdb_resource.query("SELECT * FROM gold_daily_temperature ORDER BY location_name")
    assert len(df) == len(locations)
    for loc in locations:
        expected = _local_day(silver_df, loc, past_date)
        row = df[df["location_name"] == loc.name].iloc[0]
        assert row["date"] == pk
        assert row["location_id"] == loc.id
        assert row["hours_covered"] == 24
        assert row["temp_min_c"] == expected["temperature_2m"].min()
        assert row["temp_max_c"] == expected["temperature_2m"].max()
        assert row["temp_avg_c"] == pytest.approx(expected["temperature_2m"].mean(), abs=0.01)
        daytime = expected[expected["timestamp_local"].dt.hour.between(6, 18)]
        assert row["daytime_temp_min_c"] == daytime["temperature_2m"].min()
        assert row["daytime_temp_max_c"] == daytime["temperature_2m"].max()
        assert row["wind_max_kmh"] == 23.0
        assert row["uv_max"] == 5.0
        # Proves the grain: the UTC day's maximum would be different
        utc_day = silver_df[
            (silver_df["location_name"] == loc.name)
            & (silver_df["timestamp_utc"].dt.date == past_date)
        ]
        assert row["temp_max_c"] != utc_day["temperature_2m"].max()
    parquet = pd.read_parquet(data_dir / "gold" / "daily_temperature" / f"{pk}.parquet")
    assert len(parquet) == len(locations)
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed, evaluation.description


def test_daily_precipitation_over_local_day(
    data_dir, duckdb_resource, locations, past_date, silver_df
):
    pk = past_date.isoformat()

    result = _materialize(pk, duckdb_resource)

    assert result.success
    df = duckdb_resource.query("SELECT * FROM gold_daily_precipitation")
    assert len(df) == len(locations)
    for _, row in df.iterrows():
        # Rain falls in local hours 00-05: exactly 6 rainy hours in the local day
        assert row["total_precipitation_mm"] == 12.0
        assert row["rainy_hours"] == 6
        assert row["total_hours"] == 24
        assert row["pct_hours_with_rain"] == 25.0
        # Archive-style data: probability is null, not zero
        assert pd.isna(row["max_precip_probability_pct"])
    assert (data_dir / "gold" / "daily_precipitation" / f"{pk}.parquet").exists()


def test_local_day_hours_across_dst_start(data_dir, locations):
    # Adelaide and Melbourne move clocks forward on 2026-10-04: a 23-hour local day.
    # Calls the loader directly so the test doesn't depend on the rolling partition range.
    silver = _write_silver(data_dir, locations, date(2026, 10, 4))

    hourly = load_local_day_hourly("2026-10-04")

    counts = hourly.groupby("location_id").size().to_dict()
    assert counts == {loc.id: 23 for loc in locations}
    assert len(silver) == 3 * 24 * len(locations)


def test_missing_neighbouring_day_is_flagged(data_dir, duckdb_resource, locations, past_date):
    # Only the partition's own UTC day exists: Australian local days also need the day before
    _write_silver(data_dir, locations, past_date, days=(0,))

    result = _materialize(past_date.isoformat(), duckdb_resource)

    assert result.success
    hours = duckdb_resource.query("SELECT hours_covered FROM gold_daily_temperature")
    assert (hours["hours_covered"] < 23).all()
    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert evaluation.severity == dg.AssetCheckSeverity.WARN
    assert "hours of the local day" in evaluation.description


def test_disagreement_with_silver_daily_is_a_warning(
    data_dir, duckdb_resource, locations, past_date, silver_df
):
    pk = past_date.isoformat()
    expected_max = _local_day(silver_df, locations[0], past_date)["temperature_2m"].max()
    daily = pd.DataFrame(
        {
            "location_id": [locations[0].id],
            "location_name": [locations[0].name],
            "temperature_2m_max": [expected_max + 2.0],
            "temperature_2m_min": [None],
        }
    )
    path = silver_path("daily", pk)
    path.parent.mkdir(parents=True, exist_ok=True)
    daily.to_parquet(path, index=False)

    result = _materialize(pk, duckdb_resource)

    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert evaluation.severity == dg.AssetCheckSeverity.WARN
    assert evaluation.metadata["warnings"].value == [
        f"{locations[0].id}: temp_max_c {expected_max} vs API temperature_2m_max "
        f"{expected_max + 2.0}"
    ]


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


class TestGoldRequirements:
    def test_project_config_is_valid(self):
        validate_gold_requirements(load_variables_config())

    def test_extra_variables_are_fine(self, variables_config):
        config = load_variables_config()
        hourly = [v.model_dump() for v in config.hourly_variables]
        hourly.append({"name": "visibility", "description": "v", "unit": "m"})

        validate_gold_requirements(variables_config(hourly_variables=hourly))

    def test_missing_required_variable(self, variables_config):
        hourly = [
            v.model_dump()
            for v in load_variables_config().hourly_variables
            if v.name not in ("temperature_2m", "precipitation_probability")
        ]
        config = variables_config(hourly_variables=hourly)

        with pytest.raises(ValueError) as exc_info:
            validate_gold_requirements(config)

        message = str(exc_info.value)
        assert "gold_daily_temperature needs hourly variables ['temperature_2m']" in message
        assert "gold_daily_precipitation needs hourly variables" in message

    def test_non_metric_units(self, variables_config):
        settings = load_variables_config().settings.model_dump()
        settings.update(temperature_unit="fahrenheit", wind_speed_unit="mph")
        config = variables_config(settings=settings)

        with pytest.raises(ValueError, match="temperature_unit='celsius', got 'fahrenheit'"):
            validate_gold_requirements(config)


def test_gold_tables_carry_location_id(
    data_dir, duckdb_resource, locations, past_date, silver_df
):
    _materialize(past_date.isoformat(), duckdb_resource)

    for table in ("gold_daily_temperature", "gold_daily_precipitation"):
        ids = duckdb_resource.query(f"SELECT location_id FROM {table} ORDER BY 1")["location_id"]
        assert ids.tolist() == sorted(loc.id for loc in locations)


def test_old_silver_without_location_id_is_filled_from_config(
    data_dir, duckdb_resource, locations, past_date
):
    _write_silver(data_dir, locations, past_date, with_location_id=False)

    result = _materialize(past_date.isoformat(), duckdb_resource)

    assert result.success
    ids = duckdb_resource.query("SELECT location_id FROM gold_daily_temperature ORDER BY 1")
    assert ids["location_id"].tolist() == sorted(loc.id for loc in locations)


def test_outdated_gold_table_is_recreated(
    data_dir, duckdb_resource, locations, past_date, silver_df
):
    # A table from before location_id existed
    duckdb_resource.execute(
        "CREATE TABLE gold_daily_temperature (location_name VARCHAR, date VARCHAR, "
        "temp_max_c DOUBLE, PRIMARY KEY (location_name, date))"
    )
    duckdb_resource.execute("INSERT INTO gold_daily_temperature VALUES ('Old', '2000-01-01', 1.0)")

    result = _materialize(past_date.isoformat(), duckdb_resource)

    assert result.success
    df = duckdb_resource.query("SELECT * FROM gold_daily_temperature")
    assert list(df.columns)[:3] == ["location_id", "location_name", "date"]
    assert "Old" not in set(df["location_name"])  # derived rows are rebuilt by the backfill
