"""Tests for dimension assets: dim_weather_code and the weather code coverage checks."""

import dagster as dg
import pandas as pd
import pytest
from pydantic import ValidationError

from tests.conftest import make_api_response, write_bronze_file
from weather_forecast.assets.bronze import bronze_forecast
from weather_forecast.assets.dimensions import (
    daily_weather_code_check,
    dim_weather_code,
    dimension_path,
    hourly_weather_code_check,
)
from weather_forecast.assets.silver import silver_daily_weather, silver_hourly_weather
from weather_forecast.models.weather import WeatherCodesConfig
from weather_forecast.utils.helpers import load_weather_codes

INGESTED = pd.Timestamp("2026-10-24", tz="UTC").to_pydatetime()


class TestWeatherCodeSeed:
    def test_matches_open_meteo_table(self):
        codes = {c.code: c for c in load_weather_codes().weather_codes}

        # Open-Meteo's published table (2026-10): 29 codes incl. 97 "Heavy thunderstorm"
        assert sorted(codes) == [
            0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67,
            71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 97, 99,
        ]
        assert codes[0].description == "Clear sky"
        assert codes[97].description == "Heavy thunderstorm"
        assert codes[99].description == "Thunderstorm with heavy hail"

    def test_classification(self):
        codes = {c.code: c for c in load_weather_codes().weather_codes}

        assert (codes[0].condition_group, codes[0].is_precipitation) == ("clear", False)
        assert (codes[65].condition_group, codes[65].intensity) == ("rain", "heavy")
        assert codes[66].is_freezing and codes[48].is_freezing and not codes[45].is_freezing
        assert codes[82].intensity == "violent"
        assert codes[96].precipitation_type == "hail" and codes[96].note
        assert all(c.is_thunderstorm == (c.code >= 95) for c in codes.values())
        ranks = [codes[c].severity_rank for c in sorted(codes)]
        assert ranks == sorted(ranks) == list(range(len(codes)))

    def _code(self, **overrides):
        return {
            "code": 0,
            "description": "Clear sky",
            "condition_group": "clear",
            "precipitation_type": "none",
            "intensity": "none",
            "is_precipitation": False,
            "is_freezing": False,
            "is_thunderstorm": False,
            "severity_rank": 0,
            **overrides,
        }

    def test_duplicate_codes_rejected(self):
        with pytest.raises(ValidationError, match="duplicate weather code codes"):
            WeatherCodesConfig(weather_codes=[self._code(), self._code(severity_rank=1)])

    @pytest.mark.parametrize(
        "field,value", [("condition_group", "sunny"), ("intensity", "extreme"), ("code", 120)]
    )
    def test_invalid_values_rejected(self, field, value):
        with pytest.raises(ValidationError):
            WeatherCodesConfig(weather_codes=[self._code(**{field: value})])


def test_dim_weather_code_asset(data_dir, duckdb_resource):
    result = dg.materialize([dim_weather_code], resources={"duckdb_resource": duckdb_resource})

    assert result.success
    table = duckdb_resource.query("SELECT * FROM dim_weather_code ORDER BY weather_code")
    assert len(table) == 29
    assert table.loc[table["weather_code"] == 61, "description"].item() == "Slight rain"
    parquet = pd.read_parquet(dimension_path("dim_weather_code"))
    assert parquet["weather_code"].dtype == "int32"
    assert list(parquet.columns) == list(table.columns)


def test_dim_weather_code_follows_edited_seed(
    data_dir, duckdb_resource, variables_config, tmp_path
):
    # Relabel a code in the (temp) seed: no code change needed
    seed = tmp_path / "config" / "weather_codes.yaml"
    seed.write_text(seed.read_text().replace('"Clear sky"', '"Ciel dégagé"'))
    load_weather_codes.cache_clear()

    dg.materialize([dim_weather_code], resources={"duckdb_resource": duckdb_resource})

    label = duckdb_resource.query("SELECT description FROM dim_weather_code WHERE weather_code = 0")
    assert label["description"].item() == "Ciel dégagé"


def _run_silver_with_codes(data_dir, locations, past_date, mutate=None):
    pk = past_date.isoformat()
    raw = make_api_response(
        past_date, daily_variables=["temperature_2m_max", "weather_code"]
    )
    if mutate:
        mutate(raw)
    for loc in locations:
        write_bronze_file(data_dir, loc.name, pk, [(raw, INGESTED)])
    return dg.materialize(
        [bronze_forecast, silver_hourly_weather, silver_daily_weather,
         hourly_weather_code_check, daily_weather_code_check],
        partition_key=pk,
        selection=dg.AssetSelection.assets(silver_hourly_weather, silver_daily_weather)
        | dg.AssetSelection.checks(hourly_weather_code_check, daily_weather_code_check),
        resources={"open_meteo_client": None},
    )


def test_coverage_checks_pass_for_known_codes(data_dir, locations, past_date):
    result = _run_silver_with_codes(data_dir, locations, past_date)

    evaluations = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert evaluations["hourly_weather_code_check"].passed
    assert evaluations["hourly_weather_code_check"].metadata["codes_seen"].value == [2, 61]
    assert evaluations["daily_weather_code_check"].passed


def test_coverage_check_fails_for_unknown_code(data_dir, locations, past_date):
    def add_unknown(raw):
        raw["hourly"]["weather_code"][5] = 42

    result = _run_silver_with_codes(data_dir, locations, past_date, add_unknown)

    evaluation = next(
        e for e in result.get_asset_check_evaluations()
        if e.check_name == "hourly_weather_code_check"
    )
    assert not evaluation.passed
    assert evaluation.metadata["unknown_codes"].value == [42]
    assert "config/weather_codes.yaml" in evaluation.description
