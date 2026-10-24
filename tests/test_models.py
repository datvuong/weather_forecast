"""Tests for Pydantic models and YAML config loaders."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from weather_forecast.models.weather import (
    APISettings,
    BronzeRecord,
    HourlyWeatherRecord,
    LocationConfig,
)
from weather_forecast.utils.helpers import (
    get_data_dir,
    load_locations_config,
    load_variables_config,
)


def _record(**overrides):
    base = {
        "location_name": "Adelaide",
        "latitude": -34.9,
        "longitude": 138.6,
        "timestamp_utc": datetime(2026, 10, 21, 0, tzinfo=timezone.utc),
        "timestamp_local": datetime(2026, 10, 21, 10, 30),
        "partition_date": "2026-10-21",
        "ingestion_timestamp": datetime(2026, 10, 24, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


class TestLocationConfig:
    def test_valid(self):
        loc = LocationConfig(name="Sydney", latitude=-33.87, longitude=151.21, timezone="UTC")
        assert loc.name == "Sydney"

    @pytest.mark.parametrize("lat,lon", [(-91, 0), (91, 0), (0, -181), (0, 181)])
    def test_out_of_range_coordinates(self, lat, lon):
        with pytest.raises(ValidationError):
            LocationConfig(name="X", latitude=lat, longitude=lon, timezone="UTC")


class TestAPISettings:
    def test_defaults(self):
        s = APISettings()
        assert (s.forecast_days, s.backfill_days) == (7, 30)

    @pytest.mark.parametrize("days", [0, 17])
    def test_forecast_days_bounds(self, days):
        with pytest.raises(ValidationError):
            APISettings(forecast_days=days)


class TestHourlyWeatherRecord:
    def test_weather_fields_default_to_none(self):
        rec = HourlyWeatherRecord(**_record())
        assert rec.temperature_2m is None
        assert rec.uv_index is None

    def test_numeric_coercion(self):
        rec = HourlyWeatherRecord(**_record(temperature_2m="21.5", relative_humidity_2m=60))
        assert rec.temperature_2m == 21.5
        assert isinstance(rec.relative_humidity_2m, float)

    def test_non_numeric_value_rejected(self):
        with pytest.raises(ValidationError):
            HourlyWeatherRecord(**_record(temperature_2m="hot"))

    @pytest.mark.parametrize("bad", ["2026/10/21", "21-10-2026", "2026-13-01", ""])
    def test_invalid_partition_date(self, bad):
        with pytest.raises(ValidationError):
            HourlyWeatherRecord(**_record(partition_date=bad))


class TestBronzeRecord:
    @pytest.mark.parametrize("endpoint", ["forecast", "archive"])
    def test_valid_endpoints(self, endpoint):
        rec = BronzeRecord(
            raw_json="{}",
            location_name="Adelaide",
            latitude=0,
            longitude=0,
            partition_date="2026-10-21",
            ingestion_timestamp=datetime.now(timezone.utc),
            api_endpoint=endpoint,
        )
        assert rec.api_endpoint == endpoint

    def test_invalid_endpoint(self):
        with pytest.raises(ValidationError):
            BronzeRecord(
                raw_json="{}",
                location_name="Adelaide",
                latitude=0,
                longitude=0,
                partition_date="2026-10-21",
                ingestion_timestamp=datetime.now(timezone.utc),
                api_endpoint="historical",
            )


class TestConfigLoaders:
    def test_project_locations(self):
        names = [loc.name for loc in load_locations_config()]
        assert "Melbourne" in names and "Adelaide" in names

    def test_project_variables(self):
        config = load_variables_config()
        assert "temperature_2m" in config.variable_names
        assert config.settings.forecast_days >= 1

    def test_custom_locations_file(self, tmp_path):
        path = tmp_path / "locations.yaml"
        path.write_text(
            "locations:\n"
            "  - name: Sydney\n"
            "    latitude: -33.8688\n"
            "    longitude: 151.2093\n"
            "    timezone: Australia/Sydney\n"
        )
        [loc] = load_locations_config(path)
        assert loc.name == "Sydney"

    def test_custom_variables_file_without_settings(self, tmp_path):
        path = tmp_path / "variables.yaml"
        path.write_text(
            "hourly_variables:\n"
            "  - name: cloud_cover\n"
            "    description: Total cloud cover\n"
            "    unit: '%'\n"
        )
        config = load_variables_config(path)
        assert config.variable_names == ["cloud_cover"]
        assert config.settings == APISettings()

    def test_data_dir_override(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WEATHER_FORECAST_DATA_DIR", str(tmp_path))
        assert get_data_dir() == tmp_path
