"""Tests for Pydantic models and YAML config loaders."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from weather_forecast.models.weather import (
    APISettings,
    BronzeRecord,
    LocationConfig,
    VariableDefinition,
    VariablesConfig,
    build_record_model,
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


def _var(name, **kwargs):
    return {"name": name, "description": name, "unit": "x", **kwargs}


class TestVariableDefinition:
    def test_valid_range(self):
        v = VariableDefinition(**_var("temperature_2m", valid_range=[-60, 65]))
        assert v.valid_range == (-60.0, 65.0)

    def test_valid_range_optional(self):
        assert VariableDefinition(**_var("sunrise")).valid_range is None

    def test_inverted_range_rejected(self):
        with pytest.raises(ValidationError, match="min must be <= max"):
            VariableDefinition(**_var("x", valid_range=[10, 0]))

    @pytest.mark.parametrize("name", ["time", "location_name", "timestamp_utc", "date_local"])
    def test_reserved_column_names_rejected(self, name):
        with pytest.raises(ValidationError, match="reserved"):
            VariableDefinition(**_var(name))


class TestAPISettings:
    def test_defaults(self):
        s = APISettings()
        assert (s.forecast_days, s.backfill_days) == (7, 30)
        assert s.current_interval_minutes == 15
        assert s.extra_params == {}

    @pytest.mark.parametrize("days", [0, 17])
    def test_forecast_days_bounds(self, days):
        with pytest.raises(ValidationError):
            APISettings(forecast_days=days)

    @pytest.mark.parametrize(
        "field,value",
        [("temperature_unit", "kelvin"), ("wind_speed_unit", "fps"), ("precipitation_unit", "cm")],
    )
    def test_invalid_units_rejected(self, field, value):
        with pytest.raises(ValidationError):
            APISettings(**{field: value})

    @pytest.mark.parametrize("minutes", [15, 30, 60])
    def test_current_interval_allowed(self, minutes):
        assert APISettings(current_interval_minutes=minutes).current_interval_minutes == minutes

    @pytest.mark.parametrize("minutes", [0, 5, 45, 120])
    def test_current_interval_rejected(self, minutes):
        with pytest.raises(ValidationError):
            APISettings(current_interval_minutes=minutes)

    def test_request_params_merge_units_and_extras(self):
        s = APISettings(wind_speed_unit="ms", extra_params={"models": "best_match"})
        assert s.request_params == {
            "temperature_unit": "celsius",
            "wind_speed_unit": "ms",
            "precipitation_unit": "mm",
            "models": "best_match",
        }

    @pytest.mark.parametrize("key", ["hourly", "timezone", "timeformat", "start_date", "latitude"])
    def test_reserved_extra_params_rejected(self, key):
        with pytest.raises(ValidationError, match="pipeline-controlled"):
            APISettings(extra_params={key: "x"})


class TestVariablesConfig:
    def test_sections_default_empty(self):
        config = VariablesConfig(hourly_variables=[_var("temperature_2m")])
        assert config.daily_names == []
        assert config.current_names == []
        assert config.variable_names == config.hourly_names == ["temperature_2m"]

    def test_section_accessors(self):
        config = VariablesConfig(
            hourly_variables=[_var("temperature_2m")],
            daily_variables=[_var("sunrise"), _var("precipitation_sum")],
            current_variables=[_var("is_day")],
        )
        assert config.names("daily") == ["sunrise", "precipitation_sum"]
        assert [v.name for v in config.section("current")] == ["is_day"]

    def test_same_name_allowed_across_sections(self):
        config = VariablesConfig(
            hourly_variables=[_var("temperature_2m")],
            current_variables=[_var("temperature_2m")],
        )
        assert config.current_names == ["temperature_2m"]

    def test_duplicate_within_section_rejected(self):
        with pytest.raises(ValidationError, match="duplicate daily variables"):
            VariablesConfig(
                hourly_variables=[_var("temperature_2m")],
                daily_variables=[_var("sunrise"), _var("sunrise")],
            )


HourlyRecord = build_record_model(
    "hourly", {"temperature_2m": False, "relative_humidity_2m": False, "uv_index": False}
)


class TestBuildRecordModel:
    def test_weather_fields_default_to_none(self):
        rec = HourlyRecord(**_record())
        assert rec.temperature_2m is None
        assert rec.uv_index is None

    def test_numeric_coercion(self):
        rec = HourlyRecord(**_record(temperature_2m="21.5", relative_humidity_2m=60))
        assert rec.temperature_2m == 21.5
        assert isinstance(rec.relative_humidity_2m, float)

    def test_non_numeric_value_rejected(self):
        with pytest.raises(ValidationError):
            HourlyRecord(**_record(temperature_2m="hot"))

    @pytest.mark.parametrize("bad", ["2026/10/21", "21-10-2026", "2026-13-01", ""])
    def test_invalid_partition_date(self, bad):
        with pytest.raises(ValidationError):
            HourlyRecord(**_record(partition_date=bad))

    def test_missing_key_rejected(self):
        record = _record()
        del record["timestamp_utc"]
        with pytest.raises(ValidationError):
            HourlyRecord(**record)

    def test_fields_follow_config(self):
        model = build_record_model("hourly", {"cloud_cover": False})
        assert "cloud_cover" in model.model_fields
        assert "temperature_2m" not in model.model_fields

    def test_time_valued_variable_parses_epoch(self):
        model = build_record_model("daily", {"sunrise": True, "precipitation_sum": False})
        rec = model(
            location_name="Adelaide",
            latitude=-34.9,
            longitude=138.6,
            date_local="2026-10-21",
            sunrise=1792439864,
            precipitation_sum=3,
            partition_date="2026-10-21",
            ingestion_timestamp=datetime(2026, 10, 24, tzinfo=timezone.utc),
        )
        # 06:27:44 local (UTC+10:30) on the 20th
        assert rec.sunrise == datetime(2026, 10, 19, 19, 57, 44, tzinfo=timezone.utc)
        assert rec.precipitation_sum == 3.0


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
        assert config.daily_names and config.current_names
        assert config.settings.forecast_days >= 1
        assert config.section("hourly")[0].valid_range is not None

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
