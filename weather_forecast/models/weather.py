"""Pydantic data models for weather forecast pipeline.

Defines strict schemas for:
- Configuration (locations, variables, API settings)
- API responses (raw Open-Meteo JSON structure)
- Domain models (cleaned hourly weather records)
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

# Query parameters the pipeline sets itself; they can't be overridden via extra_params
RESERVED_API_PARAMS = frozenset(
    {
        "latitude",
        "longitude",
        "hourly",
        "daily",
        "current",
        "timezone",
        "timeformat",
        "start_date",
        "end_date",
        "forecast_days",
        "past_days",
    }
)

# Key/metadata columns of the Silver tables (and raw API time fields); variable
# names must not collide with them
RESERVED_COLUMN_NAMES = frozenset(
    {
        "time",
        "interval",
        "location_name",
        "latitude",
        "longitude",
        "timestamp_utc",
        "timestamp_local",
        "date_local",
        "observed_at_utc",
        "observed_at_local",
        "interval_seconds",
        "partition_date",
        "ingestion_timestamp",
    }
)

# ---------------------------------------------------------------------------
# Configuration models (parsed from YAML)
# ---------------------------------------------------------------------------


class LocationConfig(BaseModel):
    """A single location to track weather for.

    Parsed from config/locations.yaml.
    """

    name: str = Field(..., description="Human-readable city name")
    latitude: float = Field(..., ge=-90, le=90, description="WGS84 latitude")
    longitude: float = Field(..., ge=-180, le=180, description="WGS84 longitude")
    timezone: str = Field(..., description="IANA timezone string")


class VariableDefinition(BaseModel):
    """A single weather variable to request from the API.

    Parsed from config/variables.yaml.
    """

    name: str = Field(..., description="Open-Meteo API parameter name")
    description: str = Field(..., description="Human-readable description")
    unit: str = Field(..., description="Unit of measurement")
    valid_range: Optional[tuple[float, float]] = Field(
        default=None, description="Inclusive [min, max] used by Silver quality checks"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Reject names that collide with Silver key columns."""
        if v in RESERVED_COLUMN_NAMES:
            raise ValueError(f"variable name '{v}' is reserved for a Silver key column")
        return v

    @field_validator("valid_range")
    @classmethod
    def validate_range(cls, v: Optional[tuple[float, float]]) -> Optional[tuple[float, float]]:
        """Ensure min <= max."""
        if v is not None and v[0] > v[1]:
            raise ValueError(f"valid_range min must be <= max, got {list(v)}")
        return v


class APISettings(BaseModel):
    """API query settings.

    Parsed from the 'settings' section of config/variables.yaml.
    """

    temperature_unit: Literal["celsius", "fahrenheit"] = "celsius"
    wind_speed_unit: Literal["kmh", "ms", "mph", "kn"] = "kmh"
    precipitation_unit: Literal["mm", "inch"] = "mm"
    forecast_days: int = Field(default=7, ge=1, le=16)
    backfill_days: int = Field(default=30, ge=1, le=365)
    current_interval_minutes: Literal[15, 30, 60] = Field(
        default=15, description="Poll cadence for current weather"
    )
    extra_params: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
        description="Additional Open-Meteo query parameters, e.g. models, cell_selection",
    )

    @field_validator("extra_params")
    @classmethod
    def validate_extra_params(
        cls, v: dict[str, str | int | float | bool]
    ) -> dict[str, str | int | float | bool]:
        """Reject parameters the pipeline controls itself."""
        reserved = sorted(RESERVED_API_PARAMS & v.keys())
        if reserved:
            raise ValueError(f"extra_params cannot set pipeline-controlled parameters: {reserved}")
        return v

    @property
    def request_params(self) -> dict[str, str | int | float | bool]:
        """Unit settings plus extra_params, as Open-Meteo query parameters."""
        return {
            "temperature_unit": self.temperature_unit,
            "wind_speed_unit": self.wind_speed_unit,
            "precipitation_unit": self.precipitation_unit,
            **self.extra_params,
        }


class VariablesConfig(BaseModel):
    """Full variables configuration file.

    Parsed from config/variables.yaml.
    """

    hourly_variables: list[VariableDefinition]
    daily_variables: list[VariableDefinition] = Field(default_factory=list)
    current_variables: list[VariableDefinition] = Field(default_factory=list)
    settings: APISettings = Field(default_factory=APISettings)

    @model_validator(mode="after")
    def validate_unique_names(self) -> VariablesConfig:
        """Reject duplicate variable names within a section."""
        for section in ("hourly", "daily", "current"):
            names = self.names(section)
            duplicates = sorted({n for n in names if names.count(n) > 1})
            if duplicates:
                raise ValueError(f"duplicate {section} variables: {duplicates}")
        return self

    def section(self, section: Literal["hourly", "daily", "current"]) -> list[VariableDefinition]:
        """Return the variable definitions of one request section."""
        return getattr(self, f"{section}_variables")

    def names(self, section: Literal["hourly", "daily", "current"]) -> list[str]:
        """Return the API parameter names of one request section."""
        return [v.name for v in self.section(section)]

    @property
    def hourly_names(self) -> list[str]:
        return self.names("hourly")

    @property
    def daily_names(self) -> list[str]:
        return self.names("daily")

    @property
    def current_names(self) -> list[str]:
        return self.names("current")

    @property
    def variable_names(self) -> list[str]:
        """Return list of hourly API parameter names (alias of hourly_names)."""
        return self.hourly_names


class LocationsConfig(BaseModel):
    """Full locations configuration file.

    Parsed from config/locations.yaml.
    """

    locations: list[LocationConfig]


# ---------------------------------------------------------------------------
# Domain models (cleaned data)
# ---------------------------------------------------------------------------


class HourlyWeatherRecord(BaseModel):
    """A single hourly weather observation — used in the Silver layer.

    All weather measurement fields are Optional because:
    - Some variables may not be available (e.g., precipitation_probability
      is forecast-only and null for historical data).
    - Some values are legitimately null (e.g., UV index at night).
    """

    location_name: str
    latitude: float
    longitude: float
    timestamp_utc: datetime
    timestamp_local: datetime
    temperature_2m: Optional[float] = None
    relative_humidity_2m: Optional[float] = None
    precipitation: Optional[float] = None
    precipitation_probability: Optional[float] = None
    wind_speed_10m: Optional[float] = None
    wind_direction_10m: Optional[float] = None
    wind_gusts_10m: Optional[float] = None
    uv_index: Optional[float] = None
    partition_date: str
    ingestion_timestamp: datetime

    @field_validator("partition_date")
    @classmethod
    def validate_partition_date(cls, v: str) -> str:
        """Ensure partition_date is in YYYY-MM-DD format."""
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"partition_date must be YYYY-MM-DD format, got: {v}")
        return v


# ---------------------------------------------------------------------------
# Bronze layer models (raw storage metadata)
# ---------------------------------------------------------------------------


class BronzeRecord(BaseModel):
    """Metadata for a raw Bronze layer Parquet record.

    The raw_json field contains the full API response body.
    """

    raw_json: str
    location_name: str
    latitude: float
    longitude: float
    partition_date: str
    ingestion_timestamp: datetime
    api_endpoint: str = Field(..., pattern=r"^(forecast|archive)$")
