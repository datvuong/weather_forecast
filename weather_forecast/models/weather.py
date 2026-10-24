"""Pydantic data models for weather forecast pipeline.

Defines strict schemas for:
- Configuration (locations, variables, API settings)
- API responses (raw Open-Meteo JSON structure)
- Domain models (cleaned hourly weather records)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator

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


class APISettings(BaseModel):
    """API query settings.

    Parsed from the 'settings' section of config/variables.yaml.
    """

    temperature_unit: str = Field(default="celsius")
    wind_speed_unit: str = Field(default="kmh")
    precipitation_unit: str = Field(default="mm")
    forecast_days: int = Field(default=7, ge=1, le=16)
    backfill_days: int = Field(default=30, ge=1, le=365)


class VariablesConfig(BaseModel):
    """Full variables configuration file.

    Parsed from config/variables.yaml.
    """

    hourly_variables: list[VariableDefinition]
    settings: APISettings = Field(default_factory=APISettings)

    @property
    def variable_names(self) -> list[str]:
        """Return list of API parameter names."""
        return [v.name for v in self.hourly_variables]


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
