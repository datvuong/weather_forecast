"""Shared utilities: YAML configuration loaders and helper functions."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import yaml

from weather_forecast.models.weather import (
    LocationConfig,
    LocationsConfig,
    VariablesConfig,
    WeatherCodesConfig,
)

logger = logging.getLogger(__name__)


class _StrictLoader(yaml.SafeLoader):
    """YAML loader that rejects duplicate keys (PyYAML keeps the last one silently)."""


def _construct_mapping_no_duplicates(loader, node, deep=False):
    keys = [loader.construct_object(key_node, deep=deep) for key_node, _ in node.value]
    duplicates = sorted({str(k) for k in keys if keys.count(k) > 1})
    if duplicates:
        raise yaml.constructor.ConstructorError(
            None, None, f"duplicate keys {duplicates}", node.start_mark
        )
    return loader.construct_mapping(node, deep)


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping_no_duplicates
)


def load_yaml(path: str | Path) -> dict:
    """Load a YAML config file, rejecting duplicate keys."""
    with open(path, encoding="utf-8") as f:
        return yaml.load(f, Loader=_StrictLoader)

# Open-Meteo free tier
API_DAILY_CALL_LIMIT = 10_000
API_BUDGET_WARNING_RATIO = 0.8


def _get_config_dir() -> Path:
    """Return the path to the config/ directory.

    Can be overridden with the WEATHER_FORECAST_CONFIG_DIR environment variable.
    Otherwise looks for config/ relative to the project root. The project root
    is determined by walking up from this file's location until we find
    pyproject.toml, or falls back to CWD.
    """
    override = os.environ.get("WEATHER_FORECAST_CONFIG_DIR")
    if override:
        return Path(override)
    # Walk up from this file to find the project root
    current = Path(__file__).resolve().parent
    for _ in range(5):  # max 5 levels up
        if (current / "pyproject.toml").exists():
            return current / "config"
        current = current.parent

    # Fallback: relative to CWD
    return Path(os.getcwd()) / "config"


@lru_cache(maxsize=1)
def load_locations_config(config_path: str | Path | None = None) -> list[LocationConfig]:
    """Load and validate the locations configuration.

    Args:
        config_path: Optional explicit path to locations.yaml.
            If None, uses the default config/locations.yaml.

    Returns:
        List of validated LocationConfig objects.
    """
    if config_path is None:
        config_path = _get_config_dir() / "locations.yaml"
    else:
        config_path = Path(config_path)

    raw = load_yaml(config_path)

    config = LocationsConfig(**raw)
    return config.locations


@lru_cache(maxsize=1)
def load_variables_config(config_path: str | Path | None = None) -> VariablesConfig:
    """Load and validate the variables configuration.

    Args:
        config_path: Optional explicit path to variables.yaml.
            If None, uses the default config/variables.yaml.

    Returns:
        Validated VariablesConfig object.
    """
    if config_path is None:
        config_path = _get_config_dir() / "variables.yaml"
    else:
        config_path = Path(config_path)

    raw = load_yaml(config_path)

    return VariablesConfig.model_validate(raw)


@lru_cache(maxsize=1)
def load_weather_codes(config_path: str | Path | None = None) -> WeatherCodesConfig:
    """Load and validate the WMO weather code seed (config/weather_codes.yaml).

    Args:
        config_path: Optional explicit path. If None, uses the default config dir.

    Returns:
        Validated WeatherCodesConfig object.
    """
    if config_path is None:
        config_path = _get_config_dir() / "weather_codes.yaml"
    return WeatherCodesConfig.model_validate(load_yaml(config_path))


def get_variable_names(config_path: str | Path | None = None) -> list[str]:
    """Get the list of Open-Meteo variable names from config.

    Convenience wrapper around load_variables_config().
    """
    config = load_variables_config(config_path)
    return config.variable_names


def get_project_root() -> Path:
    """Return the project root directory (containing pyproject.toml)."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return Path(os.getcwd())


def get_data_dir() -> Path:
    """Return the path to the data/ directory.

    Can be overridden with the WEATHER_FORECAST_DATA_DIR environment variable.
    """
    override = os.environ.get("WEATHER_FORECAST_DATA_DIR")
    if override:
        return Path(override)
    return get_project_root() / "data"


def get_warehouse_dir() -> Path:
    """Return the path to the warehouse/ directory."""
    return get_project_root() / "warehouse"


def utc_today() -> date:
    """Return today's date in UTC — the timezone of the daily partitions."""
    return datetime.now(timezone.utc).date()


def epoch_to_utc(seconds: int | float) -> datetime:
    """Convert an Open-Meteo unixtime value (UTC epoch seconds) to an aware UTC datetime."""
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def daily_epoch_to_date(seconds: int | float, utc_offset_seconds: int) -> date:
    """Return the date of an Open-Meteo daily row in unixtime format.

    Open-Meteo applies one fixed UTC offset per response (`utc_offset_seconds`,
    the location's offset at request time) and stamps each daily row with
    local midnight under that offset. The date must therefore be recovered
    with the same fixed offset, as Open-Meteo documents ("apply
    utc_offset_seconds again"). Converting with the true per-date offset
    (zoneinfo) would label rows across a DST change one day off.
    """
    local = datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(seconds=utc_offset_seconds)
    return local.date()


def api_calls_per_day(num_locations: int, config: VariablesConfig) -> int:
    """Estimate scheduled Open-Meteo calls per day.

    The hourly pipeline makes one call per location per hour; current weather
    (if configured) one call per location per poll.
    """
    per_location = 24
    if config.current_names:
        per_location += 24 * 60 // config.settings.current_interval_minutes
    return num_locations * per_location


def warn_if_over_api_budget(num_locations: int, config: VariablesConfig) -> int:
    """Log a warning when scheduled calls exceed 80% of the daily free-tier limit."""
    calls = api_calls_per_day(num_locations, config)
    if calls > API_DAILY_CALL_LIMIT * API_BUDGET_WARNING_RATIO:
        logger.warning(
            "Scheduled Open-Meteo usage is ~%d calls/day for %d locations, above %d%% of the "
            "%d/day free-tier limit. Reduce locations or raise current_interval_minutes.",
            calls,
            num_locations,
            int(API_BUDGET_WARNING_RATIO * 100),
            API_DAILY_CALL_LIMIT,
        )
    return calls
