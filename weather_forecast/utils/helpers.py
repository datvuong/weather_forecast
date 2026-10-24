"""Shared utilities: YAML configuration loaders and helper functions."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

from weather_forecast.models.weather import (
    APISettings,
    LocationConfig,
    LocationsConfig,
    VariableDefinition,
    VariablesConfig,
)


def _get_config_dir() -> Path:
    """Return the path to the config/ directory.

    Looks for config/ relative to the project root. The project root is
    determined by walking up from this file's location until we find
    pyproject.toml, or falls back to CWD.
    """
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

    with open(config_path) as f:
        raw = yaml.safe_load(f)

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

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    return VariablesConfig(
        hourly_variables=[VariableDefinition(**v) for v in raw["hourly_variables"]],
        settings=APISettings(**raw.get("settings", {})),
    )


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
    """Return the path to the data/ directory."""
    return get_project_root() / "data"


def get_warehouse_dir() -> Path:
    """Return the path to the warehouse/ directory."""
    return get_project_root() / "warehouse"
