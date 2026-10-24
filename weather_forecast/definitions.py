"""Dagster definitions entry point.

This module wires together all assets, resources, schedules, and asset checks
for the weather forecast pipeline. Dagster discovers this module via the
[tool.dagster] section in pyproject.toml.
"""

import dagster as dg

from weather_forecast.assets.bronze import bronze_file_exists_check, bronze_forecast
from weather_forecast.assets.silver import silver_hourly_weather, silver_quality_check
from weather_forecast.resources.api_client import OpenMeteoClient

defs = dg.Definitions(
    assets=[
        bronze_forecast,
        silver_hourly_weather,
    ],
    asset_checks=[
        bronze_file_exists_check,
        silver_quality_check,
    ],
    resources={
        "open_meteo_client": OpenMeteoClient(),
    },
)
