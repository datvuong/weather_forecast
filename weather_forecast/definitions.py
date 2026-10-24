"""Dagster definitions entry point.

This module wires together all assets, resources, schedules, and asset checks
for the weather forecast pipeline. Dagster discovers this module via the
[tool.dagster] section in pyproject.toml.
"""

import dagster as dg

from weather_forecast.assets.bronze import bronze_file_exists_check, bronze_forecast
from weather_forecast.assets.current import (
    bronze_current,
    gold_current_conditions,
    gold_current_freshness_check,
    silver_current_check,
    silver_current_weather,
)
from weather_forecast.assets.dimensions import (
    current_weather_code_check,
    daily_weather_code_check,
    dim_location,
    dim_location_check,
    dim_weather_code,
    hourly_weather_code_check,
)
from weather_forecast.assets.gold import (
    gold_daily_precipitation,
    gold_daily_temperature,
    gold_temperature_check,
    validate_gold_requirements,
)
from weather_forecast.assets.silver import (
    silver_daily_check,
    silver_daily_weather,
    silver_hourly_weather,
    silver_quality_check,
)
from weather_forecast.resources.api_client import OpenMeteoClient
from weather_forecast.resources.duckdb_resource import DuckDBResource
from weather_forecast.schedules import (
    current_weather_job,
    current_weather_schedule,
    hourly_weather_schedule,
    weather_pipeline_job,
)
from weather_forecast.utils.helpers import (
    get_warehouse_dir,
    load_locations_config,
    load_variables_config,
    warn_if_over_api_budget,
)

validate_gold_requirements(load_variables_config())
warn_if_over_api_budget(len(load_locations_config()), load_variables_config())

defs = dg.Definitions(
    assets=[
        bronze_forecast,
        silver_hourly_weather,
        silver_daily_weather,
        gold_daily_temperature,
        gold_daily_precipitation,
        bronze_current,
        silver_current_weather,
        gold_current_conditions,
        dim_weather_code,
        dim_location,
    ],
    asset_checks=[
        bronze_file_exists_check,
        silver_quality_check,
        silver_daily_check,
        gold_temperature_check,
        silver_current_check,
        gold_current_freshness_check,
        hourly_weather_code_check,
        daily_weather_code_check,
        current_weather_code_check,
        dim_location_check,
    ],
    jobs=[weather_pipeline_job, current_weather_job],
    schedules=[hourly_weather_schedule, current_weather_schedule],
    resources={
        "open_meteo_client": OpenMeteoClient(),
        "duckdb_resource": DuckDBResource(
            database_path=str(get_warehouse_dir() / "weather.duckdb")
        ),
    },
)
