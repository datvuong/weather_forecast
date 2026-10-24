"""Partition definitions for the weather forecast pipeline.

Uses daily partitions so each partition represents one day of weather data.
The start date is set to 30 days before the configured backfill period.
"""

from __future__ import annotations

from datetime import date, timedelta

from dagster import DailyPartitionsDefinition

from weather_forecast.utils.helpers import load_variables_config

# Calculate start date: 30 days back from today (or from config backfill_days)
_config = load_variables_config()
_start_date = date.today() - timedelta(days=_config.settings.backfill_days)

daily_partitions = DailyPartitionsDefinition(
    start_date=_start_date.isoformat(),
    timezone="UTC",
)
