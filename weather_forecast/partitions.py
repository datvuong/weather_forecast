"""Partition definitions for the weather forecast pipeline.

Uses daily partitions so each partition represents one day of weather data.
The start date is set backfill_days (from config) before today.
"""

from __future__ import annotations

from datetime import timedelta

from dagster import DailyPartitionsDefinition

from weather_forecast.utils.helpers import load_variables_config, utc_today

# Calculate start date: backfill_days (default 30) back from today (UTC)
_config = load_variables_config()
_start_date = utc_today() - timedelta(days=_config.settings.backfill_days)

daily_partitions = DailyPartitionsDefinition(
    start_date=_start_date.isoformat(),
    timezone="UTC",
    # Include today's (still in-progress) partition so the hourly schedule can target it
    end_offset=1,
)
