"""Schedule definitions for the weather forecast pipeline.

Defines two jobs, each with a schedule targeting today's (UTC) partition:
- weather_pipeline_job (hourly): the full Bronze → Silver → Gold chain for
  hourly and daily data. Also used for historical backfills.
- current_weather_job (every settings.current_interval_minutes): current
  conditions snapshots. Kept out of the pipeline job because they can't be
  backfilled.
"""

from datetime import timezone

from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from weather_forecast.assets.current import (
    bronze_current,
    gold_current_conditions,
    silver_current_weather,
)
from weather_forecast.utils.helpers import load_variables_config

CURRENT_WEATHER_ASSETS = AssetSelection.assets(
    bronze_current, silver_current_weather, gold_current_conditions
)

# Define a job covering the full asset pipeline (partitioning is inferred from the assets)
weather_pipeline_job = define_asset_job(
    name="weather_pipeline_job",
    selection=AssetSelection.groups("bronze", "silver", "gold") - CURRENT_WEATHER_ASSETS,
    description="Full weather pipeline: Bronze → Silver → Gold for a given partition date.",
)

current_weather_job = define_asset_job(
    name="current_weather_job",
    selection=CURRENT_WEATHER_ASSETS,
    description="Current-conditions snapshot: Bronze → Silver history → Gold latest (today only).",
)


def current_weather_cron(interval_minutes: int) -> str:
    """Cron expression polling every interval_minutes (15, 30 or 60)."""
    if interval_minutes == 60:
        return "0 * * * *"
    if interval_minutes in (15, 30):
        return f"*/{interval_minutes} * * * *"
    raise ValueError(f"current_interval_minutes must be 15, 30 or 60, got {interval_minutes}")


@schedule(
    job=weather_pipeline_job,
    cron_schedule="0 * * * *",  # Every hour at :00
    execution_timezone="UTC",
)
def hourly_weather_schedule(context: ScheduleEvaluationContext):
    """Hourly schedule that materialises today's partition.

    Runs every hour and targets today's (UTC) date partition, refreshing
    the latest forecast data through the full pipeline.
    """
    scheduled_utc = context.scheduled_execution_time.astimezone(timezone.utc)
    today = scheduled_utc.date().isoformat()
    return RunRequest(
        run_key=f"hourly_{today}_{scheduled_utc.hour:02d}",
        partition_key=today,
        tags={"trigger": "hourly_schedule"},
    )


@schedule(
    job=current_weather_job,
    cron_schedule=current_weather_cron(load_variables_config().settings.current_interval_minutes),
    execution_timezone="UTC",
)
def current_weather_schedule(context: ScheduleEvaluationContext):
    """Poll current conditions for today's (UTC) partition."""
    scheduled_utc = context.scheduled_execution_time.astimezone(timezone.utc)
    today = scheduled_utc.date().isoformat()
    return RunRequest(
        run_key=f"current_{today}_{scheduled_utc:%H%M}",
        partition_key=today,
        tags={"trigger": "current_weather_schedule"},
    )
