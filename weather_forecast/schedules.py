"""Schedule definitions for the weather forecast pipeline.

Defines an hourly schedule that materializes today's partition
through the full Bronze → Silver → Gold asset chain.
"""

from datetime import timezone

from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

# Define a job covering the full asset pipeline (partitioning is inferred from the assets)
weather_pipeline_job = define_asset_job(
    name="weather_pipeline_job",
    selection=AssetSelection.groups("bronze", "silver", "gold"),
    description="Full weather pipeline: Bronze → Silver → Gold for a given partition date.",
)


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
