"""Tests for the hourly schedule, partitions, and Definitions wiring."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import dagster as dg

from weather_forecast.definitions import defs
from weather_forecast.partitions import daily_partitions
from weather_forecast.schedules import hourly_weather_schedule
from weather_forecast.utils.helpers import utc_today


def _run_request(scheduled_time):
    context = dg.build_schedule_context(scheduled_execution_time=scheduled_time)
    return hourly_weather_schedule(context)


def test_schedule_targets_utc_date():
    request = _run_request(datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc))

    assert request.partition_key == "2026-10-24"
    assert request.run_key == "hourly_2026-10-24_14"


def test_schedule_uses_utc_date_even_when_local_date_differs():
    # 00:30 on the 25th in Adelaide is still the 24th in UTC
    local = datetime(2026, 10, 25, 0, 30, tzinfo=ZoneInfo("Australia/Adelaide"))

    request = _run_request(local)

    assert request.partition_key == "2026-10-24"


def test_todays_partition_exists():
    assert daily_partitions.get_last_partition_key() == utc_today().isoformat()


def test_definitions_load():
    repo = defs.get_repository_def()
    repo.load_all_definitions()
    asset_keys = {key.to_user_string() for key in repo.asset_graph.get_all_asset_keys()}
    assert asset_keys == {
        "bronze_forecast",
        "silver_hourly_weather",
        "silver_daily_weather",
        "gold_daily_temperature",
        "gold_daily_precipitation",
    }
    assert defs.resolve_job_def("weather_pipeline_job").partitions_def == daily_partitions
