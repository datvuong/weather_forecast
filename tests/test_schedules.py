"""Tests for the hourly schedule, partitions, and Definitions wiring."""

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import dagster as dg
import pytest
import yaml

from weather_forecast.definitions import defs
from weather_forecast.partitions import daily_partitions
from weather_forecast.schedules import (
    current_weather_cron,
    current_weather_schedule,
    hourly_weather_schedule,
)
from weather_forecast.utils.helpers import (
    api_calls_per_day,
    get_project_root,
    load_variables_config,
    utc_today,
    warn_if_over_api_budget,
)


def _run_request(scheduled_time, schedule=hourly_weather_schedule):
    context = dg.build_schedule_context(scheduled_execution_time=scheduled_time)
    return schedule(context)


def test_schedule_targets_utc_date():
    request, _ = _run_request(datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc))

    assert request.partition_key == "2026-10-24"
    assert request.run_key == "hourly_2026-10-24_14"
    assert request.asset_selection is None  # the whole pipeline job


def test_schedule_refreshes_previous_local_day_gold():
    _, refresh = _run_request(datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc))

    assert refresh.partition_key == "2026-10-23"
    assert refresh.run_key == "gold_refresh_2026-10-23_14"
    assert {k.to_user_string() for k in refresh.asset_selection} == {
        "gold_daily_temperature",
        "gold_daily_precipitation",
    }


def test_schedule_uses_utc_date_even_when_local_date_differs():
    # 00:30 on the 25th in Adelaide is still the 24th in UTC
    local = datetime(2026, 10, 25, 0, 30, tzinfo=ZoneInfo("Australia/Adelaide"))

    request, _ = _run_request(local)

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
        "bronze_current",
        "silver_current_weather",
        "gold_current_conditions",
        "gold_daily_temperature",
        "gold_daily_precipitation",
        "dim_weather_code",
        "dim_location",
        "warehouse_views",
    }
    assert defs.resolve_job_def("weather_pipeline_job").partitions_def == daily_partitions


def _job_assets(job_name):
    job = defs.resolve_job_def(job_name)
    return {key.to_user_string() for key in job.asset_layer.executable_asset_keys}


def test_pipeline_job_excludes_current_weather():
    assets = _job_assets("weather_pipeline_job")

    assert "bronze_forecast" in assets and "silver_daily_weather" in assets
    assert not assets & {"bronze_current", "silver_current_weather", "gold_current_conditions"}


def test_current_weather_job_assets():
    assert _job_assets("current_weather_job") == {
        "bronze_current",
        "silver_current_weather",
        "gold_current_conditions",
    }


@pytest.mark.parametrize(
    "minutes,cron", [(15, "*/15 * * * *"), (30, "*/30 * * * *"), (60, "0 * * * *")]
)
def test_current_weather_cron(minutes, cron):
    assert current_weather_cron(minutes) == cron


def test_current_weather_cron_rejects_other_intervals():
    with pytest.raises(ValueError):
        current_weather_cron(45)


def test_current_weather_schedule_uses_configured_cadence():
    assert current_weather_schedule.cron_schedule == "*/15 * * * *"


def test_current_weather_schedule_targets_utc_date():
    request = _run_request(
        datetime(2026, 10, 24, 23, 45, tzinfo=timezone.utc), current_weather_schedule
    )

    assert request.partition_key == "2026-10-24"
    assert request.run_key == "current_2026-10-24_2345"


class TestApiBudget:
    def test_calls_per_day_with_current_weather(self):
        # 24 hourly-pipeline calls + 96 current polls (15 min) per location
        assert api_calls_per_day(2, load_variables_config()) == 2 * (24 + 96)

    def test_calls_per_day_without_current_weather(self, variables_config):
        assert api_calls_per_day(2, variables_config(current_variables=[])) == 2 * 24

    def test_warns_above_80_percent(self, caplog):
        config = load_variables_config()
        with caplog.at_level(logging.WARNING):
            assert warn_if_over_api_budget(67, config) == 8040
        assert "free-tier limit" in caplog.text

    def test_no_warning_within_budget(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_if_over_api_budget(2, load_variables_config())
        assert caplog.text == ""


@pytest.mark.parametrize(
    "job_name", ["weather_pipeline_job", "current_weather_job", "dimensions_job"]
)
def test_jobs_run_in_a_single_process(job_name):
    # One process per run instead of one per step keeps backfills within memory (#37)
    assert defs.resolve_job_def(job_name).executor_def.name == "in_process"


def test_instance_limits_concurrent_runs():
    config = yaml.safe_load((get_project_root() / "dagster.yaml").read_text())
    assert config["concurrency"]["runs"]["max_concurrent_runs"] == 4
