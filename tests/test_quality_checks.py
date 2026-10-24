"""Failing-case tests for the Silver and Gold quality-check logic (#42).

The checks run against partition files inside Dagster; their logic lives in
plain functions (hourly_quality_issues, temperature_issues) so each failure
condition can be proven directly, without crafting broken data the assets
themselves would never write.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from weather_forecast.assets.gold import temperature_issues
from weather_forecast.assets.silver import hourly_quality_issues
from weather_forecast.models.weather import VariableDefinition

START = datetime(2026, 10, 21, tzinfo=timezone.utc)
TEMPERATURE = [VariableDefinition(name="temperature_2m", description="t", unit="°C",
                                  valid_range=(-60, 65))]


def _hourly(num_locations=2, hours=24, temperature=20.0):
    return pd.DataFrame(
        [
            {
                "location_id": f"city_{i}",
                "timestamp_utc": START + timedelta(hours=h),
                "temperature_2m": temperature,
            }
            for i in range(num_locations)
            for h in range(hours)
        ]
    )


class TestHourlyQualityIssues:
    def test_clean_partition_passes(self):
        assert hourly_quality_issues(_hourly(), 2, TEMPERATURE) == []

    def test_duplicates(self):
        df = _hourly()
        df = pd.concat([df, df.head(3)], ignore_index=True)
        # 51 rows is also above the maximum; check just the duplicate message
        issues = hourly_quality_issues(df, 2, TEMPERATURE)
        assert "3 duplicate (location, timestamp) pairs found" in issues

    def test_temperature_null_rate_above_5_percent(self):
        df = _hourly()
        df.loc[:3, "temperature_2m"] = None  # 4/48 = 8.3%
        assert hourly_quality_issues(df, 2, TEMPERATURE) == [
            "temperature_2m null rate 8.3% exceeds 5%"
        ]

    def test_temperature_null_rate_at_most_5_percent_passes(self):
        df = _hourly()
        df.loc[:1, "temperature_2m"] = None  # 2/48 = 4.2%
        assert hourly_quality_issues(df, 2, TEMPERATURE) == []

    def test_row_count_above_maximum(self):
        # 25 distinct hours per location: no duplicates, but more than a day's worth
        assert hourly_quality_issues(_hourly(hours=25), 2, TEMPERATURE) == [
            "Row count 50 above expected maximum 48"
        ]

    def test_row_count_below_minimum(self):
        assert hourly_quality_issues(_hourly(hours=19), 2, TEMPERATURE) == [
            "Row count 38 below expected minimum 40"
        ]

    def test_value_out_of_range(self):
        df = _hourly()
        df.loc[0, "temperature_2m"] = 70.0
        [issue] = hourly_quality_issues(df, 2, TEMPERATURE)
        assert issue.startswith("temperature_2m out of range [-60.0, 65.0]")


def _gold(**overrides):
    rows = [
        {"location_id": "adelaide", "location_name": "Adelaide", "date": "2026-10-21",
         "temp_min_c": 10.0, "temp_avg_c": 15.0, "temp_max_c": 20.0, "hours_covered": 24},
        {"location_id": "melbourne", "location_name": "Melbourne", "date": "2026-10-21",
         "temp_min_c": 9.0, "temp_avg_c": 14.0, "temp_max_c": 19.0, "hours_covered": 24},
    ]
    df = pd.DataFrame(rows)
    for column, value in overrides.items():
        df.loc[0, column] = value
    return df


class TestTemperatureIssues:
    def test_clean_partition_passes(self):
        assert temperature_issues(_gold(), 2) == ([], [])

    @pytest.mark.parametrize(
        "overrides,expected",
        [
            ({"temp_min_c": 16.0}, "adelaide: temp_min (16.0) > temp_avg (15.0)"),
            ({"temp_max_c": 14.0}, "adelaide: temp_avg (15.0) > temp_max (14.0)"),
        ],
    )
    def test_min_avg_max_order_violation(self, overrides, expected):
        errors, _ = temperature_issues(_gold(**overrides), 2)
        assert errors == [expected]

    @pytest.mark.parametrize("key", ["location_id", "location_name", "date"])
    def test_null_key(self, key):
        errors, _ = temperature_issues(_gold(**{key: None}), 2)
        assert errors == [f"Null or missing {key} found"]

    def test_missing_key_column(self):
        errors, _ = temperature_issues(_gold().drop(columns=["location_id"]), 2)
        assert "Null or missing location_id found" in errors

    @pytest.mark.parametrize("num_locations", [1, 3])
    def test_row_count_differs_from_locations(self, num_locations):
        errors, _ = temperature_issues(_gold(), num_locations)
        assert errors == [f"Row count 2 != {num_locations} configured locations"]

    @pytest.mark.parametrize("hours,ok", [(23, True), (25, True), (22, False), (26, False)])
    def test_incomplete_local_day_is_a_warning(self, hours, ok):
        errors, warnings = temperature_issues(_gold(hours_covered=hours), 2)
        assert errors == []
        assert (warnings == []) is ok
