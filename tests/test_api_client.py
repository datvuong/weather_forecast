"""Tests for the Open-Meteo API client resource (HTTP mocked with respx)."""

from datetime import date

import httpx
import pytest
import respx
from pydantic import ValidationError

from weather_forecast.resources import api_client
from weather_forecast.resources.api_client import (
    OpenMeteoAPIError,
    OpenMeteoClient,
    OpenMeteoRequest,
    unsupported_variables,
)

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Skip real backoff waits."""
    monkeypatch.setattr(api_client.time, "sleep", lambda _: None)


@pytest.fixture
def client():
    return OpenMeteoClient()


def _request(**kwargs):
    return OpenMeteoRequest(latitude=-34.9, longitude=138.6, **kwargs)


class TestToQuery:
    def test_sections_are_comma_joined_and_empty_ones_omitted(self):
        query = _request(hourly=["temperature_2m", "cloud_cover"], daily=["sunrise"]).to_query(
            "forecast"
        )

        assert query["hourly"] == "temperature_2m,cloud_cover"
        assert query["daily"] == "sunrise"
        assert "current" not in query

    def test_defaults(self):
        query = _request(hourly=["temperature_2m"]).to_query("forecast")

        assert query["timezone"] == "GMT"
        assert query["timeformat"] == "unixtime"
        assert "forecast_days" not in query and "start_date" not in query

    def test_forecast_day_counts(self):
        query = _request(current=["is_day"], forecast_days=7, past_days=1).to_query("forecast")

        assert query["current"] == "is_day"
        assert (query["forecast_days"], query["past_days"]) == (7, 1)

    def test_date_range(self):
        query = _request(
            hourly=["temperature_2m"],
            timezone="Australia/Adelaide",
            start_date=date(2026, 10, 20),
            end_date=date(2026, 10, 22),
        ).to_query("archive")

        assert (query["start_date"], query["end_date"]) == ("2026-10-20", "2026-10-22")
        assert query["timezone"] == "Australia/Adelaide"

    def test_archive_drops_current(self):
        query = _request(
            hourly=["temperature_2m"],
            current=["temperature_2m"],
            start_date=date(2026, 10, 20),
            end_date=date(2026, 10, 20),
        ).to_query("archive")

        assert "current" not in query
        assert query["hourly"] == "temperature_2m"

    def test_archive_requires_dates(self):
        with pytest.raises(ValueError, match="start_date and end_date"):
            _request(hourly=["temperature_2m"], forecast_days=3).to_query("archive")

    def test_params_passthrough(self):
        query = _request(
            hourly=["temperature_2m"],
            params={"temperature_unit": "fahrenheit", "models": "best_match", "flag": True},
        ).to_query("forecast")

        assert query["temperature_unit"] == "fahrenheit"
        assert query["models"] == "best_match"
        assert query["flag"] == "true"

    def test_partial_date_range_rejected(self):
        with pytest.raises(ValidationError, match="together"):
            _request(start_date=date(2026, 10, 20))

    def test_date_range_with_day_counts_rejected(self):
        with pytest.raises(ValidationError, match="can't be combined"):
            _request(start_date=date(2026, 10, 20), end_date=date(2026, 10, 20), forecast_days=1)


class TestUnsupportedVariables:
    def test_detects_undefined_units_per_section(self):
        response = {
            "hourly_units": {"time": "unixtime", "uv_index": "undefined", "temperature_2m": "°C"},
            "daily_units": {"time": "unixtime", "uv_index_max": "undefined", "sunrise": "unixtime"},
        }

        assert unsupported_variables(response) == {
            "hourly": ["uv_index"],
            "daily": ["uv_index_max"],
        }

    def test_empty_unit_is_supported(self):
        # Dimensionless variables (uv_index, is_day) report "" rather than "undefined"
        assert unsupported_variables({"current_units": {"is_day": ""}}) == {}


class TestFetch:
    @respx.mock
    def test_forecast_url_and_params(self, client):
        route = respx.get(FORECAST_URL).mock(return_value=httpx.Response(200, json={"ok": 1}))

        result = client.fetch("forecast", _request(hourly=["temperature_2m"], forecast_days=3))

        assert result == {"ok": 1}
        params = route.calls.last.request.url.params
        assert params["hourly"] == "temperature_2m"
        assert params["forecast_days"] == "3"

    @respx.mock
    def test_archive_url(self, client):
        route = respx.get(ARCHIVE_URL).mock(return_value=httpx.Response(200, json={}))

        client.fetch(
            "archive",
            _request(daily=["sunrise"], start_date=date(2026, 10, 1), end_date=date(2026, 10, 2)),
        )

        assert route.calls.last.request.url.params["daily"] == "sunrise"

    @respx.mock
    def test_invalid_request_fails_fast_with_reason(self, client):
        route = respx.get(FORECAST_URL).mock(
            return_value=httpx.Response(
                400, json={"error": True, "reason": "Cannot initialize ... not_a_var"}
            )
        )

        with pytest.raises(OpenMeteoAPIError) as exc_info:
            client.fetch("forecast", _request(hourly=["not_a_var"]))

        assert exc_info.value.status_code == 400
        assert "not_a_var" in exc_info.value.reason
        assert route.call_count == 1

    @respx.mock
    def test_error_body_with_200_raises(self, client):
        respx.get(FORECAST_URL).mock(
            return_value=httpx.Response(200, json={"error": True, "reason": "bad"})
        )

        with pytest.raises(OpenMeteoAPIError, match="bad"):
            client.fetch("forecast", _request(hourly=["temperature_2m"]))

    @respx.mock
    def test_retries_then_succeeds(self, client):
        route = respx.get(FORECAST_URL).mock(
            side_effect=[
                httpx.Response(500),
                httpx.ConnectError("boom"),
                httpx.Response(200, json={"ok": 1}),
            ]
        )

        assert client.fetch("forecast", _request(hourly=["temperature_2m"])) == {"ok": 1}
        assert route.call_count == 3

    @respx.mock
    def test_rate_limit_is_retried(self, client):
        route = respx.get(FORECAST_URL).mock(
            side_effect=[httpx.Response(429), httpx.Response(200, json={"ok": 1})]
        )

        assert client.fetch("forecast", _request(hourly=["temperature_2m"])) == {"ok": 1}
        assert route.call_count == 2

    @respx.mock
    def test_server_error_raises_after_max_retries(self, client):
        route = respx.get(FORECAST_URL).mock(return_value=httpx.Response(503))

        with pytest.raises(OpenMeteoAPIError) as exc_info:
            client.fetch("forecast", _request(hourly=["temperature_2m"]))

        assert exc_info.value.status_code == 503
        assert route.call_count == api_client.MAX_RETRIES

    @respx.mock
    def test_timeout_is_retried(self, client):
        route = respx.get(ARCHIVE_URL).mock(side_effect=httpx.ReadTimeout("slow"))

        with pytest.raises(httpx.TimeoutException):
            client.fetch(
                "archive",
                _request(hourly=["temperature_2m"], start_date=date(2026, 10, 1),
                         end_date=date(2026, 10, 1)),
            )
        assert route.call_count == api_client.MAX_RETRIES
