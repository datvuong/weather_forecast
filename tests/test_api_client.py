"""Tests for the Open-Meteo API client resource (HTTP mocked with respx)."""

from datetime import date

import httpx
import pytest
import respx

from weather_forecast.resources import api_client
from weather_forecast.resources.api_client import OpenMeteoClient

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
VARIABLES = ["temperature_2m", "precipitation", "precipitation_probability"]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Skip real backoff waits."""
    monkeypatch.setattr(api_client.time, "sleep", lambda _: None)


@pytest.fixture
def client():
    return OpenMeteoClient()


@respx.mock
def test_fetch_forecast_params(client):
    route = respx.get(FORECAST_URL).mock(return_value=httpx.Response(200, json={"ok": 1}))

    result = client.fetch_forecast(-34.9, 138.6, VARIABLES, forecast_days=3)

    assert result == {"ok": 1}
    params = route.calls.last.request.url.params
    assert params["hourly"] == "temperature_2m,precipitation,precipitation_probability"
    assert params["forecast_days"] == "3"
    assert params["timezone"] == "GMT"
    assert "start_date" not in params


@respx.mock
def test_fetch_forecast_with_date_range(client):
    route = respx.get(FORECAST_URL).mock(return_value=httpx.Response(200, json={}))

    client.fetch_forecast(
        -34.9, 138.6, VARIABLES, start_date=date(2026, 10, 22), end_date=date(2026, 10, 22)
    )

    params = route.calls.last.request.url.params
    assert params["start_date"] == params["end_date"] == "2026-10-22"
    assert "forecast_days" not in params


@respx.mock
def test_fetch_historical_drops_forecast_only_variables(client):
    route = respx.get(ARCHIVE_URL).mock(return_value=httpx.Response(200, json={}))

    client.fetch_historical(-34.9, 138.6, VARIABLES, date(2026, 10, 1), date(2026, 10, 2))

    params = route.calls.last.request.url.params
    assert params["hourly"] == "temperature_2m,precipitation"
    assert params["start_date"] == "2026-10-01"
    assert params["end_date"] == "2026-10-02"
    assert params["timezone"] == "GMT"


@respx.mock
def test_retries_then_succeeds(client):
    route = respx.get(FORECAST_URL).mock(
        side_effect=[
            httpx.Response(500),
            httpx.ConnectError("boom"),
            httpx.Response(200, json={"ok": 1}),
        ]
    )

    assert client.fetch_forecast(0, 0, VARIABLES) == {"ok": 1}
    assert route.call_count == 3


@respx.mock
def test_raises_after_max_retries(client):
    route = respx.get(FORECAST_URL).mock(return_value=httpx.Response(503))

    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_forecast(0, 0, VARIABLES)
    assert route.call_count == api_client.MAX_RETRIES


@respx.mock
def test_timeout_is_retried(client):
    route = respx.get(ARCHIVE_URL).mock(side_effect=httpx.ReadTimeout("slow"))

    with pytest.raises(httpx.TimeoutException):
        client.fetch_historical(0, 0, VARIABLES, date(2026, 10, 1), date(2026, 10, 1))
    assert route.call_count == api_client.MAX_RETRIES
