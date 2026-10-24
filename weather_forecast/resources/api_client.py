"""Open-Meteo API client — Dagster ConfigurableResource.

Wraps the Open-Meteo forecast and archive HTTP endpoints with:
- A generic request model (any hourly/daily/current variables, any extra params)
- httpx with timeout and retry logic for transient failures
- Fail-fast errors carrying Open-Meteo's `reason` for invalid requests
- Returns raw JSON dicts for Bronze layer storage
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Literal

import httpx
from dagster import ConfigurableResource
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.0  # seconds: 1, 2, 4
REQUEST_TIMEOUT = 30.0  # seconds

Endpoint = Literal["forecast", "archive"]
Section = Literal["hourly", "daily", "current"]
SECTIONS: tuple[Section, ...] = ("hourly", "daily", "current")

# Unit Open-Meteo reports for a variable the endpoint doesn't support (values are null)
UNSUPPORTED_UNIT = "undefined"


class OpenMeteoAPIError(Exception):
    """Open-Meteo rejected a request (HTTP error status or an error body)."""

    def __init__(self, status_code: int, reason: str):
        self.status_code = status_code
        self.reason = reason
        super().__init__(f"Open-Meteo API error {status_code}: {reason}")


class OpenMeteoRequest(BaseModel):
    """A single-location Open-Meteo query, independent of specific variables.

    Attributes:
        hourly/daily/current: Variable names per request section; empty sections
            are omitted from the query.
        timezone: IANA timezone for local times and daily aggregation windows.
        timeformat: "iso8601" local-time strings or "unixtime" UTC epoch seconds.
        start_date/end_date: Explicit date range (interpreted in `timezone`).
            Mutually exclusive with forecast_days/past_days.
        params: Any other query parameters (units, models, cell_selection, ...).
    """

    latitude: float
    longitude: float
    hourly: list[str] = Field(default_factory=list)
    daily: list[str] = Field(default_factory=list)
    current: list[str] = Field(default_factory=list)
    timezone: str = "GMT"
    timeformat: Literal["iso8601", "unixtime"] = "unixtime"
    start_date: date | None = None
    end_date: date | None = None
    forecast_days: int | None = None
    past_days: int | None = None
    params: dict[str, str | int | float | bool] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_window(self) -> OpenMeteoRequest:
        """Require both or neither of start/end date, exclusive of day counts."""
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("start_date and end_date must be given together")
        if self.start_date is not None and (
            self.forecast_days is not None or self.past_days is not None
        ):
            raise ValueError("start_date/end_date can't be combined with forecast_days/past_days")
        return self

    def to_query(self, endpoint: Endpoint) -> dict[str, str | int | float]:
        """Build the HTTP query parameters for an endpoint.

        The archive endpoint has no `current` section and needs an explicit
        date range; forecast-only day counts are dropped for it.
        """
        if endpoint == "archive" and self.start_date is None:
            raise ValueError("archive requests need start_date and end_date")

        query: dict[str, str | int | float] = {
            "latitude": self.latitude,
            "longitude": self.longitude,
        }
        for section in SECTIONS:
            names = getattr(self, section)
            if names and not (section == "current" and endpoint == "archive"):
                query[section] = ",".join(names)
        query["timezone"] = self.timezone
        query["timeformat"] = self.timeformat

        if self.start_date is not None:
            query["start_date"] = self.start_date.isoformat()
            query["end_date"] = self.end_date.isoformat()
        elif endpoint == "forecast":
            if self.forecast_days is not None:
                query["forecast_days"] = self.forecast_days
            if self.past_days is not None:
                query["past_days"] = self.past_days

        for key, value in self.params.items():
            query[key] = str(value).lower() if isinstance(value, bool) else value
        return query


def unsupported_variables(response: dict) -> dict[str, list[str]]:
    """Return variables per section that the endpoint reported as unsupported.

    Open-Meteo answers unsupported variables (e.g. precipitation_probability
    from the archive) with unit "undefined" and null values instead of an error.
    """
    result = {}
    for section in SECTIONS:
        units = response.get(f"{section}_units") or {}
        names = [name for name, unit in units.items() if unit == UNSUPPORTED_UNIT]
        if names:
            result[section] = names
    return result


def _error_reason(response: httpx.Response) -> str:
    """Extract Open-Meteo's error reason from a response body."""
    try:
        return response.json().get("reason") or response.text
    except ValueError:
        return response.text


def _is_retryable(error: Exception) -> bool:
    """Transport failures, rate limiting and server errors are worth retrying."""
    if isinstance(error, httpx.TransportError):
        return True
    if isinstance(error, OpenMeteoAPIError):
        return error.status_code == 429 or error.status_code >= 500
    return False


class OpenMeteoClient(ConfigurableResource):
    """Dagster resource for fetching weather data from Open-Meteo API.

    The client knows nothing about specific variables: callers describe the
    query with an OpenMeteoRequest. Transient failures are retried with
    exponential backoff; invalid requests fail immediately with Open-Meteo's
    reason. Raw JSON responses are returned for Bronze layer storage.

    Attributes:
        base_url: Open-Meteo forecast API base URL.
        archive_url: Open-Meteo historical archive API base URL.
    """

    base_url: str = "https://api.open-meteo.com/v1/forecast"
    archive_url: str = "https://archive-api.open-meteo.com/v1/archive"

    def _get(self, url: str, params: dict) -> dict:
        """Make a single GET request, raising OpenMeteoAPIError on API errors."""
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            response = client.get(url, params=params)
        if response.is_error:
            raise OpenMeteoAPIError(response.status_code, _error_reason(response))
        body = response.json()
        if isinstance(body, dict) and body.get("error"):
            raise OpenMeteoAPIError(response.status_code, body.get("reason", "unknown error"))
        return body

    def _request_with_retry(self, url: str, params: dict) -> dict:
        """Make an HTTP GET request with retry and exponential backoff.

        Args:
            url: The API endpoint URL.
            params: Query parameters for the request.

        Returns:
            Parsed JSON response as a dict.

        Raises:
            OpenMeteoAPIError: Immediately for non-retryable API errors (e.g. an
                invalid variable name), or after all retries for 429/5xx.
            httpx.TransportError: If connection/timeout errors persist after all retries.
        """
        for attempt in range(MAX_RETRIES):
            try:
                return self._get(url, params)
            except (httpx.TransportError, OpenMeteoAPIError) as e:
                if not _is_retryable(e) or attempt == MAX_RETRIES - 1:
                    logger.error("Request to %s failed: %s", url, e)
                    raise
                wait_time = BACKOFF_FACTOR * (2**attempt)
                logger.warning(
                    "Request to %s failed (attempt %d/%d): %s. Retrying in %.1fs...",
                    url,
                    attempt + 1,
                    MAX_RETRIES,
                    str(e),
                    wait_time,
                )
                time.sleep(wait_time)
        raise AssertionError("unreachable")

    def fetch(self, endpoint: Endpoint, request: OpenMeteoRequest) -> dict:
        """Fetch raw data for a single location from the forecast or archive API.

        Args:
            endpoint: "forecast" (current/future, recent past) or "archive" (history).
            request: The query description.

        Returns:
            Raw JSON response dict.
        """
        url = self.base_url if endpoint == "forecast" else self.archive_url
        logger.info(
            "Fetching %s for (%.4f, %.4f): %s",
            endpoint,
            request.latitude,
            request.longitude,
            {s: len(getattr(request, s)) for s in SECTIONS if getattr(request, s)},
        )
        return self._request_with_retry(url, request.to_query(endpoint))
