"""Open-Meteo API client — Dagster ConfigurableResource.

Wraps the Open-Meteo forecast and archive HTTP endpoints with:
- Configurable base URLs
- httpx with timeout and retry logic
- Returns raw JSON dicts for Bronze layer storage
"""

from __future__ import annotations

import logging
import time
from datetime import date

import httpx
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.0  # seconds: 1, 2, 4
REQUEST_TIMEOUT = 30.0  # seconds


class OpenMeteoClient(ConfigurableResource):
    """Dagster resource for fetching weather data from Open-Meteo API.

    This resource provides methods to fetch forecast and historical weather
    data. It handles retries with exponential backoff and returns raw JSON
    responses suitable for Bronze layer storage.

    Attributes:
        base_url: Open-Meteo forecast API base URL.
        archive_url: Open-Meteo historical archive API base URL.
    """

    base_url: str = "https://api.open-meteo.com/v1/forecast"
    archive_url: str = "https://archive-api.open-meteo.com/v1/archive"

    def _request_with_retry(self, url: str, params: dict) -> dict:
        """Make an HTTP GET request with retry and exponential backoff.

        Args:
            url: The API endpoint URL.
            params: Query parameters for the request.

        Returns:
            Parsed JSON response as a dict.

        Raises:
            httpx.HTTPStatusError: If the request fails after all retries.
            httpx.TimeoutException: If the request times out after all retries.
        """
        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
                    response = client.get(url, params=params)
                    response.raise_for_status()
                    return response.json()
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.ConnectError) as e:
                last_exception = e
                if attempt < MAX_RETRIES - 1:
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
                else:
                    logger.error(
                        "Request to %s failed after %d attempts: %s",
                        url,
                        MAX_RETRIES,
                        str(e),
                    )

        raise last_exception  # type: ignore[misc]

    def fetch_forecast(
        self,
        latitude: float,
        longitude: float,
        hourly_variables: list[str],
        forecast_days: int = 7,
        temperature_unit: str = "celsius",
        wind_speed_unit: str = "kmh",
        precipitation_unit: str = "mm",
    ) -> dict:
        """Fetch forecast data for a single location.

        Args:
            latitude: WGS84 latitude.
            longitude: WGS84 longitude.
            hourly_variables: List of Open-Meteo variable names.
            forecast_days: Number of forecast days (1-16).
            temperature_unit: Temperature unit (celsius/fahrenheit).
            wind_speed_unit: Wind speed unit (kmh/ms/mph/kn).
            precipitation_unit: Precipitation unit (mm/inch).

        Returns:
            Raw JSON response dict from the Open-Meteo forecast API.
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(hourly_variables),
            "forecast_days": forecast_days,
            "temperature_unit": temperature_unit,
            "wind_speed_unit": wind_speed_unit,
            "precipitation_unit": precipitation_unit,
            "timezone": "auto",
        }

        logger.info(
            "Fetching forecast for (%.4f, %.4f) with %d variables",
            latitude,
            longitude,
            len(hourly_variables),
        )

        return self._request_with_retry(self.base_url, params)

    def fetch_historical(
        self,
        latitude: float,
        longitude: float,
        hourly_variables: list[str],
        start_date: date,
        end_date: date,
        temperature_unit: str = "celsius",
        wind_speed_unit: str = "kmh",
        precipitation_unit: str = "mm",
    ) -> dict:
        """Fetch historical data from the archive API.

        Args:
            latitude: WGS84 latitude.
            longitude: WGS84 longitude.
            hourly_variables: List of Open-Meteo variable names.
            start_date: Start date for the historical range.
            end_date: End date for the historical range.
            temperature_unit: Temperature unit (celsius/fahrenheit).
            wind_speed_unit: Wind speed unit (kmh/ms/mph/kn).
            precipitation_unit: Precipitation unit (mm/inch).

        Returns:
            Raw JSON response dict from the Open-Meteo archive API.

        Note:
            The archive API does NOT return precipitation_probability.
            That variable will be absent from the response for historical data.
        """
        # Filter out forecast-only variables for the archive endpoint
        archive_variables = [v for v in hourly_variables if v != "precipitation_probability"]

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(archive_variables),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "temperature_unit": temperature_unit,
            "wind_speed_unit": wind_speed_unit,
            "precipitation_unit": precipitation_unit,
            "timezone": "auto",
        }

        logger.info(
            "Fetching historical data for (%.4f, %.4f) from %s to %s",
            latitude,
            longitude,
            start_date,
            end_date,
        )

        return self._request_with_retry(self.archive_url, params)
