"""Bronze layer asset — raw weather data ingestion.

Fetches weather data from the Open-Meteo API and stores the raw JSON
responses as Parquet files. Each partition represents one day, and
data is stored per-location.

File layout:
    data/bronze/{location_name}/{partition_date}.parquet

The asset auto-switches between forecast and archive API endpoints
based on whether the partition date is in the past or present/future.
"""

import json
import logging
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MetadataValue,
    asset,
    asset_check,
)

from weather_forecast.models.weather import APISettings, LocationConfig
from weather_forecast.partitions import daily_partitions
from weather_forecast.resources.api_client import OpenMeteoClient, OpenMeteoRequest
from weather_forecast.utils.helpers import (
    get_data_dir,
    load_locations_config,
    load_variables_config,
    utc_today,
)

logger = logging.getLogger(__name__)


def _has_hourly_values(raw_response: dict) -> bool:
    """Return True if any hourly variable array contains a non-null value.

    The archive API lags behind real time and returns null-filled arrays for
    days it does not have yet, rather than an error.
    """
    hourly = raw_response.get("hourly", {})
    return any(
        v is not None
        for key, values in hourly.items()
        if key != "time"
        for v in values
    )


def _fetch_location(
    context: AssetExecutionContext,
    client: OpenMeteoClient,
    location: LocationConfig,
    partition_date: date,
    variable_names: list[str],
    settings: APISettings,
) -> tuple[dict, str]:
    """Fetch raw data for one location/day, choosing the right endpoint.

    Returns:
        Tuple of (raw JSON response, endpoint name: "forecast" or "archive").
    """
    def request(**window) -> OpenMeteoRequest:
        return OpenMeteoRequest(
            latitude=location.latitude,
            longitude=location.longitude,
            hourly=variable_names,
            timezone="GMT",
            timeformat="iso8601",
            params=settings.request_params,
            **window,
        )

    if partition_date >= utc_today():
        # Current day — full forecast horizon is kept in Bronze
        raw_response = client.fetch("forecast", request(forecast_days=settings.forecast_days))
        return raw_response, "forecast"

    # Historical data — use archive endpoint
    one_day = {"start_date": partition_date, "end_date": partition_date}
    try:
        raw_response = client.fetch("archive", request(**one_day))
        if _has_hourly_values(raw_response):
            return raw_response, "archive"
        context.log.warning(
            f"Archive API has no data yet for {location.name} on {partition_date}, "
            f"falling back to forecast"
        )
    except Exception as e:
        context.log.warning(
            f"Archive API failed for {location.name} on {partition_date}, "
            f"falling back to forecast: {e}"
        )

    raw_response = client.fetch("forecast", request(**one_day))
    return raw_response, "forecast"


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    compute_kind="python",
    description="Raw weather data from Open-Meteo API, stored as Parquet files.",
)
def bronze_forecast(
    context: AssetExecutionContext,
    open_meteo_client: OpenMeteoClient,
) -> None:
    """Ingest raw weather data from Open-Meteo for all configured locations.

    For each location:
    1. Determine whether to use the forecast or archive API endpoint
       based on the partition date relative to today (UTC).
    2. Fetch the raw JSON response.
    3. Write it as a Parquet file with metadata columns.
    """
    partition_date_str = context.partition_key  # "YYYY-MM-DD"
    partition_date = date.fromisoformat(partition_date_str)

    locations = load_locations_config()
    variables_config = load_variables_config()
    variable_names = variables_config.variable_names
    settings = variables_config.settings

    bronze_dir = get_data_dir() / "bronze"
    ingestion_ts = datetime.now(timezone.utc)
    total_locations = 0
    total_bytes = 0

    endpoints_used: dict[str, str] = {}

    for location in locations:
        raw_response, api_endpoint = _fetch_location(
            context, open_meteo_client, location, partition_date, variable_names, settings
        )
        endpoints_used[location.name] = api_endpoint

        # Build the Bronze record
        raw_json_str = json.dumps(raw_response)

        record = {
            "raw_json": [raw_json_str],
            "location_name": [location.name],
            "latitude": [location.latitude],
            "longitude": [location.longitude],
            "partition_date": [partition_date_str],
            "ingestion_timestamp": [ingestion_ts],
            "api_endpoint": [api_endpoint],
        }

        # Write Parquet file
        location_dir = bronze_dir / location.name
        location_dir.mkdir(parents=True, exist_ok=True)
        output_path = location_dir / f"{partition_date_str}.parquet"

        table = pa.table(record)
        pq.write_table(table, str(output_path), compression="snappy")

        file_size = output_path.stat().st_size
        total_bytes += file_size
        total_locations += 1

        context.log.info(
            f"Wrote Bronze data for {location.name} on {partition_date_str} "
            f"({api_endpoint} endpoint, {file_size:,} bytes)"
        )

    # Log metadata for Dagster UI
    context.add_output_metadata(
        {
            "partition_date": partition_date_str,
            "locations_processed": total_locations,
            "total_bytes": total_bytes,
            "api_endpoints": MetadataValue.json(endpoints_used),
            "ingestion_timestamp": MetadataValue.text(ingestion_ts.isoformat()),
        }
    )


@asset_check(
    asset=bronze_forecast,
    description="Verify Bronze Parquet files exist and are non-empty for all configured locations.",
)
def bronze_file_exists_check(context) -> AssetCheckResult:
    """Check that Bronze Parquet files were written for all locations."""
    partition_date_str = context.partition_key
    locations = load_locations_config()
    bronze_dir = get_data_dir() / "bronze"

    missing = []
    empty = []
    file_sizes = {}

    for location in locations:
        filepath = bronze_dir / location.name / f"{partition_date_str}.parquet"
        if not filepath.exists():
            missing.append(location.name)
        elif filepath.stat().st_size == 0:
            empty.append(location.name)
        else:
            file_sizes[location.name] = filepath.stat().st_size

    passed = len(missing) == 0 and len(empty) == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "missing_locations": missing,
            "empty_locations": empty,
            "file_sizes": file_sizes,
            "partition_date": partition_date_str,
        },
        description=(
            f"All {len(locations)} location files present and non-empty"
            if passed
            else f"Missing: {missing}, Empty: {empty}"
        ),
    )
