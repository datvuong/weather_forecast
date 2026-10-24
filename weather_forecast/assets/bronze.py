"""Bronze layer asset — raw weather data ingestion.

Fetches weather data from the Open-Meteo API and stores the raw JSON
responses as Parquet files. Each partition represents one day, and
data is stored per-location.

File layout:
    data/bronze/{location_name}/{partition_date}.parquet

The asset auto-switches between forecast and archive API endpoints
based on whether the partition date is in the past or present/future.
Each request covers the configured hourly and daily variables, in the
location's timezone with unixtime timestamps (see _fetch_location).
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MetadataValue,
    asset,
    asset_check,
)

from weather_forecast.models.weather import LocationConfig, VariablesConfig
from weather_forecast.partitions import daily_partitions
from weather_forecast.resources.api_client import (
    OpenMeteoClient,
    OpenMeteoRequest,
    unsupported_variables,
)
from weather_forecast.utils.helpers import (
    epoch_to_local_date,
    epoch_to_utc,
    get_data_dir,
    load_locations_config,
    load_variables_config,
    utc_today,
)

logger = logging.getLogger(__name__)


def _covers_partition(raw_response: dict, partition_date: date, tz_name: str) -> bool:
    """Return True if the response has data for the partition day.

    Requested sections are checked separately: some hourly value within the
    UTC day, and some daily value for the local date equal to the partition.
    The archive API lags behind real time and returns null-filled arrays for
    days it does not have yet, rather than an error.
    """
    checks = {
        "hourly": lambda t: epoch_to_utc(t).date() == partition_date,
        "daily": lambda t: epoch_to_local_date(t, tz_name) == partition_date,
    }
    for section, in_partition in checks.items():
        data = raw_response.get(section)
        if not data or len(data) <= 1:  # section not requested (only "time")
            continue
        rows = [i for i, t in enumerate(data.get("time", [])) if in_partition(t)]
        has_values = any(
            values[i] is not None
            for key, values in data.items()
            if key != "time"
            for i in rows
        )
        if not has_values:
            return False
    return True


def _fetch_location(
    context: AssetExecutionContext,
    client: OpenMeteoClient,
    location: LocationConfig,
    partition_date: date,
    variables_config: VariablesConfig,
) -> tuple[dict, str]:
    """Fetch hourly + daily data for one location/day, choosing the right endpoint.

    Requests use the location's timezone with unixtime timestamps: hourly
    times are exact UTC epochs (filtered to the UTC partition day in Silver)
    and daily aggregates cover local calendar days. A UTC day spans local
    dates d-1..d+1 for any UTC offset, so that window is requested.

    Returns:
        Tuple of (raw JSON response, endpoint name: "forecast" or "archive").
    """
    settings = variables_config.settings

    def request(**window) -> OpenMeteoRequest:
        return OpenMeteoRequest(
            latitude=location.latitude,
            longitude=location.longitude,
            hourly=variables_config.hourly_names,
            daily=variables_config.daily_names,
            timezone=location.timezone,
            timeformat="unixtime",
            params=settings.request_params,
            **window,
        )

    if partition_date >= utc_today():
        # Current day — full forecast horizon is kept in Bronze. past_days=1 covers the
        # early UTC hours (local yesterday for positive offsets); at least 2 forecast
        # days covers local tomorrow (for negative offsets).
        forecast = request(past_days=1, forecast_days=max(settings.forecast_days, 2))
        return client.fetch("forecast", forecast), "forecast"

    # Historical data — use archive endpoint
    window = request(
        start_date=partition_date - timedelta(days=1),
        end_date=partition_date + timedelta(days=1),
    )
    try:
        raw_response = client.fetch("archive", window)
        if _covers_partition(raw_response, partition_date, location.timezone):
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

    return client.fetch("forecast", window), "forecast"


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
    2. Fetch the raw JSON response (hourly + daily sections from config).
    3. Write it as a Parquet file with metadata columns.
    """
    partition_date_str = context.partition_key  # "YYYY-MM-DD"
    partition_date = date.fromisoformat(partition_date_str)

    locations = load_locations_config()
    variables_config = load_variables_config()

    bronze_dir = get_data_dir() / "bronze"
    ingestion_ts = datetime.now(timezone.utc)
    total_locations = 0
    total_bytes = 0

    endpoints_used: dict[str, str] = {}
    unsupported: dict[str, dict[str, list[str]]] = {}

    for location in locations:
        raw_response, api_endpoint = _fetch_location(
            context, open_meteo_client, location, partition_date, variables_config
        )
        endpoints_used[location.name] = api_endpoint
        missing = unsupported_variables(raw_response)
        if missing:
            unsupported[location.name] = missing
            context.log.info(
                f"{api_endpoint} endpoint doesn't provide {missing} for {location.name}; "
                f"they will be null"
            )

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
            "unsupported_variables": MetadataValue.json(unsupported),
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
