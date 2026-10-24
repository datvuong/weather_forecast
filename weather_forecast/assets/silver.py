"""Silver layer asset — cleaned, typed, validated hourly weather records.

Reads raw Bronze Parquet files, parses the JSON responses, explodes
hourly arrays into individual records, validates with Pydantic, and
writes clean Parquet files.

File layout:
    data/silver/{partition_date}.parquet
    (one file per date, containing all locations)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MetadataValue,
    asset,
    asset_check,
)

from weather_forecast.assets.bronze import bronze_forecast
from weather_forecast.models.weather import HourlyWeatherRecord
from weather_forecast.partitions import daily_partitions
from weather_forecast.utils.helpers import get_data_dir, load_locations_config

logger = logging.getLogger(__name__)

# Weather variable columns that appear in Silver Parquet
WEATHER_COLUMNS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "uv_index",
]

# Valid ranges for data quality checks
VALUE_RANGES = {
    "temperature_2m": (-60.0, 65.0),
    "relative_humidity_2m": (0.0, 100.0),
    "precipitation": (0.0, 500.0),
    "precipitation_probability": (0.0, 100.0),
    "wind_speed_10m": (0.0, 400.0),
    "wind_direction_10m": (0.0, 360.0),
    "wind_gusts_10m": (0.0, 500.0),
    "uv_index": (0.0, 20.0),
}

SILVER_COLUMNS = [
    "location_name",
    "latitude",
    "longitude",
    "timestamp_utc",
    "timestamp_local",
    *WEATHER_COLUMNS,
    "partition_date",
    "ingestion_timestamp",
]


def _parse_hourly_data(
    raw_json: dict[str, Any],
    location_name: str,
    latitude: float,
    longitude: float,
    local_timezone: str,
    partition_date: str,
    ingestion_ts: datetime,
) -> list[dict[str, Any]]:
    """Parse Open-Meteo hourly JSON response into flat records.

    The API is queried with UTC timestamps (see api_client.API_TIMEZONE), so
    hourly times are filtered against the UTC partition date and converted to
    the location's local time for timestamp_local.

    Args:
        raw_json: Parsed API response JSON.
        location_name: City name.
        latitude: Location latitude.
        longitude: Location longitude.
        local_timezone: IANA timezone of the location.
        partition_date: The partition date string (YYYY-MM-DD, UTC).
        ingestion_ts: When the data was ingested (from Bronze).

    Returns:
        List of dicts, one per hourly observation.
    """
    hourly = raw_json.get("hourly", {})
    timestamps = hourly.get("time", [])

    if not timestamps:
        return []

    utc_offset = raw_json.get("utc_offset_seconds", 0)
    if utc_offset != 0:
        raise ValueError(
            f"Expected UTC timestamps from API, got utc_offset_seconds={utc_offset}"
        )
    tz = ZoneInfo(local_timezone)

    records = []
    for i, ts_str in enumerate(timestamps):
        # Only include records that belong to this partition date
        if not ts_str.startswith(partition_date):
            continue

        ts_utc = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        record = {
            "location_name": location_name,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp_utc": ts_utc,
            "timestamp_local": ts_utc.astimezone(tz).replace(tzinfo=None),
            "partition_date": partition_date,
            "ingestion_timestamp": ingestion_ts,
        }

        # Extract each weather variable
        for var in WEATHER_COLUMNS:
            values = hourly.get(var, [])
            record[var] = values[i] if i < len(values) else None

        records.append(record)

    return records


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    compute_kind="python",
    deps=[bronze_forecast],
    description="Cleaned, typed, and validated hourly weather records.",
)
def silver_hourly_weather(
    context: AssetExecutionContext,
) -> None:
    """Parse, clean, validate, and deduplicate Bronze data into Silver records.

    Steps:
    1. Read Bronze Parquet files for all locations on this partition date
    2. Parse raw JSON → extract hourly arrays
    3. Explode into individual hourly records
    4. Filter records to only this partition date
    5. Validate each record with Pydantic
    6. Deduplicate on (location_name, timestamp_utc)
    7. Write clean Parquet to Silver layer
    """
    partition_date_str = context.partition_key
    locations = load_locations_config()
    data_dir = get_data_dir()
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    all_records: list[dict] = []
    invalid_count = 0
    total_raw = 0

    for location in locations:
        bronze_file = bronze_dir / location.name / f"{partition_date_str}.parquet"

        if not bronze_file.exists():
            context.log.warning(
                f"Bronze file not found for {location.name} on {partition_date_str}, skipping"
            )
            continue

        # Read Bronze Parquet
        bronze_table = pq.read_table(str(bronze_file))
        bronze_df = bronze_table.to_pandas()

        for _, row in bronze_df.iterrows():
            raw_json = json.loads(row["raw_json"])

            # Parse hourly data from the JSON response
            hourly_records = _parse_hourly_data(
                raw_json=raw_json,
                location_name=location.name,
                latitude=raw_json.get("latitude", location.latitude),
                longitude=raw_json.get("longitude", location.longitude),
                local_timezone=location.timezone,
                partition_date=partition_date_str,
                ingestion_ts=pd.Timestamp(row["ingestion_timestamp"]).to_pydatetime(),
            )

            total_raw += len(hourly_records)

            # Validate each record with Pydantic
            for rec in hourly_records:
                try:
                    validated = HourlyWeatherRecord(**rec)
                    all_records.append(validated.model_dump())
                except Exception as e:
                    invalid_count += 1
                    if invalid_count <= 5:  # Only log first 5
                        context.log.warning(f"Invalid record for {location.name}: {e}")

    if not all_records:
        context.log.warning(f"No valid records for partition {partition_date_str}")
        # Write empty Parquet with the Silver columns
        empty_df = pd.DataFrame(columns=SILVER_COLUMNS)
        empty_df.to_parquet(
            str(silver_dir / f"{partition_date_str}.parquet"), compression="snappy", index=False
        )
        return

    # Create DataFrame and deduplicate
    df = pd.DataFrame(all_records)

    # Convert timestamp columns and enforce float64 for weather variables
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df["timestamp_local"] = pd.to_datetime(df["timestamp_local"])
    df["ingestion_timestamp"] = pd.to_datetime(df["ingestion_timestamp"], utc=True)
    df[WEATHER_COLUMNS] = df[WEATHER_COLUMNS].astype("float64")
    df = df[SILVER_COLUMNS]

    # Deduplicate: keep last (most recent ingestion) for each location+timestamp
    before_dedup = len(df)
    df = df.sort_values("ingestion_timestamp").drop_duplicates(
        subset=["location_name", "timestamp_utc"],
        keep="last",
    )
    duplicates_removed = before_dedup - len(df)

    # Sort by location and timestamp
    df = df.sort_values(["location_name", "timestamp_utc"]).reset_index(drop=True)

    # Write Silver Parquet
    output_path = silver_dir / f"{partition_date_str}.parquet"
    df.to_parquet(str(output_path), compression="snappy", index=False)

    # Calculate null statistics for metadata
    null_stats = {}
    for col in WEATHER_COLUMNS:
        if col in df.columns:
            null_count = df[col].isna().sum()
            null_stats[f"{col}_null_pct"] = round(null_count / len(df) * 100, 1)

    context.add_output_metadata(
        {
            "partition_date": partition_date_str,
            "total_records": len(df),
            "raw_records_parsed": total_raw,
            "invalid_records_skipped": invalid_count,
            "duplicates_removed": duplicates_removed,
            "locations": MetadataValue.text(", ".join(df["location_name"].unique())),
            "null_statistics": MetadataValue.json(null_stats),
        }
    )

    context.log.info(
        f"Silver: {len(df)} records for {partition_date_str} "
        f"({invalid_count} invalid, {duplicates_removed} duplicates removed)"
    )


@asset_check(
    asset=silver_hourly_weather,
    description=(
        "Validate Silver data quality: row counts, null rates, value ranges, no duplicates."
    ),
)
def silver_quality_check(context) -> AssetCheckResult:
    """Comprehensive data quality check on Silver layer data."""
    partition_date_str = context.partition_key
    silver_dir = get_data_dir() / "silver"
    silver_file = silver_dir / f"{partition_date_str}.parquet"

    if not silver_file.exists():
        return AssetCheckResult(
            passed=False,
            metadata={"error": "Silver file not found"},
            description=f"Silver file missing for {partition_date_str}",
        )

    df = pd.read_parquet(str(silver_file))
    locations = load_locations_config()
    issues: list[str] = []

    # 1. Row count: expect ~24 rows per location per day
    expected_min = len(locations) * 20  # Allow some missing hours
    expected_max = len(locations) * 24
    if len(df) < expected_min:
        issues.append(f"Row count {len(df)} below expected minimum {expected_min}")
    elif len(df) > expected_max:
        issues.append(f"Row count {len(df)} above expected maximum {expected_max}")

    # 2. Null rate for temperature (should be < 5%)
    if "temperature_2m" in df.columns and len(df) > 0:
        temp_null_pct = df["temperature_2m"].isna().sum() / len(df) * 100
        if temp_null_pct > 5.0:
            issues.append(f"temperature_2m null rate {temp_null_pct:.1f}% exceeds 5%")

    # 3. Value range checks
    for col, (min_val, max_val) in VALUE_RANGES.items():
        if col in df.columns:
            valid_values = df[col].dropna()
            if len(valid_values) > 0:
                actual_min = valid_values.min()
                actual_max = valid_values.max()
                if actual_min < min_val or actual_max > max_val:
                    issues.append(
                        f"{col} out of range [{min_val}, {max_val}]: "
                        f"actual [{actual_min}, {actual_max}]"
                    )

    # 4. No duplicate (location_name, timestamp_utc) pairs
    if "timestamp_utc" in df.columns and "location_name" in df.columns:
        dupes = df.duplicated(subset=["location_name", "timestamp_utc"]).sum()
        if dupes > 0:
            issues.append(f"{dupes} duplicate (location, timestamp) pairs found")

    passed = len(issues) == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "row_count": len(df),
            "locations_found": (
                list(df["location_name"].unique()) if "location_name" in df.columns else []
            ),
            "issues": issues if issues else ["All checks passed"],
            "partition_date": partition_date_str,
        },
        description=(
            f"All quality checks passed ({len(df)} rows)"
            if passed
            else f"Quality issues: {'; '.join(issues)}"
        ),
    )
