"""Silver layer assets — cleaned, typed, validated weather records.

Reads raw Bronze Parquet files, parses the JSON responses, explodes each
request section into individual records, validates them with Pydantic models
built from config, and writes clean Parquet files.

The schema is wide and config-driven: every variable configured for a
section in config/variables.yaml becomes a column, with no code changes.
Time-valued variables (e.g. sunrise) are UTC timestamps, variables with
`dtype: integer` (codes, flags) nullable Int64, everything else float64.
Variables an endpoint doesn't provide are kept as null columns.

File layout (one file per partition date, containing all locations):
    data/silver/hourly/{partition_date}.parquet
    data/silver/daily/{partition_date}.parquet
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
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
from weather_forecast.models.weather import (
    SILVER_KEY_FIELDS,
    SILVER_METADATA_FIELDS,
    TIME_UNITS,
    LocationConfig,
    Section,
    VariableDefinition,
    VariableKind,
    build_record_model,
)
from weather_forecast.partitions import daily_partitions
from weather_forecast.resources.api_client import unsupported_variables
from weather_forecast.utils.helpers import (
    daily_epoch_to_date,
    epoch_to_utc,
    fill_location_ids,
    get_data_dir,
    load_locations_config,
    load_variables_config,
    write_parquet,
)

logger = logging.getLogger(__name__)

# Columns identifying one record per section (deduplication keys)
SECTION_KEYS: dict[str, list[str]] = {
    "hourly": ["location_id", "timestamp_utc"],
    "daily": ["location_id", "date_local"],
    "current": ["location_id", "observed_at_utc"],
}

# A raw API response with its location and Bronze ingestion time
BronzeRow = tuple[LocationConfig, dict[str, Any], datetime]


def silver_path(section: Section, partition_date: str) -> Path:
    """Return the Silver Parquet path of a section for a partition date."""
    return get_data_dir() / "silver" / section / f"{partition_date}.parquet"


def section_columns(section: Section, variables: list[str]) -> list[str]:
    """Return the Silver column order: keys, configured variables, metadata."""
    return [*SILVER_KEY_FIELDS[section], *variables, *SILVER_METADATA_FIELDS]


def variable_kinds(
    definitions: list[VariableDefinition], responses: list[dict[str, Any]], section: Section
) -> dict[str, VariableKind]:
    """Return how each configured variable of a section is stored.

    "time" if its configured unit or any response's unit is iso8601/unixtime
    (e.g. sunrise), otherwise the configured dtype ("integer" or "float").
    """
    reported_time = set()
    for raw in responses:
        units = raw.get(f"{section}_units") or {}
        reported_time |= {n for n, unit in units.items() if unit in TIME_UNITS and n != "time"}
    return {d.name: "time" if d.name in reported_time else d.kind for d in definitions}


def _section_keys(
    section: Section,
    epoch: int,
    tz: ZoneInfo,
    utc_offset_seconds: int,
    partition_date: date,
    data: dict,
    index: int,
) -> dict[str, Any] | None:
    """Return the key columns of one row, or None if it's outside the partition.

    Hourly and current times are true UTC instants, so local time comes from
    the location's timezone (DST-correct). Daily rows are dated with the
    response's fixed offset, matching how Open-Meteo stamps them.
    """
    if section == "hourly":
        ts_utc = epoch_to_utc(epoch)
        if ts_utc.date() != partition_date:
            return None
        return {
            "timestamp_utc": ts_utc,
            "timestamp_local": ts_utc.astimezone(tz).replace(tzinfo=None),
        }
    if section == "daily":
        local_date = daily_epoch_to_date(epoch, utc_offset_seconds)
        if local_date != partition_date:
            return None
        return {"date_local": local_date}
    # current: one snapshot per response
    observed = epoch_to_utc(epoch)
    if observed.date() != partition_date:
        return None
    return {
        "observed_at_utc": observed,
        "observed_at_local": observed.astimezone(tz).replace(tzinfo=None),
        "interval_seconds": data.get("interval", [None])[index],
    }


def parse_section(
    raw_json: dict[str, Any],
    section: Section,
    kinds: dict[str, VariableKind],
    location: LocationConfig,
    partition_date: date,
    ingestion_ts: datetime,
) -> list[dict[str, Any]]:
    """Parse one section of an Open-Meteo response into flat records.

    Bronze requests unixtime timestamps: hourly rows are kept if their UTC
    date is the partition date, daily rows if their date (local midnight
    under the response's fixed utc_offset_seconds) is, and current snapshots
    if observed (UTC) that day.

    Args:
        raw_json: Parsed API response JSON.
        section: "hourly", "daily" or "current".
        kinds: Configured variables (become columns) mapped to their kind.
        location: The location the response belongs to.
        partition_date: The partition date (UTC day).
        ingestion_ts: When the data was ingested (from Bronze).

    Returns:
        List of dicts, one per record; missing variables are None.
    """
    data = raw_json.get(section) or {}
    if section == "current" and data:
        # A current snapshot is a dict of scalars; treat it as a single row
        data = {key: [value] for key, value in data.items()}
    timestamps = data.get("time", [])
    if not timestamps:
        return []

    time_format = (raw_json.get(f"{section}_units") or {}).get("time")
    if time_format != "unixtime":
        raise ValueError(f"Expected unixtime {section} timestamps from API, got {time_format!r}")
    tz = ZoneInfo(location.timezone)
    utc_offset = raw_json.get("utc_offset_seconds", 0)

    records = []
    for i, epoch in enumerate(timestamps):
        keys = _section_keys(section, epoch, tz, utc_offset, partition_date, data, i)
        if keys is None:
            continue

        record = {
            "location_id": location.id,
            "location_name": location.name,
            "latitude": raw_json.get("latitude", location.latitude),
            "longitude": raw_json.get("longitude", location.longitude),
            **keys,
            "partition_date": partition_date.isoformat(),
            "ingestion_timestamp": ingestion_ts,
        }
        for var in kinds:
            values = data.get(var, [])
            value = values[i] if i < len(values) else None
            if kinds[var] == "time" and isinstance(value, (int, float)):
                value = epoch_to_utc(value)
            record[var] = value

        records.append(record)

    return records


def to_silver_frame(
    section: Section, records: list[dict[str, Any]], kinds: dict[str, VariableKind]
) -> pd.DataFrame:
    """Build a typed Silver DataFrame (also for zero records).

    Variables: "time" -> UTC timestamp, "integer" -> nullable Int64,
    "float" -> float64.
    """
    df = pd.DataFrame(records, columns=section_columns(section, list(kinds)))
    for col in ("timestamp_utc", "observed_at_utc", "ingestion_timestamp"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)
    for col in ("timestamp_local", "observed_at_local"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    for col in ("latitude", "longitude"):
        df[col] = df[col].astype("float64")
    if "interval_seconds" in df.columns:
        df["interval_seconds"] = df["interval_seconds"].astype("Int64")
    for var, kind in kinds.items():
        if kind == "time":
            df[var] = pd.to_datetime(df[var], utc=True)
        elif kind == "integer":
            df[var] = df[var].astype("Int64")
        else:
            df[var] = df[var].astype("float64")
    return df


def build_silver_section(
    context: AssetExecutionContext,
    section: Section,
    bronze_rows: list[BronzeRow],
    output_path: Path,
) -> pd.DataFrame:
    """Parse, validate, deduplicate and write one Silver section for a partition.

    Steps:
    1. Parse each raw response's section into records for this partition
    2. Validate each record with a Pydantic model built from config
    3. Deduplicate on the section keys, keeping the latest ingestion
    4. Write clean Parquet and report metadata (null %, unsupported variables)
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)
    config = load_variables_config()
    definitions = config.section(section)
    variables = [d.name for d in definitions]
    kinds = variable_kinds(definitions, [raw for _, raw, _ in bronze_rows], section)
    record_model = build_record_model(section, kinds)

    records: list[dict] = []
    invalid_count = 0
    total_raw = 0
    unsupported: dict[str, list[str]] = {}

    for location, raw_json, ingestion_ts in bronze_rows:
        parsed = parse_section(
            raw_json, section, kinds, location, partition_date, ingestion_ts
        )
        total_raw += len(parsed)
        missing = unsupported_variables(raw_json).get(section)
        if missing:
            unsupported[location.name] = missing

        for rec in parsed:
            try:
                records.append(record_model(**rec).model_dump())
            except Exception as e:
                invalid_count += 1
                if invalid_count <= 5:  # Only log first 5
                    context.log.warning(f"Invalid {section} record for {location.name}: {e}")

    df = to_silver_frame(section, records, kinds)

    # Deduplicate: keep last (most recent ingestion) for each key
    keys = SECTION_KEYS[section]
    before_dedup = len(df)
    df = df.sort_values("ingestion_timestamp", kind="stable").drop_duplicates(
        subset=keys, keep="last"
    )
    duplicates_removed = before_dedup - len(df)
    df = df.sort_values(keys).reset_index(drop=True)

    write_parquet(df, output_path)

    if df.empty:
        context.log.warning(f"No valid {section} records for partition {partition_date_str}")
    null_stats = {
        f"{var}_null_pct": round(float(df[var].isna().mean()) * 100, 1)
        for var in variables
        if len(df)
    }
    context.add_output_metadata(
        {
            "partition_date": partition_date_str,
            "total_records": len(df),
            "raw_records_parsed": total_raw,
            "invalid_records_skipped": invalid_count,
            "duplicates_removed": duplicates_removed,
            "columns": MetadataValue.json(list(df.columns)),
            "locations": MetadataValue.text(", ".join(df["location_name"].unique())),
            "null_statistics": MetadataValue.json(null_stats),
            "unsupported_variables": MetadataValue.json(unsupported),
        }
    )
    context.log.info(
        f"Silver {section}: {len(df)} records for {partition_date_str} "
        f"({invalid_count} invalid, {duplicates_removed} duplicates removed)"
    )
    return df


def _bronze_weather_rows(context: AssetExecutionContext) -> list[BronzeRow]:
    """Load the hourly/daily Bronze responses of all locations for the partition."""
    partition_date_str = context.partition_key
    bronze_dir = get_data_dir() / "bronze"
    rows: list[BronzeRow] = []

    for location in load_locations_config():
        bronze_file = bronze_dir / location.name / f"{partition_date_str}.parquet"
        if not bronze_file.exists():
            context.log.warning(
                f"Bronze file not found for {location.name} on {partition_date_str}, skipping"
            )
            continue
        for row in pq.read_table(str(bronze_file)).to_pylist():
            ingestion_ts = pd.Timestamp(row["ingestion_timestamp"]).to_pydatetime()
            rows.append((location, json.loads(row["raw_json"]), ingestion_ts))
    return rows


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    compute_kind="python",
    deps=[bronze_forecast],
    description="Cleaned, typed, validated hourly weather records (columns from config).",
)
def silver_hourly_weather(context: AssetExecutionContext) -> None:
    """Hourly records for the UTC partition day, one column per hourly variable."""
    output_path = silver_path("hourly", context.partition_key)
    build_silver_section(context, "hourly", _bronze_weather_rows(context), output_path)


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    compute_kind="python",
    deps=[bronze_forecast],
    description="Daily aggregates per location for the local day equal to the partition date.",
)
def silver_daily_weather(context: AssetExecutionContext) -> None:
    """Open-Meteo daily aggregates over each location's local calendar day."""
    output_path = silver_path("daily", context.partition_key)
    build_silver_section(context, "daily", _bronze_weather_rows(context), output_path)


def range_issues(df: pd.DataFrame, definitions: list[VariableDefinition]) -> list[str]:
    """Return a message per variable whose values fall outside its valid_range."""
    issues = []
    for d in definitions:
        if d.valid_range is None or d.name not in df.columns:
            continue
        values = df[d.name].dropna()
        if values.empty:
            continue
        min_val, max_val = d.valid_range
        actual_min, actual_max = values.min(), values.max()
        if actual_min < min_val or actual_max > max_val:
            issues.append(
                f"{d.name} out of range [{min_val}, {max_val}]: "
                f"actual [{actual_min}, {actual_max}]"
            )
    return issues


def check_result(df: pd.DataFrame, issues: list[str], partition_date: str) -> AssetCheckResult:
    """Build an AssetCheckResult from a list of issues."""
    passed = not issues
    return AssetCheckResult(
        passed=passed,
        metadata={
            "row_count": len(df),
            "locations_found": list(df["location_name"].unique()),
            "issues": issues if issues else ["All checks passed"],
            "partition_date": partition_date,
        },
        description=(
            f"All quality checks passed ({len(df)} rows)"
            if passed
            else f"Quality issues: {'; '.join(issues)}"
        ),
    )


def _missing_file_result(path: Path, partition_date: str) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        metadata={"error": f"Silver file not found: {path.name}"},
        description=f"Silver file missing for {partition_date}",
    )


@asset_check(
    asset=silver_hourly_weather,
    description=(
        "Validate Silver data quality: row counts, null rates, value ranges, no duplicates."
    ),
)
def silver_quality_check(context) -> AssetCheckResult:
    """Comprehensive data quality check on hourly Silver data."""
    partition_date_str = context.partition_key
    silver_file = silver_path("hourly", partition_date_str)
    if not silver_file.exists():
        return _missing_file_result(silver_file, partition_date_str)

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

    # 3. Value range checks (valid_range in config)
    issues += range_issues(df, load_variables_config().hourly_variables)

    # 4. No duplicate (location_name, timestamp_utc) pairs
    dupes = df.duplicated(subset=SECTION_KEYS["hourly"]).sum()
    if dupes > 0:
        issues.append(f"{dupes} duplicate (location, timestamp) pairs found")

    return check_result(df, issues, partition_date_str)


@asset_check(
    asset=silver_daily_weather,
    description="Validate Silver daily data: one row per location, value ranges.",
)
def silver_daily_check(context) -> AssetCheckResult:
    """Check daily Silver data has exactly one in-range row per configured location."""
    partition_date_str = context.partition_key
    silver_file = silver_path("daily", partition_date_str)
    if not silver_file.exists():
        return _missing_file_result(silver_file, partition_date_str)

    df = pd.read_parquet(str(silver_file))
    expected = sorted(loc.id for loc in load_locations_config())
    issues: list[str] = []

    found = sorted(fill_location_ids(df)["location_id"])
    if found != expected:
        issues.append(f"Expected one row per location {expected}, found {found}")
    issues += range_issues(df, load_variables_config().daily_variables)

    return check_result(df, issues, partition_date_str)
