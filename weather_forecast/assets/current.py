"""Current weather assets — 15-minute snapshot history and latest conditions.

Open-Meteo's `current` section is the model value for the current 15-minute
slot (a nowcast, not a station observation). It is only available from the
forecast endpoint and can't be requested for a past time, so:

- bronze_current appends one snapshot per poll, named by observation time,
  and only runs for today's (UTC) partition. Missed polls are permanent gaps.
- silver_current_weather rebuilds the day from all snapshots (deduplicated)
  and compacts them into one Parquet file.
- gold_current_conditions keeps the latest snapshot per location.

These assets run on their own job/schedule (see schedules.py) and are kept
out of the hourly pipeline job, so historical backfills never touch them.

File layout:
    data/bronze/current/{location_name}/{partition_date}/{HHMM}.parquet
    data/silver/current/{partition_date}.parquet
    data/gold/current_conditions/latest.parquet
    warehouse/weather.duckdb (table: gold_current_conditions)
"""

import json
import logging
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)

from weather_forecast.assets.bronze import write_bronze_record
from weather_forecast.assets.silver import (
    SECTION_KEYS,
    BronzeRow,
    build_silver_section,
    check_result,
    range_issues,
    silver_path,
)
from weather_forecast.models.weather import LocationConfig
from weather_forecast.partitions import daily_partitions
from weather_forecast.resources.api_client import (
    OpenMeteoClient,
    OpenMeteoRequest,
    unsupported_variables,
)
from weather_forecast.resources.duckdb_resource import DuckDBResource
from weather_forecast.utils.helpers import (
    epoch_to_utc,
    fill_location_ids,
    get_data_dir,
    load_locations_config,
    load_variables_config,
    utc_today,
    write_parquet,
)

logger = logging.getLogger(__name__)


def bronze_current_dir(location_name: str, partition_date: str) -> Path:
    """Return the directory holding a location's current snapshots for a day."""
    return get_data_dir() / "bronze" / "current" / location_name / partition_date


def current_conditions_path() -> Path:
    """Return the Parquet path of the latest-conditions Gold table."""
    return get_data_dir() / "gold" / "current_conditions" / "latest.parquet"


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    compute_kind="python",
    description="Raw 15-minute current-weather snapshots (today only, one file per snapshot).",
    # Yields no materialization when nothing was fetched, so past days stay "missing"
    # and the downstream current-weather steps are skipped (#35)
    output_required=False,
)
def bronze_current(
    context: AssetExecutionContext,
    open_meteo_client: OpenMeteoClient,
) -> Iterator[MaterializeResult]:
    """Fetch and store the current-conditions snapshot for every location.

    Each file is named after the snapshot's observation time (current.time),
    so re-running within the same 15-minute slot overwrites the same file.
    For any partition other than today (UTC) nothing is fetched or written:
    current conditions can't be retrieved retroactively. In that case (and
    when no snapshot could be written) the asset records NO materialization
    and silver_current_weather / gold_current_conditions are skipped, instead
    of marking the day as materialized with empty data.
    """
    partition_date_str = context.partition_key
    partition_date = date.fromisoformat(partition_date_str)
    config = load_variables_config()

    skip_reason = None
    if not config.current_names:
        skip_reason = "no current_variables configured"
    elif partition_date != utc_today():
        skip_reason = "current weather can only be fetched for today's (UTC) partition"
    if skip_reason:
        context.log.warning(
            f"Not materializing current weather for {partition_date_str}: {skip_reason}"
        )
        return

    ingestion_ts = datetime.now(timezone.utc)
    observed: dict[str, str] = {}
    unsupported: dict[str, list[str]] = {}

    for location in load_locations_config():
        request = OpenMeteoRequest(
            latitude=location.latitude,
            longitude=location.longitude,
            current=config.current_names,
            timezone=location.timezone,
            timeformat="unixtime",
            params=config.settings.request_params,
        )
        raw_response = open_meteo_client.fetch("forecast", request)

        snapshot_time = (raw_response.get("current") or {}).get("time")
        if snapshot_time is None:
            context.log.warning(f"No current section returned for {location.name}, skipping")
            continue
        observed_at = epoch_to_utc(snapshot_time)
        if observed_at.date() != partition_date:
            context.log.warning(
                f"Snapshot for {location.name} observed at {observed_at.isoformat()} is "
                f"outside partition {partition_date_str}, skipping"
            )
            continue

        missing = unsupported_variables(raw_response).get("current")
        if missing:
            unsupported[location.name] = missing

        output_path = bronze_current_dir(location.name, partition_date_str) / (
            f"{observed_at:%H%M}.parquet"
        )
        write_bronze_record(
            output_path, raw_response, location, partition_date_str, ingestion_ts, "forecast"
        )
        observed[location.name] = observed_at.isoformat()
        context.log.info(f"Wrote current snapshot for {location.name} at {observed_at:%H:%M} UTC")

    if not observed:
        context.log.warning(f"No current snapshot written for {partition_date_str}")
        return
    yield MaterializeResult(
        metadata={
            "snapshots_written": len(observed),
            "observed_at_utc": MetadataValue.json(observed),
            "unsupported_variables": MetadataValue.json(unsupported),
        }
    )


def _bronze_current_rows(context: AssetExecutionContext) -> list[BronzeRow]:
    """Load every current snapshot of all locations for the partition day."""
    rows: list[BronzeRow] = []
    for location in load_locations_config():
        snapshot_dir = bronze_current_dir(location.name, context.partition_key)
        for path in sorted(snapshot_dir.glob("*.parquet")):
            for row in pq.read_table(str(path)).to_pylist():
                ingestion_ts = pd.Timestamp(row["ingestion_timestamp"]).to_pydatetime()
                rows.append((location, json.loads(row["raw_json"]), ingestion_ts))
    return rows


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    compute_kind="python",
    deps=[bronze_current],
    description="Current-weather snapshot history for the day (columns from config).",
)
def silver_current_weather(context: AssetExecutionContext) -> None:
    """Rebuild the day's snapshot history from all Bronze snapshot files.

    Rebuilding from every file makes re-runs idempotent and compacts the
    day's small Bronze files into a single Parquet file.
    """
    rows = _bronze_current_rows(context)
    build_silver_section(context, "current", rows, silver_path("current", context.partition_key))


@asset(
    group_name="gold",
    compute_kind="duckdb",
    deps=[silver_current_weather],
    description="Latest current-weather snapshot per location, with its age in minutes.",
)
def gold_current_conditions(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> None:
    """Keep the most recent snapshot per location (unpartitioned, overwritten each run).

    Reads today's Silver history plus yesterday's, so the first runs after
    UTC midnight still find a recent snapshot. If no snapshots exist the
    previous table is kept (last known conditions); the freshness check
    flags it as stale.
    """
    today = utc_today()
    frames = [
        pd.read_parquet(path)
        for day in (today - timedelta(days=1), today)
        if (path := silver_path("current", day.isoformat())).exists()
    ]
    history = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not history.empty:
        history = fill_location_ids(history)  # files from before location_id existed
    if history.empty:
        context.log.warning("No current-weather snapshots found; keeping previous conditions")
        context.add_output_metadata({"rows": 0})
        return

    latest = (
        history.sort_values("observed_at_utc", kind="stable")
        .groupby("location_id")
        .tail(1)
        .sort_values("location_id")
        .reset_index(drop=True)
    )
    now = pd.Timestamp.now(tz="UTC")
    latest["age_minutes"] = ((now - latest["observed_at_utc"]).dt.total_seconds() / 60).round(1)

    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE gold_current_conditions AS SELECT * FROM latest")

    write_parquet(latest, current_conditions_path())

    context.add_output_metadata(
        {
            "rows": len(latest),
            "observed_at_utc": MetadataValue.json(
                dict(zip(latest["location_name"], latest["observed_at_utc"].astype(str)))
            ),
            "preview": MetadataValue.md(latest.to_markdown(index=False)),
        }
    )


@asset_check(
    asset=silver_current_weather,
    description="Validate current-weather history: no duplicate snapshots, value ranges.",
)
def silver_current_check(context) -> AssetCheckResult:
    """Check snapshot uniqueness and configured value ranges."""
    partition_date_str = context.partition_key
    path = silver_path("current", partition_date_str)
    if not path.exists():
        return AssetCheckResult(
            passed=False,
            metadata={"error": "Silver current file not found"},
            description=f"Silver current file missing for {partition_date_str}",
        )

    df = pd.read_parquet(str(path))
    issues = range_issues(df, load_variables_config().current_variables)
    dupes = df.duplicated(subset=SECTION_KEYS["current"]).sum()
    if dupes > 0:
        issues.append(f"{dupes} duplicate (location, observed_at_utc) snapshots found")
    return check_result(df, issues, partition_date_str)


def stale_locations(
    df: pd.DataFrame, locations: list[LocationConfig], max_age: timedelta, now: datetime
) -> dict[str, str]:
    """Return locations whose latest snapshot is missing or older than max_age."""
    latest = dict(zip(df["location_name"], df["observed_at_utc"])) if not df.empty else {}
    stale = {}
    for location in locations:
        observed = latest.get(location.name)
        if observed is None:
            stale[location.name] = "no snapshot"
        elif now - observed > max_age:
            stale[location.name] = f"last snapshot {observed.isoformat()}"
    return stale


@asset_check(
    asset=gold_current_conditions,
    description="Every location has a current snapshot no older than 2x the poll interval.",
)
def gold_current_freshness_check(context) -> AssetCheckResult:
    """Fail when the scheduler stopped or the API failed for some location."""
    settings = load_variables_config().settings
    max_age = timedelta(minutes=2 * settings.current_interval_minutes)
    path = current_conditions_path()
    df = pd.read_parquet(str(path)) if path.exists() else pd.DataFrame()

    stale = stale_locations(df, load_locations_config(), max_age, datetime.now(timezone.utc))
    return AssetCheckResult(
        passed=not stale,
        metadata={
            "max_age_minutes": int(max_age.total_seconds() // 60),
            "stale_locations": MetadataValue.json(stale),
        },
        description=(
            "All locations have fresh current conditions"
            if not stale
            else f"Stale current conditions: {stale}"
        ),
    )
