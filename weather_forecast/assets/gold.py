"""Gold layer assets — aggregated daily weather summaries.

Reads Silver hourly Parquet files, computes daily aggregations using DuckDB,
and dual-writes to both Parquet files and DuckDB tables.

A Gold partition is a LOCAL calendar day: partition d holds each location's
day d in its own timezone, built from the hourly rows of the neighbouring UTC
Silver partitions (d-1..d+1). `hours_covered` / `total_hours` show how many
hours went in (24, or 23/25 on DST change days).

Gold assets:
- gold_daily_temperature: min/max/avg temperature, humidity, wind, UV
- gold_daily_precipitation: precipitation totals, probability, rainy hours

The SQL depends on specific hourly variables and metric units (column names
end in _c, _kmh, _mm); validate_gold_requirements() checks the config for
them when the Dagster definitions load.

File layout:
    data/gold/daily_temperature/{partition_date}.parquet
    data/gold/daily_precipitation/{partition_date}.parquet
    warehouse/weather.duckdb (tables: gold_daily_temperature, gold_daily_precipitation)
"""

import logging
import warnings
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetDep,
    AssetExecutionContext,
    BetaWarning,
    MetadataValue,
    TimeWindowPartitionMapping,
    asset,
    asset_check,
)

from weather_forecast.assets.silver import silver_hourly_weather, silver_path
from weather_forecast.models.weather import VariablesConfig
from weather_forecast.partitions import daily_partitions
from weather_forecast.resources.duckdb_resource import DuckDBResource, ensure_table
from weather_forecast.utils.helpers import (
    fill_location_ids,
    get_data_dir,
    load_locations_config,
    write_parquet,
)

logger = logging.getLogger(__name__)

# Hourly variables each Gold asset's SQL reads
REQUIRED_HOURLY_VARIABLES: dict[str, tuple[str, ...]] = {
    "gold_daily_temperature": (
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "uv_index",
    ),
    "gold_daily_precipitation": ("precipitation", "precipitation_probability"),
}

# Units implied by the Gold column names
REQUIRED_UNITS: dict[str, str] = {
    "temperature_unit": "celsius",
    "wind_speed_unit": "kmh",
    "precipitation_unit": "mm",
}


def validate_gold_requirements(config: VariablesConfig) -> None:
    """Fail fast if the config lacks variables or units the Gold SQL relies on.

    Raises:
        ValueError: Listing every missing hourly variable and non-metric unit.
    """
    configured = set(config.hourly_names)
    problems = [
        f"{asset_name} needs hourly variables {missing}"
        for asset_name, required in REQUIRED_HOURLY_VARIABLES.items()
        if (missing := [v for v in required if v not in configured])
    ]
    problems += [
        f"Gold column names assume {field}={expected!r}, got {actual!r}"
        for field, expected in REQUIRED_UNITS.items()
        if (actual := getattr(config.settings, field)) != expected
    ]
    if problems:
        raise ValueError(
            "config/variables.yaml doesn't meet Gold layer requirements:\n- "
            + "\n- ".join(problems)
        )


TEMPERATURE_COLUMNS = {
    "location_id": "VARCHAR",
    "location_name": "VARCHAR",
    "date": "VARCHAR",
    "temp_min_c": "DOUBLE",
    "temp_max_c": "DOUBLE",
    "temp_avg_c": "DOUBLE",
    "daytime_temp_min_c": "DOUBLE",
    "daytime_temp_max_c": "DOUBLE",
    "humidity_avg_pct": "DOUBLE",
    "wind_max_kmh": "DOUBLE",
    "uv_max": "DOUBLE",
    "hours_covered": "INTEGER",
}

PRECIPITATION_COLUMNS = {
    "location_id": "VARCHAR",
    "location_name": "VARCHAR",
    "date": "VARCHAR",
    "total_precipitation_mm": "DOUBLE",
    "max_precip_probability_pct": "DOUBLE",
    "avg_precip_probability_pct": "DOUBLE",
    "rainy_hours": "INTEGER",
    "total_hours": "INTEGER",
    "pct_hours_with_rain": "DOUBLE",
}

GOLD_KEY = ["location_id", "date"]


def gold_path(metric: str, partition_date: str) -> Path:
    """Return the Gold Parquet path of a metric for a partition date."""
    return get_data_dir() / "gold" / metric / f"{partition_date}.parquet"


# A local calendar day spans the neighbouring UTC days for any UTC offset. The
# last partition (today) maps to tomorrow's not-yet-existing Silver partition,
# which must be allowed; Dagster marks that parameter beta, so its warning is
# silenced here only.
with warnings.catch_warnings():
    warnings.simplefilter("ignore", BetaWarning)
    LOCAL_DAY_PARTITION_MAPPING = TimeWindowPartitionMapping(
        start_offset=-1, end_offset=1, allow_nonexistent_upstream_partitions=True
    )
SILVER_HOURLY_LOCAL_DAY_DEP = AssetDep(
    silver_hourly_weather, partition_mapping=LOCAL_DAY_PARTITION_MAPPING
)


def load_local_day_hourly(partition_date: str) -> pd.DataFrame:
    """Silver hourly rows whose LOCAL date is partition_date, for every location.

    Silver hourly partitions hold UTC days. A location's local day d spans UTC
    days d-1..d+1 (positive offsets reach back, negative ones forward), so the
    three neighbouring files are read and filtered on timestamp_local. Rows from
    files written before location_id existed get it from config.
    """
    day = date.fromisoformat(partition_date)
    frames = [
        pd.read_parquet(str(path))
        for offset in (-1, 0, 1)
        if (path := silver_path("hourly", (day + timedelta(days=offset)).isoformat())).exists()
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    hourly = fill_location_ids(pd.concat(frames, ignore_index=True))
    hourly = hourly[hourly["timestamp_local"].dt.date == day]
    # Each Silver file should hold only its own UTC day; don't rely on it, since an
    # overlapping hour would be counted twice in sums and hours_covered
    hourly = hourly.sort_values("ingestion_timestamp", kind="stable").drop_duplicates(
        subset=["location_id", "timestamp_utc"], keep="last"
    )
    return hourly.sort_values(["location_id", "timestamp_utc"]).reset_index(drop=True)


def _load_hourly(context: AssetExecutionContext) -> pd.DataFrame | None:
    """Load the hourly rows of the partition's local day (None if there are none)."""
    hourly = load_local_day_hourly(context.partition_key)
    if hourly.empty:
        context.log.warning(f"No Silver hourly rows for local day {context.partition_key}")
        return None
    return hourly


def _clear_gold_partition(
    context: AssetExecutionContext, duckdb_resource: DuckDBResource, table: str
) -> None:
    """Remove a partition's Gold rows and Parquet file when there's no input for it.

    Without this, a re-run after its Silver input disappeared kept the previous
    rows and file, and gold_temperature_check passed on the stale data (#39).
    """
    partition_date_str = context.partition_key
    with duckdb_resource.get_connection() as conn:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        deleted = 0
        if table in tables:
            deleted = conn.execute(
                f"DELETE FROM {table} WHERE date = ? RETURNING 1", [partition_date_str]
            ).fetchall()
            deleted = len(deleted)
    path = gold_path(table.removeprefix("gold_"), partition_date_str)
    removed_file = path.exists()
    path.unlink(missing_ok=True)
    context.log.warning(
        f"{table}: no input for {partition_date_str}; cleared {deleted} stale rows"
        + (" and the Parquet file" if removed_file else "")
    )
    context.add_output_metadata(
        {"partition_date": partition_date_str, "rows": 0, "stale_rows_removed": deleted}
    )


def _write_gold(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
    table: str,
    columns: dict[str, str],
    result_df: pd.DataFrame,
) -> None:
    """Upsert a partition's rows into DuckDB and write its Parquet file."""
    partition_date_str = context.partition_key
    result_df = result_df[list(columns)]

    with duckdb_resource.get_connection() as conn:
        if ensure_table(conn, table, columns, GOLD_KEY):
            context.log.warning(f"{table} schema changed; table recreated (re-run the backfill)")
        # Upsert: delete existing records for this partition, then insert
        conn.execute(f"DELETE FROM {table} WHERE date = ?", [partition_date_str])
        conn.execute(f"INSERT INTO {table} BY NAME SELECT * FROM result_df")

    write_parquet(result_df, gold_path(table.removeprefix("gold_"), partition_date_str))

    context.add_output_metadata(
        {
            "partition_date": partition_date_str,
            "locations": MetadataValue.text(", ".join(result_df["location_name"].tolist())),
            "rows": len(result_df),
            "preview": MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )
    context.log.info(f"{table}: {len(result_df)} rows for {partition_date_str}")


def _aggregate(hourly: pd.DataFrame, select_sql: str, partition_date: str) -> pd.DataFrame:
    """Run a per-location aggregation over the hourly rows with DuckDB."""
    sql = f"""
        SELECT
            location_id,
            location_name,
            '{partition_date}' AS date,
            {select_sql}
        FROM hourly
        GROUP BY location_id, location_name
        ORDER BY location_id
    """
    with duckdb.connect() as conn:
        conn.register("hourly", hourly)
        return conn.execute(sql).fetchdf()


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    compute_kind="duckdb",
    deps=[SILVER_HOURLY_LOCAL_DAY_DEP],
    description="Daily temperature summary: min/max/avg temps, humidity, wind, UV by location.",
)
def gold_daily_temperature(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> None:
    """Compute daily temperature and related weather aggregations.

    Aggregates the Silver hourly rows of each location's LOCAL calendar day
    equal to the partition date:
    - Min/max/avg temperature (24h and daytime 6am-6pm)
    - Average humidity
    - Max wind speed
    - Max UV index

    Results are written to both Parquet and DuckDB.
    """
    hourly = _load_hourly(context)
    if hourly is None:
        _clear_gold_partition(context, duckdb_resource, "gold_daily_temperature")
        return
    result_df = _aggregate(
        hourly,
        """
            MIN(temperature_2m) AS temp_min_c,
            MAX(temperature_2m) AS temp_max_c,
            ROUND(AVG(temperature_2m), 2) AS temp_avg_c,
            MIN(CASE WHEN HOUR(timestamp_local) BETWEEN 6 AND 18
                THEN temperature_2m END) AS daytime_temp_min_c,
            MAX(CASE WHEN HOUR(timestamp_local) BETWEEN 6 AND 18
                THEN temperature_2m END) AS daytime_temp_max_c,
            ROUND(AVG(relative_humidity_2m), 2) AS humidity_avg_pct,
            MAX(wind_speed_10m) AS wind_max_kmh,
            MAX(uv_index) AS uv_max,
            COUNT(*) AS hours_covered
        """,
        context.partition_key,
    )
    if result_df.empty:
        _clear_gold_partition(context, duckdb_resource, "gold_daily_temperature")
        return
    _write_gold(context, duckdb_resource, "gold_daily_temperature", TEMPERATURE_COLUMNS, result_df)


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    compute_kind="duckdb",
    deps=[SILVER_HOURLY_LOCAL_DAY_DEP],
    description="Daily precipitation summary: totals, probability, rainy hours by location.",
)
def gold_daily_precipitation(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> None:
    """Compute daily precipitation aggregations.

    Aggregates the Silver hourly rows of each location's LOCAL calendar day
    equal to the partition date:
    - Total precipitation (mm)
    - Max and average precipitation probability
    - Number of rainy hours and percentage
    """
    hourly = _load_hourly(context)
    if hourly is None:
        _clear_gold_partition(context, duckdb_resource, "gold_daily_precipitation")
        return
    result_df = _aggregate(
        hourly,
        """
            ROUND(COALESCE(SUM(precipitation), 0), 2) AS total_precipitation_mm,
            MAX(precipitation_probability) AS max_precip_probability_pct,
            ROUND(AVG(precipitation_probability), 2) AS avg_precip_probability_pct,
            COUNT(*) FILTER (WHERE precipitation > 0) AS rainy_hours,
            COUNT(*) AS total_hours,
            ROUND(
                COUNT(*) FILTER (WHERE precipitation > 0) * 100.0 / NULLIF(COUNT(*), 0),
                1
            ) AS pct_hours_with_rain
        """,
        context.partition_key,
    )
    if result_df.empty:
        _clear_gold_partition(context, duckdb_resource, "gold_daily_precipitation")
        return
    _write_gold(
        context, duckdb_resource, "gold_daily_precipitation", PRECIPITATION_COLUMNS, result_df
    )


# Expected hours in a local day (DST change days have 23 or 25)
COMPLETE_DAY_HOURS = (23, 25)
# Gold vs the API's own daily extremes (Silver daily). The API's daily min/max
# don't come from the 24 hourly values alone: they include the day boundary
# (e.g. a daily min equal to 00:00 of the next day) and sub-hourly model data,
# so Gold (hourly) lies slightly INSIDE the API's range. Measured on the 31-day
# backfill: worst gap 0.6 °C, never more extreme than the API (#32).
SILVER_DAILY_TOLERANCE_C = 1.0
# Rounding slack when checking Gold isn't more extreme than the API
ROUNDING_SLACK_C = 0.05


def _silver_daily_differences(df: pd.DataFrame, partition_date: str) -> list[str]:
    """Compare Gold min/max with the API's daily aggregates in Silver daily.

    Warns when Gold lies outside the API's daily range (impossible for the
    same underlying data) or more than SILVER_DAILY_TOLERANCE_C inside it.
    Only a warning: the API also builds days on the response's fixed UTC
    offset, so across a DST change its window is shifted by an hour.
    """
    path = silver_path("daily", partition_date)
    if not path.exists():
        return []
    daily = fill_location_ids(pd.read_parquet(str(path))).set_index("location_id")
    differences = []
    for _, row in df.iterrows():
        if row["location_id"] not in daily.index:
            continue
        for gold_col, api_col, sign in (
            ("temp_max_c", "temperature_2m_max", 1),
            ("temp_min_c", "temperature_2m_min", -1),
        ):
            if api_col not in daily.columns:
                continue
            api_value = daily.loc[row["location_id"], api_col]
            gold_value = row[gold_col]
            if pd.isna(api_value) or pd.isna(gold_value):
                continue
            # sign * (gold - api) > 0 means Gold is more extreme than the API
            outside = sign * (gold_value - api_value) > ROUNDING_SLACK_C
            if outside or abs(gold_value - api_value) > SILVER_DAILY_TOLERANCE_C:
                reason = "outside" if outside else "far inside"
                differences.append(
                    f"{row['location_id']}: {gold_col} {gold_value} vs API {api_col} "
                    f"{api_value} ({reason} the API's daily range)"
                )
    return differences


def temperature_issues(df: pd.DataFrame, num_locations: int) -> tuple[list[str], list[str]]:
    """(errors, warnings) for one partition of gold_daily_temperature, excluding the
    comparison with Silver daily (which needs that partition's file)."""
    errors: list[str] = []
    warnings: list[str] = []

    # Check one row per configured location
    if len(df) != num_locations:
        errors.append(f"Row count {len(df)} != {num_locations} configured locations")

    # Check no null keys
    for key in ("location_id", "location_name", "date"):
        if key not in df.columns or df[key].isna().any():
            errors.append(f"Null or missing {key} found")

    # Check temperature ordering: min <= avg <= max (small float tolerance)
    for _, row in df.iterrows():
        t_min, t_avg, t_max = row.get("temp_min_c"), row.get("temp_avg_c"), row.get("temp_max_c")
        loc = row.get("location_id", "?")
        if pd.notna(t_min) and pd.notna(t_avg) and pd.notna(t_max):
            if not (t_min <= t_avg + 0.01):
                errors.append(f"{loc}: temp_min ({t_min}) > temp_avg ({t_avg})")
            if not (t_avg <= t_max + 0.01):
                errors.append(f"{loc}: temp_avg ({t_avg}) > temp_max ({t_max})")

    # Complete local day (an edge partition may lack a neighbouring UTC day)
    low, high = COMPLETE_DAY_HOURS
    for _, row in df.iterrows():
        hours = row.get("hours_covered")
        if pd.isna(hours) or not low <= hours <= high:
            warnings.append(f"{row.get('location_id', '?')}: only {hours} hours of the local day")
    return errors, warnings


@asset_check(
    asset=gold_daily_temperature,
    description=(
        "Validate Gold temperature: keys, row count, min <= avg <= max (errors); "
        "complete local day and agreement with Silver daily (warnings)."
    ),
)
def gold_temperature_check(context) -> AssetCheckResult:
    """Validate Gold daily temperature aggregations."""
    partition_date_str = context.partition_key
    gold_file = gold_path("daily_temperature", partition_date_str)

    if not gold_file.exists():
        return AssetCheckResult(
            passed=False,
            metadata={"error": "Gold temperature file not found"},
        )

    df = pd.read_parquet(str(gold_file))
    errors, warnings = temperature_issues(df, len(load_locations_config()))
    warnings += _silver_daily_differences(df, partition_date_str)

    issues = errors + warnings
    return AssetCheckResult(
        passed=not issues,
        severity=AssetCheckSeverity.ERROR if errors else AssetCheckSeverity.WARN,
        metadata={
            "row_count": len(df),
            "errors": errors,
            "warnings": warnings,
            "partition_date": partition_date_str,
        },
        description=(
            f"Temperature checks passed ({len(df)} rows)"
            if not issues
            else f"Issues: {'; '.join(issues)}"
        ),
    )
