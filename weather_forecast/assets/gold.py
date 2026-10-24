"""Gold layer assets — aggregated daily weather summaries.

Reads Silver Parquet files, computes daily aggregations using DuckDB,
and dual-writes to both Parquet files and DuckDB tables.

Gold assets:
- gold_daily_temperature: min/max/avg temperature, humidity, wind, UV
- gold_daily_precipitation: precipitation totals, probability, rainy hours

File layout:
    data/gold/daily_temperature/{partition_date}.parquet
    data/gold/daily_precipitation/{partition_date}.parquet
    warehouse/weather.duckdb (tables: gold_daily_temperature, gold_daily_precipitation)
"""

import logging

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MetadataValue,
    asset,
    asset_check,
)

from weather_forecast.assets.silver import silver_hourly_weather, silver_path
from weather_forecast.partitions import daily_partitions
from weather_forecast.resources.duckdb_resource import DuckDBResource
from weather_forecast.utils.helpers import get_data_dir, load_locations_config

logger = logging.getLogger(__name__)


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    compute_kind="duckdb",
    deps=[silver_hourly_weather],
    description="Daily temperature summary: min/max/avg temps, humidity, wind, UV by location.",
)
def gold_daily_temperature(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> None:
    """Compute daily temperature and related weather aggregations.

    Reads the Silver layer Parquet file for this partition date and computes:
    - Min/max/avg temperature (24h and daytime 6am-6pm)
    - Average humidity
    - Max wind speed
    - Max UV index

    Results are written to both Parquet and DuckDB.
    """
    partition_date_str = context.partition_key
    data_dir = get_data_dir()
    silver_file = silver_path("hourly", partition_date_str)

    if not silver_file.exists():
        context.log.warning(f"Silver file not found for {partition_date_str}, skipping Gold")
        return

    # Use DuckDB to query the Silver Parquet directly
    sql = f"""
        SELECT
            location_name,
            '{partition_date_str}' AS date,
            MIN(temperature_2m) AS temp_min_c,
            MAX(temperature_2m) AS temp_max_c,
            ROUND(AVG(temperature_2m), 2) AS temp_avg_c,
            MIN(CASE WHEN HOUR(timestamp_local) BETWEEN 6 AND 18
                THEN temperature_2m END) AS daytime_temp_min_c,
            MAX(CASE WHEN HOUR(timestamp_local) BETWEEN 6 AND 18
                THEN temperature_2m END) AS daytime_temp_max_c,
            ROUND(AVG(relative_humidity_2m), 2) AS humidity_avg_pct,
            MAX(wind_speed_10m) AS wind_max_kmh,
            MAX(uv_index) AS uv_max
        FROM read_parquet('{silver_file}')
        GROUP BY location_name
        ORDER BY location_name
    """

    with duckdb_resource.get_connection() as conn:
        result_df = conn.execute(sql).fetchdf()

        if result_df.empty:
            context.log.warning(f"No temperature data for {partition_date_str}")
            return

        # Ensure Gold DuckDB table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_daily_temperature (
                location_name VARCHAR,
                date VARCHAR,
                temp_min_c DOUBLE,
                temp_max_c DOUBLE,
                temp_avg_c DOUBLE,
                daytime_temp_min_c DOUBLE,
                daytime_temp_max_c DOUBLE,
                humidity_avg_pct DOUBLE,
                wind_max_kmh DOUBLE,
                uv_max DOUBLE,
                PRIMARY KEY (location_name, date)
            )
        """)

        # Upsert: delete existing records for this partition, then insert
        conn.execute(
            "DELETE FROM gold_daily_temperature WHERE date = ?",
            [partition_date_str],
        )
        conn.execute("INSERT INTO gold_daily_temperature SELECT * FROM result_df")

    # Also write to Parquet
    gold_temp_dir = data_dir / "gold" / "daily_temperature"
    gold_temp_dir.mkdir(parents=True, exist_ok=True)
    output_path = gold_temp_dir / f"{partition_date_str}.parquet"
    result_df.to_parquet(str(output_path), compression="snappy", index=False)

    context.add_output_metadata(
        {
            "partition_date": partition_date_str,
            "locations": MetadataValue.text(", ".join(result_df["location_name"].tolist())),
            "rows": len(result_df),
            "preview": MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )

    context.log.info(f"Gold temperature: {len(result_df)} rows for {partition_date_str}")


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    compute_kind="duckdb",
    deps=[silver_hourly_weather],
    description="Daily precipitation summary: totals, probability, rainy hours by location.",
)
def gold_daily_precipitation(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> None:
    """Compute daily precipitation aggregations.

    Reads the Silver layer Parquet file for this partition date and computes:
    - Total precipitation (mm)
    - Max and average precipitation probability
    - Number of rainy hours and percentage
    """
    partition_date_str = context.partition_key
    data_dir = get_data_dir()
    silver_file = silver_path("hourly", partition_date_str)

    if not silver_file.exists():
        context.log.warning(f"Silver file not found for {partition_date_str}, skipping Gold")
        return

    sql = f"""
        SELECT
            location_name,
            '{partition_date_str}' AS date,
            ROUND(COALESCE(SUM(precipitation), 0), 2) AS total_precipitation_mm,
            MAX(precipitation_probability) AS max_precip_probability_pct,
            ROUND(AVG(precipitation_probability), 2) AS avg_precip_probability_pct,
            COUNT(*) FILTER (WHERE precipitation > 0) AS rainy_hours,
            COUNT(*) AS total_hours,
            ROUND(
                COUNT(*) FILTER (WHERE precipitation > 0) * 100.0 / NULLIF(COUNT(*), 0),
                1
            ) AS pct_hours_with_rain
        FROM read_parquet('{silver_file}')
        GROUP BY location_name
        ORDER BY location_name
    """

    with duckdb_resource.get_connection() as conn:
        result_df = conn.execute(sql).fetchdf()

        if result_df.empty:
            context.log.warning(f"No precipitation data for {partition_date_str}")
            return

        # Ensure Gold DuckDB table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_daily_precipitation (
                location_name VARCHAR,
                date VARCHAR,
                total_precipitation_mm DOUBLE,
                max_precip_probability_pct DOUBLE,
                avg_precip_probability_pct DOUBLE,
                rainy_hours INTEGER,
                total_hours INTEGER,
                pct_hours_with_rain DOUBLE,
                PRIMARY KEY (location_name, date)
            )
        """)

        # Upsert: delete existing records for this partition, then insert
        conn.execute(
            "DELETE FROM gold_daily_precipitation WHERE date = ?",
            [partition_date_str],
        )
        conn.execute("INSERT INTO gold_daily_precipitation SELECT * FROM result_df")

    # Also write to Parquet
    gold_precip_dir = data_dir / "gold" / "daily_precipitation"
    gold_precip_dir.mkdir(parents=True, exist_ok=True)
    output_path = gold_precip_dir / f"{partition_date_str}.parquet"
    result_df.to_parquet(str(output_path), compression="snappy", index=False)

    context.add_output_metadata(
        {
            "partition_date": partition_date_str,
            "locations": MetadataValue.text(", ".join(result_df["location_name"].tolist())),
            "rows": len(result_df),
            "preview": MetadataValue.md(result_df.to_markdown(index=False)),
        }
    )

    context.log.info(f"Gold precipitation: {len(result_df)} rows for {partition_date_str}")


@asset_check(
    asset=gold_daily_temperature,
    description="Validate Gold temperature: min <= avg <= max, no null keys, correct row count.",
)
def gold_temperature_check(context) -> AssetCheckResult:
    """Validate Gold daily temperature aggregations."""
    partition_date_str = context.partition_key
    gold_file = get_data_dir() / "gold" / "daily_temperature" / f"{partition_date_str}.parquet"

    if not gold_file.exists():
        return AssetCheckResult(
            passed=False,
            metadata={"error": "Gold temperature file not found"},
        )

    df = pd.read_parquet(str(gold_file))
    issues: list[str] = []

    # Check one row per configured location
    expected_rows = len(load_locations_config())
    if len(df) != expected_rows:
        issues.append(f"Row count {len(df)} != {expected_rows} configured locations")

    # Check no null primary keys
    if df["location_name"].isna().any():
        issues.append("Null location_name found")
    if df["date"].isna().any():
        issues.append("Null date found")

    # Check temperature ordering: min <= avg <= max
    for _, row in df.iterrows():
        t_min = row.get("temp_min_c")
        t_avg = row.get("temp_avg_c")
        t_max = row.get("temp_max_c")
        loc = row.get("location_name", "?")

        if pd.notna(t_min) and pd.notna(t_avg) and pd.notna(t_max):
            if not (t_min <= t_avg + 0.01):  # small tolerance for floating point
                issues.append(f"{loc}: temp_min ({t_min}) > temp_avg ({t_avg})")
            if not (t_avg <= t_max + 0.01):
                issues.append(f"{loc}: temp_avg ({t_avg}) > temp_max ({t_max})")

    passed = len(issues) == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "row_count": len(df),
            "issues": issues if issues else ["All checks passed"],
            "partition_date": partition_date_str,
        },
        description=(
            f"Temperature checks passed ({len(df)} rows)"
            if passed
            else f"Issues: {'; '.join(issues)}"
        ),
    )
