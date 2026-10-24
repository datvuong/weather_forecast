"""Dimension assets — reference tables for joining and labelling the facts.

Dimensions are unpartitioned and rebuilt in full on each run. They're seeded
from YAML in config/, so labels and attributes change without code changes.

- dim_weather_code: WMO weather interpretation codes (config/weather_codes.yaml),
  joined to the weather_code columns of the Silver tables.
- dim_location: configured locations (config/locations.yaml) plus the API's grid
  coordinates and elevation, keyed by location_id. Locations removed from config
  stay as inactive rows so historical facts keep resolving.

File layout:
    data/gold/dimensions/{dim_weather_code,dim_location}.parquet
    warehouse/weather.duckdb (tables: dim_weather_code, dim_location)
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetsDefinition,
    MetadataValue,
    asset,
    asset_check,
)

from weather_forecast.assets.bronze import bronze_forecast
from weather_forecast.assets.current import silver_current_weather
from weather_forecast.assets.silver import silver_daily_weather, silver_hourly_weather, silver_path
from weather_forecast.models.weather import LocationConfig, Section
from weather_forecast.resources.duckdb_resource import DuckDBResource
from weather_forecast.utils.helpers import (
    get_data_dir,
    load_locations_config,
    load_weather_codes,
    write_parquet,
)

logger = logging.getLogger(__name__)


def dimension_path(name: str) -> Path:
    """Return the Parquet path of a dimension table."""
    return get_data_dir() / "gold" / "dimensions" / f"{name}.parquet"


def weather_code_frame() -> pd.DataFrame:
    """The weather code seed as a typed DataFrame (weather_code is the join key)."""
    rows = [c.model_dump() for c in load_weather_codes().weather_codes]
    df = pd.DataFrame(rows).rename(columns={"code": "weather_code"})
    df["weather_code"] = df["weather_code"].astype("int32")
    df["severity_rank"] = df["severity_rank"].astype("int32")
    return df.sort_values("weather_code").reset_index(drop=True)


@asset(
    group_name="dimensions",
    compute_kind="duckdb",
    description="WMO weather interpretation codes with descriptions and analytics categories.",
)
def dim_weather_code(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Rebuild dim_weather_code from config/weather_codes.yaml (DuckDB + Parquet)."""
    df = weather_code_frame()

    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE dim_weather_code AS SELECT * FROM df")

    write_parquet(df, dimension_path("dim_weather_code"))

    context.add_output_metadata(
        {
            "rows": len(df),
            "condition_groups": MetadataValue.json(
                df.groupby("condition_group")["weather_code"].apply(list).to_dict()
            ),
            "preview": MetadataValue.md(
                df[["weather_code", "description", "condition_group", "intensity"]].to_markdown(
                    index=False
                )
            ),
        }
    )


DIM_LOCATION_SCHEMA = pa.schema(
    [
        ("location_id", pa.string()),
        ("location_name", pa.string()),
        ("country", pa.string()),
        ("region", pa.string()),
        ("tags", pa.list_(pa.string())),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("timezone", pa.string()),
        ("utc_offset_minutes", pa.int32()),
        ("grid_latitude", pa.float64()),
        ("grid_longitude", pa.float64()),
        ("elevation_m", pa.float64()),
        ("is_active", pa.bool_()),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)


def latest_bronze_metadata(location_name: str) -> dict:
    """Grid coordinates and elevation from the location's most recent Bronze response.

    Open-Meteo snaps requests to a model grid cell and reports its coordinates
    and elevation in every response, so no extra API call is needed. Returns
    an empty dict if no Bronze file exists yet.
    """
    files = sorted((get_data_dir() / "bronze" / location_name).glob("*.parquet"))
    if not files:
        return {}
    rows = pq.read_table(str(files[-1]), columns=["raw_json"]).to_pylist()
    raw = json.loads(rows[-1]["raw_json"])
    return {
        "grid_latitude": raw.get("latitude"),
        "grid_longitude": raw.get("longitude"),
        "elevation_m": raw.get("elevation"),
    }


def _location_row(location: LocationConfig, now: datetime) -> dict:
    offset = datetime.now(ZoneInfo(location.timezone)).utcoffset()
    return {
        "location_id": location.id,
        "location_name": location.name,
        "country": location.country,
        "region": location.region,
        "tags": location.tags,
        "latitude": location.latitude,
        "longitude": location.longitude,
        "timezone": location.timezone,
        "utc_offset_minutes": int(offset.total_seconds() // 60),
        "grid_latitude": None,
        "grid_longitude": None,
        "elevation_m": None,
        **latest_bronze_metadata(location.name),
        "is_active": True,
        "updated_at": now,
    }


def build_dim_location() -> pa.Table:
    """Active rows from config; inactive rows for ids only present in the previous table."""
    now = datetime.now(timezone.utc)
    rows = [_location_row(loc, now) for loc in load_locations_config()]
    active_ids = {row["location_id"] for row in rows}

    previous_path = dimension_path("dim_location")
    if previous_path.exists():
        for row in pq.read_table(str(previous_path)).to_pylist():
            if row["location_id"] not in active_ids:
                rows.append({**row, "is_active": False})

    rows.sort(key=lambda r: (not r["is_active"], r["location_id"]))
    return pa.Table.from_pylist(rows, schema=DIM_LOCATION_SCHEMA)


@asset(
    group_name="dimensions",
    compute_kind="duckdb",
    deps=[bronze_forecast],
    description="Locations: config attributes, API grid coordinates and elevation, active flag.",
)
def dim_location(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Rebuild dim_location (DuckDB + Parquet), keeping removed locations as inactive."""
    table = build_dim_location()

    with duckdb_resource.get_connection() as conn:
        conn.register("dim_location_rows", table)
        conn.execute("CREATE OR REPLACE TABLE dim_location AS SELECT * FROM dim_location_rows")

    write_parquet(table, dimension_path("dim_location"))

    df = table.to_pandas()
    context.add_output_metadata(
        {
            "active_locations": int(df["is_active"].sum()),
            "inactive_locations": MetadataValue.json(
                df.loc[~df["is_active"], "location_id"].tolist()
            ),
            "preview": MetadataValue.md(
                df.drop(columns=["tags", "updated_at"]).to_markdown(index=False)
            ),
        }
    )


@asset_check(
    asset=dim_location,
    description="Unique location ids; every configured location active; grid data present.",
)
def dim_location_check(context) -> AssetCheckResult:
    """Check dim_location keys and coverage; missing grid data is only a warning."""
    path = dimension_path("dim_location")
    if not path.exists():
        return AssetCheckResult(passed=False, description="dim_location not built yet")
    df = pq.read_table(str(path)).to_pandas()
    issues = []

    duplicates = sorted(df.loc[df["location_id"].duplicated(), "location_id"])
    if duplicates:
        issues.append(f"Duplicate location ids: {duplicates}")
    active = set(df.loc[df["is_active"], "location_id"])
    missing = sorted({loc.id for loc in load_locations_config()} - active)
    if missing:
        issues.append(f"Configured locations missing or inactive: {missing}")
    no_grid = sorted(df.loc[df["is_active"] & df["elevation_m"].isna(), "location_id"])

    if issues:
        return AssetCheckResult(
            passed=False, metadata={"issues": issues}, description="; ".join(issues)
        )
    return AssetCheckResult(
        passed=not no_grid,
        severity=AssetCheckSeverity.WARN,
        metadata={"locations_without_grid_data": no_grid},
        description=(
            f"{len(active)} active locations"
            if not no_grid
            else f"No Bronze data yet for {no_grid}: grid coordinates/elevation empty"
        ),
    )


def _weather_code_coverage_check(
    silver_asset: AssetsDefinition, section: Section
) -> AssetChecksDefinition:
    """Build a check that every weather_code in a Silver partition is in the seed."""

    @asset_check(
        asset=silver_asset,
        name=f"{section}_weather_code_check",
        description=f"Every weather_code in Silver {section} data exists in dim_weather_code.",
    )
    def _check(context) -> AssetCheckResult:
        path = silver_path(section, context.partition_key)
        if not path.exists():
            return AssetCheckResult(passed=True, description="No Silver file; nothing to check")
        df = pd.read_parquet(str(path))
        if "weather_code" not in df.columns:
            return AssetCheckResult(passed=True, description="weather_code isn't configured")

        seen = sorted(int(c) for c in df["weather_code"].dropna().unique())
        unknown = sorted(set(seen) - load_weather_codes().codes)
        return AssetCheckResult(
            passed=not unknown,
            metadata={"codes_seen": seen, "unknown_codes": unknown},
            description=(
                f"All {len(seen)} weather codes known"
                if not unknown
                else f"Unknown weather codes {unknown}: add them to config/weather_codes.yaml"
            ),
        )

    return _check


hourly_weather_code_check = _weather_code_coverage_check(silver_hourly_weather, "hourly")
daily_weather_code_check = _weather_code_coverage_check(silver_daily_weather, "daily")
current_weather_code_check = _weather_code_coverage_check(silver_current_weather, "current")
