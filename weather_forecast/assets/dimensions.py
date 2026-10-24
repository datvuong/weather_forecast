"""Dimension assets — reference tables for joining and labelling the facts.

Dimensions are unpartitioned and rebuilt in full on each run. They're seeded
from YAML in config/, so labels and attributes change without code changes.

- dim_weather_code: WMO weather interpretation codes (config/weather_codes.yaml),
  joined to the weather_code columns of the Silver tables.

File layout:
    data/gold/dimensions/dim_weather_code.parquet
    warehouse/weather.duckdb (table: dim_weather_code)
"""

import logging
from pathlib import Path

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetExecutionContext,
    AssetsDefinition,
    MetadataValue,
    asset,
    asset_check,
)

from weather_forecast.assets.current import silver_current_weather
from weather_forecast.assets.silver import silver_daily_weather, silver_hourly_weather, silver_path
from weather_forecast.models.weather import Section
from weather_forecast.resources.duckdb_resource import DuckDBResource
from weather_forecast.utils.helpers import get_data_dir, load_weather_codes

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

    output_path = dimension_path("dim_weather_code")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(output_path), compression="snappy", index=False)

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
