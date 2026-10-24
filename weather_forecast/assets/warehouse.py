"""Warehouse views — SQL access to Silver and the dimensions in DuckDB.

The DuckDB warehouse stores Gold and dimension tables. These views add the
Silver layer (read in place from Parquet) and analytics-ready joins:

- silver_hourly_weather, silver_daily_weather, silver_current_weather:
  read_parquet over the Silver files (union_by_name, so files written before
  a variable was added still line up)
- v_hourly_weather, v_daily_weather: Silver joined to dim_location
  (country, region, elevation, ...) and dim_weather_code (description,
  condition_group, ...)
- v_current_conditions: gold_current_conditions joined to the dimensions

The SQL is generated from the columns that exist, so views keep working when
config variables change or older files lack location_id (those rows join
dim_location by name). Views point at the current data directory; rerun the
asset after moving it.
"""

import logging

import duckdb
from dagster import AssetCheckResult, AssetExecutionContext, MetadataValue, asset, asset_check

from weather_forecast.assets.current import gold_current_conditions, silver_current_weather
from weather_forecast.assets.dimensions import dim_location, dim_weather_code
from weather_forecast.assets.silver import silver_daily_weather, silver_hourly_weather
from weather_forecast.resources.duckdb_resource import DuckDBResource
from weather_forecast.utils.helpers import get_data_dir

logger = logging.getLogger(__name__)

SILVER_VIEWS = {
    "silver_hourly_weather": "hourly",
    "silver_daily_weather": "daily",
    "silver_current_weather": "current",
}

# Analytic view -> its base view/table
ANALYTIC_VIEWS = {
    "v_hourly_weather": "silver_hourly_weather",
    "v_daily_weather": "silver_daily_weather",
    "v_current_conditions": "gold_current_conditions",
}

LOCATION_COLUMNS = ["country", "region", "tags", "elevation_m", "timezone"]
WEATHER_CODE_COLUMNS = {
    "description": "weather_description",
    "condition_group": "condition_group",
    "precipitation_type": "precipitation_type",
    "intensity": "weather_intensity",
    "is_precipitation": "is_precipitation",
    "is_freezing": "is_freezing",
    "is_thunderstorm": "is_thunderstorm",
    "severity_rank": "weather_severity_rank",
}


def _relations(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    return {row[0] for row in rows}


def _columns(conn: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [row[0] for row in conn.execute(f"DESCRIBE {relation}").fetchall()]


def analytic_view_sql(conn: duckdb.DuckDBPyConnection, base: str) -> str:
    """SELECT joining a base relation to whichever dimensions apply."""
    columns = _columns(conn, base)
    relations = _relations(conn)
    select = ["f.*"]
    joins = []

    if "dim_location" in relations:
        if "location_id" in columns:
            # Rows from before location_id existed are matched by name
            select = [
                "f.* EXCLUDE (location_id)",
                "COALESCE(f.location_id, l.location_id) AS location_id",
            ]
            on = (
                "l.location_id = f.location_id "
                "OR (f.location_id IS NULL AND l.location_name = f.location_name)"
            )
        else:
            select = ["f.*", "l.location_id"]
            on = "l.location_name = f.location_name"
        select += [f"l.{col}" for col in LOCATION_COLUMNS if col not in columns]
        joins.append(f"LEFT JOIN dim_location l ON {on}")

    if "dim_weather_code" in relations and "weather_code" in columns:
        select += [f"w.{col} AS {alias}" for col, alias in WEATHER_CODE_COLUMNS.items()]
        joins.append("LEFT JOIN dim_weather_code w ON w.weather_code = f.weather_code")

    return f"SELECT {', '.join(select)} FROM {base} f {' '.join(joins)}"


def build_views(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """(Re)create all views; returns {view: "created" | "skipped: <reason>"}."""
    status = {}
    silver_dir = get_data_dir() / "silver"
    for view, section in SILVER_VIEWS.items():
        if any((silver_dir / section).glob("*.parquet")):
            pattern = str(silver_dir / section / "*.parquet").replace("'", "''")
            conn.execute(
                f"CREATE OR REPLACE VIEW {view} AS "
                f"SELECT * FROM read_parquet('{pattern}', union_by_name = true)"
            )
            status[view] = "created"
        else:
            conn.execute(f"DROP VIEW IF EXISTS {view}")
            status[view] = f"skipped: no Silver {section} files"

    relations = _relations(conn)
    for view, base in ANALYTIC_VIEWS.items():
        if base in relations:
            conn.execute(f"CREATE OR REPLACE VIEW {view} AS {analytic_view_sql(conn, base)}")
            status[view] = "created"
        else:
            conn.execute(f"DROP VIEW IF EXISTS {view}")
            status[view] = f"skipped: {base} doesn't exist"
    return status


@asset(
    group_name="dimensions",
    compute_kind="duckdb",
    deps=[
        dim_location,
        dim_weather_code,
        silver_hourly_weather,
        silver_daily_weather,
        silver_current_weather,
        gold_current_conditions,
    ],
    description="DuckDB views over Silver plus analytic views joined to the dimensions.",
)
def warehouse_views(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """(Re)create the warehouse views so their columns follow the current data and config."""
    with duckdb_resource.get_connection() as conn:
        status = build_views(conn)
        columns = {view: _columns(conn, view) for view, s in status.items() if s == "created"}

    for view, s in status.items():
        if s != "created":
            context.log.info(f"{view}: {s}")
    context.add_output_metadata(
        {
            "views": MetadataValue.json(status),
            "columns": MetadataValue.json(columns),
        }
    )


@asset_check(
    asset=warehouse_views,
    description="Every view is queryable and analytic views don't duplicate rows in their joins.",
)
def warehouse_views_check(context, duckdb_resource: DuckDBResource) -> AssetCheckResult:
    """Row counts of v_* views must equal their base (dimension keys are unique)."""
    issues = []
    counts = {}
    with duckdb_resource.get_connection() as conn:
        relations = _relations(conn)
        for view, base in ANALYTIC_VIEWS.items():
            if view not in relations:
                continue
            try:
                n_view = conn.execute(f"SELECT count(*) FROM {view}").fetchone()[0]
                n_base = conn.execute(f"SELECT count(*) FROM {base}").fetchone()[0]
            except duckdb.Error as e:
                issues.append(f"{view} isn't queryable: {e}")
                continue
            counts[view] = {"view": n_view, "base": n_base}
            if n_view != n_base:
                issues.append(f"{view} has {n_view} rows but {base} has {n_base} (join fan-out)")
    return AssetCheckResult(
        passed=not issues,
        metadata={"row_counts": MetadataValue.json(counts), "issues": issues},
        description="All views consistent" if not issues else "; ".join(issues),
    )
