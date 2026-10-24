"""DuckDB resource — Dagster ConfigurableResource.

Provides a managed DuckDB connection for Gold layer analytics queries.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
import pandas as pd
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)

# DuckDB allows a single writer process per database file. Gold assets (and
# parallel backfill runs) may contend for it, so connection attempts are retried.
LOCK_RETRY_ATTEMPTS = 20
LOCK_RETRY_WAIT = 0.5  # seconds


class DuckDBResource(ConfigurableResource):
    """Dagster resource for managing a local DuckDB database.

    Used by Gold layer assets to write aggregated tables and support
    ad-hoc analytical queries.

    Attributes:
        database_path: Path to the DuckDB database file.
    """

    database_path: str = "warehouse/weather.duckdb"

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Get a DuckDB connection as a context manager.

        Creates parent directories if they don't exist. Retries while another
        process holds the database file lock.

        Yields:
            An open DuckDB connection.
        """
        db_path = Path(self.database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(LOCK_RETRY_ATTEMPTS):
            try:
                conn = duckdb.connect(str(db_path))
                break
            except duckdb.IOException as e:
                if "lock" not in str(e).lower() or attempt == LOCK_RETRY_ATTEMPTS - 1:
                    raise
                logger.info("DuckDB file locked, retrying in %.1fs...", LOCK_RETRY_WAIT)
                time.sleep(LOCK_RETRY_WAIT)
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql: str) -> None:
        """Execute a SQL statement (no return value).

        Args:
            sql: SQL statement to execute.
        """
        with self.get_connection() as conn:
            conn.execute(sql)
            logger.debug("Executed SQL: %s", sql[:100])

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame.

        Args:
            sql: SQL query to execute.

        Returns:
            Query results as a pandas DataFrame.
        """
        with self.get_connection() as conn:
            result = conn.execute(sql).fetchdf()
            logger.debug("Query returned %d rows", len(result))
            return result


def ensure_table(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    columns: dict[str, str],
    primary_key: list[str],
) -> bool:
    """Create a table, or recreate it if its columns differ from `columns`.

    Meant for derived tables (Gold) that can be rebuilt from upstream data:
    a schema change drops the old rows, which the next (back)fill rewrites.

    Args:
        conn: Open DuckDB connection.
        name: Table name.
        columns: Column name -> DuckDB type, in table order.
        primary_key: Primary key columns.

    Returns:
        True if an existing table was dropped and recreated.
    """
    existing = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [name],
        ).fetchall()
    ]
    recreated = bool(existing) and existing != list(columns)
    if recreated:
        logger.warning(
            "Schema of %s changed (%s -> %s); recreating it. Re-run the backfill to refill it.",
            name,
            existing,
            list(columns),
        )
        conn.execute(f"DROP TABLE {name}")
    column_sql = ", ".join(f"{col} {type_}" for col, type_ in columns.items())
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {name} ({column_sql}, PRIMARY KEY ({', '.join(primary_key)}))"
    )
    return recreated
