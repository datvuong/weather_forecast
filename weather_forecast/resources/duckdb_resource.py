"""DuckDB resource — Dagster ConfigurableResource.

Provides a managed DuckDB connection for Gold layer analytics queries.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
import pandas as pd
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


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

        Creates parent directories if they don't exist.

        Yields:
            An open DuckDB connection.
        """
        db_path = Path(self.database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(str(db_path))
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
