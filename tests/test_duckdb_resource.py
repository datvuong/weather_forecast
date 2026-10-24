"""Tests for the DuckDB resource, including writer-lock retries."""

import duckdb
import pytest

from weather_forecast.resources import duckdb_resource as module
from weather_forecast.resources.duckdb_resource import ensure_table


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(module.time, "sleep", lambda _: None)


def test_execute_and_query(duckdb_resource):
    duckdb_resource.execute("CREATE TABLE t AS SELECT 1 AS x")

    assert duckdb_resource.query("SELECT x FROM t")["x"].tolist() == [1]


def test_retries_while_file_is_locked(duckdb_resource, monkeypatch):
    real_connect = duckdb.connect
    attempts = []

    def flaky_connect(path):
        attempts.append(path)
        if len(attempts) < 3:
            raise duckdb.IOException('Could not set lock on file "x": Conflicting lock is held')
        return real_connect(path)

    monkeypatch.setattr(module.duckdb, "connect", flaky_connect)

    assert duckdb_resource.query("SELECT 42 AS answer")["answer"].iloc[0] == 42
    assert len(attempts) == 3


def test_gives_up_after_max_attempts(duckdb_resource, monkeypatch):
    def locked(path):
        raise duckdb.IOException("Could not set lock on file")

    monkeypatch.setattr(module.duckdb, "connect", locked)

    with pytest.raises(duckdb.IOException):
        duckdb_resource.query("SELECT 1")


def test_other_io_errors_are_not_retried(duckdb_resource, monkeypatch):
    attempts = []

    def broken(path):
        attempts.append(path)
        raise duckdb.IOException("Permission denied")

    monkeypatch.setattr(module.duckdb, "connect", broken)

    with pytest.raises(duckdb.IOException, match="Permission denied"):
        duckdb_resource.query("SELECT 1")
    assert len(attempts) == 1


class TestEnsureTable:
    COLUMNS = {"id": "VARCHAR", "date": "VARCHAR", "value": "DOUBLE"}

    def test_creates_missing_table(self, duckdb_resource):
        with duckdb_resource.get_connection() as conn:
            assert ensure_table(conn, "t", self.COLUMNS, ["id", "date"]) is False
            assert [r[0] for r in conn.execute("DESCRIBE t").fetchall()] == list(self.COLUMNS)

    def test_keeps_matching_table_and_rows(self, duckdb_resource):
        with duckdb_resource.get_connection() as conn:
            ensure_table(conn, "t", self.COLUMNS, ["id", "date"])
            conn.execute("INSERT INTO t VALUES ('a', '2026-10-01', 1.0)")
            assert ensure_table(conn, "t", self.COLUMNS, ["id", "date"]) is False
            assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 1

    def test_recreates_table_with_different_columns(self, duckdb_resource):
        with duckdb_resource.get_connection() as conn:
            conn.execute("CREATE TABLE t (date VARCHAR, value DOUBLE)")
            conn.execute("INSERT INTO t VALUES ('2026-10-01', 1.0)")
            assert ensure_table(conn, "t", self.COLUMNS, ["id", "date"]) is True
            assert [r[0] for r in conn.execute("DESCRIBE t").fetchall()] == list(self.COLUMNS)
            assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0
