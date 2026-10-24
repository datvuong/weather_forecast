"""Tests for atomic Parquet writes (concurrent runs read each other's files, #33)."""

import threading

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from weather_forecast.utils.helpers import atomic_write, write_parquet


def test_writes_dataframe_and_table(tmp_path):
    write_parquet(pd.DataFrame({"a": [1, 2]}), tmp_path / "sub" / "df.parquet")
    write_parquet(pa.table({"b": [3]}), tmp_path / "sub" / "table.parquet")

    assert pd.read_parquet(tmp_path / "sub" / "df.parquet")["a"].tolist() == [1, 2]
    assert pq.read_table(tmp_path / "sub" / "table.parquet").column("b").to_pylist() == [3]
    assert sorted(p.name for p in (tmp_path / "sub").iterdir()) == ["df.parquet", "table.parquet"]


def test_failed_write_keeps_old_file_and_leaves_no_temp(tmp_path):
    path = tmp_path / "data.parquet"
    write_parquet(pd.DataFrame({"a": [1]}), path)

    def broken(tmp):
        with open(tmp, "wb") as f:
            f.write(b"partial")
        raise RuntimeError("disk full")

    with pytest.raises(RuntimeError):
        atomic_write(path, broken)

    assert pd.read_parquet(path)["a"].tolist() == [1]
    assert [p.name for p in tmp_path.iterdir()] == ["data.parquet"]


def test_temp_file_is_invisible_to_parquet_globs_while_writing(tmp_path):
    path = tmp_path / "data.parquet"
    write_parquet(pd.DataFrame({"a": [1]}), path)
    seen = {}

    def slow_write(tmp):
        pd.DataFrame({"a": [2]}).to_parquet(tmp)
        # Mid-write, a reader still sees exactly one complete file with the old content
        seen["glob"] = sorted(p.name for p in tmp_path.glob("*.parquet"))
        seen["value"] = pd.read_parquet(path)["a"].tolist()

    atomic_write(path, slow_write)

    assert seen == {"glob": ["data.parquet"], "value": [1]}
    assert pd.read_parquet(path)["a"].tolist() == [2]


def test_concurrent_readers_never_see_a_partial_file(tmp_path):
    # Reproduces the warehouse_views failure: one run rewrites a Silver file while another
    # reads every file in the directory
    path = tmp_path / "2026-10-03.parquet"
    frame = pd.DataFrame({"x": range(20_000)})
    write_parquet(frame, path)
    errors = []
    done = threading.Event()

    def writer():
        try:
            for _ in range(60):
                write_parquet(frame, path)
        except Exception as e:  # pragma: no cover - only on failure
            errors.append(e)
        finally:
            done.set()  # never leave the readers spinning

    def reader():
        while not done.is_set():
            try:
                for p in tmp_path.glob("*.parquet"):
                    assert len(pd.read_parquet(p)) == len(frame)
            except Exception as e:  # pragma: no cover - only on failure
                errors.append(e)

    readers = [threading.Thread(target=reader) for _ in range(3)]
    threads = [threading.Thread(target=writer), *readers]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not any(t.is_alive() for t in threads), "stress test didn't finish"
    assert errors == []
