"""Tests for TransformationTask."""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from api.models import FeedConfig
from pipeline.feed_config import load_feed_configs
from storage.local import LocalStorageBackend
from tasks.transformation.task import TransformationTask


@pytest.fixture()
def storage(tmp_path: Path) -> LocalStorageBackend:
    return LocalStorageBackend(tmp_path)


@pytest.fixture()
def feed_configs() -> dict[str, FeedConfig]:
    return load_feed_configs("data/feeds")


@pytest.fixture()
def bcb_configs(feed_configs: dict[str, FeedConfig]) -> dict[str, FeedConfig]:
    """Only BCB feeds for focused tests."""
    return {k: v for k, v in feed_configs.items() if k.startswith("bcb_")}


def _write_bronze_parquet(
    storage_path: Path,
    series: str,
    records: list[dict[str, str]],
    filename: str = "20260323_060000_000000.parquet",
) -> None:
    """Helper: write a bronze Parquet file with metadata columns."""
    now_str = datetime.now(timezone.utc).isoformat()
    enriched: list[dict[str, str | None]] = []
    for r in records:
        row: dict[str, str | None] = dict(r)
        row["_ingested_at"] = now_str
        row["_source"] = series
        row["_run_id"] = "test-run"
        row["_schema_hash"] = "testhash"
        row["_rescued_data"] = None
        enriched.append(row)

    columns: dict[str, list[str | None]] = {}
    for key in enriched[0]:
        columns[key] = [row.get(key) for row in enriched]

    arrays = {k: pa.array(v, type=pa.string()) for k, v in columns.items()}
    table = pa.table(arrays)
    out_dir = storage_path / "bronze" / series
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_dir / filename)


BCB_SAMPLE_DATA = [
    {"data": "01/01/2025", "valor": "13.25"},
    {"data": "01/02/2025", "valor": "13.25"},
    {"data": "01/03/2025", "valor": "13.75"},
    {"data": "01/04/2025", "valor": "14.25"},
    {"data": "01/05/2025", "valor": "14.75"},
    {"data": "01/06/2025", "valor": "14.75"},
    {"data": "01/07/2025", "valor": "14.75"},
    {"data": "01/08/2025", "valor": "14.75"},
    {"data": "01/09/2025", "valor": "14.75"},
    {"data": "01/10/2025", "valor": "14.75"},
    {"data": "01/11/2025", "valor": "14.75"},
    {"data": "01/12/2025", "valor": "14.75"},
    {"data": "01/01/2026", "valor": "14.75"},
]


@pytest.fixture()
def bronze_data(tmp_path: Path) -> Path:
    """Populate bronze layer with sample BCB data."""
    _write_bronze_parquet(tmp_path, "bcb_432", BCB_SAMPLE_DATA)
    _write_bronze_parquet(tmp_path, "bcb_433", [
        {"data": "01/01/2025", "valor": "0.16"},
        {"data": "01/02/2025", "valor": "1.31"},
        {"data": "01/03/2025", "valor": "0.56"},
    ])
    _write_bronze_parquet(tmp_path, "bcb_1", [
        {"data": "01/01/2025", "valor": "6.1800"},
        {"data": "01/02/2025", "valor": "5.7600"},
        {"data": "01/03/2025", "valor": "5.7300"},
    ])
    return tmp_path


@pytest.mark.asyncio()
async def test_transformation_produces_silver_and_gold(
    bronze_data: Path,
    bcb_configs: dict[str, FeedConfig],
) -> None:
    """TransformationTask should produce silver and gold from bronze."""
    storage = LocalStorageBackend(bronze_data)
    task = TransformationTask(storage=storage, feed_configs=bcb_configs)
    result = await task.run()

    assert result.success is True
    assert result.task_name == "transformation"
    assert result.rows_processed > 0

    silver_keys = await storage.list_keys("silver")
    gold_keys = await storage.list_keys("gold")
    # Filter out watermark files
    silver_parquet = [k for k in silver_keys if k.endswith(".parquet")]
    assert len(silver_parquet) > 0
    assert len(gold_keys) > 0


@pytest.mark.asyncio()
async def test_transformation_derived_metrics(
    bronze_data: Path,
    bcb_configs: dict[str, FeedConfig],
) -> None:
    """Gold layer should contain derived metrics and new columns."""
    storage = LocalStorageBackend(bronze_data)
    task = TransformationTask(storage=storage, feed_configs=bcb_configs)
    await task.run()

    gold_keys = await storage.list_keys("gold")
    assert len(gold_keys) > 0

    data = await storage.read(gold_keys[0])
    table = pq.read_table(io.BytesIO(data))
    columns = table.column_names

    assert "value" in columns
    assert "date" in columns
    assert "series" in columns
    assert "mom_delta" in columns
    assert "unit" in columns
    assert "last_updated_at" in columns
    assert "calculation_version" in columns


@pytest.mark.asyncio()
async def test_transformation_empty_bronze(tmp_path: Path) -> None:
    """TransformationTask should handle empty bronze layer gracefully."""
    storage = LocalStorageBackend(tmp_path)
    task = TransformationTask(storage=storage, feed_configs={})
    result = await task.run()

    assert result.success is False
    assert len(result.errors) > 0


@pytest.mark.asyncio()
async def test_transformation_health_check(tmp_path: Path) -> None:
    storage = LocalStorageBackend(tmp_path)
    task = TransformationTask(storage=storage)
    assert await task.health_check() is True


@pytest.mark.asyncio()
async def test_transformation_canonical_silver(
    bronze_data: Path,
    bcb_configs: dict[str, FeedConfig],
) -> None:
    """Silver should be a canonical file per series, not timestamped."""
    storage = LocalStorageBackend(bronze_data)
    task = TransformationTask(storage=storage, feed_configs=bcb_configs)
    await task.run()

    # Check that silver is written as silver/{series}.parquet
    assert await storage.exists("silver/bcb_432.parquet")
    assert await storage.exists("silver/bcb_433.parquet")
    assert await storage.exists("silver/bcb_1.parquet")


@pytest.mark.asyncio()
async def test_transformation_watermark(
    tmp_path: Path,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Watermark should track last processed bronze file."""
    bcb_config = {"bcb_432": feed_configs["bcb_432"]}

    # Write first bronze batch
    _write_bronze_parquet(tmp_path, "bcb_432", [
        {"data": "01/01/2025", "valor": "13.25"},
        {"data": "01/02/2025", "valor": "13.25"},
    ], filename="20260323_060000_000000.parquet")

    storage = LocalStorageBackend(tmp_path)
    task = TransformationTask(storage=storage, feed_configs=bcb_config)
    await task.run()

    # Watermark should exist
    assert await storage.exists("silver/bcb_432/_watermark.json")
    wm_data = json.loads(await storage.read("silver/bcb_432/_watermark.json"))
    assert "last_processed_key" in wm_data

    # Write second bronze batch
    _write_bronze_parquet(tmp_path, "bcb_432", [
        {"data": "01/03/2025", "valor": "13.75"},
    ], filename="20260324_060000_000000.parquet")

    # Run again — should process only new file
    await task.run()

    # Silver should contain all 3 rows (merged)
    silver_data = await storage.read("silver/bcb_432.parquet")
    table = pq.read_table(io.BytesIO(silver_data))
    assert table.num_rows == 3


@pytest.mark.asyncio()
async def test_transformation_silver_has_unit(
    bronze_data: Path,
    bcb_configs: dict[str, FeedConfig],
) -> None:
    """Silver should include unit column from feed config metadata."""
    storage = LocalStorageBackend(bronze_data)
    task = TransformationTask(storage=storage, feed_configs=bcb_configs)
    await task.run()

    silver_data = await storage.read("silver/bcb_432.parquet")
    table = pq.read_table(io.BytesIO(silver_data))

    assert "unit" in table.column_names
    units = table.column("unit").to_pylist()
    assert all(u == "% a.a." for u in units)


@pytest.mark.asyncio()
async def test_transformation_silver_metadata_columns(
    bronze_data: Path,
    bcb_configs: dict[str, FeedConfig],
) -> None:
    """Silver should include _cleaned_at and _transformation_version."""
    storage = LocalStorageBackend(bronze_data)
    task = TransformationTask(storage=storage, feed_configs=bcb_configs)
    await task.run()

    silver_data = await storage.read("silver/bcb_432.parquet")
    table = pq.read_table(io.BytesIO(silver_data))

    assert "_cleaned_at" in table.column_names
    assert "_transformation_version" in table.column_names


@pytest.mark.asyncio()
async def test_transformation_latest_only_dedup(
    tmp_path: Path,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """latest_only should keep only the most recent row per date."""
    bcb_config = {"bcb_432": feed_configs["bcb_432"]}

    # Write two bronze files with overlapping dates but different values
    _write_bronze_parquet(tmp_path, "bcb_432", [
        {"data": "01/01/2025", "valor": "13.00"},
        {"data": "01/02/2025", "valor": "13.25"},
    ], filename="20260323_060000_000000.parquet")

    _write_bronze_parquet(tmp_path, "bcb_432", [
        {"data": "01/01/2025", "valor": "13.50"},  # Updated value
        {"data": "01/03/2025", "valor": "14.00"},
    ], filename="20260324_060000_000000.parquet")

    storage = LocalStorageBackend(tmp_path)
    task = TransformationTask(storage=storage, feed_configs=bcb_config)
    await task.run()

    silver_data = await storage.read("silver/bcb_432.parquet")
    table = pq.read_table(io.BytesIO(silver_data))

    # Should have 3 unique dates, not 4
    assert table.num_rows == 3
