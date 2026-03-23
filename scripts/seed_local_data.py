#!/usr/bin/env python3
"""Seed local gold data from test fixtures for local API development.

Usage:
    uv run python scripts/seed_local_data.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv(".env.local")

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "local"


def seed_bcb_series(series_name: str, fixture_file: str) -> int:
    """Convert BCB JSON fixture to bronze + silver + gold Parquet."""
    data = json.loads((FIXTURES_DIR / fixture_file).read_text())

    # Write bronze
    bronze_dir = DATA_DIR / "bronze" / series_name
    bronze_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "data": [r["data"] for r in data],
        "valor": [r["valor"] for r in data],
    })
    pq.write_table(table, bronze_dir / "latest.parquet")

    # Transform to silver + gold via DuckDB
    conn = duckdb.connect()
    conn.register("bronze_raw", table)

    silver = conn.execute("""
        SELECT DISTINCT
            strptime("data", '%d/%m/%Y') AS date,
            CAST("valor" AS DOUBLE) AS value,
            ? AS series
        FROM bronze_raw
        WHERE "valor" IS NOT NULL AND "data" IS NOT NULL
        ORDER BY date
    """, [series_name]).to_arrow_table()

    silver_dir = DATA_DIR / "silver" / series_name
    silver_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(silver, silver_dir / "latest.parquet")

    conn.register("silver_data", silver)
    gold = conn.execute("""
        SELECT
            date, value, series,
            value - LAG(value) OVER (ORDER BY date) AS mom_delta,
            CASE
                WHEN LAG(value, 12) OVER (ORDER BY date) IS NOT NULL
                    AND LAG(value, 12) OVER (ORDER BY date) != 0
                THEN ((value - LAG(value, 12) OVER (ORDER BY date))
                      / ABS(LAG(value, 12) OVER (ORDER BY date))) * 100
                ELSE NULL
            END AS yoy_delta,
            AVG(value) OVER (
                ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS rolling_12m_avg,
            CASE
                WHEN COUNT(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) >= 2
                THEN (value - AVG(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                )) / NULLIF(STDDEV(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 0)
                ELSE NULL
            END AS z_score
        FROM silver_data ORDER BY date
    """).to_arrow_table()

    gold_dir = DATA_DIR / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(gold, gold_dir / f"{series_name}.parquet")

    conn.close()
    return gold.num_rows


def write_metadata(files_count: int) -> None:
    """Write metadata.json for sync status."""
    gold_dir = DATA_DIR / "gold"
    metadata = {
        "last_sync_at": datetime.now(timezone.utc).isoformat(),
        "run_id": f"seed-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "files_synced": files_count,
        "sync_duration_ms": 0.0,
        "source": "local_seed",
    }
    (gold_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))


def main() -> int:
    print("Seeding local data from test fixtures...")

    total_rows = 0
    series_map = {
        "bcb_432": "bcb_selic.json",
        "bcb_433": "bcb_ipca.json",
        "bcb_1": "bcb_usd_brl.json",
    }

    files_count = 0
    for series_name, fixture_file in series_map.items():
        fixture_path = FIXTURES_DIR / fixture_file
        if not fixture_path.exists():
            print(f"  Skipping {series_name}: {fixture_file} not found")
            continue
        rows = seed_bcb_series(series_name, fixture_file)
        total_rows += rows
        files_count += 1
        print(f"  {series_name}: {rows} rows -> gold/{series_name}.parquet")

    write_metadata(files_count)
    print(f"\nDone! {total_rows} total rows across {files_count} series.")
    print(f"Gold data: {DATA_DIR / 'gold'}")
    print(f"Metadata:  {DATA_DIR / 'gold' / 'metadata.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
