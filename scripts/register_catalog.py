"""Register existing gold and silver parquet files as Iceberg tables in R2 Data Catalog.

Usage:
    uv run python scripts/register_catalog.py

Requires env vars:
    R2_CATALOG_TOKEN, R2_CATALOG_WAREHOUSE, R2_CATALOG_URI
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET_NAME

Idempotent: uses CREATE OR REPLACE TABLE, safe to re-run.
"""

from __future__ import annotations

import os
import sys

import duckdb
import structlog

logger = structlog.get_logger()

# Series that have gold parquet files in R2
GOLD_SERIES = [
    "bcb_1",
    "bcb_432",
    "bcb_433",
    "ibge_gdp",
    "ibge_pnad",
    "tesouro_ipca",
    "tesouro_prefixado_curto",
    "tesouro_prefixado_longo",
]

SILVER_SERIES = GOLD_SERIES + ["tesouro"]


def main() -> None:
    token = os.environ.get("R2_CATALOG_TOKEN", "")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE", "")
    catalog_uri = os.environ.get("R2_CATALOG_URI", "")
    bucket = os.environ.get("R2_BUCKET_NAME", "br-economic-pulse-data")
    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")

    missing = []
    if not token:
        missing.append("R2_CATALOG_TOKEN")
    if not warehouse:
        missing.append("R2_CATALOG_WAREHOUSE")
    if not catalog_uri:
        missing.append("R2_CATALOG_URI")
    if not key_id:
        missing.append("R2_ACCESS_KEY_ID")
    if not secret:
        missing.append("R2_SECRET_ACCESS_KEY")

    if missing:
        logger.error("missing_env_vars", vars=missing)
        sys.exit(1)

    conn = duckdb.connect()

    logger.info("installing_extensions")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    # Secret for Iceberg catalog API
    conn.execute(f"""
        CREATE SECRET r2_catalog_secret (
            TYPE ICEBERG,
            TOKEN '{token}'
        );
    """)

    # Secret for reading raw parquet from R2
    conn.execute(f"""
        CREATE SECRET r2_storage (
            TYPE R2,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            ACCOUNT_ID '{account_id}'
        );
    """)

    # Attach catalog
    logger.info("attaching_catalog", warehouse=warehouse)
    conn.execute(f"""
        ATTACH '{warehouse}' AS catalog (
            TYPE ICEBERG,
            ENDPOINT '{catalog_uri}'
        );
    """)

    # Create schemas
    for schema in ["gold", "silver"]:
        logger.info("creating_schema", schema=schema)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS catalog.{schema};")

    # Register gold tables
    for series in GOLD_SERIES:
        parquet_url = f"r2://{bucket}/gold/{series}.parquet"
        logger.info("registering_gold", series=series, url=parquet_url)
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE catalog.gold.{series} AS
                SELECT * FROM read_parquet('{parquet_url}');
            """)
            row_count = conn.execute(
                f"SELECT count(*) FROM catalog.gold.{series}"
            ).fetchone()[0]
            logger.info("registered_gold", series=series, rows=row_count)
        except Exception as e:
            logger.error("failed_gold", series=series, error=str(e))

    # Register silver tables
    for series in SILVER_SERIES:
        parquet_url = f"r2://{bucket}/silver/{series}.parquet"
        logger.info("registering_silver", series=series, url=parquet_url)
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE catalog.silver.{series} AS
                SELECT * FROM read_parquet('{parquet_url}');
            """)
            row_count = conn.execute(
                f"SELECT count(*) FROM catalog.silver.{series}"
            ).fetchone()[0]
            logger.info("registered_silver", series=series, rows=row_count)
        except Exception as e:
            logger.error("failed_silver", series=series, error=str(e))

    # Summary
    tables = conn.execute("SHOW ALL TABLES;").df()
    logger.info("registration_complete", total_tables=len(tables))
    print("\n" + tables.to_string(index=False))

    conn.close()


if __name__ == "__main__":
    main()
