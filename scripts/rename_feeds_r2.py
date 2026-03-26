"""Rename feed_id prefixes in R2 after feed_id rename.

Copies bronze files from old prefixes to new ones, then deletes old
bronze, silver, gold, and watermark files for the renamed feeds.

Usage:
    uv run python scripts/rename_feeds_r2.py

Requires env vars: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID,
    R2_BUCKET_NAME, R2_ENDPOINT

Also drops old Iceberg catalog tables if catalog env vars are set.
"""

from __future__ import annotations

import asyncio
import os
import sys

import structlog

from storage.r2 import R2StorageBackend

logger = structlog.get_logger()

RENAMES = {
    "bcb_432": "bcb_selic",
    "bcb_433": "bcb_ipca",
    "bcb_1": "bcb_usd_brl",
}


async def main() -> None:
    if not os.environ.get("R2_ACCESS_KEY_ID"):
        logger.error("missing R2 env vars")
        sys.exit(1)

    r2 = R2StorageBackend()

    for old_id, new_id in RENAMES.items():
        # --- Bronze: copy files from old prefix to new prefix ---
        old_prefix = f"bronze/{old_id}/"
        new_prefix = f"bronze/{new_id}/"

        old_keys = await r2.list_keys(old_prefix)
        logger.info("bronze_copy", old=old_id, new=new_id, files=len(old_keys))

        for key in old_keys:
            new_key = key.replace(old_prefix, new_prefix, 1)
            data = await r2.read(key)
            await r2.write(new_key, data)
            logger.info("copied", src=key, dst=new_key)

        # --- Delete old bronze files ---
        for key in old_keys:
            await r2.delete(key)
            logger.info("deleted", key=key)

        # --- Delete old silver ---
        silver_key = f"silver/{old_id}.parquet"
        if await r2.exists(silver_key):
            await r2.delete(silver_key)
            logger.info("deleted", key=silver_key)

        # --- Delete old watermark ---
        watermark_key = f"silver/{old_id}/_watermark.json"
        if await r2.exists(watermark_key):
            await r2.delete(watermark_key)
            logger.info("deleted", key=watermark_key)

        # --- Delete old gold ---
        gold_key = f"gold/{old_id}.parquet"
        if await r2.exists(gold_key):
            await r2.delete(gold_key)
            logger.info("deleted", key=gold_key)

    # --- Drop old Iceberg catalog tables ---
    token = os.environ.get("R2_CATALOG_TOKEN", "")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE", "")
    catalog_uri = os.environ.get("R2_CATALOG_URI", "")

    if all([token, warehouse, catalog_uri]):
        import duckdb

        conn = duckdb.connect()
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute(f"CREATE SECRET (TYPE ICEBERG, TOKEN '{token}');")
        conn.execute(f"""
            ATTACH '{warehouse}' AS catalog (
                TYPE ICEBERG, ENDPOINT '{catalog_uri}'
            );
        """)

        for old_id in RENAMES:
            for schema in ["gold", "silver"]:
                try:
                    conn.execute(f"DROP TABLE catalog.{schema}.{old_id};")
                    logger.info("dropped_catalog_table", table=f"{schema}.{old_id}")
                except Exception:
                    pass

        conn.close()
    else:
        logger.warning("catalog_env_vars_not_set, skipping catalog cleanup")

    logger.info("rename_complete", renames=len(RENAMES))


if __name__ == "__main__":
    asyncio.run(main())
