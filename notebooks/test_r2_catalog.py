import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # R2 Data Catalog Test

        Test connectivity to Cloudflare R2 Data Catalog (managed Iceberg).
        This notebook validates that DuckDB can attach to the catalog,
        create tables, and query them.

        **Prerequisites:**
        - R2 Data Catalog enabled on the bucket (`wrangler r2 bucket catalog enable`)
        - API token with Admin Read & Write permissions
        - Env vars: `R2_CATALOG_TOKEN`, `R2_CATALOG_WAREHOUSE`, `R2_CATALOG_URI`
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import os
    return duckdb, mo, os


# --- Step 1: Connect to R2 Data Catalog ---


@app.cell
def _(duckdb, mo, os):
    token = os.environ.get("R2_CATALOG_TOKEN", "")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE", "")
    catalog_uri = os.environ.get("R2_CATALOG_URI", "")

    if not all([token, warehouse, catalog_uri]):
        mo.stop(
            True,
            mo.md(
                "**Missing env vars.** Set `R2_CATALOG_TOKEN`, "
                "`R2_CATALOG_WAREHOUSE`, and `R2_CATALOG_URI`.\n\n"
                f"- TOKEN: {'set' if token else '**missing**'}\n"
                f"- WAREHOUSE: {'set' if warehouse else '**missing**'}\n"
                f"- CATALOG_URI: {'set' if catalog_uri else '**missing**'}"
            ),
        )

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    conn.execute(f"""
        CREATE SECRET r2_catalog_secret (
            TYPE ICEBERG,
            TOKEN '{token}'
        );
    """)

    conn.execute(f"""
        ATTACH '{warehouse}' AS catalog (
            TYPE ICEBERG,
            ENDPOINT '{catalog_uri}'
        );
    """)

    mo.md("**Connected to R2 Data Catalog.**")
    return (conn,)


# --- Step 2: List existing tables ---


@app.cell
def _(conn, mo):
    mo.md("## Existing Tables")

    try:
        tables = conn.execute("SHOW ALL TABLES;").df()
        if len(tables) == 0:
            mo.md("_No tables found. The catalog is empty._")
        else:
            mo.ui.table(tables)
    except Exception as e:
        mo.md(f"Error listing tables: `{e}`")
    return


# --- Step 3: Create a test table ---


@app.cell
def _(conn, mo):
    mo.md("## Test: Create a Table")

    try:
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS catalog.test;
        """)
        conn.execute("""
            CREATE OR REPLACE TABLE catalog.test.hello AS
            SELECT 'it works' AS message, current_timestamp AS created_at;
        """)
        result = conn.execute("""
            SELECT * FROM catalog.test.hello;
        """).df()

        mo.md("**Created `catalog.test.hello` successfully.**")
        mo.ui.table(result)
    except Exception as e:
        mo.md(f"Error creating test table: `{e}`")
    return


# --- Step 4: Register a gold parquet file as Iceberg table ---


@app.cell
def _(mo):
    mo.md(
        """
        ## Test: Register Gold Parquet as Iceberg Table

        This reads an existing gold parquet file from R2 and creates
        an Iceberg table in the catalog from it.
        """
    )
    return


@app.cell
def _(conn, mo, os):
    bucket = os.environ.get("R2_BUCKET_NAME", "br-economic-pulse-data")
    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")

    # Need a separate R2 secret for reading raw parquet files
    try:
        conn.execute(f"""
            CREATE SECRET r2_storage (
                TYPE R2,
                KEY_ID '{key_id}',
                SECRET '{secret}',
                ACCOUNT_ID '{account_id}'
            );
        """)
    except Exception:
        pass  # Secret may already exist from a previous run

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS catalog.gold;")

        conn.execute(f"""
            CREATE OR REPLACE TABLE catalog.gold.bcb_432 AS
            SELECT * FROM read_parquet('r2://{bucket}/gold/bcb_432.parquet');
        """)

        preview = conn.execute("""
            SELECT * FROM catalog.gold.bcb_432 ORDER BY date DESC LIMIT 10;
        """).df()

        row_count = conn.execute("""
            SELECT count(*) AS rows FROM catalog.gold.bcb_432;
        """).fetchone()[0]

        mo.md(f"**Registered `catalog.gold.bcb_432` — {row_count:,} rows.**")
        mo.ui.table(preview)
    except Exception as e:
        mo.md(f"Error registering gold table: `{e}`")
    return


# --- Step 5: Query via catalog (prove it works end-to-end) ---


@app.cell
def _(conn, mo):
    mo.md("## Test: Query via Catalog")

    try:
        result = conn.execute("""
            SELECT
                date,
                value,
                series,
                unit,
                round(z_score, 3) AS z_score
            FROM catalog.gold.bcb_432
            WHERE date >= '2025-01-01'
            ORDER BY date DESC
            LIMIT 20;
        """).df()

        mo.md("**Querying `catalog.gold.bcb_432` via Iceberg catalog:**")
        mo.ui.table(result)
    except Exception as e:
        mo.md(f"Error querying catalog table: `{e}`")
    return


# --- Step 6: Cleanup test table ---


@app.cell
def _(conn, mo):
    mo.md("## Cleanup")

    cleanup_btn = mo.ui.run_button(label="Drop test.hello table")
    cleanup_btn
    return (cleanup_btn,)


@app.cell
def _(cleanup_btn, conn, mo):
    mo.stop(not cleanup_btn.value)

    try:
        conn.execute("DROP TABLE IF EXISTS catalog.test.hello;")
        mo.md("**Dropped `catalog.test.hello`.**")
    except Exception as e:
        mo.md(f"Error dropping table: `{e}`")
    return


# --- Summary ---


@app.cell
def _(mo):
    mo.md(
        """
        ## Env Vars Needed on Railway

        | Variable | Where to get it |
        |----------|----------------|
        | `R2_CATALOG_TOKEN` | Cloudflare dashboard → R2 → Manage API tokens → Create token (Admin Read & Write) |
        | `R2_CATALOG_WAREHOUSE` | Output of `wrangler r2 bucket catalog enable <bucket>` |
        | `R2_CATALOG_URI` | Output of `wrangler r2 bucket catalog enable <bucket>` |
        | `R2_ACCESS_KEY_ID` | (existing) — needed to read raw parquet for initial registration |
        | `R2_SECRET_ACCESS_KEY` | (existing) |
        | `R2_ACCOUNT_ID` | (existing) |
        | `R2_BUCKET_NAME` | (existing) |
        """
    )
    return


if __name__ == "__main__":
    app.run()
