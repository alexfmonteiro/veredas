import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Veredas Data Catalog

        Browse all tables registered in the R2 Data Catalog (Iceberg).
        Navigate schemas, inspect table structures, preview data, and
        check freshness.
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import os
    return duckdb, mo, os


# --- Connect to catalog ---


@app.cell
def _(duckdb, mo, os):
    token = os.environ.get("R2_CATALOG_TOKEN", "")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE", "")
    catalog_uri = os.environ.get("R2_CATALOG_URI", "")

    if not all([token, warehouse, catalog_uri]):
        mo.stop(
            True,
            mo.md(
                "**Missing catalog env vars.** Set `R2_CATALOG_TOKEN`, "
                "`R2_CATALOG_WAREHOUSE`, `R2_CATALOG_URI`."
            ),
        )

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"CREATE SECRET (TYPE ICEBERG, TOKEN '{token}');")
    conn.execute(f"""
        ATTACH '{warehouse}' AS catalog (
            TYPE ICEBERG, ENDPOINT '{catalog_uri}'
        );
    """)

    # Also set up R2 storage secret for raw parquet access
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    if key_id and secret:
        conn.execute(f"""
            CREATE SECRET r2_storage (
                TYPE R2, KEY_ID '{key_id}',
                SECRET '{secret}', ACCOUNT_ID '{account_id}'
            );
        """)

    mo.md("**Connected to R2 Data Catalog.**")
    return (conn,)


# --- All tables ---


@app.cell
def _(conn, mo):
    mo.md("## Registered Tables")

    all_tables = conn.execute("SHOW ALL TABLES;").df()
    mo.ui.table(all_tables)
    return (all_tables,)


# --- Schema picker ---


@app.cell
def _(all_tables, mo):
    schemas = sorted(all_tables["schema"].unique().tolist()) if len(all_tables) > 0 else []
    schema_picker = mo.ui.dropdown(
        options=schemas,
        value=schemas[0] if schemas else None,
        label="Schema",
    )
    schema_picker
    return (schema_picker,)


# --- Tables in schema ---


@app.cell
def _(all_tables, conn, mo, schema_picker):
    mo.stop(not schema_picker.value)

    schema = schema_picker.value
    tables_in_schema = sorted(
        all_tables[all_tables["schema"] == schema]["name"].tolist()
    )

    mo.md(f"## `catalog.{schema}` — {len(tables_in_schema)} tables")

    # Get row counts and date ranges for each table
    summaries = []
    for _tbl in tables_in_schema:
        try:
            _row = conn.execute(f"""
                SELECT
                    '{_tbl}' AS table_name,
                    count(*) AS rows,
                    min(date) AS first_date,
                    max(date) AS last_date
                FROM catalog.{schema}.{_tbl}
            """).fetchone()
            summaries.append(_row)
        except Exception:
            summaries.append((_tbl, 0, None, None))

    import pandas as pd
    summary_df = pd.DataFrame(
        summaries, columns=["table_name", "rows", "first_date", "last_date"]
    )
    mo.ui.table(summary_df)
    return schema, tables_in_schema


# --- Table picker ---


@app.cell
def _(mo, tables_in_schema):
    table_picker = mo.ui.dropdown(
        options=tables_in_schema,
        value=tables_in_schema[0] if tables_in_schema else None,
        label="Table",
    )
    table_picker
    return (table_picker,)


# --- Table detail: schema ---


@app.cell
def _(conn, mo, schema, table_picker):
    mo.stop(not table_picker.value)
    _tbl = table_picker.value
    fqn = f"catalog.{schema}.{_tbl}"

    mo.md(f"## `{fqn}`")

    mo.md("### Column Schema")
    _cols = conn.execute(f"DESCRIBE SELECT * FROM {fqn}").df()
    mo.ui.table(_cols)
    return (fqn,)


# --- Table detail: stats ---


@app.cell
def _(conn, fqn, mo):
    stats = conn.execute(f"""
        SELECT
            count(*) AS total_rows,
            min(date) AS first_date,
            max(date) AS last_date,
            round(avg(value), 4) AS avg_value,
            round(min(value), 4) AS min_value,
            round(max(value), 4) AS max_value,
            count(*) - count(value) AS null_count
        FROM {fqn}
    """).df()

    mo.md("### Statistics")
    mo.ui.table(stats)
    return


# --- Table detail: latest rows ---


@app.cell
def _(conn, fqn, mo):
    mo.md("### Latest 20 Rows")

    latest = conn.execute(f"""
        SELECT * FROM {fqn} ORDER BY date DESC LIMIT 20
    """).df()
    mo.ui.table(latest)
    return


# --- Table detail: chart ---


@app.cell
def _(conn, fqn, mo):
    mo.md("### Time Series")

    _chart_df = conn.execute(f"""
        SELECT date, value FROM {fqn}
        WHERE value IS NOT NULL ORDER BY date
    """).df()

    import altair as alt
    _chart = (
        alt.Chart(_chart_df)
        .mark_line(strokeWidth=1.5)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("value:Q", title="Value"),
            tooltip=["date:T", "value:Q"],
        )
        .properties(width="container", height=300, title=fqn)
    )
    mo.ui.altair_chart(_chart)
    return


# --- Table detail: anomalies ---


@app.cell
def _(conn, fqn, mo):
    mo.md("### Z-Score Anomalies (|z| > 2)")

    try:
        anomalies = conn.execute(f"""
            SELECT date, value, round(z_score, 3) AS z_score
            FROM {fqn}
            WHERE abs(z_score) > 2
            ORDER BY date DESC LIMIT 15
        """).df()

        if len(anomalies) == 0:
            mo.md("_No anomalies found._")
        else:
            mo.ui.table(anomalies)
    except Exception:
        mo.md("_z_score column not available for this table._")
    return


# --- Ad-hoc SQL ---


@app.cell
def _(mo):
    mo.md("## Ad-Hoc SQL")
    return


@app.cell
def _(mo):
    sql_input = mo.ui.text_area(
        value="SHOW ALL TABLES;",
        label="DuckDB SQL (catalog is attached)",
        full_width=True,
    )
    sql_input
    return (sql_input,)


@app.cell
def _(conn, mo, sql_input):
    if sql_input.value.strip():
        try:
            result = conn.execute(sql_input.value).df()
            mo.ui.table(result)
        except Exception as e:
            mo.md(f"**Error:** `{e}`")
    return


if __name__ == "__main__":
    app.run()
