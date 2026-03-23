# ADR-002: DuckDB as Analytical Engine

## Status
Accepted

## Context
The API needs to serve analytical queries over macroeconomic time series data
(SELIC, IPCA, USD/BRL, unemployment, GDP, Tesouro yields). The dataset is
small — approximately 10K rows across 6 series even with 20 years of daily
data. Options considered: Postgres with analytical queries, a dedicated OLAP
database, or DuckDB reading Parquet files in-process.

## Decision
Use DuckDB as an embedded in-process analytical engine reading gold-layer
Parquet files from local disk. No network hop, no external service.

## Consequences
- Sub-millisecond query latency for our dataset sizes
- Zero infrastructure cost — DuckDB is embedded, Parquet files are already
  produced by the pipeline
- Native support for window functions, rolling averages, and aggregations
  needed for derived metrics
- No external service dependency at query time — if the file exists, it works
- Data freshness depends on pipeline sync frequency (daily is acceptable
  for macroeconomic indicators)
- Cannot handle concurrent writes — but our use case is read-heavy with
  atomic file replacement via `os.replace()`
