# ADR-004: Cloudflare R2 + Parquet over Database Storage

## Status
Accepted

## Context
Pipeline data needs durable, versioned storage across bronze/silver/gold
layers. Options considered: storing everything in Postgres, using S3-compatible
object storage with Parquet files, or a data lakehouse service.

Postgres would add write load to the metadata database and mix analytical
data with application state. A lakehouse service (Databricks, Snowflake)
is overkill for our data volume.

## Decision
Store all pipeline data as immutable timestamped Parquet files on Cloudflare
R2 (S3-compatible object storage). Use a medallion architecture with
bronze/silver/gold prefix-based organization. Access via `StorageBackend`
Protocol abstraction.

## Consequences
- Near-zero cost — R2 has no egress fees and generous free tier
- Immutable bronze files provide full audit trail
- Parquet is columnar and self-describing — schema is embedded in the file
- DuckDB reads Parquet natively with zero conversion overhead
- No database migration needed for schema changes in pipeline data
- Requires a sync mechanism to copy gold files to API server local disk
  (see ADR-008)
- StorageBackend abstraction enables local development without R2 credentials
