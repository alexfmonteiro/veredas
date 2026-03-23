# ADR-011: Scalability and Serving Evolution

**Status**: Accepted (forward-looking, v2+)
**Date**: 2026-03-22
**Context**: v1 architecture deliberately optimizes for simplicity and cost. This ADR documents the known limitations, their expected breaking points, and concrete evolution paths for when v1 constraints no longer hold.

---

## 1. Serving Layer — DuckDB + Local Volume

### Current Design

DuckDB runs in-process on the Railway API server. Gold-layer Parquet files live on a 1GB persistent volume at `/data/gold`. Queries execute with zero network overhead and sub-millisecond latency for the current dataset (~10K rows across 6 series).

### Why It Exists

- $0 infrastructure cost — no managed OLAP service
- Sub-millisecond latency — no network hop
- Zero operational surface — no cluster to manage, no connection pool to tune
- Sufficient for v1 scale: 6 series, ~10K rows, single-digit concurrent users

### When It Breaks

| Signal | Threshold | Impact |
|---|---|---|
| Dataset size exceeds in-process comfort | > 500K rows or > 50 series | DuckDB scan times grow; memory pressure on Railway container (512MB–1GB) |
| Concurrent query load | > 20 simultaneous analytical queries | DuckDB's single-writer / reader contention degrades p99 latency |
| Horizontal scaling required | > 1 API replica needing consistent reads | Each replica has its own volume — sync divergence, no shared state |
| Query complexity growth | Multi-table joins, window functions across 20+ series | Scan time exceeds the 30s query timeout |

DuckDB in-process is not horizontally scalable. Adding a second Railway replica means a second volume, a second sync webhook call, and no guarantee of read consistency across replicas.

### Proposed Evolution

**Phase 1 — MotherDuck (managed DuckDB-as-a-service)**

- Drop-in replacement: same SQL dialect, same Parquet support
- Shared storage layer eliminates per-replica sync
- Adds concurrency handling and larger-than-memory queries
- Cost: MotherDuck free tier supports moderate workloads; paid tier starts ~$25/month
- Migration: change the DuckDB connection string; no query rewrites

**Phase 2 — Dedicated OLAP (ClickHouse Cloud or BigQuery)**

- Required only if dataset grows to millions of rows or query patterns become heavily concurrent
- ClickHouse Cloud: column-oriented, sub-second on billion-row tables, ~$50-100/month at moderate scale
- BigQuery: serverless, per-query pricing, natural fit if consolidating to GCP
- Migration: rewrite DuckDB SQL to target dialect (mostly compatible for analytical queries), update StorageBackend to write directly to the OLAP store instead of Parquet

**Trade-off summary:**

| Option | Latency | Cost | Complexity | When to adopt |
|---|---|---|---|---|
| DuckDB in-process (current) | < 1ms | $0 | Minimal | v1 — up to ~50 series, single replica |
| MotherDuck | 5-50ms | $0-25/mo | Low | v2 — when adding replicas or > 50 series |
| ClickHouse / BigQuery | 20-200ms | $50-200/mo | Moderate | v3+ — when dataset exceeds 1M rows or concurrency > 50 |

---

## 2. Data Sync and Freshness Model

### Current Design

Pipeline (GitHub Actions) writes gold Parquet to R2. On completion, it sends `POST /api/internal/sync` to Railway. The sync endpoint downloads files from R2 to the local persistent volume via atomic `os.replace()`. DuckDB reads from the local volume.

### Why It Exists

- Decouples pipeline execution (GitHub Actions) from serving (Railway)
- Persistent volume eliminates cold-start R2 downloads (3-8s → 0ms)
- Atomic swap ensures DuckDB never reads partial files
- Webhook is simple: one HTTP call, one bearer token, no message queue

### When It Breaks

| Signal | Failure Mode | Impact |
|---|---|---|
| Webhook delivery failure | GitHub Actions `curl` fails silently or times out | Gold data goes stale; no automatic retry; requires manual `workflow_dispatch` |
| Railway restart during sync | Sync endpoint receives request but container restarts mid-download | Temp directory orphaned on volume; next sync succeeds but leaves garbage |
| Pipeline succeeds, sync fails | R2 has new data, volume has old data | API serves stale data with no alert — `metadata.json` still shows old timestamp |
| Multi-replica sync | Two replicas receive the webhook | Both download simultaneously; no coordination; potential for divergent states if one fails |
| R2 eventual consistency | Rare: R2 returns stale object immediately after write | Sync downloads stale Parquet; volume updates with old data tagged as new |

**The critical gap is observability**: there is no alert when sync fails. The `sync_health` field in `/api/health` reports staleness but nothing pages or alerts on it. A silent sync failure at 06:05 UTC means the API serves stale data until someone manually checks.

### Proposed Evolution

**Phase 1 — Sync reliability hardening (v1.5)**

- Add retry logic to the GitHub Actions sync step (3 attempts, 30s apart)
- Add a `/api/internal/sync-status` endpoint that the pipeline queries after the sync call to confirm the volume was updated
- Emit a structured log event (`sync_failed`) that Sentry captures as an alert
- Add a dead-letter mechanism: if sync fails 3 times, write a `sync_failed` marker to R2 that the next pipeline run detects

**Phase 2 — Direct query from object storage (v2)**

- Replace local volume sync with DuckDB's `httpfs` extension reading directly from R2
- Eliminates the sync webhook entirely — DuckDB reads Parquet from R2 at query time
- Latency increases from <1ms to 50-200ms per query (network round-trip to R2)
- Acceptable if combined with a query cache (Redis or in-memory LRU)
- Removes the entire sync failure class of problems

**Phase 3 — Streaming / incremental sync (v3+)**

- Replace batch webhook with an event stream (R2 event notifications → SQS/webhook → incremental file update)
- Only download changed files instead of full sync
- Enables sub-minute data freshness for high-frequency series
- Required only if adding intraday data sources (e.g., real-time exchange rates)

**Phase 2 is the recommended v2 target** — it eliminates sync as a failure domain entirely and trades a small latency increase for significant operational simplification.

---

## 3. Query System — Routing and Reasoning

### Current Design

The QuerySkillRouter uses regex pattern matching (Tier 1/2) with LLM fallback (Tier 3). Haiku classify-and-route handles the ~50-60% of queries that regex cannot classify. Tier 3 sends the full question to Claude Sonnet with XML-fenced data context.

### Why It Exists

- Regex is $0 and <1ms for obvious lookups
- Haiku is cheap ($0.08/month at 2K queries) for semantic classification
- Sonnet handles complex reasoning that neither regex nor templates can
- The 3-tier design minimizes LLM cost while maintaining answer quality

### When It Breaks

| Signal | Limitation | Impact |
|---|---|---|
| Regex pattern explosion | > 30 patterns, each needing PT/EN/slang variants | Maintenance burden grows; new patterns risk breaking existing matches |
| Haiku misroutes to Tier 2 | Template response for a question that needs reasoning | User gets a shallow answer; no feedback loop to correct |
| Multi-metric reasoning | "Compare SELIC, IPCA, and USD/BRL over the last 2 years" | Tier 3 works but has no structured way to fetch and join data — relies on LLM to reason over a flat text dump |
| Follow-up context | "Now show me just the last 6 months" (referring to previous query) | In-memory history is fragile; no persistent conversation state; context lost on server restart |
| New metric types | Adding 10+ series in v2 | Every new series needs new regex patterns, new template handlers, new data context — linear scaling of maintenance |

**The core limitation is the absence of an intermediate query planning layer.** The system jumps from regex (rigid, no reasoning) to full LLM (expensive, slow, unstructured). There is no layer that understands the data schema, generates targeted SQL, and only invokes the LLM for natural language explanation.

### Proposed Evolution

**Phase 1 — Structured query abstraction (v2)**

Introduce a query planner between the router and DuckDB:

```
User question → Router → Query Planner → SQL generation → DuckDB → Result formatting → LLM (explanation only)
```

- The query planner maps parsed intent (metric, time range, aggregation, comparison) to a structured query object
- SQL generation is deterministic from the query object — no LLM needed for data retrieval
- The LLM receives structured results and generates only the natural language explanation
- This reduces Sonnet input tokens by 60-80% (no raw data dump) and improves answer grounding

**Phase 2 — Hybrid analytical execution (v2-v3)**

- Query planner handles single-metric and comparison queries entirely without LLM
- LLM invoked only for: causal reasoning, explanation of economic relationships, questions that require domain knowledge beyond the data
- Add a lightweight DSL or structured intent schema that the planner understands:

```python
@dataclass
class QueryIntent:
    metrics: list[str]          # ["bcb_432", "bcb_433"]
    time_range: TimeRange       # last_6_months, ytd, custom(start, end)
    aggregation: Aggregation    # latest, trend, comparison, distribution
    explanation_needed: bool    # True only when reasoning required
```

- Router (regex + Haiku) populates the `QueryIntent`. Planner generates SQL. LLM explains results only when `explanation_needed=True`.

**Phase 3 — Agentic query execution (v3+)**

- For complex multi-step questions ("What drove the SELIC increase in Q3, and how did it affect USD/BRL in the following quarter?"):
  - Decompose into sub-queries
  - Execute each against DuckDB
  - Synthesize results with LLM
- This is the "agent with tools" pattern — the LLM orchestrates data retrieval rather than receiving a data dump

---

## 4. Multi-Provider Infrastructure

### Current Design

Six providers: Vercel (frontend), Railway (API), Cloudflare R2 (storage), Neon (Postgres), Upstash (Redis), Sentry (observability). Total cost: ~$6-7/month. Managed via Terraform (v1.5+).

### Why It Exists

- Each provider was selected for its free tier generosity
- Total cost is 85-90% lower than single-cloud equivalent
- Terraform abstracts the provider diversity — migration is configuration, not rewrite

### When It Breaks

| Signal | Risk | Impact |
|---|---|---|
| Provider outage | Any one of 6 providers can independently fail | Partial system degradation; debugging requires checking 6 dashboards |
| Credential sprawl | 6 sets of API keys, tokens, and secrets | Rotation overhead; increased attack surface; env var management complexity |
| Cross-provider latency | Railway (US) → Neon (US) → Upstash (US) is fine; adding non-US regions is not | Geographic expansion requires rethinking provider selection per region |
| Team onboarding | New contributor needs accounts on 6 platforms | Friction; documentation overhead; access management across 6 services |
| Compliance requirements | SOC 2, GDPR DPA needed per provider | 6 vendor assessments instead of 1 |

### When Consolidation Becomes Justified

Consolidate when **any two** of these conditions are true:

1. Monthly revenue exceeds $100/month (cost delta with single cloud becomes negligible)
2. Team grows beyond 2 engineers (onboarding friction outweighs cost savings)
3. Compliance audit is required (vendor consolidation reduces assessment scope)
4. Geographic expansion is needed (single cloud provides regional consistency)

### Proposed Evolution

**Target: GCP (Cloud Run + Cloud Storage + Cloud SQL + Memorystore)**

- Cloud Run: closest to Railway's deployment model, scale-to-zero billing
- Cloud Storage: R2 replacement, same S3-compatible API via `boto3`
- Cloud SQL (Postgres): Neon replacement, managed backups, IAM auth
- Memorystore (Redis): Upstash replacement, VPC-internal latency
- Estimated cost: $30-50/month (see ROADMAP Section 13)
- Migration path: update Terraform providers, update env vars, redeploy — no code changes required thanks to StorageBackend Protocol and standard Postgres/Redis clients

**Alternative: AWS (ECS Fargate + S3 + RDS + ElastiCache)**

- More mature ecosystem, broader service catalog
- Higher base cost ($40-65/month) due to less granular scale-to-zero
- Better fit if targeting enterprise B2B customers who require AWS

**The StorageBackend Protocol is the key enabler.** Because all file I/O goes through the protocol, switching from R2 to S3 or GCS requires only a new `GCSStorageBackend` implementation — no changes to Tasks, Agents, or API code.

---

## 5. Observability and Reliability

### Current Design

- Structured JSON logs via `structlog`
- Sentry for error tracking (frontend + API)
- `/api/health` endpoint with sync status
- `/api/quality/latest` for pipeline quality reports
- Uptime Robot pinging `/api/health` every 5 minutes

### Why It Exists

- Minimal operational overhead for a solo/small-team project
- Sentry free tier handles error alerting
- Health endpoint is sufficient for uptime monitoring
- Quality endpoint provides pipeline transparency

### What Is Missing

| Gap | Current State | Risk |
|---|---|---|
| Query latency distribution | Not measured | Cannot detect p95/p99 degradation; no baseline for SLA |
| Routing tier usage metrics | `tier_used` logged per query but not aggregated | Cannot track cost efficiency or detect router misclassification trends |
| Pipeline SLA / freshness tracking | `sync_health` in health endpoint but no alerting | Stale data goes unnoticed until a user reports it |
| LLM cost tracking | Token counts logged per query but not aggregated | Cannot detect cost spikes or budget overruns until the Anthropic invoice |
| Sync failure alerting | Sync failure logged but no alert fires | Silent data staleness — the most dangerous failure mode in the system |
| Error budget / SLO definition | No defined SLOs | Cannot make principled trade-offs between reliability investment and feature work |
| Distributed tracing | No correlation between pipeline run → sync → query | Debugging cross-component issues requires manual log correlation |

### Proposed Evolution

**Phase 1 — Metrics and alerting foundation (v1.5-v2)**

Add lightweight metrics collection without introducing a metrics backend:

- Emit key metrics as structured log events that Sentry or a log aggregator can query:
  - `query_latency_ms` with `tier`, `metric`, `status` dimensions
  - `sync_completed` / `sync_failed` with `duration_ms`, `files_synced`
  - `pipeline_completed` / `pipeline_failed` with `duration_ms`, `rows_processed`
  - `llm_tokens_used` with `agent`, `model`, `tier` dimensions
- Configure Sentry alerts for:
  - `sync_failed` events (immediate)
  - `sync_health == "stale"` persisting > 2 hours (warning)
  - `sync_health == "critical"` (page)
  - Error rate > 5% over 5-minute window
- Define initial SLOs:
  - API availability: 99.5% (measured by Uptime Robot)
  - Query p95 latency: < 3s (Tier 3), < 100ms (Tier 1/2)
  - Data freshness: gold layer updated within 2 hours of pipeline completion

**Phase 2 — Dashboards and tracing (v2-v3)**

- Add a lightweight metrics dashboard (Grafana Cloud free tier or Sentry Performance):
  - Query volume and tier distribution over time
  - LLM cost burn rate (daily/weekly)
  - Sync lag (time between pipeline completion and volume update)
  - Error rates by endpoint and error type
- Add correlation IDs that flow through: pipeline run → R2 write → sync webhook → query response
- Add pipeline run duration tracking with trend analysis (detect pipeline slowdowns before they hit timeouts)

**Phase 3 — Full observability stack (v3+)**

- OpenTelemetry instrumentation for distributed tracing
- Structured alerting with escalation policies
- Capacity planning dashboards (storage growth rate, query volume trends, LLM cost projections)
- Only justified when operating multiple dark factory projects that share infrastructure

---

## Summary: Evolution Priority Order

| Priority | Concern | Recommended Version | Effort |
|---|---|---|---|
| 1 | Sync failure alerting | v1.5 | Low — Sentry alert rules on existing log events |
| 2 | Query latency and tier metrics | v1.5 | Low — structured log events, no new infra |
| 3 | SLO definition | v2 | Low — documentation + Uptime Robot config |
| 4 | Structured query planner | v2 | Medium — new QueryIntent layer, router refactor |
| 5 | Direct-from-R2 query (eliminate sync) | v2 | Medium — DuckDB httpfs, query cache layer |
| 6 | Product readiness (auth, sessions) | v2 | Medium — see ROADMAP Section 10 |
| 7 | MotherDuck migration | v2-v3 | Low — connection string change |
| 8 | Cloud consolidation | v2-v3 | Medium — Terraform provider migration |
| 9 | Agentic query execution | v3+ | High — agent-with-tools architecture |
| 10 | Dedicated OLAP (ClickHouse/BQ) | v3+ | High — SQL dialect migration, new infra |

---

## Decision

Document these limitations and evolution paths now. Do not act on them in v1. Each phase has a clear trigger signal — adopt when the signal fires, not before. The v1 architecture is correct for its scale; premature optimization would add complexity without users to justify it.

## Consequences

- The team has a shared, explicit understanding of where the architecture will need to evolve
- No v1 scope creep — all items are gated by observable signals
- Future ADRs for individual migrations (e.g., ADR-012 for MotherDuck migration) can reference this document for context
- Product readiness gaps are tracked in ROADMAP Section 10 with cross-references here
