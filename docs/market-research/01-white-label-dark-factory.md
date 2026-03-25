# Deep Dive 1: White-Label & Dark Factory Strategy

**BR Economic Pulse -- Market Research Series**
**Date: March 2026 | Perspective: VC Analyst (Seed/Series A)**

---

## Executive Summary

BR Economic Pulse is architecturally positioned to operate as a **dark factory**
-- a fully automated, lights-out data intelligence operation that produces and
sells economic data products with zero human intervention during steady-state
operation. The YAML-driven feed configuration system, medallion pipeline, and
automated quality gates make multi-tenant white-labeling and multi-country
expansion a configuration exercise rather than a rewrite.

---

## 1. What "Dark Factory" Means for This Product

A dark factory is a fully automated operation that produces and sells a commodity
with zero daily human intervention. For BR Economic Pulse, the architecture
already achieves ~90% dark-factory status:

| Component | Automation Status | Human Touchpoint |
|---|---|---|
| **Data ingestion** | Automated (GitHub Actions cron, 06:00 UTC daily) | None in steady state |
| **Quality gates** | Automated (`QualityTask` validates bronze/silver layers) | Alert review on failure |
| **Transformation** | Automated (medallion pipeline: bronze -> silver -> gold via DuckDB SQL in YAML) | None |
| **Insight generation** | Automated (`InsightAgent` generates bilingual AI summaries + anomaly reports) | None |
| **Data serving** | Automated (FastAPI + DuckDB, 24/7) | None |
| **Natural language queries** | Automated (two-tier: Tier 1 DuckDB $0, Tier 2 Claude Haiku $0.001/query) | None |
| **Rate limiting** | Automated (Upstash Redis, session-based, 10 queries/day free tier) | None |

### Remaining Steps to 100% Dark Factory

| Gap | Current State | Required Investment |
|---|---|---|
| **Self-healing feeds** | Retry logic exists in feed YAML (`retry_attempts: 3, retry_delay_seconds: 60`). No automatic fallback to alternative sources. | Add secondary source URLs per feed YAML + automated failover. 1-2 weeks. |
| **Alerting integration** | Quality gates log failures via structlog. No PagerDuty/Slack integration. | Add webhook notifications on quality gate failures. 1 week. |
| **Auto-scaling** | Railway provides basic auto-scaling. No load-based scaling rules. | Configure Railway scaling policies or migrate to Kubernetes. 1-2 weeks. |
| **Automated billing** | No billing system. | Integrate Stripe for API key provisioning + metered billing. 3-4 weeks. |
| **Feed health dashboard** | Quality monitoring exists on React dashboard. No automated feed health scoring. | Add feed health endpoint + SLA tracking. 1-2 weeks. |

**Total investment to reach 100% dark factory: 8-12 weeks of engineering effort.**

---

## 2. The Feed Configuration System: Foundation of Scalability

The YAML-driven feed configuration (`data/feeds/*.yaml`) is the architectural
cornerstone. Each feed is a self-contained data contract:

```yaml
# Example: data/feeds/bcb_selic.yaml
feed_id: bcb_432
name: "BCB SELIC Rate"
version: "1.0.0"
status: active

source:
  type: api
  url: "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/20?formato=json"
  format: json
  auth_method: none
  rate_limit_rpm: 30
  backfill_url: "https://api.bcb.gov.br/dados/serie/...&dataInicial={start}&dataFinal={end}"
  backfill_window_years: 10

schedule:
  cron: "0 6 * * 1-5"
  timezone: "America/Sao_Paulo"
  retry_attempts: 3

schema_fields:
  - name: data
    source_field: data
    type: string
    silver_type: DATE
    silver_expression: "strptime(\"{col}\", '%d/%m/%Y')"
  - name: valor
    source_field: valor
    type: string
    silver_type: DOUBLE
    silver_expression: "CAST(\"{col}\" AS DOUBLE)"

quality:
  bronze:
    max_null_rate: 0.02
    min_row_count: 1
  gold:
    value_range_min: 0.0
    value_range_max: 50.0
```

**Why this matters for white-labeling:** Adding a new data series is a YAML file,
not a code change. Adding a new country is a collection of YAML files. This is
the basis for the dark-factory "product line" model.

### Current Feed Inventory (8 Series)

| Feed ID | Series | Source | Domain |
|---|---|---|---|
| `bcb_432` | SELIC Rate | BCB | Monetary Policy |
| `bcb_433` | IPCA Inflation | BCB | Inflation |
| `bcb_1` | USD/BRL Exchange | BCB | FX |
| `ibge_pnad` | Unemployment Rate | IBGE | Labor |
| `ibge_gdp` | GDP Proxy | IBGE | Output |
| `tesouro_prefixado_curto` | Short-Term Bond Yield | Tesouro | Fixed Income |
| `tesouro_prefixado_longo` | Long-Term Bond Yield | Tesouro | Fixed Income |
| `tesouro_ipca` | Real Interest Rate (IPCA+) | Tesouro | Fixed Income |

---

## 3. White-Label Architecture

The product has clean separation between data pipeline, API, and frontend,
making white-labeling structurally feasible:

```
┌─────────────────────────────────────────────────────────┐
│  TENANT A (Fintech App)     TENANT B (Bank Dashboard)   │
│  ┌─────────────────────┐    ┌─────────────────────────┐ │
│  │  Custom Branding     │    │  Custom Branding         │ │
│  │  Subset of Series    │    │  All Series + Custom     │ │
│  │  API Key: tk_abc     │    │  API Key: tk_xyz         │ │
│  └────────┬────────────┘    └────────┬─────────────────┘ │
│           │                          │                    │
│  ┌────────▼──────────────────────────▼─────────────────┐ │
│  │           Shared Dark Factory Pipeline               │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │ │
│  │  │  Ingest  ├──▶  Quality ├──▶  Transform (MoM,  │   │ │
│  │  │  (YAML)  │  │  Gates   │  │  YoY, z-score)   │   │ │
│  │  └──────────┘  └──────────┘  └──────────────────┘   │ │
│  │                                                      │ │
│  │  ┌──────────────────────────────────────────────┐    │ │
│  │  │  Gold Layer (DuckDB) + InsightAgent (Claude) │    │ │
│  │  └──────────────────────────────────────────────┘    │ │
│  └──────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### White-Label Opportunities by Segment

| Segment | White-Label Model | Revenue/Client | Effort | Year 1 Targets |
|---|---|---|---|---|
| **Fintech apps** | Embed economic data widgets + API | $2K-10K/mo | Low (API-only) | 3-5 clients |
| **Banks / brokerages** | Branded dashboard for their retail clients | $5K-25K/mo | Medium (custom frontend) | 1-2 clients |
| **News organizations** | Automated economic reports + embeddable charts | $1K-5K/mo | Low (API + insight feed) | 3-5 clients |
| **Consulting firms** | Data intelligence platform under their brand | $3K-15K/mo | Medium | 2-3 clients |
| **Government / central banks** | Economic monitoring dashboard | $5K-20K/mo | High (compliance) | 0-1 clients |
| **EdTech platforms** | Economics education with real data | $500-2K/mo | Low | 2-3 clients |

### Implementation Requirements for Multi-Tenant

| Requirement | Current State | Gap |
|---|---|---|
| **API key management** | Session-based rate limiter (cookie + Redis) | Need per-API-key auth, metered billing |
| **Tenant isolation** | Single-tenant | Need tenant ID in API routes, data filtering |
| **Branding** | Single React dashboard | Need CSS theme variables, logo injection |
| **Series access control** | All series visible to all users | Need per-tenant series permissions |
| **Usage metering** | Daily query count per session | Need per-key call counting + Stripe integration |
| **Custom feeds** | Shared feed pool | Need tenant-specific feed configuration |

**Estimated effort for multi-tenant MVP: 6-8 weeks.**

---

## 4. Multi-Country Expansion

### Expansion Playbook Per Country

Each new country requires:

1. **YAML feed configs** (1-3 days per series, 5-10 series per country)
2. **Source API integration** (country-specific API quirks)
3. **Query router updates** (`agents/query/router.py` -- add country-specific keywords to `METRIC_KEYWORDS`)
4. **Series config updates** (`api/series_config.py` -- add display labels)
5. **Localization** (add Spanish to bilingual insight system)

### Country Priority Matrix

| Country | Central Bank API Quality | Stats Bureau | Effort | Market Size | Priority |
|---|---|---|---|---|---|
| **Mexico** (Banxico, INEGI) | Excellent REST API | Good | 1-2 weeks | $1.3T GDP, 800+ fintechs | **#1** |
| **Chile** (BCCh, INE) | Good API | Good | 1-2 weeks | $335B GDP, strong fintech scene | **#2** |
| **Colombia** (BanRep, DANE) | Moderate | Moderate | 2-3 weeks | $363B GDP, growing market | **#3** |
| **Peru** (BCRP, INEI) | Basic | Basic | 2-3 weeks | $268B GDP | **#4** |
| **Argentina** (BCRA, INDEC) | Good REST API | Poor/unstable | 2-3 weeks | $641B GDP, but inflation data politically sensitive | **#5** |

### Code Changes Required for Multi-Country

**`api/series_config.py`** -- Currently a hardcoded Python dict of 8 Brazilian
series. Must become a dynamic registry:

```python
# Current (Brazil-only, hardcoded)
SERIES_DISPLAY: dict[str, dict[str, str]] = {
    "bcb_432": {"label": "SELIC", "unit": "% a.a.", ...},
    ...
}

# Future (multi-country, config-driven)
# Load from database or YAML, keyed by country + feed_id
```

**`agents/query/router.py`** -- `METRIC_KEYWORDS` dict is hardcoded for Brazilian
series (selic, ipca, dolar, pib, etc.). Must become country-aware:

```python
# Current
METRIC_KEYWORDS = {"selic": "bcb_432", "ipca": "bcb_433", ...}

# Future
# Keyed by (country, keyword) or loaded from per-country config
```

**Estimated cost per country: $15K-30K engineering + $500-1K/mo maintenance.**

---

## 5. Revenue Models

| Model | Description | Margin | Scalability |
|---|---|---|---|
| **B2C Freemium** | Dashboard + 10 queries/day free, Pro tier for power users | 60-70% | High volume, high churn |
| **B2B API** (Dark Factory core) | Raw API subscriptions, metered billing, no UI | **90%+** | **Zero marginal cost, infinite scale** |
| **B2B2C White-Label** | Branded platform embedded in partner apps | 85-95% | High leverage (partner distribution) |
| **B2B Enterprise** | Custom dashboards, SLA, dedicated support | 80-90% | Sales-driven, moderate scale |

### Recommended Strategy

**B2C freemium funnel -> B2B/B2B2C paid tiers -> Data-as-a-Service (dark factory)**

The free tier (already implemented: 10 queries/day via rate limiter) serves as
lead generation. Convert power users to Pro ($49/mo). Use Pro adoption data to
identify companies for B2B sales. Build white-label partnerships for B2B2C
distribution. Scale the dark factory API as the highest-margin, most scalable
revenue stream.

---

## 6. Dark Factory Unit Economics

| Metric | Value | Notes |
|---|---|---|
| **Infrastructure cost (100K users)** | $800-3K/month | Railway + R2 + Upstash + Vercel |
| **Cost per free user** | $0.01-0.05/month | DuckDB queries at $0 |
| **Cost per API call (Tier 1)** | $0.00 | DuckDB, no LLM |
| **Cost per API call (Tier 2)** | ~$0.001 | Claude Haiku |
| **Marginal cost per white-label tenant** | $50-200/month | Proportional infra + monitoring |
| **Gross margin (API product)** | 90-95% | Infrastructure cost is negligible at scale |
| **Break-even (dark factory)** | ~$5K ARR | Covers base infrastructure |

### Why the Cost Structure Is a Moat

Bloomberg charges $24K/user/year. BR Economic Pulse can serve macro-only use
cases at $300-1,000/year with 90%+ margins. This is a **100x cost advantage**
for the specific use case of macroeconomic data intelligence. The advantage is
structural (DuckDB + YAML pipeline + cloud-native) and durable.

---

## 7. Implementation Roadmap

### Phase 1: Dark Factory MVP (Weeks 1-8)
- [ ] Stripe integration for API key provisioning + metered billing
- [ ] Per-API-key rate limiting (extend existing `rate_limiter.py`)
- [ ] API documentation portal (FastAPI already generates OpenAPI spec)
- [ ] Developer SDK (Python, JavaScript wrappers)
- [ ] Feed health monitoring with webhook alerts

### Phase 2: White-Label MVP (Weeks 8-16)
- [ ] Tenant ID in API routes + data access filtering
- [ ] CSS theme variables + logo injection for React dashboard
- [ ] Per-tenant series permissions
- [ ] Self-service tenant provisioning
- [ ] First 2-3 white-label partnerships signed

### Phase 3: Multi-Country (Weeks 16-28)
- [ ] Mexico feeds (Banxico + INEGI): 5-8 series
- [ ] Chile feeds (BCCh + INE): 5-8 series
- [ ] Country-aware query router
- [ ] Spanish language support in insight generation
- [ ] Dynamic series registry (replace hardcoded `SERIES_DISPLAY`)

---

## Key Takeaway

The dark factory model is the highest-leverage strategy for BR Economic Pulse.
The architecture already supports it at 90% -- the remaining 10% is billing
infrastructure and alerting. Each new country or white-label tenant is a
configuration exercise, not a rewrite. The unit economics are compelling:
90%+ gross margins with zero marginal cost per Tier 1 query.
