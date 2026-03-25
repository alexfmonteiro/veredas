# Deep Dive 4: Pricing & ARR Projections

**BR Economic Pulse -- Market Research Series**
**Date: March 2026**

---

## Executive Summary

A four-tier pricing model (Free, Pro, Team, Enterprise) plus a standalone
Data API product targets 90%+ gross margins at scale. Conservative
projections show $750K ARR by Year 3 (bootstrapped); moderate scenario
reaches $4.5M ARR (seed-funded); aggressive scenario hits $12M ARR
(Series A, LATAM expansion). The dark-factory Data API is the
highest-margin, most scalable revenue stream.

---

## 1. Pricing Tiers

| Tier | Price | Includes | Target Buyer |
|---|---|---|---|
| **Free** | $0 | Dashboard, 10 AI queries/day, 8 series, basic charts | Students, journalists, casual users |
| **Pro** | $49/mo ($470/yr) | Unlimited queries, API (1K calls/mo), CSV export, anomaly alerts, historical data | Individual analysts, small teams |
| **Team** | $199/mo ($1,900/yr) | 5 seats, 10K API calls/mo, custom dashboards, priority support | FP&A teams, small funds |
| **Enterprise** | $999-4,999/mo | Unlimited seats, unlimited API, white-label, SLA, dedicated support, custom feeds | Banks, large funds, fintechs |
| **Data API** | $299-2,999/mo | Raw API access, webhooks, bulk export, no UI | Fintechs, data aggregators |

### Pricing Rationale

- **Free tier** already implemented (10 queries/day via `rate_limiter.py`
  with Upstash Redis). Serves as lead generation funnel.
- **Pro at $49/mo** is 40x cheaper than Trading Economics ($990/yr) for
  a more capable product (AI queries, anomaly detection).
- **Enterprise at $999-4,999/mo** is 5-24x cheaper than Bloomberg
  ($2,000/mo/seat) while providing AI capabilities Bloomberg lacks.
- **Data API** is the dark-factory product: zero UI cost, zero marginal
  cost per Tier 1 query, pure infrastructure margin.

---

## 2. Unit Economics

### Cost Structure

| Component | Monthly Cost | Notes |
|---|---|---|
| Railway (API hosting) | $50-300 | Scales with traffic |
| Cloudflare R2 (storage) | $5-50 | Parquet files, very cheap |
| Upstash Redis (rate limiting) | $10-30 | Per-request pricing |
| Vercel (frontend) | $0-20 | Free tier covers most usage |
| Claude API (Tier 2 queries) | Variable | ~$0.001/query (Haiku) |
| GitHub Actions (pipeline) | $0-50 | Free tier for public repos |
| Domain + DNS | $15 | Fixed |
| **Total base infrastructure** | **$80-465/mo** | |

At 100K users: $800-3K/month total infrastructure.

### Per-User Economics

| Metric | Free | Pro | Team | Enterprise |
|---|---|---|---|---|
| Infrastructure cost/user/mo | $0.01-0.05 | $2-5 | $5-15 | $10-30 |
| Revenue/user/mo | $0 | $49 | $40/seat | $200-1,000/seat |
| **Gross margin** | N/A | **90-95%** | **85-92%** | **85-92%** |
| Tier 2 queries/mo (est.) | 5 | 50 | 200 | 500+ |
| Tier 2 cost/mo | $0.005 | $0.05 | $0.20 | $0.50+ |

### Customer Acquisition Cost (CAC)

| Channel | Estimated CAC | Target Tier |
|---|---|---|
| Organic/SEO | $0-20 | Free -> Pro conversion |
| Content marketing | $30-80 | Pro |
| Product Hunt / community | $10-50 | Pro |
| LinkedIn campaigns | $100-300 | Team |
| Direct sales (outbound) | $1,000-3,000 | Enterprise |
| Fintech partnerships | $500-1,500 | Data API |

### Lifetime Value (LTV)

| Tier | Avg. Monthly Revenue | Avg. Lifetime | LTV | LTV:CAC |
|---|---|---|---|---|
| Pro | $49 | 24 months | $1,176 | 7.5:1 - 24:1 |
| Team | $199 | 30 months | $5,970 | 20:1 - 60:1 |
| Enterprise | $2,500 | 36 months | $90,000 | 30:1 - 90:1 |
| Data API | $1,000 | 36 months | $36,000 | 24:1 - 72:1 |

All tiers show excellent LTV:CAC ratios (>3:1 is considered healthy for
SaaS). Enterprise and Data API are the most capital-efficient.

---

## 3. ARR Projections

### Scenario A: Conservative (Bootstrapped / 1-2 Person Team)

Assumptions: Organic growth only, no paid marketing, no sales team.

| Metric | Year 1 | Year 2 | Year 3 |
|---|---|---|---|
| Free users | 2,000 | 8,000 | 20,000 |
| Pro subscribers | 50 | 200 | 500 |
| Team subscribers | 5 | 20 | 50 |
| Enterprise clients | 1 | 3 | 8 |
| Data API clients | 0 | 2 | 5 |
| **Monthly Revenue** | **$5K** | **$21K** | **$63K** |
| **ARR** | **$60K** | **$250K** | **$750K** |

### Scenario B: Moderate (Seed Funded, 3-5 Person Team)

Assumptions: $2M seed raise. 2 engineers, 1 sales, 1 marketing.
Mexico added in Year 2.

| Metric | Year 1 | Year 2 | Year 3 |
|---|---|---|---|
| Free users | 5,000 | 25,000 | 80,000 |
| Pro subscribers | 150 | 600 | 1,500 |
| Team subscribers | 15 | 60 | 150 |
| Enterprise clients | 3 | 10 | 25 |
| Data API clients | 2 | 10 | 30 |
| White-label contracts | 0 | 2 | 8 |
| **Monthly Revenue** | **$17K** | **$100K** | **$375K** |
| **ARR** | **$200K** | **$1.2M** | **$4.5M** |

### Scenario C: Aggressive (Series A, 10+ Person Team, LATAM)

Assumptions: Series A at $8-15M. Full sales team, 6-country coverage
by Year 3.

| Metric | Year 1 | Year 2 | Year 3 |
|---|---|---|---|
| Free users | 10,000 | 50,000 | 200,000 |
| Pro subscribers | 300 | 1,500 | 5,000 |
| Team subscribers | 30 | 150 | 400 |
| Enterprise clients | 5 | 25 | 60 |
| Data API clients | 5 | 25 | 80 |
| White-label contracts | 1 | 5 | 15 |
| Countries covered | 1 | 3 | 6 |
| **Monthly Revenue** | **$42K** | **$292K** | **$1M** |
| **ARR** | **$500K** | **$3.5M** | **$12M** |

---

## 4. Revenue Mix Analysis (Year 3, Moderate Scenario)

| Revenue Stream | ARR | % of Total | Margin |
|---|---|---|---|
| Pro subscriptions | $846K | 19% | 92% |
| Team subscriptions | $342K | 8% | 88% |
| Enterprise contracts | $1,500K | 33% | 85% |
| Data API | $720K | 16% | 95% |
| White-label | $1,080K | 24% | 90% |
| **Total** | **$4,488K** | **100%** | **~90%** |

Enterprise + Data API + White-label = **73% of revenue** and the
highest-margin streams. This validates the B2B/dark-factory strategy.

---

## 5. SaaS Comparables

| Company | Category | Est. ARR | Age | Funding | Multiple |
|---|---|---|---|---|---|
| Trading Economics | Macro data dashboard | $10-20M | 15 yrs | Bootstrapped | N/A |
| Koyfin | Financial data platform | $5-15M | 5 yrs | $3M seed | 10-15x |
| Macrobond | Macro data SaaS | $30-50M | 12 yrs | PE-backed | 8-12x |
| AlphaSense | AI research platform | $200M+ | 10 yrs | $4B valuation | 15-20x |

### Implied Valuation (Year 3)

| Scenario | ARR | Revenue Multiple | Implied Valuation |
|---|---|---|---|
| Conservative | $750K | 10-15x | $7.5M-11M |
| Moderate | $4.5M | 10-15x | $45M-67M |
| Aggressive | $12M | 10-15x | $120M-180M |

---

## 6. Path to Profitability

### Break-Even Analysis

| Scenario | Monthly Burn | Monthly Revenue at Break-Even | Timeline |
|---|---|---|---|
| Bootstrapped | $3-5K | $5K | Month 6-12 |
| Seed-funded | $80-120K | $100K | Month 14-20 |
| Series A | $200-350K | $300K | Month 20-30 |

The bootstrapped path can reach profitability within 12 months due to
the extremely low infrastructure costs ($80-465/month base). This is a
meaningful advantage -- many data SaaS companies require $1M+/year in
data licensing fees before serving a single customer.

### Cash Flow Dynamics

- **Months 0-6**: Negative cash flow ($5-15K/month if bootstrapped)
- **Months 6-12**: Approaching break-even with 50-100 Pro subscribers
- **Months 12-24**: Cash flow positive, reinvest in growth
- **Months 24-36**: Strong positive cash flow, consider raising to
  accelerate or continue bootstrapping

---

## 7. Funding Strategy

### Pre-Seed / Bootstrap (Current State)
- Product is operational, could generate revenue immediately
- $800-3K/month cost structure is manageable
- **Advantage**: Maintain 100% equity, prove revenue first

### Seed ($100-300K ARR milestone)
- Raise $1.5-3M at $10-20M pre-money
- Use for: 2-3 engineers, 1 sales, Mexico expansion, SOC 2
- **Target investors**: Kaszek, Monashees, SoftBank LATAM, Valor Capital,
  QED Investors, Ribbit Capital

### Series A ($2-5M ARR milestone)
- Raise $8-15M at $60-120M valuation
- Use for: full sales team, 5+ countries, enterprise features

### Alternative: Strategic Acquisition
- At $1-3M ARR, attractive target for Bloomberg, LSEG, Trading
  Economics, or B3 (Brazilian exchange)
- Estimated range: 8-15x ARR = $8-45M

---

## 8. Risks to Projections

| Risk | Impact on ARR | Mitigation |
|---|---|---|
| Low free-to-paid conversion (<1%) | -40% Year 1 | A/B test pricing, improve onboarding |
| Enterprise sales cycle >6 months | -30% Year 1-2 | Start with SMB, move upmarket |
| Brazil macro willingness-to-pay low | -25% overall | Reverse freemium (free B2C, charge B2B) |
| Claude API cost increases | -5% margin | Tier 1 DuckDB handles most queries at $0 |
| Government API instability | -10% (churn) | Multi-source fallbacks per feed |
| Competitor enters with VC funding | -20% growth | Speed + switching costs + white-label |

---

## Key Takeaway

The unit economics are compelling: 90%+ gross margins, strong LTV:CAC
ratios across all tiers, and infrastructure costs that are negligible
relative to revenue. The dark-factory Data API and white-label contracts
are the highest-margin revenue streams. The bootstrapped path to
profitability is viable within 12 months, while the funded path can
reach $4.5M-12M ARR by Year 3. The cost structure is a durable
competitive advantage -- $80-465/month base infrastructure vs.
Bloomberg's $24K/user/year creates a pricing moat that incumbents
cannot match without cannibalizing their core business.
