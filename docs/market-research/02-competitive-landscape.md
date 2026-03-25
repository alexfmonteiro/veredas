# Deep Dive 2: Competitive Landscape

**BR Economic Pulse -- Market Research Series**
**Date: March 2026**

---

## Executive Summary

BR Economic Pulse occupies a unique position: an AI-native, LATAM-focused
macroeconomic intelligence platform with a cost structure 100x cheaper than
incumbents for macro-only use cases. The opportunity lies in the unserved
middle: users who need more than raw CSV downloads but cannot justify
Bloomberg pricing.

---

## 1. Direct Competitors

### Bloomberg Terminal
- **Price**: $24,000/user/year
- **Strengths**: Gold standard, comprehensive, real-time, all asset classes
- **Weakness vs. us**: Overkill for macro-only. No AI query. 100x more expensive.
- **Threat**: Medium. They'll add AI but can't price down to $49/mo.

### Refinitiv/LSEG Workspace
- **Price**: $12-22K/user/year
- **Strengths**: Deep fixed income/FX. Enterprise integration.
- **Weakness**: Enterprise-only, rigid, legacy UI, no AI.
- **Threat**: Low-Medium.

### Trading Economics
- **Price**: $490-990/year
- **Strengths**: 196 countries, 300K+ indicators, clean charts, good API.
- **Weakness**: No AI insights, no anomaly detection, no NL queries, no white-label, limited Brazil depth, bootstrapped/slow innovation.
- **Threat**: **High -- most direct competitor.** But 15-year-old architecture with no AI layer.

### CEIC Data
- **Price**: $5-20K/year
- **Strengths**: Excellent emerging market macro data, deep LATAM.
- **Weakness**: No AI, legacy UI, enterprise-only.
- **Threat**: Medium.

### Economatica
- **Price**: $3-8K/year
- **Strengths**: Strong Brazil/LATAM equity + some macro.
- **Weakness**: Desktop-bound, aging tech, equity-focused.
- **Threat**: Low.

### Valor Data / Broadcast
- **Price**: $2-15K/year
- **Strengths**: Real-time Brazil market data, news.
- **Weakness**: Portuguese-only, no AI, news-focused.
- **Threat**: Low.

---

## 2. Indirect Competitors

### Government Portals (BCB SGS, IBGE SIDRA, Tesouro Transparente)
The raw data sources we already ingest. Free but terrible UX, no
cross-source correlation, no derived metrics, no AI, no production API.

### "Spreadsheet Analysts" (The Real Incumbent)
~200K+ Brazilian professionals manually downloading CSV from BCB, pasting
into Excel. This is the workflow we directly displace.

### ChatGPT / Generic AI
Can answer macro questions but with stale data, hallucination risk, no
real-time access, no structured output. Low threat for professional use.

---

## 3. Structural Differentiation

### 3.1 Two-Tier Query System

| Query Type | Tier | Engine | Cost |
|---|---|---|---|
| "Current SELIC rate?" | Tier 1 | DuckDB regex -> direct lookup | **$0** |
| "IPCA vs USD/BRL correlation?" | Tier 2 | Claude Haiku | **~$0.001** |

The `QuerySkillRouter` (`agents/query/router.py`) classifies via regex.
Simple lookups go to DuckDB free. Complex queries go to Claude. Bloomberg
does not offer this at any price.

### 3.2 Cost Advantage (40-400x)

| Platform | Annual Cost | AI Queries? |
|---|---|---|
| Bloomberg | $24,000/user | No |
| Refinitiv | $12-22K/user | No |
| CEIC | $5-20K | No |
| Trading Economics | $490-990 | No |
| **BR Economic Pulse Pro** | **$588 ($49/mo)** | **Unlimited** |
| **BR Economic Pulse Free** | **$0** | **10/day** |

### 3.3 Automated Intelligence Layer

| Feature | Bloomberg | Trading Econ | BR Economic Pulse |
|---|---|---|---|
| Raw data | Yes | Yes | Yes |
| Derived metrics (MoM, YoY) | Manual | No | **Automated** |
| Anomaly detection | No | No | **Z-score, automated** |
| AI insights | No | No | **Bilingual PT/EN** |
| NL queries | No | No | **Two-tier** |

### 3.4 YAML-Driven Pipeline
Adding a series = adding a YAML file. Adding a country = a collection of
YAML files. Competitors have monolithic proprietary pipelines.

---

## 4. Moat Analysis

| Moat Type | Now | 12 Months | 36 Months |
|---|---|---|---|
| **Data** | Weak (public sources) | Medium (derived metrics, composites) | Strong (6 countries + alt data) |
| **Intelligence** | Medium (AI insights, anomaly) | Medium-Strong (improving models) | Strong (fine-tuned on econ data) |
| **Network** | None | Emerging (community) | Medium (B2B2C distribution) |
| **Switching costs** | Low | Medium (API integrations) | High (white-label contracts) |
| **Cost structure** | **Strong (100x cheaper)** | **Durable** | **Durable** |
| **Brand** | None | Emerging | Medium |

**Honest assessment**: Moat is thin today. Defensibility comes from execution
speed, structural cost advantage, and switching costs from integrations. This
mirrors early Plaid, Algolia, and Segment -- thin moats built through adoption.

---

## 5. Competitive Response Scenarios

| Scenario | Probability | Impact | Mitigation |
|---|---|---|---|
| Bloomberg adds AI | High | Low-Medium | They can't price at $49/mo |
| Trading Economics adds AI + LATAM depth | Medium | High | Move faster, build switching costs |
| Brazilian fintech builds in-house | Low | Low | Make build-vs-buy obvious |
| New AI-native competitor enters | Medium | Medium-High | First-mover + white-label contracts |

---

## 6. Positioning Statement

> **For** LATAM financial professionals, fintechs, and corporate teams
> **who need** macro data intelligence without Bloomberg pricing,
> **BR Economic Pulse is** an AI-native data platform
> **that** provides real-time macro data, NL queries, automated insights,
> and anomaly detection --
> **unlike** Bloomberg (100x cheaper), Trading Economics (AI-native), or
> government portals (actually usable).

---

## Key Takeaway

Incumbents are expensive and bloated. Government portals are free but
unusable. No one offers AI-native macro intelligence for LATAM. The risk
is not existing competitors but building moats before new entrants arrive.
Strategy: move fast, sign white-label contracts, build switching costs.
