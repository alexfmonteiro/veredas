"""Series display configuration — maps internal IDs to user-friendly names."""

from __future__ import annotations

# Display metadata for each tracked series.
# Internal IDs (bcb_selic, etc.) should never appear in the UI.
# freshness_hours: how many hours without a new data point before the series
# is considered stale.  Stale = 1× threshold, Critical = 2× threshold.
# Values reflect each source's publication cadence (daily, monthly, quarterly).
SERIES_DISPLAY: dict[str, dict[str, str]] = {
    "bcb_selic": {
        "label": "SELIC", "unit": "% a.a.", "source": "BCB", "color": "#3b82f6", "freshness_hours": "72",
        "description": "Benchmark interest rate (taxa SELIC meta) set by Brazil's central bank (COPOM). Higher = tighter monetary policy.",
    },
    "bcb_ipca": {
        "label": "IPCA", "unit": "% a.m.", "source": "BCB", "color": "#8b5cf6", "freshness_hours": "1080",
        "description": "Official month-over-month consumer inflation index (IPCA). Above ~0.5%/month signals strong inflationary pressure.",
    },
    "bcb_usd_brl": {
        "label": "USD/BRL", "unit": "R$", "source": "BCB", "color": "#22c55e", "freshness_hours": "72",
        "description": "Exchange rate: how many reais one US dollar buys. Rising = real weakening.",
    },
    "ibge_pnad": {
        "label": "Taxa de Desemprego", "unit": "%", "source": "IBGE", "color": "#f59e0b", "freshness_hours": "2400",
        "description": "Unemployment rate from IBGE PNAD household survey. Percentage of working-age population seeking but not finding work.",
    },
    "ibge_gdp": {
        "label": "PIB", "unit": "R$ bi", "source": "IBGE", "color": "#06b6d4", "freshness_hours": "1080",
        "description": "GDP monthly proxy (BCB series 4380) in R$ billions, current prices. Broadest measure of economic output.",
    },
    "tesouro_prefixado_curto": {
        "label": "Prefixado Curto", "unit": "% a.a.", "source": "Tesouro", "color": "#ec4899", "freshness_hours": "72",
        "description": "Average yield of pre-fixed government bonds (Tesouro Prefixado) maturing within 3 years. Reflects near-term market interest rate expectations.",
    },
    "tesouro_prefixado_longo": {
        "label": "Prefixado Longo", "unit": "% a.a.", "source": "Tesouro", "color": "#f472b6", "freshness_hours": "72",
        "description": "Average yield of pre-fixed government bonds (Tesouro Prefixado) maturing beyond 3 years. Reflects long-term rate expectations. The spread vs short-term indicates yield curve slope.",
    },
    "tesouro_ipca": {
        "label": "Juros Real (IPCA+)", "unit": "% a.a.", "source": "Tesouro", "color": "#fb923c", "freshness_hours": "72",
        "description": "Average real yield of inflation-linked bonds (IPCA+, Educa+, Renda+). The real interest rate the market demands above IPCA inflation.",
    },
}


def get_display_label(series_id: str) -> str:
    """Return human-readable label for a series ID."""
    meta = SERIES_DISPLAY.get(series_id)
    return meta["label"] if meta else series_id


def get_all_series_ids() -> list[str]:
    """Return all tracked series IDs."""
    return list(SERIES_DISPLAY.keys())
