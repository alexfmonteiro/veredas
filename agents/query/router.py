"""Tier-1 intent router — regex-based metric detection with safe fallback."""

from __future__ import annotations

import re

import structlog

from api.models import QueryTier

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Regex patterns that identify simple "give me the latest value" questions.
# Each pattern maps to a lookup strategy name (currently only "latest_value").
# ---------------------------------------------------------------------------

DIRECT_LOOKUP_PATTERNS: dict[re.Pattern[str], str] = {
    re.compile(
        r"(what|qual|quanto).*(current|atual|hoje|today|now).*(selic|ipca|"
        r"dolar|dollar|usd|brl|cambio|câmbio|desemprego|unemployment|pib|gdp)",
        re.IGNORECASE,
    ): "latest_value",
    re.compile(
        r"(last|ultimo|última).*(value|valor|dado).*(selic|ipca|dolar|"
        r"dollar|usd|brl|cambio|câmbio)",
        re.IGNORECASE,
    ): "latest_value",
}

# ---------------------------------------------------------------------------
# Maps natural-language keywords to canonical series IDs in the gold layer.
# ---------------------------------------------------------------------------

METRIC_KEYWORDS: dict[str, str] = {
    "selic": "bcb_432",
    "ipca": "bcb_433",
    "dolar": "bcb_1",
    "dollar": "bcb_1",
    "usd": "bcb_1",
    "cambio": "bcb_1",
    "câmbio": "bcb_1",
    "desemprego": "ibge_pnad",
    "unemployment": "ibge_pnad",
    "pib": "ibge_gdp",
    "gdp": "ibge_gdp",
}


class QuerySkillRouter:
    """Classify user questions into QueryTier.DIRECT_LOOKUP or FULL_LLM.

    Fail-safe: any exception during routing falls back to FULL_LLM so the
    user always receives a response.
    """

    def route(self, question: str) -> tuple[QueryTier, str]:
        """Route a question to the cheapest capable tier.

        Returns:
            (tier, metric_series_id) — metric is empty string for FULL_LLM.
        """
        try:
            for pattern, _strategy in DIRECT_LOOKUP_PATTERNS.items():
                if pattern.search(question):
                    metric = self._extract_metric(question)
                    if metric != "unknown":
                        logger.info(
                            "query_routed",
                            tier="direct_lookup",
                            metric=metric,
                        )
                        return QueryTier.DIRECT_LOOKUP, metric

            # No regex matched → FULL_LLM
            logger.info("query_routed", tier="full_llm")
            return QueryTier.FULL_LLM, ""

        except Exception as exc:
            logger.warning("router_fallback", error=str(exc))
            return QueryTier.FULL_LLM, ""

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_metric(question: str) -> str:
        """Return the canonical series ID for the first keyword found."""
        lowered = question.lower()
        for keyword, series_id in METRIC_KEYWORDS.items():
            if keyword in lowered:
                return series_id
        return "unknown"
