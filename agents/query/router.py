"""Tier-1 intent router — regex-based metric detection with safe fallback."""

from __future__ import annotations

import re

import structlog

from api.models import QueryTier
from config import get_domain_config

logger = structlog.get_logger()


def _build_metric_keywords() -> dict[str, str]:
    """Build keyword→series_id mapping from DomainConfig."""
    cfg = get_domain_config()
    keywords: dict[str, str] = {}
    for sid, series in cfg.series.items():
        for kw in series.keywords:
            keywords[kw] = sid
    return keywords


def _build_lookup_patterns(
    metric_terms: str,
) -> dict[re.Pattern[str], str]:
    """Compile direct lookup patterns from config, injecting keyword terms."""
    cfg = get_domain_config()
    patterns: dict[re.Pattern[str], str] = {}
    for pat_cfg in cfg.router.direct_lookup_patterns:
        raw = pat_cfg.pattern.replace("{keywords}", metric_terms)
        patterns[re.compile(raw, re.IGNORECASE)] = pat_cfg.handler
    return patterns


# Module-level lazy cache (populated on first access via QuerySkillRouter or import)
METRIC_KEYWORDS: dict[str, str] = _build_metric_keywords()

_metric_terms = "|".join(
    re.escape(k) for k in sorted(METRIC_KEYWORDS, key=len, reverse=True)
)

DIRECT_LOOKUP_PATTERNS: dict[re.Pattern[str], str] = _build_lookup_patterns(
    _metric_terms,
)


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
        # Check longest keywords first so "prefixado longo" matches before "prefixado"
        for keyword, series_id in sorted(METRIC_KEYWORDS.items(), key=lambda kv: len(kv[0]), reverse=True):
            if keyword in lowered:
                return series_id
        return "unknown"
