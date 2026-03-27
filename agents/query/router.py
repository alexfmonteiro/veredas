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


def _build_domain_map() -> dict[str, list[str]]:
    """Build domain→[series_ids] mapping from DomainConfig."""
    cfg = get_domain_config()
    domain_map: dict[str, list[str]] = {}
    for sid, series in cfg.series.items():
        domain_map.setdefault(series.domain, []).append(sid)
    return domain_map


def _build_domain_keywords() -> dict[str, list[str]]:
    """Build domain→[keywords] for domain-level question classification."""
    cfg = get_domain_config()
    dkw: dict[str, list[str]] = {}
    for _sid, series in cfg.series.items():
        dkw.setdefault(series.domain, []).extend(series.keywords)
    return dkw


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


# Module-level lazy caches
METRIC_KEYWORDS: dict[str, str] = _build_metric_keywords()
DOMAIN_MAP: dict[str, list[str]] = _build_domain_map()
DOMAIN_KEYWORDS: dict[str, list[str]] = _build_domain_keywords()

_metric_terms = "|".join(
    re.escape(k) for k in sorted(METRIC_KEYWORDS, key=len, reverse=True)
)

DIRECT_LOOKUP_PATTERNS: dict[re.Pattern[str], str] = _build_lookup_patterns(
    _metric_terms,
)


def detect_domains(question: str) -> list[str]:
    """Detect which domains a question relates to by scanning domain keywords.

    Returns a list of domain names. Empty list if no domain detected (ambiguous).
    """
    lowered = question.lower()
    found: set[str] = set()
    for domain, keywords in DOMAIN_KEYWORDS.items():
        for kw in keywords:
            if kw in lowered:
                found.add(domain)
                break
    return list(found)


def get_series_for_domains(domains: list[str]) -> list[str]:
    """Return all series IDs belonging to the given domains."""
    series: list[str] = []
    for domain in domains:
        series.extend(DOMAIN_MAP.get(domain, []))
    return series


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
