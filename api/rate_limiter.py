"""IP-based rate limiting backed by Upstash Redis."""

from __future__ import annotations

import os

import httpx
import structlog

logger = structlog.get_logger()


async def check_rate_limit(ip: str, limit: int = 20) -> bool:
    """Check if IP is within daily rate limit. Returns True if allowed."""
    redis_url = os.environ.get("UPSTASH_REDIS_URL", "")
    redis_token = os.environ.get("UPSTASH_REDIS_TOKEN", "")
    if not redis_url or not redis_token:
        return True  # fail-open

    key = f"ratelimit:query:{ip}"
    try:
        async with httpx.AsyncClient() as client:
            # INCR the key
            resp = await client.post(
                f"{redis_url}/incr/{key}",
                headers={"Authorization": f"Bearer {redis_token}"},
                timeout=5.0,
            )
            count = resp.json().get("result", 0)

            if count == 1:
                # Set expiry to 24 hours on first request
                await client.post(
                    f"{redis_url}/expire/{key}/86400",
                    headers={"Authorization": f"Bearer {redis_token}"},
                    timeout=5.0,
                )

            return count <= limit
    except Exception as exc:
        logger.warning("rate_limit_check_failed", error=str(exc))
        return True  # fail-open
