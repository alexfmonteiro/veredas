"""Feed config loader — reads YAML data contracts from data/feeds/."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from pathlib import Path

import structlog
import yaml

from api.models import FeedConfig, FeedStatus

logger = structlog.get_logger()


def normalize_column_name(name: str) -> str:
    """Normalize a column name to lowercase with underscores only.

    Removes accents, replaces spaces/special chars with underscores,
    collapses consecutive underscores, strips leading/trailing underscores.
    """
    # Decompose unicode and strip accents
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    # Lowercase
    lower = ascii_str.lower()
    # Replace non-alphanumeric with underscore
    cleaned = re.sub(r"[^a-z0-9]", "_", lower)
    # Collapse consecutive underscores
    collapsed = re.sub(r"_+", "_", cleaned)
    # Strip leading/trailing underscores
    return collapsed.strip("_")


def _validate_normalized_names(feed: FeedConfig) -> None:
    """Warn if any schema field names are not normalized."""
    for field in feed.schema_fields:
        normalized = normalize_column_name(field.name)
        if field.name != normalized:
            logger.warning(
                "field_name_not_normalized",
                feed_id=feed.feed_id,
                field_name=field.name,
                suggested=normalized,
            )


def compute_schema_hash(feed: FeedConfig) -> str:
    """Compute SHA-256 hash of sorted schema field names."""
    field_names = sorted(f.name for f in feed.schema_fields)
    payload = ",".join(field_names).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def load_feed_configs(
    feeds_dir: str | Path = "data/feeds",
    *,
    include_inactive: bool = False,
) -> dict[str, FeedConfig]:
    """Load all YAML feed configs from the given directory.

    Returns a dict keyed by feed_id. Only active feeds by default.
    """
    feeds_path = Path(feeds_dir)
    if not feeds_path.is_dir():
        logger.warning("feed_config_dir_not_found", path=str(feeds_path))
        return {}

    configs: dict[str, FeedConfig] = {}

    for yaml_file in sorted(feeds_path.glob("*.yaml")):
        try:
            raw = yaml.safe_load(yaml_file.read_text())
            if raw is None:
                continue
            feed = FeedConfig.model_validate(raw, strict=False)

            if not include_inactive and feed.status != FeedStatus.ACTIVE:
                logger.info("feed_config_skipped", feed_id=feed.feed_id, status=feed.status.value)
                continue

            _validate_normalized_names(feed)
            configs[feed.feed_id] = feed
            logger.info(
                "feed_config_loaded",
                feed_id=feed.feed_id,
                schema_hash=compute_schema_hash(feed),
                fields=len(feed.schema_fields),
            )
        except Exception as exc:
            logger.error("feed_config_load_error", file=str(yaml_file), error=str(exc))

    logger.info("feed_configs_loaded", total=len(configs))
    return configs
