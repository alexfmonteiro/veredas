"""Gold data sync — downloads gold Parquet from R2 to local persistent volume."""

from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog

logger = structlog.get_logger()


async def sync_gold_from_r2(gold_dir: Path) -> tuple[int, float, list[str]]:
    """Download gold/*.parquet from R2 to the local persistent volume.

    Uses atomic os.replace() so DuckDB readers never see partial writes.

    Returns (files_synced, duration_ms, errors).
    """
    errors: list[str] = []
    start = time.perf_counter()

    # Only import R2 backend when actually syncing — Railway has R2 creds,
    # local dev does not.
    try:
        from storage.r2 import R2StorageBackend
        r2 = R2StorageBackend()
    except Exception as exc:
        return 0, 0.0, [f"Cannot initialize R2 backend: {exc}"]

    gold_dir.mkdir(parents=True, exist_ok=True)

    # List gold files on R2
    try:
        keys = await r2.list_keys("gold/")
    except Exception as exc:
        return 0, 0.0, [f"Cannot list R2 gold keys: {exc}"]

    parquet_keys = [k for k in keys if k.endswith(".parquet")]
    if not parquet_keys:
        return 0, 0.0, ["No gold parquet files found on R2"]

    files_synced = 0
    for key in parquet_keys:
        filename = key.split("/")[-1]
        target = gold_dir / filename

        try:
            data = await r2.read(key)

            # Write to a temp file in the same directory, then atomic swap
            fd, tmp_path = tempfile.mkstemp(
                dir=str(gold_dir), suffix=".tmp", prefix=f".sync_{filename}_"
            )
            try:
                os.write(fd, data)
                os.close(fd)
                os.replace(tmp_path, str(target))
                files_synced += 1
                logger.debug("sync_file_written", key=key, size=len(data))
            except Exception:
                os.close(fd) if not os.get_inheritable(fd) else None  # noqa: B018
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        except Exception as exc:
            errors.append(f"Failed to sync {key}: {exc}")
            logger.warning("sync_file_failed", key=key, error=str(exc))

    duration_ms = round((time.perf_counter() - start) * 1000, 2)

    # Write metadata.json
    metadata = {
        "last_sync_at": datetime.now(timezone.utc).isoformat(),
        "run_id": f"sync-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "files_synced": files_synced,
        "sync_duration_ms": duration_ms,
        "source": "r2",
    }
    (gold_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

    logger.info(
        "sync_complete",
        files_synced=files_synced,
        duration_ms=duration_ms,
        errors=len(errors),
    )

    return files_synced, duration_ms, errors
