"""Tests for FastAPI app."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from httpx import ASGITransport, AsyncClient

from api.main import app


@pytest.fixture(autouse=True)
def env_vars(tmp_path: Path) -> None:  # type: ignore[misc]
    """Set required env vars for tests."""
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()

    # Write a sample gold parquet
    dates = [datetime(2026, 1, 1), datetime(2026, 2, 1), datetime(2026, 3, 1)]
    table = pa.table({
        "date": dates,
        "value": [14.75, 14.75, 14.75],
        "series": ["bcb_432", "bcb_432", "bcb_432"],
        "mom_delta": [None, 0.0, 0.0],
        "yoy_delta": [None, None, None],
        "rolling_12m_avg": [14.75, 14.75, 14.75],
        "z_score": [None, None, None],
    })
    pq.write_table(table, gold_dir / "bcb_432.parquet")

    # Write metadata
    metadata = {
        "last_sync_at": "2026-03-22T06:05:12Z",
        "run_id": "pipeline-20260322-060000",
        "files_synced": 1,
        "sync_duration_ms": 100.0,
        "source": "local",
    }
    (gold_dir / "metadata.json").write_text(json.dumps(metadata))

    with patch.dict(os.environ, {
        "GOLD_DATA_DIR": str(gold_dir),
        "SYNC_WEBHOOK_SECRET": "test-secret-token",
        "ALLOWED_ORIGINS": "http://localhost:5173",
        "STORAGE_BACKEND": "local",
        "LOCAL_DATA_DIR": str(tmp_path),
    }):
        yield


@pytest.mark.asyncio
async def test_health_endpoint() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data


@pytest.mark.asyncio
async def test_metrics_endpoint() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/metrics/bcb_432")
    assert resp.status_code == 200
    data = resp.json()
    assert data["series"] == "bcb_432"
    assert len(data["data_points"]) > 0


@pytest.mark.asyncio
async def test_metrics_unknown_series() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/metrics/unknown_series")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_quality_latest() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/latest")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_sync_status() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/sync-status")
    assert resp.status_code == 200
    data = resp.json()
    assert "sync_health" in data
    assert "last_sync_at" in data


@pytest.mark.asyncio
async def test_sync_endpoint_requires_auth() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/internal/sync")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_sync_endpoint_rejects_bad_token() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/internal/sync",
            headers={"Authorization": "Bearer wrong-token"},
        )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_sync_endpoint_accepts_valid_token() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/internal/sync",
            headers={"Authorization": "Bearer test-secret-token"},
        )
    # May succeed or fail depending on storage state, but should not be 401
    assert resp.status_code != 401


@pytest.mark.asyncio
async def test_security_headers() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    assert resp.headers.get("X-Frame-Options") == "DENY"


@pytest.mark.asyncio
async def test_sync_not_in_openapi() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/openapi.json")
    schema = resp.json()
    paths = schema.get("paths", {})
    assert "/api/internal/sync" not in paths
