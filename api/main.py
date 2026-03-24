"""FastAPI application — BR Economic Pulse API."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import asyncpg
import structlog
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from agents.query.agent import QueryAgent
from api.dependencies import (
    get_gold_dir,
    get_sync_health,
    query_gold_series,
    read_sync_metadata,
    verify_sync_token,
)
from api.models import (
    HealthResponse,
    InsightRecord,
    InsightResponse,
    MetricDataPoint,
    MetricsResponse,
    QueryRequest,
    QueryResponse,
    QueryTier,
    SyncResult,
    SyncStatusResponse,
)
from api.rate_limiter import check_rate_limit

logger = structlog.get_logger()

# In-memory conversation history per IP (max 10 turns per IP).
_conversation_history: dict[str, list[dict[str, str]]] = {}

_MAX_HISTORY_TURNS = 10


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Load environment variables on startup."""
    load_dotenv(".env.local")
    load_dotenv()
    logger.info("app_started", env=os.environ.get("APP_ENV", "unknown"))
    yield


app = FastAPI(title="BR Economic Pulse API", version="0.5.0", lifespan=lifespan)

# --- Middleware ---


def _get_allowed_origins() -> list[str]:
    return os.environ.get("ALLOWED_ORIGINS", "http://localhost:5173").split(",")


app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_allowed_origins(),
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    allow_credentials=False,
)


@app.middleware("http")
async def security_headers(request: Request, call_next: Any) -> Response:
    """Add security headers to all responses."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    return response


# --- Helpers ---


def _get_client_ip(request: Request) -> str:
    """Extract client IP from request, respecting X-Forwarded-For."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _get_history(ip: str) -> list[dict[str, str]]:
    """Return conversation history for an IP, capped at max turns."""
    return _conversation_history.get(ip, [])


def _append_history(ip: str, role: str, content: str) -> None:
    """Append a turn to conversation history, enforcing max turns."""
    if ip not in _conversation_history:
        _conversation_history[ip] = []
    _conversation_history[ip].append({"role": role, "content": content})
    # Keep only the last N turns
    if len(_conversation_history[ip]) > _MAX_HISTORY_TURNS:
        _conversation_history[ip] = _conversation_history[ip][-_MAX_HISTORY_TURNS:]


# --- Routes ---


@app.get("/api/health")
async def health() -> HealthResponse:
    sync_info = read_sync_metadata()
    sync_health = get_sync_health(sync_info)

    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        sync=sync_info,
        data_freshness={"sync": sync_health.value},
    )


@app.get("/api/metrics/{series}")
async def get_metrics(series: str, after: str | None = None) -> MetricsResponse:
    rows = await query_gold_series(series, after=after)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Series '{series}' not found")

    data_points = []
    for row in rows:
        d = row["date"]
        # DuckDB may return date or datetime depending on the SQL function used
        if not isinstance(d, datetime):
            d = datetime(d.year, d.month, d.day)
        data_points.append(MetricDataPoint(
            date=d,
            value=float(row["value"]),
            series=row["series"],
        ))

    return MetricsResponse(
        series=series,
        data_points=data_points,
        last_updated=data_points[-1].date if data_points else None,
    )


@app.get("/api/quality/latest")
async def quality_latest() -> dict[str, Any]:
    """Return the latest quality report if available."""
    sync_info = read_sync_metadata()
    sync_health = get_sync_health(sync_info)

    return {
        "status": "ok",
        "sync_health": sync_health.value,
        "last_sync": sync_info.model_dump() if sync_info else None,
    }


@app.get("/api/quality/sync-status")
async def sync_status() -> SyncStatusResponse:
    sync_info = read_sync_metadata()
    sync_health = get_sync_health(sync_info)

    seconds_since = None
    if sync_info and sync_info.last_sync_at:
        last = sync_info.last_sync_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        seconds_since = (datetime.now(timezone.utc) - last).total_seconds()

    return SyncStatusResponse(
        last_sync_at=sync_info.last_sync_at if sync_info else None,
        run_id=sync_info.run_id if sync_info else None,
        files_synced=sync_info.files_synced if sync_info else 0,
        sync_duration_ms=sync_info.sync_duration_ms if sync_info else 0.0,
        source=sync_info.source if sync_info else "",
        seconds_since_sync=seconds_since,
        sync_health=sync_health,
    )


@app.post("/api/query")
async def post_query(body: QueryRequest, request: Request) -> QueryResponse:
    """Answer a user question about Brazilian economic data."""
    correlation_id = uuid4().hex[:12]
    ip = _get_client_ip(request)
    log = logger.bind(correlation_id=correlation_id, ip=ip)

    # Rate limiting
    if not await check_rate_limit(ip):
        log.warning("rate_limit_exceeded")
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded (20 requests/day). Try again tomorrow.",
        )

    # Build agent with conversation history
    history = _get_history(ip)
    agent = QueryAgent(question=body.question, history=history)

    # Execute with 30s timeout
    try:
        result = await asyncio.wait_for(agent.run(), timeout=30.0)
    except asyncio.TimeoutError:
        log.error("query_timeout")
        raise HTTPException(
            status_code=504,
            detail=f"Query timed out after 30s. correlation_id={correlation_id}",
        )
    except Exception as exc:
        log.error("query_failed", error=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Internal error. correlation_id={correlation_id}",
        )

    if not result.success or agent.query_response is None:
        log.warning("query_agent_failure", errors=result.errors)
        raise HTTPException(
            status_code=500,
            detail=f"Query failed. correlation_id={correlation_id}",
        )

    response = agent.query_response

    # Update conversation history
    _append_history(ip, "user", body.question)
    _append_history(ip, "assistant", response.answer)

    log.info(
        "query_completed",
        tier=response.tier_used.value,
        tokens=response.llm_tokens_used,
    )

    return response


@app.post("/api/query/stream")
async def query_stream(
    body: QueryRequest, request: Request
) -> StreamingResponse:
    """Answer a user question via Server-Sent Events."""
    correlation_id = uuid4().hex[:12]
    ip = _get_client_ip(request)
    log = logger.bind(correlation_id=correlation_id, ip=ip)

    # Rate limiting
    if not await check_rate_limit(ip):
        log.warning("rate_limit_exceeded")
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded (20 requests/day). Try again tomorrow.",
        )

    history = _get_history(ip)

    async def _event_generator() -> AsyncGenerator[str, None]:
        agent = QueryAgent(question=body.question, history=history)

        try:
            result = await asyncio.wait_for(agent.run(), timeout=30.0)
        except asyncio.TimeoutError:
            error_payload = json.dumps(
                {"error": f"Query timed out. correlation_id={correlation_id}"}
            )
            yield f"data: {error_payload}\n\n"
            return
        except Exception as exc:
            log.error("stream_query_failed", error=str(exc))
            error_payload = json.dumps(
                {"error": f"Internal error. correlation_id={correlation_id}"}
            )
            yield f"data: {error_payload}\n\n"
            return

        if not result.success or agent.query_response is None:
            log.warning("stream_query_agent_failure", errors=result.errors)
            error_payload = json.dumps(
                {"error": f"Query failed. correlation_id={correlation_id}"}
            )
            yield f"data: {error_payload}\n\n"
            return

        qr = agent.query_response

        # For Tier 1 (direct lookup), send full answer as single chunk
        # For Tier 3 (LLM), send answer in chunks to simulate streaming
        if qr.tier_used == QueryTier.DIRECT_LOOKUP:
            chunk_payload = json.dumps({"chunk": qr.answer})
            yield f"data: {chunk_payload}\n\n"
        else:
            # Split LLM answer into word-group chunks
            words = qr.answer.split(" ")
            chunk_size = 8
            for i in range(0, len(words), chunk_size):
                chunk_text = " ".join(words[i : i + chunk_size])
                if i > 0:
                    chunk_text = " " + chunk_text
                chunk_payload = json.dumps({"chunk": chunk_text})
                yield f"data: {chunk_payload}\n\n"

        # Final event with metadata
        done_payload = json.dumps(
            {
                "done": True,
                "tier_used": qr.tier_used.value,
                "llm_tokens_used": qr.llm_tokens_used,
            }
        )
        yield f"data: {done_payload}\n\n"

        # Update conversation history
        _append_history(ip, "user", body.question)
        _append_history(ip, "assistant", qr.answer)

        log.info(
            "stream_query_completed",
            tier=qr.tier_used.value,
            tokens=qr.llm_tokens_used,
        )

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
    )


@app.get("/api/insights/latest")
async def insights_latest() -> InsightResponse:
    """Return the most recent InsightAgent output from Postgres."""
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        logger.warning("insights_no_database_url")
        return InsightResponse()

    try:
        conn: asyncpg.Connection[Any] = await asyncpg.connect(database_url)
    except Exception as exc:
        logger.warning("insights_db_connect_failed", error=str(exc))
        return InsightResponse()

    try:
        # Find the latest run_id
        row = await conn.fetchrow(
            "SELECT run_id FROM insights ORDER BY generated_at DESC LIMIT 1"
        )
        if row is None:
            return InsightResponse()

        latest_run_id: str = row["run_id"]

        # Fetch all records for that run
        rows = await conn.fetch(
            "SELECT content, language, metric_refs, model_version, "
            "run_id, generated_at, confidence_flag "
            "FROM insights WHERE run_id = $1 ORDER BY generated_at",
            latest_run_id,
        )

        insights = [
            InsightRecord(
                content=r["content"],
                language=r["language"],
                metric_refs=list(r["metric_refs"]) if r["metric_refs"] else [],
                model_version=r["model_version"],
                run_id=r["run_id"],
                generated_at=r["generated_at"],
                confidence_flag=r["confidence_flag"],
            )
            for r in rows
        ]

        return InsightResponse(insights=insights, latest_run_id=latest_run_id)
    except Exception as exc:
        logger.warning("insights_query_failed", error=str(exc))
        return InsightResponse()
    finally:
        await conn.close()


@app.post(
    "/api/internal/sync",
    include_in_schema=False,
)
async def trigger_sync(
    _token: str = Depends(verify_sync_token),
) -> SyncResult:
    """Trigger gold data sync from R2. Protected by Bearer token. Not in OpenAPI docs."""
    logger.info("sync_triggered")

    gold_dir = get_gold_dir()

    # If using local storage (dev mode), just count existing files
    storage_backend = os.environ.get("STORAGE_BACKEND", "local")
    if storage_backend == "local":
        gold_dir.mkdir(parents=True, exist_ok=True)
        parquet_files = list(gold_dir.glob("*.parquet"))
        return SyncResult(
            success=True,
            files_synced=len(parquet_files),
            sync_duration_ms=0.0,
        )

    # Production: download gold files from R2 to persistent volume
    from api.sync import sync_gold_from_r2

    files_synced, duration_ms, errors = await sync_gold_from_r2(gold_dir)

    return SyncResult(
        success=len(errors) == 0,
        files_synced=files_synced,
        sync_duration_ms=duration_ms,
        errors=errors,
    )
