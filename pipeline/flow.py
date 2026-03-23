"""PipelineFlow — orchestrates Tasks and Agents in sequence."""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import datetime, timezone

import structlog

from agents.base import BaseAgent
from api.models import PipelineRunResult, TaskResult, AgentResult
from tasks.base import BaseTask

logger = structlog.get_logger()


class PipelineFlow:
    """Runs Tasks and Agents in sequence. Halts on failure."""

    async def run(self, stages: Sequence[BaseTask | BaseAgent]) -> PipelineRunResult:
        run_id = f"pipeline-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        start = time.perf_counter()
        completed: list[str] = []
        failed: list[str] = []
        results: list[TaskResult | AgentResult] = []

        logger.info("pipeline_started", run_id=run_id, total_stages=len(stages))

        for stage in stages:
            name = stage.task_name if isinstance(stage, BaseTask) else stage.agent_name
            try:
                result = await stage.run()
                results.append(result)

                if result.success:
                    completed.append(name)
                else:
                    failed.append(name)
                    logger.error(
                        "pipeline_stage_failed",
                        run_id=run_id,
                        stage=name,
                        errors=result.errors,
                    )
                    break  # Halt pipeline
            except Exception as exc:
                logger.error(
                    "pipeline_stage_exception",
                    run_id=run_id,
                    stage=name,
                    error=str(exc),
                )
                failed.append(name)
                break

        total_ms = round((time.perf_counter() - start) * 1000, 2)
        success = len(failed) == 0

        logger.info(
            "pipeline_completed",
            run_id=run_id,
            success=success,
            completed=completed,
            failed=failed,
            total_duration_ms=total_ms,
        )

        return PipelineRunResult(
            run_id=run_id,
            success=success,
            stages_completed=completed,
            stages_failed=failed,
            total_duration_ms=total_ms,
            results=results,
        )


if __name__ == "__main__":
    # Support `python -m pipeline.flow` by delegating to __main__
    from pipeline.__main__ import main as _main

    import asyncio
    import sys

    sys.exit(asyncio.run(_main()))
