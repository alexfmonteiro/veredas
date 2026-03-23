"""Base class for all pipeline Tasks (pure Python, no LLM)."""

from __future__ import annotations

import abc
import time

import structlog

from api.models import TaskResult

logger = structlog.get_logger()


class BaseTask(abc.ABC):
    """Abstract base for pipeline tasks. Subclasses implement _execute()."""

    @property
    @abc.abstractmethod
    def task_name(self) -> str:
        ...

    @abc.abstractmethod
    async def _execute(self) -> TaskResult:
        ...

    async def run(self) -> TaskResult:
        """Run the task with timing and error handling."""
        start = time.perf_counter()
        try:
            result = await self._execute()
            duration_ms = (time.perf_counter() - start) * 1000
            result.duration_ms = round(duration_ms, 2)
            logger.info(
                "task_completed",
                task=self.task_name,
                success=result.success,
                duration_ms=result.duration_ms,
                rows_processed=result.rows_processed,
            )
            return result
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000
            logger.error(
                "task_failed",
                task=self.task_name,
                error=str(exc),
                duration_ms=round(duration_ms, 2),
            )
            return TaskResult(
                success=False,
                task_name=self.task_name,
                duration_ms=round(duration_ms, 2),
                errors=[str(exc)],
            )

    async def health_check(self) -> bool:
        """Check if the task's dependencies are available."""
        return True
