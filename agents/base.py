"""Base class for all AI Agents (calls Claude API)."""

from __future__ import annotations

import abc
import time

import structlog

from api.models import AgentResult

logger = structlog.get_logger()


class BaseAgent(abc.ABC):
    """Abstract base for AI agents. Subclasses implement _execute()."""

    @property
    @abc.abstractmethod
    def agent_name(self) -> str:
        ...

    @abc.abstractmethod
    async def _execute(self) -> AgentResult:
        ...

    async def run(self) -> AgentResult:
        """Run the agent with timing and error handling."""
        start = time.perf_counter()
        try:
            result = await self._execute()
            duration_ms = (time.perf_counter() - start) * 1000
            result.duration_ms = round(duration_ms, 2)
            logger.info(
                "agent_completed",
                agent=self.agent_name,
                success=result.success,
                duration_ms=result.duration_ms,
            )
            return result
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000
            logger.error(
                "agent_failed",
                agent=self.agent_name,
                error=str(exc),
                duration_ms=round(duration_ms, 2),
            )
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=round(duration_ms, 2),
                errors=[str(exc)],
            )

    async def health_check(self) -> bool:
        """Check if the agent's dependencies are available."""
        return True
