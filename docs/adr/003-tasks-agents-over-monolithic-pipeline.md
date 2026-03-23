# ADR-003: Tasks and Agents over Monolithic Pipeline

## Status
Accepted

## Context
The data pipeline needs to ingest, transform, validate, and generate AI
insights. A monolithic pipeline script would be simpler to write initially
but harder to test, debug, and extend. We needed a modular architecture
that separates pure data processing from LLM-powered analysis.

## Decision
Separate the pipeline into three distinct layers with strict naming
conventions:
- **Task** — pure Python, no LLM (IngestionTask, TransformationTask, QualityTask)
- **Agent** — calls Claude API (InsightAgent, QueryAgent)
- **PipelineFlow** — orchestrates Tasks and Agents in sequence

Each Task implements `BaseTask` with `run() -> TaskResult` and `health_check()`.
Each Agent implements `BaseAgent` with `run() -> AgentResult` and `health_check()`.

## Consequences
- Clear separation of concerns — Tasks are deterministic and testable
  without API mocking
- Agents are isolated — Claude API costs are contained and mockable in tests
- PipelineFlow can halt on quality failures without affecting unrelated stages
- Naming convention is self-documenting: if it says "Task", it never calls
  an LLM; if it says "Agent", it does
- Maps directly to Prefect's vocabulary for v2 migration
- More files and abstractions than a simple script, but the modularity
  pays for itself in testability and maintainability
