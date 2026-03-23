# ADR-005: GitHub Actions over Prefect for v1 Orchestration

## Status
Accepted

## Context
The daily pipeline needs a scheduler to run ingestion, transformation,
quality checks, and AI insight generation at 06:00 UTC. Options: Prefect
Cloud (purpose-built orchestrator), GitHub Actions cron (CI/CD platform
with scheduling), or a custom cron job on the API server.

## Decision
Use GitHub Actions with cron scheduling for v1. The pipeline runs as
`python -m pipeline.flow` in a GitHub Actions workflow with
`workflow_dispatch` for manual re-triggers. Migrate to Prefect Cloud in v2
when orchestration complexity justifies it.

## Consequences
- Zero additional infrastructure — GitHub Actions is free for public repos
- Pipeline runs are visible in the GitHub UI with full logs
- `workflow_dispatch` enables manual re-triggers without SSH access
- Pipeline code uses Prefect-compatible naming (Task, Flow) to ease
  future migration
- No retry orchestration beyond what we build into PipelineFlow itself
- No DAG visualization or run history dashboard (Prefect provides these)
- Acceptable for v1 with a single daily pipeline; revisit when adding
  multiple schedules or complex dependencies
