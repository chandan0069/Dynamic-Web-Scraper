from __future__ import annotations

from datetime import datetime, timezone

from src.config import get_logger
from src.registry.repository import SourceRepository

log = get_logger(__name__)


async def record_success(
    repo: SourceRepository,
    source_id: str,
    latency_ms: float,
    events_count: int,
) -> None:
    await repo.update_health(
        source_id,
        {
            "last_run_at": datetime.now(timezone.utc),
            "last_status": "success",
            "consecutive_failures": 0,
            "total_runs": 1,
            "total_events_scraped": events_count,
            "latency_ms": latency_ms,
        },
    )
    log.info(
        "Scrape health recorded as success",
        source_id=source_id,
        latency_ms=latency_ms,
        events_count=events_count,
    )


async def record_failure(
    repo: SourceRepository,
    source_id: str,
    latency_ms: float,
    error: str,
) -> None:
    await repo.update_health(
        source_id,
        {
            "last_run_at": datetime.now(timezone.utc),
            "last_status": "error",
            "consecutive_failures": 1,
            "total_runs": 1,
            "latency_ms": latency_ms,
        },
    )
    log.warning(
        "Scrape health recorded as failure",
        source_id=source_id,
        latency_ms=latency_ms,
        error=error,
    )
