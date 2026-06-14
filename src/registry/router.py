from __future__ import annotations

import time
from typing import Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from src.config import get_logger
from src.registry.models import SourceConfig, SourceConfigUpdate
from src.registry.repository import SourceRepository
from src.storage.mongo_client import MongoClient

log = get_logger(__name__)

router = APIRouter(prefix="/api/sources", tags=["sources"])

_mongo_client: Optional[MongoClient] = None


def _get_mongo_client() -> MongoClient:
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient()
    return _mongo_client


def set_mongo_client(client: MongoClient) -> None:
    global _mongo_client
    _mongo_client = client


def get_repository(
    client: MongoClient = Depends(_get_mongo_client),
) -> SourceRepository:
    db = client.get_database()
    return SourceRepository(db)


@router.post("/", status_code=201, response_model=SourceConfig)
async def create_source(
    source: SourceConfig,
    repo: SourceRepository = Depends(get_repository),
) -> SourceConfig:
    from pymongo.errors import DuplicateKeyError
    try:
        created = await repo.create_source(source)
        log.info("New source added", source_id=created.source_id)
        return created
    except DuplicateKeyError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"Source with id '{source.source_id}' already exists."
        ) from exc


@router.get("/", response_model=list[SourceConfig])
async def list_sources(
    enabled_only: bool = Query(False),
    repo: SourceRepository = Depends(get_repository),
) -> list[SourceConfig]:
    return await repo.list_sources(enabled_only=enabled_only)


@router.get("/{source_id}", response_model=SourceConfig)
async def get_source(
    source_id: str,
    repo: SourceRepository = Depends(get_repository),
) -> SourceConfig:
    source = await repo.get_source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")
    return source


@router.patch("/{source_id}", response_model=SourceConfig)
async def update_source(
    source_id: str,
    update: SourceConfigUpdate,
    repo: SourceRepository = Depends(get_repository),
) -> SourceConfig:
    updated = await repo.update_source(source_id, update)
    if updated is None:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")
    return updated


@router.delete("/{source_id}", status_code=204)
async def delete_source(
    source_id: str,
    repo: SourceRepository = Depends(get_repository),
) -> Response:
    deleted = await repo.delete_source(source_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")
    return Response(status_code=204)


@router.post("/{source_id}/dry-run")
async def dry_run_source(
    source_id: str,
    repo: SourceRepository = Depends(get_repository),
) -> dict[str, Any]:
    source = await repo.get_source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    try:
        from src.engine.adapter_registry import resolve_adapter, load_adapters
        from src.engine.base_adapter import ScraperError
    except ImportError as exc:
        raise HTTPException(
            status_code=501,
            detail="Adapter registry or base classes are not available yet.",
        ) from exc

    load_adapters()
    try:
        adapter = resolve_adapter(source.adapter_type)
    except KeyError as exc:
        raise HTTPException(
            status_code=501,
            detail=f"No adapter available for type: {source.adapter_type}",
        ) from exc

    start = time.monotonic()
    try:
        await adapter.setup()
        events = await adapter.scrape(source)
    except ScraperError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "ScraperError",
                "message": str(exc),
                "details": exc.details,
            }
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Dry run failed with unexpected error: {exc}"
        ) from exc
    finally:
        try:
            await adapter.teardown()
        except Exception as teardown_exc:
            log.error("Failed to teardown adapter during dry-run", error=str(teardown_exc))
    elapsed_ms = (time.monotonic() - start) * 1000

    log.info(
        "Dry run finished",
        source_id=source_id,
        event_count=len(events),
        latency_ms=round(elapsed_ms, 2),
    )

    return {
        "source_id": source_id,
        "event_count": len(events),
        "latency_ms": round(elapsed_ms, 2),
        "events": [event.to_json_dict() for event in events],
    }
