from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from src.config import get_logger
from src.registry.models import SourceConfig, SourceConfigUpdate
from src.storage.collections import COLLECTION_SOURCE_REGISTRY

log = get_logger(__name__)


class SourceRepository:

    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._collection = db[COLLECTION_SOURCE_REGISTRY]

    async def create_source(self, config: SourceConfig) -> SourceConfig:
        doc = config.to_storage_dict()
        doc["_id"] = config.source_id
        await self._collection.insert_one(doc)
        log.info(
            "Created source in registry",
            source_id=config.source_id,
            name=config.name,
        )
        return config

    async def get_source(self, source_id: str) -> Optional[SourceConfig]:
        doc = await self._collection.find_one({"source_id": source_id})
        if doc is None:
            return None
        doc.pop("_id", None)
        from pydantic import ValidationError
        try:
            return SourceConfig(**doc)
        except ValidationError as exc:
            log.error(
                "Database record failed configuration validation, source skipped",
                source_id=source_id,
                error=str(exc),
            )
            return None

    async def list_sources(
        self,
        enabled_only: bool = False,
    ) -> list[SourceConfig]:
        query: dict[str, Any] = {}
        if enabled_only:
            query["enabled"] = True

        from pydantic import ValidationError
        sources: list[SourceConfig] = []
        async for doc in self._collection.find(query):
            doc.pop("_id", None)
            source_id = doc.get("source_id", "unknown")
            try:
                sources.append(SourceConfig(**doc))
            except ValidationError as exc:
                log.error(
                    "Skipping corrupted/invalid database source configuration in list",
                    source_id=source_id,
                    error=str(exc),
                )
        return sources

    async def update_source(
        self,
        source_id: str,
        update: SourceConfigUpdate,
    ) -> Optional[SourceConfig]:
        changes = update.model_dump(exclude_none=True)
        if not changes:
            return await self.get_source(source_id)

        set_fields: dict[str, Any] = {}
        for k, v in changes.items():
            if k == "extraction" and isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    if sub_k == "fields" and isinstance(sub_v, list):
                        set_fields[f"extraction.{sub_k}"] = [
                            f.model_dump() if hasattr(f, "model_dump") else f for f in sub_v
                        ]
                    else:
                        set_fields[f"extraction.{sub_k}"] = sub_v
            else:
                set_fields[k] = v

        set_fields["updated_at"] = datetime.now(timezone.utc)

        result = await self._collection.find_one_and_update(
            {"source_id": source_id},
            {"$set": set_fields},
            return_document=True,
        )
        if result is None:
            return None

        result.pop("_id", None)
        log.info(
            "Updated source config",
            source_id=source_id,
            changed_fields=list(set_fields.keys()),
        )
        from pydantic import ValidationError
        try:
            return SourceConfig(**result)
        except ValidationError as exc:
            log.error(
                "Updated database record is invalid or corrupted",
                source_id=source_id,
                error=str(exc),
            )
            return None

    async def delete_source(self, source_id: str) -> bool:
        result = await self._collection.delete_one({"source_id": source_id})
        deleted = result.deleted_count > 0
        if deleted:
            log.info("Removed source from registry", source_id=source_id)
        return deleted

    async def update_health(
        self,
        source_id: str,
        health_update: dict[str, Any],
    ) -> None:
        set_fields: dict[str, Any] = {}
        inc_fields: dict[str, Any] = {}

        latency_ms = health_update.pop("latency_ms", None)

        for key, value in health_update.items():
            if key in ("total_runs", "total_events_scraped", "consecutive_failures") and value != 0:
                inc_fields[f"health.{key}"] = value
            else:
                set_fields[f"health.{key}"] = value

        if latency_ms is not None:
            doc = await self._collection.find_one(
                {"source_id": source_id},
                {"health.average_latency_ms": 1, "health.total_runs": 1},
            )
            if doc is not None:
                health = doc.get("health", {})
                old_avg = health.get("average_latency_ms", 0.0)
                existing_runs = health.get("total_runs", 0)
                run_increment = inc_fields.get("health.total_runs", 0)
                new_total = existing_runs + run_increment
                if new_total > 0:
                    new_avg = old_avg + (latency_ms - old_avg) / new_total
                    set_fields["health.average_latency_ms"] = new_avg

        set_fields["updated_at"] = datetime.now(timezone.utc)

        update_op: dict[str, Any] = {"$set": set_fields}
        if inc_fields:
            update_op["$inc"] = inc_fields

        await self._collection.update_one(
            {"source_id": source_id},
            update_op,
        )
        log.debug(
            "Health stats updated for source",
            source_id=source_id,
        )
