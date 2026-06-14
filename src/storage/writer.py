from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Any

from pymongo import UpdateOne

from src.config import get_logger
from src.schema.canonical_event import CanonicalEvent
from src.storage.collections import COLLECTION_SCRAPED_EVENTS, COLLECTION_SCRAPE_RUNS
from src.storage.mongo_client import MongoClient

log = get_logger(__name__)


class EventWriter:
    def __init__(self, client: MongoClient) -> None:
        self._client = client

    async def upsert_event(self, event: CanonicalEvent) -> str:
        collection = self._client.get_collection(COLLECTION_SCRAPED_EVENTS)
        doc = event.to_storage_dict()

        result = await collection.update_one(
            {"content_hash": event.content_hash},
            {"$set": doc},
            upsert=True,
        )

        if result.upserted_id is not None:
            log.info(
                "New event stored",
                content_hash=event.content_hash,
                source_id=event.source_id,
            )
            return "inserted"

        log.info(
            "Existing event updated",
            content_hash=event.content_hash,
            source_id=event.source_id,
        )
        return "updated"

    async def bulk_upsert_events(
        self, events: list[CanonicalEvent]
    ) -> dict[str, int]:
        if not events:
            return {"inserted": 0, "updated": 0}

        collection = self._client.get_collection(COLLECTION_SCRAPED_EVENTS)

        operations = [
            UpdateOne(
                {"content_hash": event.content_hash},
                {"$set": event.to_storage_dict()},
                upsert=True,
            )
            for event in events
        ]

        result = await collection.bulk_write(operations, ordered=False)

        summary = {
            "inserted": result.upserted_count,
            "updated": result.modified_count,
        }
        log.info(
            "Bulk events written",
            total=len(events),
            **summary,
        )
        return summary

    async def record_scrape_run(
        self,
        source_id: str,
        status: str,
        duration_ms: int,
        event_count: int,
        error: Optional[str] = None,
    ) -> None:
        collection = self._client.get_collection(COLLECTION_SCRAPE_RUNS)

        doc: dict[str, Any] = {
            "source_id": source_id,
            "status": status,
            "started_at": datetime.now(timezone.utc),
            "duration_ms": duration_ms,
            "event_count": event_count,
        }
        if error is not None:
            doc["error"] = error

        await collection.insert_one(doc)
        log.info(
            "Scrape run logged",
            source_id=source_id,
            status=status,
            duration_ms=duration_ms,
            event_count=event_count,
        )
