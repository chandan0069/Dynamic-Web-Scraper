from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, IndexModel

from src.config import get_logger

log = get_logger(__name__)

COLLECTION_SCRAPED_EVENTS: str = "scraped_events"
COLLECTION_SOURCE_REGISTRY: str = "source_registry"
COLLECTION_SCRAPE_RUNS: str = "scrape_runs"


async def ensure_indexes(db: AsyncIOMotorDatabase) -> None:

    scraped_events = db[COLLECTION_SCRAPED_EVENTS]
    await scraped_events.create_indexes(
        [
            IndexModel(
                [("content_hash", ASCENDING)],
                unique=True,
                name="ux_content_hash",
            ),
            IndexModel(
                [("source_id", ASCENDING), ("scraped_at", DESCENDING)],
                name="ix_source_scraped_at",
            ),
        ]
    )
    log.info("Indexes ensured for collection", collection=COLLECTION_SCRAPED_EVENTS)

    source_registry = db[COLLECTION_SOURCE_REGISTRY]
    await source_registry.create_indexes(
        [
            IndexModel(
                [("source_id", ASCENDING)],
                unique=True,
                name="ux_source_id",
            ),
        ]
    )
    log.info("Indexes ensured for collection", collection=COLLECTION_SOURCE_REGISTRY)

    scrape_runs = db[COLLECTION_SCRAPE_RUNS]
    await scrape_runs.create_indexes(
        [
            IndexModel(
                [("source_id", ASCENDING), ("started_at", DESCENDING)],
                name="ix_source_started_at",
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="ix_status",
            ),
        ]
    )
    log.info("Indexes ensured for collection", collection=COLLECTION_SCRAPE_RUNS)
