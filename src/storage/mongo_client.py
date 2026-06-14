from __future__ import annotations
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from src.config import get_settings
from src.config import get_logger

log = get_logger(__name__)


class MongoClient:

    def __init__(self) -> None:
        self._settings = get_settings()
        self._client: Optional[AsyncIOMotorClient] = None

    async def connect(self) -> None:
        if self._client is not None:
            log.warning("MongoDB connection already open")
            return

        log.info(
            "Connecting to MongoDB",
            uri=self._settings.mongodb_uri,
            database=self._settings.mongodb_database,
        )
        self._client = AsyncIOMotorClient(self._settings.mongodb_uri)

        await self._client.admin.command("ping")
        log.info("MongoDB connection established")

    async def disconnect(self) -> None:
        if self._client is None:
            log.warning("MongoDB already disconnected")
            return

        self._client.close()
        self._client = None
        log.info("MongoDB connection closed")

    def get_database(self) -> AsyncIOMotorDatabase:
        if self._client is None:
            raise RuntimeError(
                "MongoClient is not connected. Call connect() first."
            )
        return self._client[self._settings.mongodb_database]

    def get_collection(self, name: str) -> AsyncIOMotorCollection:
        return self.get_database()[name]
