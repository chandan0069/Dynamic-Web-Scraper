from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.registry.models import SourceConfig
    from src.schema.canonical_event import CanonicalEvent


class BaseAdapter(ABC):

    async def setup(self) -> None:
        pass

    @abstractmethod
    async def scrape(self, source: "SourceConfig") -> "list[CanonicalEvent]":
        pass

    async def teardown(self) -> None:
        pass


class ScraperError(Exception):

    def __init__(self, source_id: str, message: str, details: Optional[dict] = None):
        self.source_id = source_id
        self.details = details or {}
        super().__init__(f"[{source_id}] {message}")
