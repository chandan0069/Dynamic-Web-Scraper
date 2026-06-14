from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import xxhash
from pydantic import BaseModel, Field


class CanonicalEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_id: str
    content_hash: str = ""
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    url: str
    title: str
    body: Optional[str] = None
    author: Optional[str] = None
    timestamp: Optional[datetime] = None
    score: Optional[float] = None
    tags: list[str] = Field(default_factory=list)

    metadata: dict[str, Any] = Field(default_factory=dict)

    extraction_method: str = "css_selector"

    def compute_content_hash(self) -> str:
        content = f"{self.title}|{self.url}|{self.body or ''}"
        self.content_hash = xxhash.xxh64(content.encode("utf-8")).hexdigest()
        return self.content_hash

    def model_post_init(self, __context: Any) -> None:
        if not self.content_hash:
            self.compute_content_hash()

    def to_storage_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        return data

    def to_json_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        if data.get("scraped_at"):
            data["scraped_at"] = data["scraped_at"].isoformat()
        if data.get("timestamp"):
            data["timestamp"] = data["timestamp"].isoformat()
        return data
