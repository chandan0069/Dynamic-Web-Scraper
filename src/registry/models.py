from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class AdapterType(str, Enum):
    RATE_LIMITED_HTTP = "rate_limited_http"
    PLAYWRIGHT = "playwright"
    LIVE_UPDATE = "live_update"


class ExtractionField(BaseModel):

    name: str
    selector: str
    attribute: Optional[str] = None
    transform: Optional[str] = None
    use_sibling_row: bool = False


class ExtractionConfig(BaseModel):

    item_selector: str
    fields: list[ExtractionField]
    next_page_selector: Optional[str] = None
    scroll_target: Optional[str] = None
    json_ld: bool = False
    mutation_target: Optional[str] = None
    max_pages: int = 10
    max_scroll_iterations: int = 20


class HealthStatus(BaseModel):
    last_run_at: Optional[datetime] = None
    last_status: Optional[str] = None
    consecutive_failures: int = 0
    average_latency_ms: float = 0.0
    total_runs: int = 0
    total_events_scraped: int = 0


class SourceConfig(BaseModel):

    source_id: str
    name: str
    url: str
    adapter_type: AdapterType
    extraction: ExtractionConfig
    scrape_interval_seconds: int = 300
    enabled: bool = True
    auth_ref: Optional[str] = None
    health: HealthStatus = Field(default_factory=HealthStatus)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )

    def to_storage_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="python")


class ExtractionFieldUpdate(BaseModel):

    name: Optional[str] = None
    selector: Optional[str] = None
    attribute: Optional[str] = None
    transform: Optional[str] = None


class ExtractionConfigUpdate(BaseModel):

    item_selector: Optional[str] = None
    fields: Optional[list[ExtractionField]] = None
    next_page_selector: Optional[str] = None
    scroll_target: Optional[str] = None
    json_ld: Optional[bool] = None
    mutation_target: Optional[str] = None
    max_pages: Optional[int] = None
    max_scroll_iterations: Optional[int] = None


class SourceConfigUpdate(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None
    adapter_type: Optional[AdapterType] = None
    extraction: Optional[ExtractionConfigUpdate] = None
    scrape_interval_seconds: Optional[int] = None
    enabled: Optional[bool] = None
    auth_ref: Optional[str] = None
