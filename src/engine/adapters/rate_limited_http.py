from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional, Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from src.engine.base_adapter import BaseAdapter, ScraperError
from src.engine.adapter_registry import register_adapter
from src.engine.throttler import AdaptiveThrottler
from src.config import get_logger
from src.registry.models import AdapterType, SourceConfig, ExtractionField
from src.schema.canonical_event import CanonicalEvent

logger = get_logger(__name__)

_WORD_TO_NUM = {"one": 1.0, "two": 2.0, "three": 3.0, "four": 4.0, "five": 5.0}


@register_adapter(AdapterType.RATE_LIMITED_HTTP)
class RateLimitedHttpAdapter(BaseAdapter):

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None
        self._throttler = AdaptiveThrottler()

    async def setup(self) -> None:
        if self._client and not self._client.is_closed:
            return
        await self.teardown()
        self._client = httpx.AsyncClient(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            },
            follow_redirects=True,
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        logger.info("HTTP adapter initialized")

    async def scrape(self, source: SourceConfig) -> list[CanonicalEvent]:
        if not self._client or self._client.is_closed:
            await self.setup()

        events: list[CanonicalEvent] = []
        domain = urlparse(source.url).netloc
        current_url = source.url
        page_count = 0
        extraction = source.extraction

        while current_url and page_count < extraction.max_pages:
            page_count += 1
            log = logger.bind(
                source_id=source.source_id,
                adapter_type="rate_limited_http",
                page=page_count,
                url=current_url,
            )

            await self._throttler.acquire(domain)

            try:
                t0 = time.monotonic()
                response = await self._client.get(current_url)
                elapsed = (time.monotonic() - t0) * 1000

                self._throttler.record_response(domain, response.status_code)

                if response.status_code == 429:
                    log.warning(
                        "Rate limited by server, backing off",
                        status_code=429,
                        latency_ms=round(elapsed, 2),
                    )
                    continue

                response.raise_for_status()

                log.info(
                    "Page fetched successfully",
                    status_code=response.status_code,
                    latency_ms=round(elapsed, 2),
                    content_length=len(response.content),
                )

                soup = BeautifulSoup(response.text, "lxml")
                found = self._extract_items(soup, source, current_url)
                events.extend(found)

                log.info("Extracted items from page", count=len(found))

                current_url = self._get_next_page_url(
                    soup, extraction.next_page_selector, current_url
                )

            except httpx.HTTPStatusError as e:
                self._throttler.record_response(domain, e.response.status_code)
                log.error(
                    "HTTP error received",
                    status_code=e.response.status_code,
                    error=str(e),
                )
                raise ScraperError(
                    source.source_id,
                    f"HTTP {e.response.status_code} on page {page_count}",
                    {"url": current_url, "status_code": e.response.status_code},
                )
            except httpx.RequestError as e:
                log.error("Request failed", error=str(e))
                raise ScraperError(
                    source.source_id,
                    f"Request failed on page {page_count}: {e}",
                    {"url": current_url},
                )

        logger.info(
            "Scraping finished for source",
            source_id=source.source_id,
            total_events=len(events),
            pages_scraped=page_count,
        )
        return events

    def _extract_items(
        self, soup: BeautifulSoup, source: SourceConfig, page_url: str
    ) -> list[CanonicalEvent]:
        events: list[CanonicalEvent] = []
        extraction = source.extraction

        items = soup.select(extraction.item_selector)
        for item in items:
            try:
                fields = self._extract_fields(item, extraction.fields, page_url)
                if not fields.get("title") and not fields.get("url"):
                    continue

                event = CanonicalEvent(
                    source_id=source.source_id,
                    url=fields.get("url", page_url),
                    title=fields.get("title", ""),
                    body=fields.get("body"),
                    author=fields.get("author"),
                    timestamp=self._parse_timestamp(fields.get("timestamp")),
                    score=self._parse_score(fields.get("score")),
                    tags=self._parse_tags(fields.get("tags")),
                    metadata={
                        k: v
                        for k, v in fields.items()
                        if k not in ("title", "url", "body", "author", "timestamp", "score", "tags")
                    },
                    extraction_method="css_selector",
                )
                events.append(event)
            except Exception as e:
                logger.warning(
                    "Failed to extract item, skipping",
                    source_id=source.source_id,
                    error=str(e),
                )
                continue

        return events

    def _extract_fields(
        self, item: Any, fields: list[ExtractionField], page_url: str
    ) -> dict[str, Optional[str]]:
        result: dict[str, Optional[str]] = {}

        for field in fields:
            try:
                if field.use_sibling_row:
                    sibling = item.find_next_sibling("tr")
                    element = sibling.select_one(field.selector) if sibling else None
                else:
                    element = item.select_one(field.selector)

                if element is None:
                    result[field.name] = None
                    continue

                if field.attribute:
                    raw = element.get(field.attribute, "")
                    value = " ".join(raw) if isinstance(raw, list) else (raw or "")
                    if field.attribute in ("href", "src") and value:
                        value = urljoin(page_url, value)
                else:
                    value = element.get_text(strip=True)

                if field.transform == "strip" and value:
                    value = value.strip()
                elif field.transform == "int" and value:
                    value = str(int("".join(filter(str.isdigit, value)) or 0))
                elif field.transform == "float" and value:
                    cleaned = "".join(c for c in value if c.isdigit() or c in ".-")
                    value = str(float(cleaned)) if cleaned else "0.0"

                result[field.name] = value
            except Exception:
                result[field.name] = None

        return result

    def _get_next_page_url(
        self, soup: BeautifulSoup, selector: Optional[str], current_url: str
    ) -> Optional[str]:
        if not selector:
            return None

        next_link = soup.select_one(selector)
        if next_link and next_link.get("href"):
            return urljoin(current_url, next_link["href"])
        return None

    def _parse_timestamp(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        cleaned = value.strip()
        try:
            dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, AttributeError):
            pass

        formats = (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d",
            "%b %d, %Y",
            "%B %d, %Y",
            "%d %b %Y",
            "%d %B %Y",
        )
        for fmt in formats:
            try:
                dt = datetime.strptime(cleaned, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue

        first_word = cleaned.split()[0] if cleaned.split() else ""
        if first_word and first_word != cleaned:
            try:
                dt = datetime.fromisoformat(first_word.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, AttributeError):
                pass

        return None

    def _parse_score(self, value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        lower = value.lower()
        for word, num in _WORD_TO_NUM.items():
            if word in lower:
                return num
        try:
            cleaned = "".join(c for c in value if c.isdigit() or c in ".k-")
            if cleaned.endswith("k"):
                try:
                    return float(cleaned[:-1]) * 1000
                except ValueError:
                    pass
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    def _parse_tags(self, value: Optional[str]) -> list[str]:
        if not value:
            return []
        if "," in value:
            return [t.strip() for t in value.split(",") if t.strip()]
        cleaned = re.sub(r"(?i)^tags?\s*:?\s*", "", value.strip())
        lines = [l.strip() for l in cleaned.splitlines() if l.strip()]
        if len(lines) > 1:
            return lines
        return [t for t in cleaned.split() if t]

    async def teardown(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("HTTP adapter shut down")
