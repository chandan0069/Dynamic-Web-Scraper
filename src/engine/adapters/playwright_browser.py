from __future__ import annotations


import asyncio
import re
import time
from datetime import datetime, timezone
from typing import Optional, Any
from urllib.parse import urljoin

from src.engine.base_adapter import BaseAdapter, ScraperError
from src.engine.adapter_registry import register_adapter
from src.config import get_logger
from src.registry.models import AdapterType, SourceConfig
from src.schema.canonical_event import CanonicalEvent

logger = get_logger(__name__)

_WORD_TO_NUM = {"one": 1.0, "two": 2.0, "three": 3.0, "four": 4.0, "five": 5.0}


INFINITE_SCROLL_JS = """
async (args) => {
    const MAX_ITERATIONS = args[1] || 20;
    const IDLE_TIMEOUT = args[2] || 6000;

    return new Promise((resolve) => {
        let mutationCount = 0;
        let scrollIterations = 0;
        let idleTimer = null;
        let finished = false;

        const finish = (reachedEnd) => {
            if (finished) return;
            finished = true;
            observer.disconnect();
            if (idleTimer) clearTimeout(idleTimer);
            resolve({ scrollIterations, mutationCount, reachedEnd });
        };

        const resetIdle = () => {
            if (idleTimer) clearTimeout(idleTimer);
            idleTimer = setTimeout(() => finish(true), IDLE_TIMEOUT);
        };

        const observer = new MutationObserver((mutations) => {
            mutationCount += mutations.length;
            resetIdle();
        });

        observer.observe(document.body, { childList: true, subtree: true });

        function doScroll() {
            if (finished) return;
            if (scrollIterations >= MAX_ITERATIONS) {
                resetIdle();
                return;
            }
            scrollIterations++;
            window.scrollTo(0, document.body.scrollHeight);
            setTimeout(doScroll, 800);
        }

        resetIdle();
        doScroll();

        setTimeout(() => finish(false), 120000);
    });
}
"""

JSON_LD_EXTRACT_JS = """
() => {
    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    const results = [];
    scripts.forEach(script => {
        try {
            results.push(JSON.parse(script.textContent));
        } catch (e) {}
    });
    return results;
}
"""


@register_adapter(AdapterType.PLAYWRIGHT)
class PlaywrightBrowserAdapter(BaseAdapter):

    def __init__(self) -> None:
        self._playwright = None
        self._browser = None

    async def setup(self) -> None:
        if self._browser and self._browser.is_connected:
            return
        await self.teardown()

        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-web-security",
            ],
        )
        logger.info("Playwright adapter initialized")

    async def scrape(self, source: SourceConfig) -> list[CanonicalEvent]:

        if not self._browser or not self._browser.is_connected:
            await self.setup()

        log = logger.bind(
            source_id=source.source_id,
            adapter_type="playwright",
            url=source.url,
        )

        context = None
        page = None
        events: list[CanonicalEvent] = []
        current_url = source.url
        page_count = 0
        extraction = source.extraction

        is_cancelled = False
        try:
            context = await self._browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                java_script_enabled=True,
            )

            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                window.chrome = { runtime: {} };
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) =>
                    parameters.name === 'notifications'
                        ? Promise.resolve({ state: Notification.permission })
                        : originalQuery(parameters);
            """)

            page = await context.new_page()

            await page.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in ("image", "media", "font")
                or any(
                    tracker in route.request.url
                    for tracker in (
                        "google-analytics",
                        "googletagmanager",
                        "doubleclick",
                        "facebook",
                        "amplitude",
                        "mixpanel",
                        "redditstatic.com/ads",
                        "adzerk",
                    )
                )
                else route.continue_(),
            )

            start_time = time.monotonic()

            while current_url and page_count < extraction.max_pages:
                page_count += 1
                log_page = log.bind(page=page_count, url=current_url)

                log_page.info("Navigating to page")
                await page.goto(current_url, wait_until="load", timeout=30000)

                try:
                    await page.wait_for_load_state("networkidle", timeout=2000)
                except Exception:
                    pass

                log_page.info("Page loaded")

                if extraction.scroll_target:
                    log_page.info("Starting infinite scroll")
                    scroll_result = await page.evaluate(
                        INFINITE_SCROLL_JS,
                        [extraction.scroll_target, extraction.max_scroll_iterations, 5000],
                    )
                    log_page.info(
                        "Infinite scroll complete",
                        scroll_iterations=scroll_result.get("scrollIterations", 0),
                        mutation_count=scroll_result.get("mutationCount", 0),
                        reached_end=scroll_result.get("reachedEnd", False),
                    )

                try:
                    await page.wait_for_load_state("networkidle", timeout=2000)
                except Exception:
                    pass

                json_ld_data: list[dict] = []
                if extraction.json_ld:
                    json_ld_data = await page.evaluate(JSON_LD_EXTRACT_JS)
                    log_page.info("JSON-LD data extracted", count=len(json_ld_data))

                html_events = await self._extract_items_from_page(page, source)
                log_page.info("HTML items extracted", count=len(html_events))

                if json_ld_data:
                    page_events = self._merge_json_ld(html_events, json_ld_data, source.source_id)
                else:
                    page_events = html_events

                events.extend(page_events)

                if extraction.next_page_selector:
                    next_element = await page.query_selector(extraction.next_page_selector)
                    if next_element:
                        href = await next_element.get_attribute("href")
                        current_url = urljoin(current_url, href) if href else None
                    else:
                        current_url = None
                else:
                    current_url = None

            log.info(
                "Scrape complete",
                total_events=len(events),
                pages_scraped=page_count,
                latency_ms=round((time.monotonic() - start_time) * 1000, 2),
            )

        except asyncio.CancelledError:
            is_cancelled = True
            raise
        except Exception as e:
            log.error("Scrape failed", error=str(e))
            raise ScraperError(
                source.source_id,
                f"Playwright scrape failed: {e}",
                {"url": source.url},
            )
        finally:
            if page or context:
                if is_cancelled:
                    pass
                else:
                    async def _cleanup():
                        try:
                            if page:
                                await page.close()
                        except Exception:
                            pass
                        try:
                            if context:
                                await context.close()
                        except Exception:
                            pass
                    try:
                        await _cleanup()
                    except Exception:
                        pass

        return events

    async def _extract_items_from_page(
        self, page: Any, source: SourceConfig
    ) -> list[CanonicalEvent]:
        events: list[CanonicalEvent] = []
        extraction = source.extraction

        items = await page.query_selector_all(extraction.item_selector)

        for item in items:
            try:
                fields: dict[str, Any] = {}

                for field in extraction.fields:
                    try:
                        if field.selector == ":self":
                            element = item
                        else:
                            element = await item.query_selector(field.selector)
                        if element is None:
                            fields[field.name] = None
                            continue

                        if field.attribute:
                            value = await element.get_attribute(field.attribute)
                            if field.attribute in ("href", "src") and value:
                                value = urljoin(source.url, value)
                        else:
                            value = await element.inner_text()
                            if value:
                                value = value.strip()

                        if field.transform == "strip" and value:
                            value = value.strip()
                        elif field.transform == "int" and value:
                            digits = "".join(filter(str.isdigit, value))
                            value = str(int(digits)) if digits else "0"
                        elif field.transform == "float" and value:
                            cleaned = "".join(c for c in value if c.isdigit() or c in ".-")
                            value = str(float(cleaned)) if cleaned else "0.0"
                        elif field.transform == "absolute_url" and value:
                            value = urljoin(source.url, value)

                        fields[field.name] = value
                    except Exception:
                        fields[field.name] = None

                if not fields.get("title") and not fields.get("url"):
                    continue

                event = CanonicalEvent(
                    source_id=source.source_id,
                    url=fields.get("url", source.url) or source.url,
                    title=fields.get("title", "") or "",
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

    def _merge_json_ld(
        self,
        html_events: list[CanonicalEvent],
        json_ld_data: list[dict],
        source_id: str,
    ) -> list[CanonicalEvent]:

        if not html_events and json_ld_data:
            events = []
            for ld_item in json_ld_data:
                if isinstance(ld_item, dict):
                    events.extend(self._json_ld_to_events(ld_item, source_id))
            return events

        for event in html_events:
            event.metadata["json_ld_available"] = True
            if event.extraction_method == "css_selector":
                event.extraction_method = "hybrid_css_json_ld"

        return html_events

    def _json_ld_to_events(
        self, ld_data: dict, source_id: str
    ) -> list[CanonicalEvent]:
        events = []

        if ld_data.get("@type") == "ItemList":
            for item in ld_data.get("itemListElement", []):
                if isinstance(item, dict):
                    url = item.get("url", "")
                    name = item.get("name", item.get("headline", ""))
                    if url or name:
                        event = CanonicalEvent(
                            source_id=source_id,
                            url=url,
                            title=name,
                            body=item.get("description"),
                            author=self._extract_ld_author(item),
                            timestamp=self._parse_timestamp(
                                item.get("datePublished") or item.get("dateCreated")
                            ),
                            metadata={"json_ld_type": item.get("@type", "unknown")},
                            extraction_method="json_ld",
                        )
                        events.append(event)
        elif ld_data.get("@type") in ("NewsArticle", "Article", "WebPage"):
            url = ld_data.get("url", "")
            name = ld_data.get("headline", ld_data.get("name", ""))
            if url or name:
                event = CanonicalEvent(
                    source_id=source_id,
                    url=url,
                    title=name,
                    body=ld_data.get("description"),
                    author=self._extract_ld_author(ld_data),
                    timestamp=self._parse_timestamp(
                        ld_data.get("datePublished") or ld_data.get("dateCreated")
                    ),
                    metadata={"json_ld_type": ld_data.get("@type", "unknown")},
                    extraction_method="json_ld",
                )
                events.append(event)

        return events

    def _extract_ld_author(self, ld_data: dict) -> Optional[str]:
        author = ld_data.get("author")
        if isinstance(author, dict):
            return author.get("name")
        if isinstance(author, str):
            return author
        if isinstance(author, list) and author:
            first = author[0]
            return first.get("name") if isinstance(first, dict) else str(first)
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
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        finally:
            self._browser = None

        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        finally:
            self._playwright = None

        try:
            await asyncio.sleep(0.25)
        except Exception:
            pass
        logger.info("Playwright adapter shut down")
