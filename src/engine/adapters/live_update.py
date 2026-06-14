from __future__ import annotations
import asyncio
from typing import Optional


import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.engine.base_adapter import BaseAdapter, ScraperError
from src.engine.adapter_registry import register_adapter
from src.config import get_logger
from src.registry.models import AdapterType, SourceConfig
from src.schema.canonical_event import CanonicalEvent

logger = get_logger(__name__)

MUTATION_OBSERVER_JS = """
async (args) => {
    const targetSelector = args[0];
    const timeoutMs = args[1];
    return new Promise((resolve) => {
        const target = document.querySelector(targetSelector);
        if (!target) {
            resolve({ error: 'Target element not found', selector: targetSelector });
            return;
        }

        const collectedEntries = [];
        let lastEntryTime = Date.now();

        const observer = new MutationObserver((mutations) => {
            for (const mutation of mutations) {
                if (mutation.type === 'childList') {
                    for (const node of mutation.addedNodes) {
                        if (node.nodeType === Node.ELEMENT_NODE) {
                            lastEntryTime = Date.now();
                            const entry = {
                                html: node.outerHTML,
                                text: node.innerText || node.textContent || '',
                                tagName: node.tagName,
                                timestamp: new Date().toISOString()
                            };

                            const links = node.querySelectorAll('a');
                            if (links.length > 0) {
                                entry.links = Array.from(links).map(a => ({
                                    href: a.href,
                                    text: a.innerText || a.textContent || ''
                                }));
                            }

                            collectedEntries.push(entry);
                        }
                    }
                }
            }
        });

        observer.observe(target, {
            childList: true,
            subtree: true
        });

        setTimeout(() => {
            observer.disconnect();
            resolve({
                entries: collectedEntries,
                totalDetected: collectedEntries.length,
                durationMs: timeoutMs
            });
        }, timeoutMs);
    });
}
"""

INITIAL_EXTRACT_JS = """
(itemSelector) => {
    const items = document.querySelectorAll(itemSelector);
    return Array.from(items).map(item => {
        const links = item.querySelectorAll('a');
        return {
            text: item.innerText || item.textContent || '',
            html: item.outerHTML,
            links: Array.from(links).map(a => ({
                href: a.href,
                text: a.innerText || a.textContent || ''
            }))
        };
    });
}
"""


@register_adapter(AdapterType.LIVE_UPDATE)
class LiveUpdateAdapter(BaseAdapter):

    def __init__(self) -> None:
        self._playwright = None
        self._browser = None
        self._observation_duration_ms: int = 30000

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
            ],
        )
        logger.info("Live update adapter initialized")

    async def scrape(self, source: SourceConfig) -> list[CanonicalEvent]:
        if not self._browser or not self._browser.is_connected:
            await self.setup()

        log = logger.bind(
            source_id=source.source_id,
            adapter_type="live_update",
            url=source.url,
        )

        context = None
        page = None
        events: list[CanonicalEvent] = []

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

            page = await context.new_page()

            await page.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in ("image", "media", "font", "stylesheet")
                else route.continue_(),
            )

            t0 = time.monotonic()

            log.info("Navigating to page")
            await page.goto(source.url, wait_until="load", timeout=60000)
            log.info(
                "Page loaded",
                latency_ms=round((time.monotonic() - t0) * 1000, 2),
            )

            cfg = source.extraction
            watch_target = cfg.mutation_target or cfg.item_selector

            try:
                toggle = await page.query_selector('.mw-rcfilters-ui-live-update-button button, [aria-label*="live"], [title*="live"]')
                if toggle:
                    await toggle.click()
                    try:
                        await page.wait_for_load_state("networkidle", timeout=3000)
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                await page.wait_for_selector(cfg.item_selector.split(",")[0].strip(), timeout=10000)
            except Exception:
                pass

            items = await page.evaluate(INITIAL_EXTRACT_JS, cfg.item_selector)
            log.info("Initial entries extracted", count=len(items))

            first_events = self._parse_entries(items, source, is_live=False)
            events.extend(first_events)

            timeout_ms = (
                cfg.max_scroll_iterations * 1000
                if getattr(cfg, "max_scroll_iterations", 0) > 0
                else 60000
            )

            log.info(
                "Starting MutationObserver",
                target=watch_target,
                duration_ms=timeout_ms,
            )

            result = await page.evaluate(MUTATION_OBSERVER_JS, [watch_target, timeout_ms])

            if result.get("error"):
                log.warning(
                    "MutationObserver returned an error",
                    error=result["error"],
                )
            else:
                new_entries = result.get("entries", [])
                log.info(
                    "Live entries detected",
                    count=len(new_entries),
                    total_detected=result.get("totalDetected", 0),
                    duration_ms=result.get("durationMs", 0),
                )

                live_evts = self._parse_entries(new_entries, source, is_live=True)
                events.extend(live_evts)

            elapsed = (time.monotonic() - t0) * 1000
            log.info(
                "Scrape complete",
                total_events=len(events),
                initial_events=len(first_events),
                live_events=len(events) - len(first_events),
                latency_ms=round(elapsed, 2),
            )

        except asyncio.CancelledError:
            is_cancelled = True
            raise
        except Exception as e:
            log.error("Scrape failed", error=str(e))
            raise ScraperError(
                source.source_id,
                f"Live update scrape failed: {e}",
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

    def _parse_entries(
        self,
        entries: list[dict],
        source: SourceConfig,
        is_live: bool,
    ) -> list[CanonicalEvent]:
        events: list[CanonicalEvent] = []
        fields_config = source.extraction.fields

        for entry in entries:
            try:
                html = entry.get("html", "")
                if not html:
                    continue

                soup = BeautifulSoup(html, "lxml")
                item = soup.find()
                if item is None:
                    continue

                fields = self._extract_fields_from_item(item, fields_config, source.url)

                title = fields.get("title") or ""
                url = fields.get("url") or source.url

                if not title and url == source.url:
                    continue

                if len(title) > 500:
                    title = title[:497] + "..."

                event = CanonicalEvent(
                    source_id=source.source_id,
                    url=url,
                    title=title,
                    body=fields.get("body"),
                    author=fields.get("author"),
                    timestamp=self._parse_ts(fields.get("timestamp")),
                    tags=["live_update"] if is_live else ["initial"],
                    metadata={
                        "is_live_update": is_live,
                        "change_size": fields.get("change_size"),
                        "edit_summary": fields.get("edit_summary"),
                        "link_count": len(entry.get("links", [])),
                    },
                    extraction_method="mutation_observer" if is_live else "css_selector",
                )
                events.append(event)

            except Exception as e:
                logger.warning(
                    "Failed to parse entry, skipping",
                    source_id=source.source_id,
                    error=str(e),
                )
                continue

        return events

    def _extract_fields_from_item(
        self,
        item,
        fields,
        page_url: str,
    ) -> dict:
        result = {}
        for field in fields:
            try:
                element = item.select_one(field.selector)
                if element is None:
                    result[field.name] = None
                    continue
                if field.attribute:
                    value = element.get(field.attribute, "") or ""
                    if field.attribute in ("href", "src") and value:
                        value = urljoin(page_url, value)
                else:
                    value = element.get_text(strip=True)
                if field.transform == "strip" and value:
                    value = value.strip()
                result[field.name] = value or None
            except Exception:
                result[field.name] = None
        return result

    def _parse_ts(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, AttributeError):
            pass
        match = re.search(r"(\d{1,2}):(\d{2})", value)
        if match:
            try:
                now = datetime.now(timezone.utc)
                return now.replace(hour=int(match.group(1)), minute=int(match.group(2)), second=0, microsecond=0)
            except (ValueError, AttributeError):
                pass
        return None

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
        logger.info("Live update adapter shut down")
