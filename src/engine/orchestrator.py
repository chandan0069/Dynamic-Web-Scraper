from __future__ import annotations

import asyncio
import gc
import inspect
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorClient

from src.engine.adapter_registry import load_adapters, resolve_adapter
from src.engine.base_adapter import ScraperError, BaseAdapter
from src.registry.repository import SourceRepository
from src.registry.health import record_success, record_failure
from src.storage.writer import EventWriter
from src.registry.models import SourceConfig
from src.schema.canonical_event import CanonicalEvent
from src.config import Settings, get_logger

logger = get_logger(__name__, component="orchestrator")


class ScrapingOrchestrator:
    def __init__(self, mongo_client: AsyncIOMotorClient, settings: Settings) -> None:
        self._mongo_client = mongo_client
        self._settings = settings
        self._tasks: dict[str, asyncio.Task] = {}
        self._sources: dict[str, SourceConfig] = {}
        self._stop_event = asyncio.Event()
        self._events: list[dict[str, Any]] = []


    async def start(
        self,
        duration_seconds: Optional[int] = None,
        output_file: Optional[str] = None,
    ) -> None:
        logger.info(
            "Starting scraping orchestrator",
            duration_seconds=duration_seconds,
            output_file=output_file,
        )

        load_adapters()

        repo = self._create_repo()
        writer = self._create_writer()

        self._install_signal_handlers()

        timer: Optional[asyncio.TimerHandle] = None
        if duration_seconds is not None:
            loop = asyncio.get_running_loop()
            timer = loop.call_later(duration_seconds, self._stop_event.set)
            logger.info("Scheduled auto-stop", after_seconds=duration_seconds)

        try:
            await self._load_and_reconcile_sources(repo, writer)

            reload_task = asyncio.create_task(
                self._registry_reloader(repo, writer),
                name="registry-reloader",
            )

            await self._stop_event.wait()
            logger.info("Stop signal received, shutting down")

            reload_task.cancel()
            try:
                await reload_task
            except asyncio.CancelledError:
                pass

        finally:
            if timer is not None:
                timer.cancel()
            await self._shutdown(output_file)


    async def _load_and_reconcile_sources(self, repo: Any, writer: Any) -> None:
        sources = await self._fetch_sources(repo)

        current_ids = set(self._sources.keys())
        all_ids = set(sources.keys())
        enabled_ids = {sid for sid, s in sources.items() if s.enabled}

        started = []
        for sid in enabled_ids - current_ids:
            self._start_source_task(sources[sid], writer, repo)
            started.append(sid)

        cancelled = []
        for sid in current_ids - all_ids:
            await self._cancel_source_task(sid)
            cancelled.append(sid)

        for sid in current_ids & all_ids:
            if not sources[sid].enabled and sid in self._tasks:
                logger.info(
                    "Source disabled mid-run, cancelling task",
                    source_id=sid,
                    adapter_type=str(sources[sid].adapter_type),
                )
                await self._cancel_source_task(sid)
                cancelled.append(sid)
            else:
                self._sources[sid] = sources[sid]

        logger.info(
            "Sources reconciled",
            started=len(started),
            cancelled=len(cancelled),
            updated=len(self._tasks),
            total_active=len(self._tasks),
        )

    async def _fetch_sources(self, repo: Optional[SourceRepository]) -> dict[str, SourceConfig]:
        if repo is None:
            return {}
        sources_list = await repo.list_sources(enabled_only=False)
        return {s.source_id: s for s in sources_list}

    def _start_source_task(
        self, source: SourceConfig, writer: Any, repo: Any
    ) -> None:
        try:
            adapter = resolve_adapter(source.adapter_type)
        except KeyError:
            logger.error(
                "No adapter found for source, skipping",
                source_id=source.source_id,
                adapter_type=str(source.adapter_type),
            )
            return

        task = asyncio.create_task(
            self._run_source_loop(source, adapter, writer, repo),
            name=f"source-{source.source_id}",
        )
        self._tasks[source.source_id] = task
        self._sources[source.source_id] = source
        logger.info(
            "Started scraper for source",
            source_id=source.source_id,
            adapter_type=str(source.adapter_type),
        )

    async def _cancel_source_task(self, source_id: str) -> None:
        task = self._tasks.pop(source_id, None)
        self._sources.pop(source_id, None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Cancelled source task", source_id=source_id)

    async def _run_source_loop(
        self,
        source: SourceConfig,
        adapter: BaseAdapter,
        writer: Any,
        repo: Any,
    ) -> None:
        source_id = source.source_id

        try:
            await adapter.setup()
        except Exception as exc:
            logger.error(
                "Failed to initialize adapter",
                source_id=source_id,
                error=str(exc),
                exc_info=True,
            )
            return

        try:
            while not self._stop_event.is_set():
                current_source = self._sources.get(source_id, source)

                if not current_source.enabled:
                    logger.info(
                        "Source disabled mid-run, skipping scrape cycle",
                        source_id=source_id,
                        adapter_type=str(current_source.adapter_type),
                    )
                    await self._wait_interval(
                        self._settings.default_scrape_interval
                    )
                    continue

                events: list[CanonicalEvent] = []
                start_time = time.monotonic()
                try:
                    events = await adapter.scrape(current_source)
                    latency_ms = (time.monotonic() - start_time) * 1000
                    logger.info(
                        "Scrape completed successfully",
                        source_id=source_id,
                        events_count=len(events),
                        latency_ms=round(latency_ms, 2),
                    )
                except ScraperError as exc:
                    latency_ms = (time.monotonic() - start_time) * 1000
                    logger.error(
                        "Scrape failed with error",
                        source_id=source_id,
                        error=str(exc),
                        details=exc.details,
                    )
                    if writer is not None:
                        try:
                            res = writer.record_scrape_run(
                                source_id=source_id,
                                status="error",
                                duration_ms=int(latency_ms),
                                event_count=0,
                                error=str(exc),
                            )
                            if inspect.isawaitable(res):
                                await res
                        except Exception as record_exc:
                            logger.error(
                                "Failed to record scrape run error status in database",
                                source_id=source_id,
                                error=str(record_exc)
                            )
                    await self._record_health(repo, source_id, success=False, latency_ms=latency_ms)
                    await self._wait_interval(current_source.scrape_interval_seconds)
                    continue
                except Exception as exc:
                    latency_ms = (time.monotonic() - start_time) * 1000
                    logger.error(
                        "Unexpected error while scraping",
                        source_id=source_id,
                        error=str(exc),
                        exc_info=True,
                    )
                    if writer is not None:
                        try:
                            res = writer.record_scrape_run(
                                source_id=source_id,
                                status="error",
                                duration_ms=int(latency_ms),
                                event_count=0,
                                error=str(exc),
                            )
                            if inspect.isawaitable(res):
                                await res
                        except Exception as record_exc:
                            logger.error(
                                "Failed to record scrape run unexpected error status in database",
                                source_id=source_id,
                                error=str(record_exc)
                            )
                    await self._record_health(repo, source_id, success=False, latency_ms=latency_ms)
                    await self._wait_interval(current_source.scrape_interval_seconds)
                    continue

                if events:
                    await self._write_events(writer, events)
                    for ev in events:
                        self._events.append(ev.to_json_dict())

                if writer is not None:
                    try:
                        res = writer.record_scrape_run(
                            source_id=source_id,
                            status="success",
                            duration_ms=int(latency_ms),
                            event_count=len(events),
                        )
                        if inspect.isawaitable(res):
                            await res
                    except Exception as record_exc:
                        logger.error(
                            "Failed to record scrape run success status in database",
                            source_id=source_id,
                            error=str(record_exc)
                        )

                await self._record_health(
                    repo,
                    source_id,
                    success=True,
                    latency_ms=latency_ms,
                    events_count=len(events),
                )

                await self._wait_interval(current_source.scrape_interval_seconds)

        finally:
            teardown_task = asyncio.shield(adapter.teardown())
            try:
                await teardown_task
            except asyncio.CancelledError:
                try:
                    await teardown_task
                except Exception as exc:
                    logger.error(
                        "Error during adapter cleanup after cancellation",
                        source_id=source_id,
                        error=str(exc),
                        exc_info=True,
                    )
                raise
            except Exception as exc:
                logger.error(
                    "Error during adapter cleanup",
                    source_id=source_id,
                    error=str(exc),
                    exc_info=True,
                )


    async def _registry_reloader(self, repo: Any, writer: Any) -> None:
        interval = self._settings.registry_reload_interval
        logger.info("Registry reloader started", interval_seconds=interval)

        while not self._stop_event.is_set():
            await self._wait_interval(interval)
            if self._stop_event.is_set():
                break

            try:
                await self._load_and_reconcile_sources(repo, writer)
                logger.debug("Registry reloaded successfully")
            except Exception as exc:
                logger.error(
                    "Failed to reload registry",
                    error=str(exc),
                    exc_info=True,
                )

    async def _write_events(
        self, writer: Optional[EventWriter], events: list[CanonicalEvent]
    ) -> None:
        if writer is None:
            logger.debug(
                "Storage writer unavailable, events collected in memory only",
                count=len(events),
            )
            return
        try:
            result = await writer.bulk_upsert_events(events)
            logger.debug("Events written to database", count=len(events), **result)
        except Exception as exc:
            logger.error(
                "Failed to write events to database",
                error=str(exc),
                exc_info=True,
            )

    async def _record_health(
        self, repo: Optional[SourceRepository], source_id: str, *, success: bool,
        latency_ms: float = 0.0, events_count: int = 0,
    ) -> None:
        if repo is None:
            return
        try:
            if success:
                await record_success(repo, source_id, latency_ms, events_count)
            else:
                await record_failure(repo, source_id, latency_ms, "scrape_error")
        except Exception as exc:
            logger.warning(
                "Failed to update health stats",
                source_id=source_id,
                error=str(exc),
            )

    def _create_repo(self) -> SourceRepository:
        db = self._mongo_client.get_database()
        return SourceRepository(db)

    def _create_writer(self) -> EventWriter:
        return EventWriter(self._mongo_client)

    async def _wait_interval(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(
                self._stop_event.wait(), timeout=seconds
            )
        except asyncio.TimeoutError:
            pass

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._handle_signal, sig)
            except NotImplementedError:
                logger.debug(
                    "Signal handler not supported on this platform",
                    signal=sig.name,
                )

    def _handle_signal(self, sig: signal.Signals) -> None:
        logger.info("Received shutdown signal", signal=sig.name)
        self._stop_event.set()

    async def _shutdown(self, output_file: Optional[str] = None) -> None:
        logger.info("Shutting down, cancelling active tasks", active_tasks=len(self._tasks))

        for sid in list(self._tasks.keys()):
            await self._cancel_source_task(sid)

        if output_file and self._events:
            try:
                seen_hashes = set()
                unique_events = []
                for event in self._events:
                    h = event.get("content_hash")
                    if h not in seen_hashes:
                        seen_hashes.add(h)
                        unique_events.append(event)

                order_map = {
                    "hacker_news": 0,
                    "reddit_worldnews": 1,
                    "quotes_to_scrape": 2,
                    "books_to_scrape": 3,
                    "wikipedia_recent_changes": 4,
                }
                out = sorted(
                    unique_events,
                    key=lambda e: (order_map.get(e.get("source_id"), 99), e.get("scraped_at", ""))
                )
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(
                    json.dumps(out, indent=2, default=str),
                    encoding="utf-8",
                )
                logger.info(
                    "Wrote output to file",
                    path=str(output_path),
                    events_count=len(unique_events),
                )
            except Exception as write_exc:
                logger.error(
                    "Failed to write output events to file during shutdown",
                    path=output_file,
                    error=str(write_exc),
                    exc_info=True
                )

        gc.collect()
        await asyncio.sleep(0.25)

        logger.info("Orchestrator stopped")
