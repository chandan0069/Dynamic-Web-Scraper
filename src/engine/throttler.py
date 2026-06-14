from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field

from src.config import get_logger

logger = get_logger(__name__, component="throttler")


@dataclass
class _DomainState:
    last_refill: float = 0.0
    rate_per_second: float = 1.0
    backoff_until: float = 0.0
    consecutive_blocks: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    request_count: int = 0
    block_count: int = 0
    total_latency: float = 0.0


class AdaptiveThrottler:

    def __init__(self) -> None:
        self._domains: dict[str, _DomainState] = {}

    def _get_state(self, domain: str) -> _DomainState:
        if domain not in self._domains:
            self._domains[domain] = _DomainState()
        return self._domains[domain]

    async def acquire(self, domain: str) -> None:
        state = self._get_state(domain)
        async with state.lock:
            now = time.monotonic()
            if state.backoff_until > now:
                wait_secs = state.backoff_until - now
                logger.info("Rate limited, waiting before next request", domain=domain, wait_seconds=round(wait_secs, 2))
                await asyncio.sleep(wait_secs)
                now = time.monotonic()

            delay = 1.0 / state.rate_per_second
            elapsed = now - state.last_refill
            if elapsed < delay:
                await asyncio.sleep(delay - elapsed)

            state.last_refill = time.monotonic()
            state.request_count += 1

    def record_response(self, domain: str, status_code: int) -> None:
        state = self._get_state(domain)
        if 200 <= status_code < 400:
            old_rate = state.rate_per_second
            state.rate_per_second = min(state.rate_per_second * 1.05, 10.0)
            state.consecutive_blocks = 0
            if state.rate_per_second != old_rate:
                logger.info(
                    "Rate limit increased after successful response",
                    domain=domain,
                    old_rate=round(old_rate, 3),
                    new_rate=round(state.rate_per_second, 3),
                    status_code=status_code,
                )
        elif status_code in {429, 503}:
            old_rate = state.rate_per_second
            state.rate_per_second = max(state.rate_per_second * 0.5, 0.1)
            state.consecutive_blocks += 1
            state.block_count += 1
            delay = min(1.0 * (2 ** state.consecutive_blocks) + random.uniform(0, 1.0), 120.0)
            state.backoff_until = time.monotonic() + delay
            logger.warning(
                "Rate limit decreased due to throttling response",
                domain=domain,
                old_rate=round(old_rate, 3),
                new_rate=round(state.rate_per_second, 3),
                consecutive_blocks=state.consecutive_blocks,
                backoff_seconds=round(delay, 2),
                status_code=status_code,
            )

    def record_latency(self, domain: str, latency: float) -> None:
        state = self._get_state(domain)
        state.total_latency += latency

