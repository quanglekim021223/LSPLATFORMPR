from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import urlsplit

import httpx

from app.core.retry import backoff_seconds, retry_after_seconds

logger = logging.getLogger(__name__)


class RetryingHttpClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        max_retries: int,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self.client = client
        self.max_retries = max_retries
        self.sleep = sleep
        self.jitter = jitter

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        endpoint = urlsplit(url).path or "/"
        for retry_number in range(self.max_retries + 1):
            attempt = retry_number + 1
            started_at = time.perf_counter()
            try:
                response = await self.client.request(method, url, **kwargs)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                duration_ms = (time.perf_counter() - started_at) * 1000
                if retry_number >= self.max_retries:
                    logger.error(
                        "HTTP request failed method=%s endpoint=%s attempt=%d "
                        "duration_ms=%.1f error_type=%s",
                        method.upper(),
                        endpoint,
                        attempt,
                        duration_ms,
                        type(exc).__name__,
                    )
                    raise
                wait_seconds = self._backoff_seconds(retry_number)
                logger.warning(
                    "Retrying HTTP request method=%s endpoint=%s attempt=%d "
                    "duration_ms=%.1f error_type=%s wait_seconds=%.3f",
                    method.upper(),
                    endpoint,
                    attempt,
                    duration_ms,
                    type(exc).__name__,
                    wait_seconds,
                )
                await self.sleep(wait_seconds)
                continue

            duration_ms = (time.perf_counter() - started_at) * 1000
            logger.debug(
                "HTTP response received method=%s endpoint=%s status_code=%d "
                "attempt=%d duration_ms=%.1f",
                method.upper(),
                endpoint,
                response.status_code,
                attempt,
                duration_ms,
            )
            if response.status_code == 429 or response.status_code >= 500:
                if retry_number < self.max_retries:
                    retry_after = self._retry_after_seconds(response.headers)
                    wait_seconds = (
                        retry_after
                        if retry_after is not None
                        else self._backoff_seconds(retry_number)
                    )
                    logger.warning(
                        "Retrying HTTP request method=%s endpoint=%s attempt=%d "
                        "status_code=%d wait_seconds=%.3f",
                        method.upper(),
                        endpoint,
                        attempt,
                        response.status_code,
                        wait_seconds,
                    )
                    await self.sleep(wait_seconds)
                    continue
            return response
        raise AssertionError("retry loop exhausted unexpectedly")

    def _backoff_seconds(self, retry_number: int) -> float:
        return backoff_seconds(retry_number, self.jitter)

    @staticmethod
    def _retry_after_seconds(headers: Mapping[str, str]) -> float | None:
        return retry_after_seconds(headers)
