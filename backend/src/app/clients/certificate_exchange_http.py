"""HTTP error handling shared by certificate exchange clients.

No automatic POST retries until the vendor's idempotency contract is confirmed.
"""

from typing import Any

import httpx


class CertificateExchangeError(RuntimeError):
    pass


async def exchange_request(
    client: httpx.AsyncClient, method: str, url: str, **kwargs: Any
) -> httpx.Response:
    try:
        response = await client.request(method, url, follow_redirects=False, timeout=10, **kwargs)
    except httpx.RequestError:
        raise CertificateExchangeError("HTTP request failed; POST was not retried") from None
    if not 200 <= response.status_code < 300:
        raise CertificateExchangeError(f"{method} failed: HTTP {response.status_code}")
    return response
