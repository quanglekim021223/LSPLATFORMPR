"""Akajob adapter. Source path/auth/pagination remain provisional."""

import httpx
from pydantic import TypeAdapter

from app.clients.certificate_exchange_http import exchange_request
from app.schemas.akajob.responses import AkajobCertificate


class AkajobClient:
    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self.client = client
        self.base_url = base_url.rstrip("/")

    async def list_certificates(self) -> tuple[list[AkajobCertificate], bytes]:
        response = await exchange_request(self.client, "GET", f"{self.base_url}/certificates")
        records = TypeAdapter(list[AkajobCertificate]).validate_python(response.json()["items"])
        return records, response.content
