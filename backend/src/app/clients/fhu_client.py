"""FHU adapter. Source path/auth/pagination remain provisional."""

import httpx
from pydantic import TypeAdapter

from app.clients.certificate_exchange_http import exchange_request
from app.schemas.fhu.responses import FHUEmployee


class FHUClient:
    def __init__(self, client: httpx.AsyncClient, base_url: str) -> None:
        self.client = client
        self.base_url = base_url.rstrip("/")

    async def list_employees(self) -> tuple[list[FHUEmployee], bytes]:
        response = await exchange_request(self.client, "GET", f"{self.base_url}/employees")
        records = TypeAdapter(list[FHUEmployee]).validate_python(response.json()["items"])
        return records, response.content
