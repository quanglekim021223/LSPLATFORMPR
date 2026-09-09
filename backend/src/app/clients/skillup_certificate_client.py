"""SkillUp certificate write/read adapter; no assumptions about source systems."""

from urllib.parse import quote

import httpx
from pydantic import SecretStr

from app.clients.certificate_exchange_http import CertificateExchangeError, exchange_request
from app.schemas.skillup.certificates import CreateCertificate, EmployeeCertificate
from app.schemas.skillup.responses import SkillInventoryResponse


class SkillUpCertificateClient:
    def __init__(self, client: httpx.AsyncClient, base_url: str, api_key: SecretStr) -> None:
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    async def create_certificate(self, certificate: CreateCertificate) -> bytes:
        response = await exchange_request(
            self.client,
            "POST",
            f"{self.base_url}/certificates/create",
            headers={"x-api-key": self.api_key.get_secret_value()},
            json=certificate.model_dump(by_alias=True),
        )
        if response.status_code != 200:
            raise CertificateExchangeError("Create certificate did not return documented HTTP 200")
        CreateCertificate.model_validate(response.json())
        return response.content

    async def upload_certificates(
        self, employee_id: str, certificates: list[EmployeeCertificate]
    ) -> bytes:
        response = await exchange_request(
            self.client,
            "POST",
            f"{self.base_url}/employees/{quote(employee_id, safe='')}/certificates",
            headers={"x-api-key": self.api_key.get_secret_value()},
            json=[item.model_dump(by_alias=True, exclude_none=True) for item in certificates],
        )
        if response.status_code != 201 or not isinstance(response.json(), dict):
            raise CertificateExchangeError(
                "Upload certificate did not return HTTP 201 with an object"
            )
        return response.content

    async def get_skill_profiles(self, page: int) -> tuple[SkillInventoryResponse, bytes]:
        response = await exchange_request(
            self.client,
            "GET",
            f"{self.base_url}/employees/skills-profile",
            headers={"x-api-key": self.api_key.get_secret_value()},
            params={"pageNumber": page, "pageSize": 100},
        )
        inventory = SkillInventoryResponse.model_validate(response.json())
        if inventory.page_number != page:
            raise CertificateExchangeError("SkillUp returned an unexpected page number")
        return inventory, response.content
