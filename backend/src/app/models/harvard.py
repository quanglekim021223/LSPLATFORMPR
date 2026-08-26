from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, Self

from app.core.config import Settings


@dataclass(frozen=True, slots=True)
class HarvardVendorConfig:
    vendor: str
    display_name: str
    catalog_code: str
    client_id: str
    client_secret: str
    org_key: str
    report_filename_prefix: str

    def sensitive_values(self) -> tuple[str, ...]:
        return tuple(value for value in (self.client_id, self.client_secret) if value)


@dataclass(frozen=True, slots=True)
class RemoteFile:
    remote_path: str
    file_name: str
    content: bytes
    size: int
    modified_at: datetime


class SFTPTransport(Protocol):
    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None: ...

    async def fetch(self, remote_path: str) -> RemoteFile | None: ...


def vendor_config(settings: Settings, vendor: str) -> HarvardVendorConfig:
    if vendor == "harvard_hmm":
        return HarvardVendorConfig(
            vendor=vendor,
            display_name="Harvard HMM",
            catalog_code="HMM",
            client_id=settings.harvard_hmm_client_id.get_secret_value(),
            client_secret=settings.harvard_hmm_client_secret.get_secret_value(),
            org_key=settings.harvard_hmm_org_key,
            report_filename_prefix="harvard_hmm_reporting_",
        )
    if vendor == "harvard_spark":
        return HarvardVendorConfig(
            vendor=vendor,
            display_name="Harvard Spark",
            catalog_code="HBR_SPARK",
            client_id=settings.harvard_spark_client_id.get_secret_value(),
            client_secret=settings.harvard_spark_client_secret.get_secret_value(),
            org_key=settings.harvard_spark_org_key,
            report_filename_prefix="harvard_Spark_reporting_",
        )
    raise ValueError(f"Unsupported Harvard vendor: {vendor}")
