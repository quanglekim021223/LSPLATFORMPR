from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, NoReturn, Protocol, Self, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictStr,
    ValidationError,
    field_validator,
)

from app.config import Settings

NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
PositiveInt = Annotated[int, Field(strict=True, gt=0)]
ModelT = TypeVar("ModelT", bound="HarvardContractModel")


class HarvardResponseContractError(RuntimeError):
    pass


class HarvardContractModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class HarvardTokenResponse(HarvardContractModel):
    access_token: StrictStr
    expires_in: PositiveInt

    @field_validator("access_token")
    @classmethod
    def validate_access_token(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("access_token must not be blank")
        return value


class HarvardCatalogItem(HarvardContractModel):
    product_id: StrictStr = Field(alias="ProductId")
    asset_type: StrictStr = Field(alias="AssetType")
    asset_format: StrictStr = Field(alias="AssetFormat")
    title: StrictStr = Field(alias="Title")
    description: StrictStr = Field(alias="Description")
    url: StrictStr = Field(alias="URL")
    image_url: StrictStr = Field(alias="ImageURL")
    authors: StrictStr = Field(alias="Authors")
    duration: StrictStr = Field(alias="Duration")
    language: StrictStr = Field(alias="Language")
    publication_date: StrictStr = Field(alias="PublicationDate")
    copyright_holder: StrictStr = Field(alias="CopyrightHolder")
    subject_tags: StrictStr = Field(alias="SubjectTags")
    status: StrictStr = Field(alias="Status")
    major_discipline: StrictStr = Field(alias="MajorDiscipline")
    series: StrictStr = Field(alias="Series")
    skills: StrictStr = Field(alias="Skills")
    last_modified_date: StrictStr = Field(alias="LastModifiedDate")


class HarvardCatalogResponse(HarvardContractModel):
    count: NonNegativeInt
    limit: PositiveInt
    items: list[HarvardCatalogItem] = Field(alias="list")
    start: NonNegativeInt


class HarvardCSVModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HarvardHMMHistoryRow(HarvardCSVModel):
    event_date: StrictStr = Field(alias="EventDate", pattern=r"^\d{8}$")
    username: StrictStr = Field(alias="Username")
    first_name: StrictStr = Field(alias="FirstName")
    last_name: StrictStr = Field(alias="LastName")
    email: StrictStr = Field(alias="Email")
    event_name: StrictStr = Field(alias="EventName")
    title: StrictStr = Field(alias="Title")
    product: StrictStr = Field(alias="Product")


class HarvardSparkHistoryRow(HarvardCSVModel):
    event_date: StrictStr = Field(alias="Event Date", pattern=r"^\d{4}-\d{2}-\d{2}$")
    username: StrictStr = Field(alias="Username")
    first_name: StrictStr = Field(alias="First Name")
    last_name: StrictStr = Field(alias="Last Name")
    email: StrictStr = Field(alias="Email")
    role: StrictStr = Field(alias="Role")
    event_name: StrictStr = Field(alias="Event Name")
    title: StrictStr = Field(alias="Title")
    asset_type: StrictStr = Field(alias="Asset Type")
    product_id: StrictStr = Field(alias="Product ID")
    skills: StrictStr = Field(alias="Skills")
    duration: StrictStr = Field(alias="Duration")
    registration_date: StrictStr = Field(
        alias="Registration Date", pattern=r"^\d{4}-\d{2}-\d{2}$"
    )


def validate_token(payload: Any) -> HarvardTokenResponse:
    return _validate(payload, HarvardTokenResponse, "Token")


def validate_catalog(payload: Any) -> HarvardCatalogResponse:
    contract = _validate(payload, HarvardCatalogResponse, "Catalog")
    if contract.count < len(contract.items):
        raise HarvardResponseContractError(
            "Harvard Catalog contract validation failed: count:smaller_than_list"
        )
    return contract


def validate_history_csv(payload: bytes, vendor: str) -> int:
    model: type[HarvardCSVModel]
    if vendor == "harvard_hmm":
        model = HarvardHMMHistoryRow
    elif vendor == "harvard_spark":
        model = HarvardSparkHistoryRow
    else:
        raise ValueError(f"Unsupported Harvard vendor: {vendor}")

    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HarvardResponseContractError(
            "Harvard Learning History contract validation failed: invalid UTF-8"
        ) from exc
    reader = csv.DictReader(io.StringIO(text, newline=""))
    expected_headers = [field.alias or name for name, field in model.model_fields.items()]
    if reader.fieldnames != expected_headers:
        raise HarvardResponseContractError(
            "Harvard Learning History contract validation failed: CSV headers mismatch"
        )

    records_count = 0
    for row_number, row in enumerate(reader, start=2):
        if None in row or any(value is None for value in row.values()):
            raise HarvardResponseContractError(
                "Harvard Learning History contract validation failed: "
                f"row {row_number}:column count mismatch"
            )
        try:
            model.model_validate(row)
        except ValidationError as exc:
            _raise_contract_error(exc, f"Learning History row {row_number}")
        records_count += 1
    return records_count


def extra_field_paths(model: HarvardContractModel) -> list[str]:
    return sorted(_collect_extra_field_paths(model, ""))


def _collect_extra_field_paths(value: object, prefix: str) -> list[str]:
    if isinstance(value, list):
        return [
            path
            for index, item in enumerate(value)
            for path in _collect_extra_field_paths(item, f"{prefix}.{index}")
        ]
    if not isinstance(value, HarvardContractModel):
        return []
    paths = [
        f"{prefix}.{name}" if prefix else name
        for name in (value.model_extra or {})
    ]
    for name, field in type(value).model_fields.items():
        alias = field.alias or name
        child_prefix = f"{prefix}.{alias}" if prefix else alias
        paths.extend(_collect_extra_field_paths(getattr(value, name), child_prefix))
    return paths


def _validate(payload: Any, model: type[ModelT], contract_name: str) -> ModelT:
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        _raise_contract_error(exc, contract_name)


def _raise_contract_error(exc: ValidationError, contract_name: str) -> NoReturn:
    details = ", ".join(
        f"{'.'.join(str(part) for part in error['loc'])}:{error['type']}"
        for error in exc.errors(include_input=False, include_url=False)
    )
    raise HarvardResponseContractError(
        f"Harvard {contract_name} contract validation failed: {details}"
    ) from None


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
