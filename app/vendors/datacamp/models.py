from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, NoReturn, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
    model_validator,
)

from app.vendors.datacamp.client import DataCampResponseContractError

NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
PositiveInt = Annotated[int, Field(strict=True, gt=0)]
EventPageSize = Annotated[int, Field(strict=True, ge=1, le=1000)]
StrictNumber = StrictInt | StrictFloat
ModelT = TypeVar("ModelT", bound="DataCampContractModel")


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class DataCampContractModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        extra="allow",
    )


class DataCampImageUrl(DataCampContractModel):
    jpg: StrictStr
    png: StrictStr
    svg: StrictStr


class DataCampInstructor(DataCampContractModel):
    full_name: StrictStr


class DataCampTopic(DataCampContractModel):
    name: StrictStr
    description: StrictStr | None


class DataCampChapter(DataCampContractModel):
    id: StrictStr
    description: StrictStr
    title: StrictStr
    url: StrictStr


class DataCampCourse(DataCampContractModel):
    id: StrictStr
    title: StrictStr
    description: StrictStr
    url: StrictStr
    image_url: DataCampImageUrl
    technology: StrictStr | None
    instructors: list[DataCampInstructor]
    time_needed_in_hours: StrictNumber
    topic: DataCampTopic | None
    updated_at: StrictStr
    live: StrictBool
    chapters: list[DataCampChapter]
    info_url: StrictStr
    public_info_url: StrictStr
    included_in_licenses: list[Any]

    @field_validator("updated_at")
    @classmethod
    def validate_datetime(cls, value: str) -> str:
        _validate_iso_datetime(value)
        return value


class DataCampCatalogResponse(DataCampContractModel):
    data: list[DataCampCourse]


class DataCampEventUser(DataCampContractModel):
    email: StrictStr
    nameid: StrictStr
    lms_username: StrictStr | None


class DataCampEvent(DataCampContractModel):
    event_type: StrictStr
    content_id: StrictStr
    timestamp: StrictStr
    parent_content_id: StrictStr | None
    user: DataCampEventUser
    assessment_score: Any | None
    knowledge_level: Any | None

    @field_validator("timestamp")
    @classmethod
    def validate_datetime(cls, value: str) -> str:
        _validate_iso_datetime(value)
        return value


class DataCampEventsMeta(DataCampContractModel):
    page: PositiveInt
    page_size: EventPageSize
    number_of_pages: NonNegativeInt


class DataCampEventsResponse(DataCampContractModel):
    data: list[DataCampEvent]
    meta: DataCampEventsMeta

    @model_validator(mode="after")
    def validate_page_size(self) -> DataCampEventsResponse:
        if len(self.data) > self.meta.page_size:
            raise ValueError("events data must not exceed meta.pageSize")
        if (
            self.meta.number_of_pages > 0
            and self.meta.page > self.meta.number_of_pages
        ):
            raise ValueError("meta.page must not be greater than meta.numberOfPages")
        return self


def validate_live_catalog(payload: Any) -> DataCampCatalogResponse:
    contract = _validate(payload, DataCampCatalogResponse, "Live Course Catalog")
    if any(not course.live for course in contract.data):
        raise DataCampResponseContractError(
            "DataCamp Live Course Catalog contract validation failed: data.live:false"
        )
    return contract


def validate_archived_catalog(payload: Any) -> DataCampCatalogResponse:
    contract = _validate(payload, DataCampCatalogResponse, "Archived Course Catalog")
    if any(course.live for course in contract.data):
        raise DataCampResponseContractError(
            "DataCamp Archived Course Catalog contract validation failed: data.live:true"
        )
    return contract


def validate_events(
    payload: Any, *, expected_page: int, expected_page_size: int
) -> DataCampEventsResponse:
    contract = _validate(payload, DataCampEventsResponse, "Learning History Events")
    if contract.meta.page != expected_page:
        raise DataCampResponseContractError(
            "DataCamp Learning History Events contract validation failed: meta.page:mismatch"
        )
    if contract.meta.page_size != expected_page_size:
        raise DataCampResponseContractError(
            "DataCamp Learning History Events contract validation failed: meta.pageSize:mismatch"
        )
    return contract


def extra_field_paths(model: DataCampContractModel) -> list[str]:
    return sorted(_collect_extra_field_paths(model, ""))


def _collect_extra_field_paths(value: object, prefix: str) -> list[str]:
    if isinstance(value, list):
        return [
            path
            for index, item in enumerate(value)
            for path in _collect_extra_field_paths(item, f"{prefix}.{index}")
        ]
    if not isinstance(value, DataCampContractModel):
        return []

    paths = [
        f"{prefix}.{name}" if prefix else name
        for name in (value.model_extra or {})
    ]
    for name, field in type(value).model_fields.items():
        field_value = getattr(value, name)
        alias = field.alias or name
        child_prefix = f"{prefix}.{alias}" if prefix else alias
        paths.extend(_collect_extra_field_paths(field_value, child_prefix))
    return paths


def _validate(
    payload: Any, model: type[ModelT], contract_name: str
) -> ModelT:
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        _raise_contract_error(exc, contract_name)


def _raise_contract_error(exc: ValidationError, contract_name: str) -> NoReturn:
    details = ", ".join(
        f"{'.'.join(str(part) for part in error['loc'])}:{error['type']}"
        for error in exc.errors(include_input=False, include_url=False)
    )
    raise DataCampResponseContractError(
        f"DataCamp {contract_name} contract validation failed: {details}"
    ) from None


def _validate_iso_datetime(value: str) -> None:
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("datetime must use ISO-8601 format") from exc
