from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, NoReturn, Self

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

from app.vendors.levelup.client import ResponseContractError

NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
PositiveInt = Annotated[int, Field(strict=True, gt=0)]


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class LevelUpContractModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        extra="allow",
    )


class ExpireDuration(LevelUpContractModel):
    years: NonNegativeInt
    months: NonNegativeInt
    days: NonNegativeInt
    hours: NonNegativeInt


class LevelUpCourse(LevelUpContractModel):
    id: StrictStr
    course_type: StrictStr
    name: StrictStr
    description: StrictStr
    notes: Any | None
    external_id: Any | None
    access_date: Any | None
    expire_type: StrictInt
    expire_duration: ExpireDuration
    expiry_date: Any | None
    active_status: StrictInt
    tag_ids: list[StrictStr]
    resource_ids: list[StrictStr]
    editor_ids: list[StrictStr]
    prices: list[Any]
    competency_definition_ids: list[StrictStr]
    prerequisite_course_ids: list[StrictStr]
    post_enrollment_course_ids: list[StrictStr]
    allow_course_evaluation: StrictBool
    category_id: StrictStr
    certificate_url: StrictStr | None
    audience: Any | None
    goals: Any | None
    vendor: Any | None
    company_cost: Any | None
    learner_cost: Any | None
    company_time: Any | None
    learner_time: Any | None
    date_edited: StrictStr
    date_added: StrictStr

    @field_validator("date_edited", "date_added")
    @classmethod
    def validate_datetime(cls, value: str) -> str:
        _validate_iso_datetime(value)
        return value


class LevelUpEnrollment(LevelUpContractModel):
    id: StrictStr
    course_id: StrictStr
    course_name: StrictStr
    progress: StrictFloat
    score: StrictFloat
    status: StrictInt
    date_completed: StrictStr | None
    date_expires: StrictStr | None
    full_name: StrictStr
    job_title: StrictStr
    course_version_id: Any | None
    user_id: StrictStr
    accepted_terms_and_conditions: StrictBool
    time_spent: StrictStr
    date_started: StrictStr
    enrollment_key_id: Any | None
    certificate_id: StrictStr | None
    credits: Any | None
    is_active: StrictBool
    date_due: StrictStr | None
    date_edited: StrictStr
    date_added: StrictStr

    @field_validator(
        "date_completed",
        "date_expires",
        "date_started",
        "date_due",
        "date_edited",
        "date_added",
    )
    @classmethod
    def validate_datetime(cls, value: str | None) -> str | None:
        if value is not None:
            _validate_iso_datetime(value)
        return value


class LevelUpCourseListResponse(LevelUpContractModel):
    total_items: NonNegativeInt
    returned_items: NonNegativeInt
    limit: PositiveInt
    offset: NonNegativeInt
    courses: list[LevelUpCourse]

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        _validate_page_counts(self.total_items, self.returned_items, len(self.courses))
        return self


class LevelUpEnrollmentResponse(LevelUpContractModel):
    total_items: NonNegativeInt
    returned_items: NonNegativeInt
    limit: PositiveInt
    offset: NonNegativeInt
    enrollments: list[LevelUpEnrollment]

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        _validate_page_counts(
            self.total_items,
            self.returned_items,
            len(self.enrollments),
        )
        return self


def validate_course_list(payload: Any) -> LevelUpCourseListResponse:
    try:
        return LevelUpCourseListResponse.model_validate(payload)
    except ValidationError as exc:
        _raise_contract_error(exc, "Course List")


def validate_enrollments(payload: Any) -> LevelUpEnrollmentResponse:
    try:
        return LevelUpEnrollmentResponse.model_validate(payload)
    except ValidationError as exc:
        _raise_contract_error(exc, "Enrollments")


def extra_field_paths(model: LevelUpContractModel) -> list[str]:
    paths = list(model.model_extra or {})
    if isinstance(model, LevelUpCourseListResponse):
        for index, course in enumerate(model.courses):
            paths.extend(_nested_extra_paths(f"courses.{index}", course))
            paths.extend(
                _nested_extra_paths(
                    f"courses.{index}.expireDuration",
                    course.expire_duration,
                )
            )
    elif isinstance(model, LevelUpEnrollmentResponse):
        for index, enrollment in enumerate(model.enrollments):
            paths.extend(_nested_extra_paths(f"enrollments.{index}", enrollment))
    return sorted(paths)


def _nested_extra_paths(prefix: str, model: LevelUpContractModel) -> list[str]:
    return [f"{prefix}.{name}" for name in (model.model_extra or {})]


def _raise_contract_error(exc: ValidationError, contract_name: str) -> NoReturn:
    details = ", ".join(
        f"{'.'.join(str(part) for part in error['loc'])}:{error['type']}"
        for error in exc.errors(include_input=False, include_url=False)
    )
    raise ResponseContractError(
        f"LevelUP {contract_name} contract validation failed: {details}"
    ) from None


def _validate_page_counts(total: int, returned: int, actual: int) -> None:
    if returned != actual:
        raise ValueError("returnedItems must equal the number of response records")
    if total < returned:
        raise ValueError("totalItems must not be smaller than returnedItems")


def _validate_iso_datetime(value: str) -> None:
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("datetime must use ISO-8601 format") from exc
