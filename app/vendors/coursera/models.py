from __future__ import annotations

from typing import Annotated, Any, Literal, NoReturn, TypeVar

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

from app.vendors.coursera.client import CourseraResponseContractError

NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
PositiveInt = Annotated[int, Field(strict=True, gt=0)]
StrictNumber = StrictInt | StrictFloat
ModelT = TypeVar("ModelT", bound="CourseraContractModel")


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class CourseraContractModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        extra="allow",
    )


class CourseraTokenResponse(CourseraContractModel):
    token_type: Literal["Bearer"]
    access_token: StrictStr
    grant_type: Literal["client_credentials"]
    issued_at: NonNegativeInt
    expires_in: PositiveInt

    @field_validator("access_token")
    @classmethod
    def validate_access_token(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("access_token must not be blank")
        return value


class CourseraInstructor(CourseraContractModel):
    photo_url: StrictStr
    name: StrictStr
    title: StrictStr
    department: StrictStr


class CourseraPartner(CourseraContractModel):
    name: StrictStr
    logo_url: StrictStr


class CourseraProgram(CourseraContractModel):
    content_url: StrictStr
    program_id: StrictStr


class CourseraSkill(CourseraContractModel):
    skill_name: StrictStr
    skill_id: StrictStr


class CourseraNamedTaxonomy(CourseraContractModel):
    name: StrictStr


class CourseraDomain(CourseraNamedTaxonomy):
    domain_id: StrictStr


class CourseraSubdomain(CourseraNamedTaxonomy):
    subdomain_id: StrictStr


class CourseraDomainType(CourseraContractModel):
    domain: CourseraDomain
    subdomain: CourseraSubdomain


class CourseraMetadataDefinition(CourseraContractModel):
    skills: list[CourseraSkill]
    estimated_learning_time: NonNegativeInt
    promo_photo: StrictStr
    domain_types: list[CourseraDomainType]


class CourseraExtraMetadata(CourseraContractModel):
    type_name: StrictStr
    definition: CourseraMetadataDefinition


class CourseraContent(CourseraContractModel):
    subtitle_language_codes: list[StrictStr]
    last_updated_at: NonNegativeInt
    difficulty_level: StrictStr
    content_id: StrictStr
    description: StrictStr
    language_code: StrictStr
    instructors: list[CourseraInstructor]
    partners: list[CourseraPartner]
    name: StrictStr
    programs: list[CourseraProgram]
    id: StrictStr
    extra_metadata: CourseraExtraMetadata
    content_type: StrictStr
    slug: StrictStr


class CourseraPaging(CourseraContractModel):
    next: StrictStr | StrictInt | None = None
    total: NonNegativeInt | None = None


class CourseraContentResponse(CourseraContractModel):
    elements: list[CourseraContent]
    paging: CourseraPaging
    linked: dict[str, Any]

    @model_validator(mode="after")
    def validate_total(self) -> CourseraContentResponse:
        if self.paging.total is not None and self.paging.total < len(self.elements):
            raise ValueError("paging.total must not be smaller than elements")
        return self


class CourseraEnrollment(CourseraContractModel):
    id: StrictStr
    program_id: StrictStr
    external_id: StrictStr
    content_id: StrictStr
    content_type: StrictStr
    is_completed: StrictBool
    completed_at: NonNegativeInt | None = None
    grade: StrictNumber | None = None
    last_activity_at: NonNegativeInt
    membership_state: StrictStr
    enrolled_at: NonNegativeInt
    overall_progress: StrictNumber
    approx_total_course_hrs: StrictNumber
    updated_at: NonNegativeInt
    content_name: StrictStr
    content_slug: StrictStr
    partner_names: list[StrictStr]
    content_certificate_url: StrictStr | None = None
    full_name: StrictStr
    email: StrictStr
    program_name: StrictStr
    program_slug: StrictStr
    contract_id: StrictStr
    contract_name: StrictStr
    course_type: StrictStr


class CourseraEnrollmentResponse(CourseraContractModel):
    elements: list[CourseraEnrollment]
    paging: CourseraPaging
    linked: dict[str, Any]

    @model_validator(mode="after")
    def validate_total(self) -> CourseraEnrollmentResponse:
        if self.paging.total is not None and self.paging.total < len(self.elements):
            raise ValueError("paging.total must not be smaller than elements")
        return self


def validate_token(payload: Any) -> CourseraTokenResponse:
    return _validate(payload, CourseraTokenResponse, "Token")


def validate_course_list(payload: Any) -> CourseraContentResponse:
    return _validate(payload, CourseraContentResponse, "Course List")


def validate_course_detail(
    payload: Any, *, expected_content_id: str
) -> CourseraContentResponse:
    contract = _validate(payload, CourseraContentResponse, "Course Detail")
    if len(contract.elements) != 1:
        raise CourseraResponseContractError(
            "Coursera Course Detail contract validation failed: elements must "
            "contain exactly one course"
        )
    if contract.elements[0].content_id != expected_content_id:
        raise CourseraResponseContractError(
            "Coursera Course Detail contract validation failed: contentId:mismatch"
        )
    return contract


def validate_learning_history(payload: Any) -> CourseraEnrollmentResponse:
    return _validate(payload, CourseraEnrollmentResponse, "Learning History")


def extra_field_paths(model: CourseraContractModel) -> list[str]:
    return sorted(_collect_extra_field_paths(model, ""))


def _collect_extra_field_paths(value: object, prefix: str) -> list[str]:
    if isinstance(value, list):
        return [
            path
            for index, item in enumerate(value)
            for path in _collect_extra_field_paths(item, f"{prefix}.{index}")
        ]
    if not isinstance(value, CourseraContractModel):
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
    raise CourseraResponseContractError(
        f"Coursera {contract_name} contract validation failed: {details}"
    ) from None
