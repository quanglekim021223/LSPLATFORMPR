from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, NoReturn, Self, TypeVar

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

from app.clients.skillup_client import SkillUpResponseContractError

NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
PositiveInt = Annotated[int, Field(strict=True, gt=0)]
StrictNumber = StrictInt | StrictFloat
ModelT = TypeVar("ModelT", bound="SkillUpContractModel")


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class SkillUpContractModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        extra="allow",
    )


class SkillClassification(SkillUpContractModel):
    classification_id: StrictInt
    classification_name: StrictStr


class TaxonomyNamedEntity(SkillUpContractModel):
    id: StrictInt
    name: StrictStr


class TaxonomySkillDefinition(SkillUpContractModel):
    id: StrictInt
    name: StrictStr
    description: StrictStr


class SkillTaxonomyItem(SkillUpContractModel):
    taxonomy_skill_id: StrictInt
    external_id: Any | None
    domain: TaxonomyNamedEntity
    subdomain: TaxonomyNamedEntity
    skill_cluster: TaxonomyNamedEntity
    skill_classification: SkillClassification
    skill: TaxonomySkillDefinition
    display_name: StrictStr
    description: StrictStr
    is_critical: StrictBool
    taxonomy_skill_tags: list[Any]
    skill_rubrics: Any | None


class SkillTaxonomyResponse(SkillUpContractModel):
    items: list[SkillTaxonomyItem]
    page_number: PositiveInt
    total_pages: NonNegativeInt
    total_count: NonNegativeInt
    has_previous_page: StrictBool
    has_next_page: StrictBool

    @model_validator(mode="after")
    def validate_page(self) -> Self:
        _validate_page_metadata(
            self.page_number,
            self.total_pages,
            self.total_count,
            len(self.items),
            self.has_previous_page,
            self.has_next_page,
        )
        return self


class InventorySkillDefinition(SkillUpContractModel):
    skill_id: StrictInt
    skill_name: StrictStr
    taxonomy_skill_external_id: Any | None
    modified_on: StrictStr
    skill_classification: SkillClassification

    @field_validator("modified_on")
    @classmethod
    def validate_datetime(cls, value: str) -> str:
        _validate_iso_datetime(value)
        return value


class EmployeeSkill(SkillUpContractModel):
    skill: InventorySkillDefinition
    self_validation_score: StrictNumber
    i_mocha_validation_score: StrictNumber | None = Field(alias="iMochaValidationScore")
    manager_validation_score: StrictNumber | None
    weighted_proficiency_score: StrictNumber
    multi_rater_validation_score: StrictNumber | None
    weighted_ai_inference_score: StrictNumber | None = Field(
        alias="weightedAIInferenceScore"
    )
    experience_in_months: StrictInt | None
    ai_inferred_ratings: list[Any] = Field(alias="aiInferredRatings")
    is_job_profile_skill: StrictBool
    skill_priorirty: Any | None
    skill_required_proficiency: StrictNumber
    skill_gap_in_percentage: StrictNumber


class SkillInventoryItem(SkillUpContractModel):
    employee_id: StrictInt
    external_employee_id: StrictStr
    email: StrictStr
    full_name: StrictStr
    skills: list[EmployeeSkill]


class SkillInventoryResponse(SkillUpContractModel):
    items: list[SkillInventoryItem]
    page_number: PositiveInt
    total_pages: NonNegativeInt
    total_count: NonNegativeInt
    has_previous_page: StrictBool
    has_next_page: StrictBool

    @model_validator(mode="after")
    def validate_page(self) -> Self:
        _validate_page_metadata(
            self.page_number,
            self.total_pages,
            self.total_count,
            len(self.items),
            self.has_previous_page,
            self.has_next_page,
        )
        return self


class AssessmentSection(SkillUpContractModel):
    section_id: StrictInt = Field(alias="sectionID")
    section_name: StrictStr
    no_of_que: NonNegativeInt
    section_time: NonNegativeInt
    section_time_taken: NonNegativeInt
    candidate_score: StrictNumber
    section_score: StrictNumber
    negative_mark: StrictNumber
    correct_questions: NonNegativeInt
    wrong_questions: NonNegativeInt
    skipped_questions: NonNegativeInt
    not_answered_questions: NonNegativeInt


class AssessmentReport(SkillUpContractModel):
    candidate_full_name: StrictStr
    status: StrictStr
    appeared_on: StrictStr
    test_invitation_id: StrictInt
    test_id: StrictInt
    candidate_email: StrictStr
    completed_on: StrictStr
    score: StrictNumber
    candidate_points: StrictNumber
    total_test_points: StrictNumber
    score_percentage: StrictNumber
    time_taken: NonNegativeInt
    test_duration: NonNegativeInt
    performance_category: StrictStr
    test_name: StrictStr
    pdf_report_url: StrictStr
    sections: list[AssessmentSection] | None = None

    @field_validator("appeared_on", "completed_on")
    @classmethod
    def validate_datetime(cls, value: str) -> str:
        _validate_iso_datetime(value)
        return value


class AssessmentHistoryResponse(SkillUpContractModel):
    reports: list[AssessmentReport]
    page_number: PositiveInt
    total_pages: NonNegativeInt
    total_count: NonNegativeInt
    has_previous_page: StrictBool
    has_next_page: StrictBool

    @model_validator(mode="after")
    def validate_page(self) -> Self:
        _validate_page_metadata(
            self.page_number,
            self.total_pages,
            self.total_count,
            len(self.reports),
            self.has_previous_page,
            self.has_next_page,
        )
        return self


def validate_skill_taxonomy(payload: Any) -> SkillTaxonomyResponse:
    return _validate(payload, SkillTaxonomyResponse, "Skill Taxonomy")


def validate_skill_inventory(payload: Any) -> SkillInventoryResponse:
    return _validate(payload, SkillInventoryResponse, "Skill Inventory")


def validate_assessment_history(
    payload: Any, *, require_sections: bool = False
) -> AssessmentHistoryResponse:
    contract = _validate(payload, AssessmentHistoryResponse, "Assessment History")
    if require_sections and any(report.sections is None for report in contract.reports):
        raise SkillUpResponseContractError(
            "SkillUp Assessment History contract validation failed: reports.sections:missing"
        )
    return contract


def extra_field_paths(model: SkillUpContractModel) -> list[str]:
    paths: list[str] = []
    _visit_extra_fields(model, "", paths)
    return sorted(paths)


def _visit_extra_fields(value: object, prefix: str, paths: list[str]) -> None:
    if isinstance(value, list):
        for index, item in enumerate(value):
            _visit_extra_fields(item, f"{prefix}.{index}", paths)
        return
    if not isinstance(value, SkillUpContractModel):
        return
    paths.extend(
        f"{prefix}.{name}" if prefix else name
        for name in (value.model_extra or {})
    )
    for name, field_value in value:
        field = type(value).model_fields[name]
        alias = field.alias or name
        child_prefix = f"{prefix}.{alias}" if prefix else alias
        _visit_extra_fields(field_value, child_prefix, paths)


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
    raise SkillUpResponseContractError(
        f"SkillUp {contract_name} contract validation failed: {details}"
    ) from None


def _validate_page_metadata(
    page_number: int,
    total_pages: int,
    total_count: int,
    actual_count: int,
    has_previous_page: bool,
    has_next_page: bool,
) -> None:
    if total_count < actual_count:
        raise ValueError("totalCount must not be smaller than response records")
    if has_previous_page != (page_number > 1):
        raise ValueError("hasPreviousPage is inconsistent with pageNumber")
    if total_pages > 0 and page_number > total_pages:
        raise ValueError("pageNumber must not be greater than totalPages")
    expected_has_next = total_pages > 0 and page_number < total_pages
    if has_next_page != expected_has_next:
        raise ValueError("hasNextPage is inconsistent with pageNumber and totalPages")


def _validate_iso_datetime(value: str) -> None:
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("datetime must use ISO-8601 format") from exc
