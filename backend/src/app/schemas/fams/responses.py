from __future__ import annotations

import re
from datetime import date
from enum import StrEnum
from typing import Any, NoReturn, TypeVar

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
)

from app.clients.fams_client import FAMSResponseContractError

StrictNumber = StrictInt | StrictFloat
ModelT = TypeVar("ModelT", bound="FAMSContractModel")


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class FaClassStatus(StrEnum):
    PLANNING = "PLANNING"
    ASSIGNED = "ASSIGNED"
    REVIEWING = "REVIEWING"
    CANCELLED = "CANCELLED"
    DECLINED = "DECLINED"
    INPROGRESS = "INPROGRESS"
    TRAINING_COMPLETED = "TRAINING_COMPLETED"
    PENDING_CLOSE = "PENDING_CLOSE"
    CLOSED = "CLOSED"


class FAMSContractModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        extra="allow",
    )


class FAMSClassTraining(FAMSContractModel):
    id: StrictInt
    site: StrictStr
    course_code: StrictStr
    course_name: StrictStr
    course_status: FaClassStatus
    attendee_type: StrictStr | None = None
    subject_type: StrictStr | None = None
    technical_group: StrictStr | None = None
    request_group: StrictStr | None = None
    request_subgroup: StrictStr | None = None
    training_program: StrictStr | None = None
    global_se: StrictStr | None = Field(default=None, alias="globalSE")
    job_recommendation: StrictStr | None = None
    format_type: StrictStr | None = None
    delivery_type: StrictStr | None = None
    supplier_partner: StrictStr | None = None
    location: StrictStr | None = None
    planned_start_date: StrictStr | None = None
    planned_end_date: StrictStr | None = None
    planned_number_of_students: StrictInt | None = None
    planned_expense: StrictNumber | None = None
    budget_code: StrictStr | None = None
    actual_start_date: StrictStr | None = None
    early_graduated_date: StrictStr | None = None
    contract_start_date: StrictStr | None = None
    contract_end_date: StrictStr | None = None
    actual_end_date: StrictStr | None = None
    actual_enrolled: StrictInt | None = None
    training_in_fa: StrictInt | None = Field(default=None, alias="trainingInFA")
    ojt_in_fsu: StrictInt | None = Field(default=None, alias="ojtInFSU")
    drop_out: StrictInt | None = None
    postponed: StrictInt | None = None
    wait_to_allocated: StrictInt | None = None
    not_allocated: StrictInt | None = None
    graduated: StrictInt | None = None
    failed: StrictInt | None = None
    allocated: StrictInt | None = None
    reserved: StrictInt | None = None
    manage_out: StrictInt | None = None
    master_trainer: StrictStr | None = None
    trainer: StrictStr | None = None
    mentor: StrictStr | None = None
    class_admin: StrictStr | None = None
    recer: StrictStr | None = None
    actual_learning_time: StrictInt | None = None
    actual_revenue: StrictNumber | None = None
    actual_expense: StrictNumber | None = None
    training_feedback: StrictNumber | None = None
    training_feedback_content: StrictNumber | None = None
    training_feedback_teacher: StrictNumber | None = None
    training_feedback_organization: StrictNumber | None = None
    nsp_point: StrictNumber | None = None
    key_program: StrictStr | None = None
    notes: StrictStr | None = None
    update_by: StrictStr | None = None
    update_date: StrictStr | None = None

    @field_validator(
        "planned_start_date",
        "planned_end_date",
        "actual_start_date",
        "early_graduated_date",
        "contract_start_date",
        "contract_end_date",
        "actual_end_date",
        "update_date",
    )
    @classmethod
    def validate_date(cls, value: str | None) -> str | None:
        _validate_iso_date(value)
        return value


class FAMSStudentInfo(FAMSContractModel):
    account: StrictStr | None
    name: StrictStr
    university: StrictStr | None = None
    faculty: StrictStr | None = None
    university_gpa: StrictNumber | None = Field(default=None, alias="universityGPA")
    dob: StrictStr | None = None
    gender: StrictStr | None = None
    email: StrictStr | None = None
    phone: StrictStr | None = None
    address: StrictStr | None = None
    facebook: StrictStr | None = None
    university_graduation_date: StrictStr | StrictInt | None = None
    full_time_working_available_date: StrictStr | None = None
    site: StrictStr
    course_code: StrictStr
    course_name: StrictStr
    attendee_type: StrictStr | None = None
    actual_start_date: StrictStr | None = None
    actual_end_date: StrictStr | None = None
    status_in_class: StrictStr
    final_grade: StrictStr | None = None
    completion_level: StrictStr | None = None
    eng_toeic_score: StrictNumber | StrictStr | None = None
    eng_communication_skill: StrictStr | None = None
    certificate_id: StrictStr | None = None
    allocation_status: StrictStr | None = None
    salary_allocated: StrictNumber | None = None
    allocated_fsu: StrictStr | None = None
    allocated_bu: StrictStr | None = None
    allocated_start_date: StrictStr | None = None
    allocated_end_date: StrictStr | None = None
    note: StrictStr | None = None
    updated_by: StrictStr | None = None
    update_date: StrictStr | None = None
    fc_update_date: StrictStr | None = None

    @field_validator(
        "dob",
        "full_time_working_available_date",
        "actual_start_date",
        "actual_end_date",
        "allocated_start_date",
        "allocated_end_date",
        "update_date",
        "fc_update_date",
    )
    @classmethod
    def validate_date(cls, value: str | None) -> str | None:
        _validate_iso_date(value)
        return value


class FAMSTrainingData(FAMSContractModel):
    class_list: list[FAMSClassTraining]
    student_list: list[FAMSStudentInfo]


class FAMSTrainingDataResponse(FAMSContractModel):
    success: StrictBool
    message: StrictStr
    error_code: StrictStr | None = None
    data: FAMSTrainingData

    @field_validator("success")
    @classmethod
    def validate_success(cls, value: bool) -> bool:
        if not value:
            raise ValueError("success must be true")
        return value


def validate_training_data(payload: Any) -> FAMSTrainingDataResponse:
    return _validate(payload, FAMSTrainingDataResponse, "Training Data")


def extra_field_paths(model: FAMSContractModel) -> list[str]:
    return sorted(_collect_extra_field_paths(model, ""))


def _collect_extra_field_paths(value: object, prefix: str) -> list[str]:
    if isinstance(value, list):
        return [
            path
            for index, item in enumerate(value)
            for path in _collect_extra_field_paths(item, f"{prefix}.{index}")
        ]
    if not isinstance(value, FAMSContractModel):
        return []

    paths = [f"{prefix}.{name}" if prefix else name for name in (value.model_extra or {})]
    for name, field in type(value).model_fields.items():
        field_value = getattr(value, name)
        alias = field.alias or name
        child_prefix = f"{prefix}.{alias}" if prefix else alias
        paths.extend(_collect_extra_field_paths(field_value, child_prefix))
    return paths


def _validate(
    payload: Any,
    model: type[ModelT],
    contract_name: str,
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
    raise FAMSResponseContractError(
        f"FAMS {contract_name} contract validation failed: {details}"
    ) from None


def _validate_iso_date(value: str | None) -> None:
    if value is None:
        return
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        raise ValueError("date must use YYYY-MM-DD format")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("date must use YYYY-MM-DD format") from exc
