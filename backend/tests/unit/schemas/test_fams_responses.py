from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from app.clients.fams_client import FAMSResponseContractError
from app.schemas.fams import extra_field_paths, validate_training_data


def _payload() -> dict[str, Any]:
    return {
        "success": True,
        "message": "SUCCESS_GET_DATA",
        "error_code": None,
        "data": {
            "classList": [
                {
                    "id": 1024,
                    "site": "HN",
                    "courseCode": "FJ_HN_25_Java_01",
                    "courseName": "Fresher Java 2026 Batch 1",
                    "courseStatus": "INPROGRESS",
                    "plannedStartDate": "2026-01-06",
                    "actualEndDate": None,
                    "plannedExpense": 150000000.0,
                }
            ],
            "studentList": [
                {
                    "account": "anntv12",
                    "name": "Nguyen Thi An",
                    "site": "HN",
                    "courseCode": "FJ_HN_25_Java_01",
                    "courseName": "Fresher Java 2026 Batch 1",
                    "statusInClass": "InProgress",
                    "universityGPA": 3.45,
                    "dob": "2002-08-12",
                    "finalGrade": None,
                }
            ],
        },
    }


def test_training_data_contract_matches_documented_response() -> None:
    contract = validate_training_data(_payload())

    assert contract.data.class_list[0].id == 1024
    assert contract.data.class_list[0].course_status == "INPROGRESS"
    assert contract.data.student_list[0].university_gpa == 3.45
    assert contract.data.student_list[0].final_grade is None


def test_live_optional_envelope_and_graduation_date_shapes() -> None:
    payload = _payload()
    payload.pop("error_code")
    payload["data"]["studentList"][0]["universityGraduationDate"] = "2026"

    contract = validate_training_data(payload)

    assert contract.error_code is None
    assert contract.data.student_list[0].university_graduation_date == "2026"


def test_live_nullable_account_and_string_toeic_score() -> None:
    payload = _payload()
    student = payload["data"]["studentList"][0]
    student["account"] = None
    student["engToeicScore"] = "750"

    contract = validate_training_data(payload)

    assert contract.data.student_list[0].account is None
    assert contract.data.student_list[0].eng_toeic_score == "750"


@pytest.mark.parametrize(
    ("path", "value", "expected_error"),
    [
        (("success",), 1, "bool_type"),
        (("data", "classList", 0, "courseStatus"), "UNKNOWN", "enum"),
        (("data", "classList", 0, "plannedStartDate"), "20260106", "value_error"),
        (("data", "studentList", 0, "universityGPA"), "3.45", "float_type"),
    ],
)
def test_training_data_rejects_invalid_documented_fields(
    path: tuple[str | int, ...],
    value: object,
    expected_error: str,
) -> None:
    payload = deepcopy(_payload())
    target: Any = payload
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value

    with pytest.raises(FAMSResponseContractError, match=expected_error):
        validate_training_data(payload)


def test_training_data_rejects_missing_required_identity() -> None:
    payload = _payload()
    del payload["data"]["classList"][0]["id"]

    with pytest.raises(FAMSResponseContractError, match="classList.0.id:missing"):
        validate_training_data(payload)


def test_training_data_reports_additive_fields() -> None:
    payload = _payload()
    payload["newEnvelopeField"] = True
    payload["data"]["classList"][0]["newClassField"] = "new"

    contract = validate_training_data(payload)

    assert extra_field_paths(contract) == [
        "data.classList.0.newClassField",
        "newEnvelopeField",
    ]
