from __future__ import annotations

import pytest

from app.mocks.coursera import course_payload, enrollment_payload, token_payload
from app.vendors.coursera.client import CourseraResponseContractError
from app.vendors.coursera.models import (
    extra_field_paths,
    validate_course_detail,
    validate_course_list,
    validate_learning_history,
    validate_token,
)


def content_response(*content_ids: str) -> dict[str, object]:
    return {
        "elements": [
            course_payload(content_id, f"Course {content_id}")
            for content_id in content_ids
        ],
        "paging": {"next": "2", "total": 27407},
        "linked": {},
    }


def test_token_contract_matches_supplied_response() -> None:
    contract = validate_token(token_payload("abc"))

    assert contract.token_type == "Bearer"
    assert contract.access_token == "abc"
    assert contract.grant_type == "client_credentials"
    assert contract.issued_at == 1787213698
    assert contract.expires_in == 1799


def test_course_list_and_detail_contracts() -> None:
    course_list = validate_course_list(content_response("course-1", "course-2"))
    detail_payload = content_response("course-1")
    detail_payload["paging"] = {}
    detail = validate_course_detail(
        detail_payload, expected_content_id="course-1"
    )

    assert [item.content_id for item in course_list.elements] == [
        "course-1",
        "course-2",
    ]
    assert course_list.paging.next == "2"
    assert detail.elements[0].content_id == "course-1"


def test_incomplete_enrollment_accepts_absent_completion_fields() -> None:
    payload = {
        "elements": [
            enrollment_payload("enrollment-1", "course-1", completed=False)
        ],
        "paging": {"next": "52", "total": 22597},
        "linked": {},
    }

    contract = validate_learning_history(payload)

    enrollment = contract.elements[0]
    assert enrollment.completed_at is None
    assert enrollment.grade is None
    assert enrollment.content_certificate_url is None


def test_missing_required_course_field_fails_contract() -> None:
    payload = content_response("course-1")
    del payload["elements"][0]["languageCode"]  # type: ignore[index]

    with pytest.raises(CourseraResponseContractError, match="languageCode:missing"):
        validate_course_list(payload)


def test_course_detail_content_id_must_match_request() -> None:
    with pytest.raises(CourseraResponseContractError, match="contentId:mismatch"):
        validate_course_detail(
            content_response("different-course"),
            expected_content_id="requested-course",
        )


def test_additive_nested_fields_are_reported_by_path() -> None:
    payload = content_response("course-1")
    first = payload["elements"][0]  # type: ignore[index]
    first["newCourseField"] = True
    first["extraMetadata"]["definition"]["newMetadataField"] = "new"  # type: ignore[index]

    contract = validate_course_list(payload)

    assert extra_field_paths(contract) == [
        "elements.0.extraMetadata.definition.newMetadataField",
        "elements.0.newCourseField",
    ]
