from __future__ import annotations

import pytest

from app.mocks.linkedin import activity_report_payload, asset_payload, token_payload
from app.vendors.linkedin.client import LinkedInResponseContractError
from app.vendors.linkedin.models import (
    extra_field_paths,
    validate_activity_reports,
    validate_learning_asset_detail,
    validate_learning_assets,
    validate_token,
)


def _paging(start: int, count: int, total: int) -> dict[str, object]:
    return {
        "start": start,
        "count": count,
        "links": [],
        "total": total,
    }


def test_token_contract() -> None:
    contract = validate_token(token_payload("token"))
    assert contract.access_token == "token"
    assert contract.expires_in == 7_776_000


def test_learning_assets_contract_accepts_recursive_contents() -> None:
    asset = asset_payload("urn:li:lyndaCourse:1", "Python")
    asset["contents"] = [
        {
            "asset": {
                "urn": "urn:li:lyndaChapter:1",
                "title": {
                    "locale": {"country": "US", "language": "en"},
                    "value": "Chapter 1",
                },
                "type": "CHAPTER",
                "contents": [],
            }
        }
    ]
    payload = {
        "paging": _paging(0, 100, 1),
        "metadata": {},
        "elements": [asset],
    }

    contract = validate_learning_assets(
        payload, expected_start=0, expected_count=100
    )

    assert contract.elements[0].contents[0].asset.type == "CHAPTER"


def test_asset_detail_requires_requested_urn() -> None:
    payload = {
        "paging": _paging(0, 100, 1),
        "metadata": {},
        "elements": [asset_payload("urn:li:lyndaCourse:1", "Python")],
    }

    with pytest.raises(LinkedInResponseContractError, match="urn:mismatch"):
        validate_learning_asset_detail(
            payload, expected_urn="urn:li:lyndaCourse:other"
        )


def test_learning_activity_report_contract() -> None:
    payload = {
        "paging": _paging(0, 1000, 1),
        "elements": [activity_report_payload(1, 1_787_122_740_000)],
    }

    contract = validate_activity_reports(
        payload, expected_start=0, expected_count=1000
    )

    assert contract.elements[0].learner_details.name == "Learner 1"
    assert contract.elements[0].activities[0].engagement_value == 1


def test_missing_required_field_fails_contract() -> None:
    asset = asset_payload("urn:li:lyndaCourse:1", "Python")
    del asset["details"]
    payload = {
        "paging": _paging(0, 100, 1),
        "metadata": {},
        "elements": [asset],
    }

    with pytest.raises(LinkedInResponseContractError, match="details:missing"):
        validate_learning_assets(payload, expected_start=0, expected_count=100)


def test_additive_fields_are_reported_by_path() -> None:
    asset = asset_payload("urn:li:lyndaCourse:1", "Python")
    asset["details"]["vendorNewField"] = "new"  # type: ignore[index]
    payload = {
        "paging": _paging(0, 100, 1),
        "metadata": {},
        "elements": [asset],
        "newEnvelopeField": True,
    }

    contract = validate_learning_assets(
        payload, expected_start=0, expected_count=100
    )

    assert extra_field_paths(contract) == [
        "elements.0.details.vendorNewField",
        "newEnvelopeField",
    ]
