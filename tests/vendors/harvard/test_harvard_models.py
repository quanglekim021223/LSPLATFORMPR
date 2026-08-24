from __future__ import annotations

import pytest

from app.mocks.harvard import catalog_item, history_csv, token_payload
from app.vendors.harvard.models import (
    HarvardResponseContractError,
    extra_field_paths,
    validate_catalog,
    validate_history_csv,
    validate_token,
)


def test_token_contract() -> None:
    contract = validate_token(token_payload("token"))
    assert contract.access_token == "token"
    assert contract.expires_in == 3600


@pytest.mark.parametrize(
    ("product_id", "asset_type", "asset_format"),
    [
        ("186SM", "Topic", "HTML"),
        ("F1409F-PDF-ENG", "Articles", "PDF"),
    ],
)
def test_shared_catalog_contract_supports_hmm_and_spark(
    product_id: str, asset_type: str, asset_format: str
) -> None:
    item = catalog_item(product_id, "Course")
    item["AssetType"] = asset_type
    item["AssetFormat"] = asset_format
    payload = {"count": 1, "limit": 3, "list": [item], "start": 20}

    contract = validate_catalog(payload)

    assert contract.items[0].product_id == product_id
    assert contract.items[0].asset_type == asset_type


def test_missing_catalog_field_fails_contract() -> None:
    item = catalog_item("186SM", "Course")
    del item["ProductId"]

    with pytest.raises(HarvardResponseContractError, match="ProductId:missing"):
        validate_catalog({"count": 1, "limit": 3, "list": [item], "start": 0})


def test_catalog_additive_field_is_reported() -> None:
    item = catalog_item("186SM", "Course")
    item["VendorNewField"] = "new"
    payload = {
        "count": 1,
        "limit": 3,
        "list": [item],
        "start": 0,
        "newEnvelopeField": True,
    }

    contract = validate_catalog(payload)

    assert extra_field_paths(contract) == [
        "list.0.VendorNewField",
        "newEnvelopeField",
    ]


def test_hmm_csv_contract() -> None:
    assert validate_history_csv(history_csv("harvard_hmm"), "harvard_hmm") == 1


def test_spark_csv_contract() -> None:
    assert validate_history_csv(history_csv("harvard_spark"), "harvard_spark") == 1


def test_csv_header_change_fails_contract() -> None:
    invalid = history_csv("harvard_spark").replace(b"Product ID", b"ProductId", 1)

    with pytest.raises(HarvardResponseContractError, match="headers mismatch"):
        validate_history_csv(invalid, "harvard_spark")
