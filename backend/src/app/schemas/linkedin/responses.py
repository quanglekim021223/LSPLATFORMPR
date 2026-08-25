from __future__ import annotations

from typing import Annotated, Any, NoReturn, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
)

from app.clients.linkedin_client import LinkedInResponseContractError

NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
PositiveInt = Annotated[int, Field(strict=True, gt=0)]
PageCount = Annotated[int, Field(strict=True, ge=1, le=1000)]
ModelT = TypeVar("ModelT", bound="LinkedInContractModel")


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class LinkedInContractModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        extra="allow",
    )


class LinkedInTokenResponse(LinkedInContractModel):
    access_token: StrictStr
    expires_in: PositiveInt

    @field_validator("access_token")
    @classmethod
    def validate_access_token(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("access_token must not be blank")
        return value


class LinkedInLocale(LinkedInContractModel):
    country: StrictStr
    language: StrictStr


class LinkedInLocalizedText(LinkedInContractModel):
    locale: LinkedInLocale
    value: StrictStr


class LinkedInPagingLink(LinkedInContractModel):
    type: StrictStr
    rel: StrictStr
    href: StrictStr


class LinkedInPaging(LinkedInContractModel):
    start: NonNegativeInt
    count: PageCount
    links: list[LinkedInPagingLink]
    total: NonNegativeInt


class LinkedInImages(LinkedInContractModel):
    primary: StrictStr


class LinkedInAccessor(LinkedInContractModel):
    name: LinkedInLocalizedText
    urn: StrictStr


class LinkedInDiscoverableBy(LinkedInContractModel):
    accessor: LinkedInAccessor


class LinkedInClassificationOwner(LinkedInContractModel):
    urn: StrictStr
    name: LinkedInLocalizedText


class LinkedInClassificationPathNode(LinkedInContractModel):
    owner: LinkedInClassificationOwner
    name: LinkedInLocalizedText
    urn: StrictStr
    type: StrictStr


class LinkedInAssociatedClassification(LinkedInContractModel):
    owner: LinkedInClassificationOwner
    name: LinkedInLocalizedText
    urn: StrictStr
    type: StrictStr


class LinkedInClassification(LinkedInContractModel):
    assigner: LinkedInClassificationOwner
    path: list[LinkedInClassificationPathNode]
    associated_classification: LinkedInAssociatedClassification


class LinkedInUrls(LinkedInContractModel):
    sso_launch: StrictStr
    web_launch: StrictStr
    aicc_launch: StrictStr


class LinkedInAuthorDetails(LinkedInContractModel):
    last_name: LinkedInLocalizedText
    first_name: LinkedInLocalizedText


class LinkedInContributor(LinkedInContractModel):
    name: LinkedInLocalizedText
    urn: StrictStr
    contribution_type: StrictStr
    author_details: LinkedInAuthorDetails


class LinkedInTimeToComplete(LinkedInContractModel):
    duration: NonNegativeInt
    unit: StrictStr


class LinkedInDetails(LinkedInContractModel):
    images: LinkedInImages
    description_including_html: LinkedInLocalizedText
    last_updated_at: NonNegativeInt
    published_at: NonNegativeInt
    discoverable_by: list[LinkedInDiscoverableBy]
    description: LinkedInLocalizedText
    short_description: LinkedInLocalizedText
    availability: StrictStr
    available_locales: list[LinkedInLocale]
    relationships: list[Any]
    classifications: list[LinkedInClassification]
    urls: LinkedInUrls
    short_description_including_html: LinkedInLocalizedText
    contributors: list[LinkedInContributor]
    time_to_complete: LinkedInTimeToComplete


class LinkedInNestedAsset(LinkedInContractModel):
    urn: StrictStr
    title: LinkedInLocalizedText
    type: StrictStr
    contents: list[LinkedInContentItem]


class LinkedInContentItem(LinkedInContractModel):
    asset: LinkedInNestedAsset


LinkedInNestedAsset.model_rebuild()


class LinkedInLearningAsset(LinkedInContractModel):
    urn: StrictStr
    details: LinkedInDetails
    title: LinkedInLocalizedText
    type: StrictStr
    contents: list[LinkedInContentItem]


class LinkedInLearningAssetsResponse(LinkedInContractModel):
    paging: LinkedInPaging
    metadata: dict[str, Any]
    elements: list[LinkedInLearningAsset]


class LinkedInLearnerEntity(LinkedInContractModel):
    profile_urn: StrictStr


class LinkedInLearnerDetails(LinkedInContractModel):
    name: StrictStr
    enterprise_groups: list[StrictStr]
    entity: LinkedInLearnerEntity
    email: StrictStr
    custom_attributes: dict[str, Any]
    unique_user_id: StrictStr


class LinkedInActivity(LinkedInContractModel):
    engagement_type: StrictStr
    last_engaged_at: NonNegativeInt
    first_engaged_at: NonNegativeInt
    asset_type: StrictStr
    engagement_metric_qualifier: StrictStr
    engagement_value: StrictInt


class LinkedInContentDetails(LinkedInContractModel):
    name: StrictStr
    content_provider_name: StrictStr
    content_urn: StrictStr
    locale: LinkedInLocale


class LinkedInActivityReport(LinkedInContractModel):
    latest_data_at: NonNegativeInt
    learner_details: LinkedInLearnerDetails
    activities: list[LinkedInActivity]
    content_details: LinkedInContentDetails


class LinkedInActivityReportsResponse(LinkedInContractModel):
    paging: LinkedInPaging
    elements: list[LinkedInActivityReport]


def validate_token(payload: Any) -> LinkedInTokenResponse:
    return _validate(payload, LinkedInTokenResponse, "Token")


def validate_learning_assets(
    payload: Any, *, expected_start: int, expected_count: int
) -> LinkedInLearningAssetsResponse:
    contract = _validate(payload, LinkedInLearningAssetsResponse, "Learning Assets")
    _validate_paging(
        contract.paging,
        len(contract.elements),
        expected_start=expected_start,
        expected_count=expected_count,
        contract_name="Learning Assets",
    )
    return contract


def validate_learning_asset_detail(
    payload: Any, *, expected_urn: str
) -> LinkedInLearningAssetsResponse:
    contract = _validate(payload, LinkedInLearningAssetsResponse, "Asset Detail")
    _validate_paging(
        contract.paging,
        len(contract.elements),
        expected_start=None,
        expected_count=None,
        contract_name="Asset Detail",
    )
    if len(contract.elements) != 1:
        raise LinkedInResponseContractError(
            "LinkedIn Asset Detail contract validation failed: elements must "
            "contain exactly one asset"
        )
    if contract.elements[0].urn != expected_urn:
        raise LinkedInResponseContractError(
            "LinkedIn Asset Detail contract validation failed: urn:mismatch"
        )
    return contract


def validate_activity_reports(
    payload: Any, *, expected_start: int, expected_count: int
) -> LinkedInActivityReportsResponse:
    contract = _validate(
        payload, LinkedInActivityReportsResponse, "Learning Activity Reports"
    )
    _validate_paging(
        contract.paging,
        len(contract.elements),
        expected_start=expected_start,
        expected_count=expected_count,
        contract_name="Learning Activity Reports",
    )
    return contract


def extra_field_paths(model: LinkedInContractModel) -> list[str]:
    return sorted(_collect_extra_field_paths(model, ""))


def _collect_extra_field_paths(value: object, prefix: str) -> list[str]:
    if isinstance(value, list):
        return [
            path
            for index, item in enumerate(value)
            for path in _collect_extra_field_paths(item, f"{prefix}.{index}")
        ]
    if not isinstance(value, LinkedInContractModel):
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


def _validate_paging(
    paging: LinkedInPaging,
    records_count: int,
    *,
    expected_start: int | None,
    expected_count: int | None,
    contract_name: str,
) -> None:
    if paging.total < records_count:
        raise LinkedInResponseContractError(
            f"LinkedIn {contract_name} contract validation failed: "
            "paging.total:smaller_than_elements"
        )
    if expected_start is not None and paging.start != expected_start:
        raise LinkedInResponseContractError(
            f"LinkedIn {contract_name} contract validation failed: "
            "paging.start:mismatch"
        )
    if expected_count is not None and paging.count != expected_count:
        raise LinkedInResponseContractError(
            f"LinkedIn {contract_name} contract validation failed: "
            "paging.count:mismatch"
        )


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
    raise LinkedInResponseContractError(
        f"LinkedIn {contract_name} contract validation failed: {details}"
    ) from None
