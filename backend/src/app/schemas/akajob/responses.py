"""Provisional akajob contract, independent of the SkillUp wire format."""

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr


class AkajobCertificate(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    award_id: str = Field(alias="awardId", min_length=1)
    external_employee_id: str = Field(alias="externalEmployeeId", min_length=1)
    skill_codes: list[str] = Field(alias="skillCodes", min_length=1)
    title: StrictStr
    issuer: StrictStr | None = None
    valid_till: StrictStr | None = Field(default=None, alias="validTill")
    license_number: StrictStr | None = Field(default=None, alias="licenseNumber")
    is_standard_certificate: StrictBool = Field(alias="isStandardCertificate")
