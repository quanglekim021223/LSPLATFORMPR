"""Write contracts transcribed from the supplied iMocha documentation screenshots.

These describe the wire format, not certificate matching or processing semantics.
The create endpoint was labelled Developing in the supplied documentation.
"""

from typing import TypedDict

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr


class SkillMapping(TypedDict):
    taxonomySkillId: int
    skillId: int
    skillName: str


class CertificateSkill(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    taxonomy_skill_id: StrictInt = Field(alias="taxonomySkillId")


class CreateCertificate(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    title: StrictStr
    certificate_issuer: StrictStr = Field(alias="certificateIssuer")
    skills: list[CertificateSkill]


class EmployeeCertificate(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    title: StrictStr
    issuer: StrictStr | None = None
    valid_till: StrictStr | None = Field(default=None, alias="validTill")
    license_number: StrictStr | None = Field(default=None, alias="licenseNumber")
    is_standard_certificate: StrictBool = Field(alias="isStandardCertificate")
