"""Provisional FHU contract; replace mapping when the source API is confirmed."""

from pydantic import BaseModel, ConfigDict, Field


class FHUEmployee(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    external_employee_id: str = Field(alias="externalEmployeeId", min_length=1)
    full_name: str = Field(alias="fullName", min_length=1)
    email: str
