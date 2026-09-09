"""FHU/akajob -> SkillUp orchestration shared by local and future entrypoints.

Source contracts and certificate matching semantics still require confirmation.
Clients, mapping and storage are supplied by the caller; no mock imports or keys.
"""

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from app.clients.akajob_client import AkajobClient
from app.clients.certificate_exchange_http import CertificateExchangeError
from app.clients.fhu_client import FHUClient
from app.clients.skillup_certificate_client import SkillUpCertificateClient
from app.models import PageWrite
from app.repositories.writer import BronzeWriter
from app.schemas.akajob.responses import AkajobCertificate
from app.schemas.skillup.certificates import (
    CertificateSkill,
    CreateCertificate,
    EmployeeCertificate,
    SkillMapping,
)


def map_employee_certificate(award: AkajobCertificate) -> EmployeeCertificate:
    """Explicit source-to-target mapping; do not inherit a vendor's wire model."""
    return EmployeeCertificate(
        title=award.title,
        issuer=award.issuer,
        valid_till=award.valid_till,
        license_number=award.license_number,
        is_standard_certificate=award.is_standard_certificate,
    )


async def run_certificate_exchange(
    fhu: FHUClient,
    akajob: AkajobClient,
    skillup: SkillUpCertificateClient,
    writer: BronzeWriter,
    skill_mapping: Mapping[str, SkillMapping],
    *,
    max_polls: int = 20,
    poll_interval: float = 0.1,
) -> dict[str, Any]:
    if max_polls < 1 or poll_interval < 0:
        raise CertificateExchangeError("Invalid polling limits")
    run_id = str(uuid4())
    stored_pages = 0

    async def persist(raw: bytes, vendor: str, domain: str, offset: int, count: int) -> None:
        nonlocal stored_pages
        now = datetime.now(UTC)
        await writer.write_page(
            PageWrite(
                vendor=vendor,
                data_domain=domain,
                ingestion_date=now.date().isoformat(),
                run_id=run_id,
                offset=offset,
                raw_payload=raw,
                records_count=count,
                request_parameters={},
                fetched_at=now,
            )
        )
        stored_pages += 1

    employees, raw = await fhu.list_employees()
    await persist(raw, "fhu", "employees", 1, len(employees))
    awards, raw = await akajob.list_certificates()
    await persist(raw, "akajob", "certificates", 1, len(awards))

    # Validate the complete current single-page source batch before any POST.
    employee_ids = {item.external_employee_id for item in employees}
    if len(employee_ids) != len(employees):
        raise CertificateExchangeError("Duplicate FHU employee identifiers")
    if not awards:
        raise CertificateExchangeError("Exchange requires at least one certificate award")
    if len({item.award_id for item in awards}) != len(awards):
        raise CertificateExchangeError("Duplicate akajob award identifiers")
    catalog: dict[tuple[str, str], CreateCertificate] = {}
    expected: dict[str, set[int]] = {}
    for award in awards:
        if award.external_employee_id not in employee_ids:
            raise CertificateExchangeError("Award employee is missing from FHU")
        if any(code not in skill_mapping for code in award.skill_codes):
            raise CertificateExchangeError("Missing source skill to SkillUp taxonomy mapping")
        if not award.title.strip() or not award.issuer or not award.is_standard_certificate:
            raise CertificateExchangeError(
                "Current exchange supports a titled standard certificate with issuer"
            )
        certificate = CreateCertificate(
            title=award.title,
            certificate_issuer=award.issuer,
            skills=[
                CertificateSkill(taxonomy_skill_id=skill_mapping[code]["taxonomySkillId"])
                for code in sorted(set(award.skill_codes))
            ],
        )
        key = (award.title, award.issuer)
        if key in catalog and catalog[key] != certificate:
            raise CertificateExchangeError("Conflicting skills for the same certificate")
        catalog[key] = certificate
        expected.setdefault(award.external_employee_id, set()).update(
            skill_mapping[code]["skillId"] for code in award.skill_codes
        )

    for index, certificate in enumerate(catalog.values(), start=1):
        raw = await skillup.create_certificate(certificate)
        await persist(raw, "skillup", "certificate_catalog_responses", index, 1)

    for index, award in enumerate(awards, start=1):
        raw = await skillup.upload_certificates(
            award.external_employee_id, [map_employee_certificate(award)]
        )
        await persist(raw, "skillup", "employee_certificate_responses", index, 1)

    for poll in range(1, max_polls + 1):
        observed: dict[str, set[int]] = {}
        for page in range(1, 101):
            inventory, raw = await skillup.get_skill_profiles(page)
            await persist(
                raw, "skillup", "skill_inventory", (poll - 1) * 100 + page, len(inventory.items)
            )
            for employee in inventory.items:
                observed.setdefault(employee.external_employee_id, set()).update(
                    item.skill.skill_id for item in employee.skills
                )
            if not inventory.has_next_page:
                break
        else:
            raise CertificateExchangeError("Read exceeded 100 pages per poll")
        if all(skills <= observed.get(employee, set()) for employee, skills in expected.items()):
            return {
                "status": "succeeded",
                "run_id": run_id,
                "employees": len(expected),
                "awards": len(awards),
                "certificates_created_or_updated": len(catalog),
                "polls": poll,
                "bronze_pages": stored_pages,
            }
        if poll < max_polls:
            await asyncio.sleep(poll_interval)
    raise CertificateExchangeError(
        "Certificates accepted but expected skills were not observed within the polling limit"
    )
