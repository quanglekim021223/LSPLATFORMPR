from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from math import ceil
from pathlib import Path
from typing import Any

BACKEND_ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_VENDORS = (
    "levelup",
    "skillup",
    "datacamp",
    "coursera",
    "linkedin",
    "harvard_hmm",
    "harvard_spark",
    "fams",
)
sys.path.insert(0, str(BACKEND_ROOT / "src"))

from app.mocks.coursera import course_payload as coursera_course  # noqa: E402
from app.mocks.coursera import enrollment_payload as coursera_enrollment  # noqa: E402
from app.mocks.datacamp import course_payload as datacamp_course  # noqa: E402
from app.mocks.datacamp import event_payload as datacamp_event  # noqa: E402
from app.mocks.harvard import catalog_item as harvard_catalog_item  # noqa: E402
from app.mocks.levelup import course_payload as levelup_course  # noqa: E402
from app.mocks.levelup import enrollment_payload as levelup_enrollment  # noqa: E402
from app.mocks.linkedin import activity_report_payload as linkedin_activity  # noqa: E402
from app.mocks.linkedin import asset_payload as linkedin_asset  # noqa: E402
from app.mocks.skillup import (  # noqa: E402
    assessment_report,
    skill_profile,
    taxonomy_item,
)
from app.schemas.coursera import (  # noqa: E402
    validate_course_detail as validate_coursera_course_detail,
)
from app.schemas.coursera import (  # noqa: E402
    validate_course_list as validate_coursera_course_list,
)
from app.schemas.coursera import (  # noqa: E402
    validate_learning_history as validate_coursera_learning_history,
)
from app.schemas.datacamp import (  # noqa: E402
    validate_archived_catalog,
    validate_events,
    validate_live_catalog,
)
from app.schemas.harvard import validate_catalog, validate_history_csv  # noqa: E402
from app.schemas.levelup import (  # noqa: E402
    validate_course_list as validate_levelup_course_list,
)
from app.schemas.levelup import validate_enrollments  # noqa: E402
from app.schemas.linkedin import (  # noqa: E402
    validate_activity_reports,
    validate_learning_asset_detail,
    validate_learning_assets,
)
from app.schemas.skillup import (  # noqa: E402
    validate_assessment_history,
    validate_skill_inventory,
    validate_skill_taxonomy,
)


def _split(total: int, parts: int) -> list[int]:
    base, remainder = divmod(total, parts)
    return [base + (index < remainder) for index in range(parts)]


def _levelup(total: int, timestamp: str) -> dict[str, Any]:
    course_count = max(1, total // 10)
    enrollment_count = total - course_count
    courses = [
        {
            **levelup_course(
                f"perf-course-{index:05d}",
                f"Performance Course {index}",
                "LevelUP",
            ),
            "dateEdited": timestamp,
        }
        for index in range(course_count)
    ]
    enrollments: dict[str, list[dict[str, object]]] = {
        str(course["id"]): [] for course in courses
    }
    for index in range(enrollment_count):
        course_id = str(courses[index % course_count]["id"])
        record = levelup_enrollment(
            f"perf-enrollment-{index:06d}",
            course_id,
            f"perf-user-{index:06d}",
        )
        record["dateEdited"] = timestamp
        enrollments[course_id].append(record)
    return {"courses": courses, "enrollments": enrollments}


def _skillup(total: int, timestamp: str) -> dict[str, Any]:
    taxonomy_count, profile_count, report_count = _split(total, 3)
    taxonomy: list[dict[str, Any]] = []
    taxonomy_modified_on: dict[str, str] = {}
    for index in range(taxonomy_count):
        record = taxonomy_item(index % 3)
        record["taxonomySkillId"] = 100_000 + index
        record["displayName"] = f"Performance Skill {index}"
        taxonomy.append(record)
        taxonomy_modified_on[str(record["taxonomySkillId"])] = timestamp
    profiles: list[dict[str, Any]] = []
    profile_modified_on: dict[str, str] = {}
    for index in range(profile_count):
        record = skill_profile(index % 3)
        record["employeeId"] = 200_000 + index
        record["externalEmployeeId"] = f"PERF-{index:06d}"
        record["email"] = f"skillup-{index}@example.test"
        record["fullName"] = f"Performance Employee {index}"
        profiles.append(record)
        profile_modified_on[str(record["employeeId"])] = timestamp
    reports: list[dict[str, Any]] = []
    for index in range(report_count):
        record = assessment_report(index % 3)
        record["testInvitationId"] = 300_000 + index
        record["candidateEmail"] = f"assessment-{index}@example.test"
        record["candidateFullName"] = f"Assessment Candidate {index}"
        reports.append(record)
    return {
        "taxonomy": taxonomy,
        "taxonomy_modified_on": taxonomy_modified_on,
        "skill_profiles": profiles,
        "skill_profile_modified_on": profile_modified_on,
        "reports": reports,
    }


def _datacamp(total: int, timestamp: str) -> dict[str, Any]:
    live_count, archived_count, event_count = _split(total, 3)
    live = [
        {
            **datacamp_course(
                f"course-perf-live-{index:05d}",
                f"Performance Live Course {index}",
                live=True,
            ),
            "updatedAt": timestamp,
        }
        for index in range(live_count)
    ]
    archived = [
        {
            **datacamp_course(
                f"course-perf-archived-{index:05d}",
                f"Performance Archived Course {index}",
                live=False,
                technology=None,
            ),
            "updatedAt": timestamp,
        }
        for index in range(archived_count)
    ]
    events = []
    for index in range(event_count):
        event = datacamp_event(index)
        event["timestamp"] = timestamp
        events.append(event)
    return {"live_courses": live, "archived_courses": archived, "events": events}


def _coursera(total: int, epoch_seconds: int) -> dict[str, Any]:
    content_count, enrollment_count = _split(total, 2)
    contents = []
    for index in range(content_count):
        record = coursera_course(
            f"perf-content-{index:05d}", f"Performance Content {index}"
        )
        record["lastUpdatedAt"] = epoch_seconds
        contents.append(record)
    enrollments = []
    for index in range(enrollment_count):
        content_id = str(contents[index % content_count]["contentId"])
        record = coursera_enrollment(
            f"perf-enrollment-{index:06d}",
            content_id,
            completed=index % 2 == 0,
        )
        record["lastActivityAt"] = epoch_seconds * 1000
        enrollments.append(record)
    return {"contents": contents, "enrollments": enrollments}


def _linkedin(total: int, epoch_millis: int) -> dict[str, Any]:
    asset_count, report_count = _split(total, 2)
    assets = []
    for index in range(asset_count):
        record = linkedin_asset(
            f"urn:li:lyndaCourse:perf-{index:05d}",
            f"Performance LinkedIn Course {index}",
        )
        record["details"]["lastUpdatedAt"] = epoch_millis
        assets.append(record)
    reports = [
        linkedin_activity(index + 1, epoch_millis) for index in range(report_count)
    ]
    return {"assets": assets, "activity_reports": reports}


def _harvard(total: int, vendor: str, last_modified_date: str) -> dict[str, Any]:
    catalog_count, history_count = _split(total, 2)
    catalog = []
    for index in range(catalog_count):
        item = harvard_catalog_item(
            f"{vendor}-perf-{index:05d}", f"Performance {vendor} Course {index}"
        )
        item["LastModifiedDate"] = last_modified_date
        catalog.append(item)
    history_rows = [
        {
            "username": f"{vendor}-{index}@example.test",
            "title": f"Performance Course {index}",
            "product_id": f"{vendor.upper()}-{index:05d}",
        }
        for index in range(history_count)
    ]
    return {"catalog": catalog, "history_rows": history_rows}


def _fams(total: int) -> dict[str, Any]:
    class_count = max(1, total // 4)
    student_count = total - class_count
    classes = [
        {
            "classId": f"perf-class-{index:05d}",
            "status": "INPROGRESS" if index % 2 else "CLOSED",
            "site": "HCM" if index % 2 else "HN",
            "actualStartDate": "20260827",
        }
        for index in range(class_count)
    ]
    students = [
        {
            "studentId": f"perf-student-{index:06d}",
            "classId": classes[index % class_count]["classId"],
        }
        for index in range(student_count)
    ]
    return {"classes": classes, "students": students}


def build_dataset(records_per_vendor: int) -> dict[str, Any]:
    if records_per_vendor < 10:
        raise ValueError("--records must be at least 10")
    now = datetime.now(UTC)
    timestamp = now.isoformat().replace("+00:00", "Z")
    epoch_seconds = int(now.timestamp())
    return {
        "generated_at": timestamp,
        "records_per_vendor": records_per_vendor,
        "vendors": {
            "levelup": _levelup(records_per_vendor, timestamp),
            "skillup": _skillup(records_per_vendor, timestamp),
            "datacamp": _datacamp(records_per_vendor, timestamp),
            "coursera": _coursera(records_per_vendor, epoch_seconds),
            "linkedin": _linkedin(records_per_vendor, epoch_seconds * 1000),
            "harvard_hmm": _harvard(records_per_vendor, "hmm", now.date().isoformat()),
            "harvard_spark": _harvard(
                records_per_vendor, "spark", now.date().isoformat()
            ),
            "fams": _fams(records_per_vendor),
        },
    }


def validate_dataset(dataset: dict[str, Any]) -> None:
    """Reject generated data that does not match the active vendor contracts."""
    vendors = dataset["vendors"]
    levelup = vendors["levelup"]
    validate_levelup_course_list(
        _offset_page("courses", levelup["courses"])
    )
    for enrollments in levelup["enrollments"].values():
        validate_enrollments(_offset_page("enrollments", enrollments))

    skillup = vendors["skillup"]
    validate_skill_taxonomy(_numbered_page("items", skillup["taxonomy"]))
    validate_skill_inventory(_numbered_page("items", skillup["skill_profiles"]))
    validate_assessment_history(_numbered_page("reports", skillup["reports"]))

    datacamp = vendors["datacamp"]
    validate_live_catalog({"data": datacamp["live_courses"]})
    validate_archived_catalog({"data": datacamp["archived_courses"]})
    event_page_size = 1_000
    event_page_count = ceil(len(datacamp["events"]) / event_page_size)
    for page in range(1, event_page_count + 1):
        start = (page - 1) * event_page_size
        validate_events(
            _datacamp_events_page(
                datacamp["events"][start : start + event_page_size],
                page=page,
                page_size=event_page_size,
                number_of_pages=event_page_count,
            ),
            expected_page=page,
            expected_page_size=event_page_size,
        )

    coursera = vendors["coursera"]
    course_page = _coursera_page(coursera["contents"])
    validate_coursera_course_list(course_page)
    validate_coursera_course_detail(
        _coursera_page([coursera["contents"][0]]),
        expected_content_id=coursera["contents"][0]["contentId"],
    )
    validate_coursera_learning_history(_coursera_page(coursera["enrollments"]))

    linkedin = vendors["linkedin"]
    linkedin_page_size = 100
    for start in range(0, len(linkedin["assets"]), linkedin_page_size):
        validate_learning_assets(
            _linkedin_page(
                linkedin["assets"][start : start + linkedin_page_size],
                start=start,
                count=linkedin_page_size,
                total=len(linkedin["assets"]),
            ),
            expected_start=start,
            expected_count=linkedin_page_size,
        )
    validate_learning_asset_detail(
        _linkedin_page([linkedin["assets"][0]]),
        expected_urn=linkedin["assets"][0]["urn"],
    )
    for start in range(0, len(linkedin["activity_reports"]), linkedin_page_size):
        validate_activity_reports(
            _linkedin_page(
                linkedin["activity_reports"][start : start + linkedin_page_size],
                start=start,
                count=linkedin_page_size,
                total=len(linkedin["activity_reports"]),
            ),
            expected_start=start,
            expected_count=linkedin_page_size,
        )

    for vendor in ("harvard_hmm", "harvard_spark"):
        harvard = vendors[vendor]
        validate_catalog(
            {
                "count": len(harvard["catalog"]),
                "limit": len(harvard["catalog"]),
                "list": harvard["catalog"],
                "start": 0,
            }
        )
        validate_history_csv(_harvard_history_csv(vendor, harvard["history_rows"]), vendor)

    fams = vendors["fams"]
    if not isinstance(fams["classes"], list) or not isinstance(fams["students"], list):
        raise ValueError("FAMS generated classList and studentList must be arrays")


def _offset_page(key: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "totalItems": len(records),
        "returnedItems": len(records),
        "limit": max(1, len(records)),
        "offset": 0,
        key: records,
    }


def _numbered_page(key: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        key: records,
        "pageNumber": 1,
        "totalPages": 1,
        "totalCount": len(records),
        "hasPreviousPage": False,
        "hasNextPage": False,
    }


def _datacamp_events_page(
    records: list[dict[str, Any]],
    *,
    page: int,
    page_size: int,
    number_of_pages: int,
) -> dict[str, Any]:
    return {
        "data": records,
        "meta": {
            "page": page,
            "pageSize": page_size,
            "numberOfPages": number_of_pages,
        },
    }


def _coursera_page(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "elements": records,
        "paging": {"next": None, "total": len(records)},
        "linked": {},
    }


def _linkedin_page(
    records: list[dict[str, Any]],
    *,
    start: int = 0,
    count: int | None = None,
    total: int | None = None,
) -> dict[str, Any]:
    page_count = count or len(records)
    return {
        "paging": {
            "start": start,
            "count": page_count,
            "links": [],
            "total": len(records) if total is None else total,
        },
        "metadata": {},
        "elements": records,
    }


def _harvard_history_csv(vendor: str, rows: list[dict[str, Any]]) -> bytes:
    if vendor == "harvard_hmm":
        header = "EventDate,Username,FirstName,LastName,Email,EventName,Title,Product\n"
        body = "".join(
            "20260827,{username},Mock,Learner,{username},Completed,{title},{product_id}\n".format(
                **row
            )
            for row in rows
        )
    else:
        header = (
            "Event Date,Username,First Name,Last Name,Email,Role,Event Name,"
            "Title,Asset Type,Product ID,Skills,Duration,Registration Date\n"
        )
        body = "".join(
            "2026-08-27,{username},Mock,Learner,{username},Learner,Views,{title},"
            "Videos,{product_id},Leadership,4,2026-01-21\n".format(**row)
            for row in rows
        )
    return (header + body).encode("utf-8")


def generate_data(records: int, output_directory: Path) -> Path:
    dataset = build_dataset(records)
    validate_dataset(dataset)
    output_directory = output_directory.resolve()
    output_directory.mkdir(parents=True, exist_ok=True)
    for vendor, data in dataset["vendors"].items():
        (output_directory / f"{vendor}.json").write_text(
            json.dumps(data, ensure_ascii=False),
            encoding="utf-8",
        )
    return output_directory


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate validated performance data for all 8 mock vendors."
    )
    parser.add_argument("--records", type=int, default=1_000)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BACKEND_ROOT / "data/mock/performance",
    )
    args = parser.parse_args()

    output = generate_data(args.records, args.output_dir)
    size_mb = sum(path.stat().st_size for path in output.glob("*.json")) / 1024 / 1024
    print(
        f"Generated 8 x {args.records:,} records in {output} ({size_mb:.2f} MB)",
        flush=True,
    )


if __name__ == "__main__":
    main()
