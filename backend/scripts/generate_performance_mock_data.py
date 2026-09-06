from __future__ import annotations

import argparse
import copy
import json
import shutil
import sys
from datetime import UTC, datetime, timedelta
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
from app.schemas.fams import validate_training_data  # noqa: E402
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
            "id": index + 1,
            "site": "HCM" if index % 2 else "HN",
            "courseCode": f"perf-class-{index:05d}",
            "courseName": f"Performance Class {index}",
            "courseStatus": "INPROGRESS" if index % 2 else "CLOSED",
            "actualStartDate": "2026-08-27",
        }
        for index in range(class_count)
    ]
    students = [
        {
            "account": f"perf-student-{index:06d}",
            "name": f"Performance Student {index}",
            "site": classes[index % class_count]["site"],
            "courseCode": classes[index % class_count]["courseCode"],
            "courseName": classes[index % class_count]["courseName"],
            "statusInClass": "InProgress",
        }
        for index in range(student_count)
    ]
    return {"classes": classes, "students": students}


def build_dataset(
    records_per_vendor: int,
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if records_per_vendor < 10:
        raise ValueError("--records must be at least 10")
    now = generated_at or datetime.now(UTC)
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


def build_incremental_dataset(
    records_per_vendor: int,
    new_records_per_vendor: int,
    *,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    """Create a contract-valid second source snapshot with edits and new records."""
    if new_records_per_vendor < 1:
        raise ValueError("--new-records must be at least 1")

    now = updated_at or datetime.now(UTC)
    baseline = build_dataset(records_per_vendor, generated_at=now - timedelta(days=1))
    expanded = build_dataset(records_per_vendor + new_records_per_vendor, generated_at=now)
    incremental = copy.deepcopy(baseline)
    incremental["generated_at"] = now.isoformat().replace("+00:00", "Z")
    incremental["records_per_vendor"] = records_per_vendor + new_records_per_vendor
    vendors = incremental["vendors"]
    expanded_vendors = expanded["vendors"]

    array_collections = {
        "skillup": ("taxonomy", "skill_profiles", "reports"),
        "datacamp": ("live_courses", "archived_courses", "events"),
        "coursera": ("contents", "enrollments"),
        "linkedin": ("assets", "activity_reports"),
        "harvard_hmm": ("catalog", "history_rows"),
        "harvard_spark": ("catalog", "history_rows"),
        "fams": ("classes", "students"),
    }
    for vendor, collections in array_collections.items():
        for collection in collections:
            existing = vendors[vendor][collection]
            source = expanded_vendors[vendor][collection]
            existing.extend(copy.deepcopy(source[len(existing) :]))

    levelup = vendors["levelup"]
    expanded_levelup = expanded_vendors["levelup"]
    levelup["courses"].extend(
        copy.deepcopy(expanded_levelup["courses"][len(levelup["courses"]) :])
    )
    existing_enrollment_ids = {
        str(record["id"])
        for records in levelup["enrollments"].values()
        for record in records
    }
    for course_id, records in expanded_levelup["enrollments"].items():
        additions = [
            copy.deepcopy(record)
            for record in records
            if str(record["id"]) not in existing_enrollment_ids
        ]
        if additions:
            levelup["enrollments"].setdefault(course_id, []).extend(additions)

    timestamp = now.isoformat().replace("+00:00", "Z")
    epoch_seconds = int(now.timestamp())
    levelup["courses"][0]["name"] += " (Updated)"
    levelup["courses"][0]["dateEdited"] = timestamp

    skillup = vendors["skillup"]
    skillup["taxonomy"][0]["displayName"] += " (Updated)"
    taxonomy_id = str(skillup["taxonomy"][0]["taxonomySkillId"])
    skillup["taxonomy_modified_on"][taxonomy_id] = timestamp
    for key, value in expanded_vendors["skillup"]["taxonomy_modified_on"].items():
        skillup["taxonomy_modified_on"].setdefault(key, value)
    for key, value in expanded_vendors["skillup"]["skill_profile_modified_on"].items():
        skillup["skill_profile_modified_on"].setdefault(key, value)

    datacamp = vendors["datacamp"]
    datacamp["live_courses"][0]["title"] += " (Updated)"
    datacamp["live_courses"][0]["updatedAt"] = timestamp

    coursera = vendors["coursera"]
    coursera["contents"][0]["name"] += " (Updated)"
    coursera["contents"][0]["lastUpdatedAt"] = epoch_seconds

    linkedin = vendors["linkedin"]
    linkedin["assets"][0]["title"]["value"] += " (Updated)"
    linkedin["assets"][0]["details"]["lastUpdatedAt"] = epoch_seconds * 1000

    for vendor in ("harvard_hmm", "harvard_spark"):
        vendors[vendor]["catalog"][0]["Title"] += " (Updated)"
        vendors[vendor]["catalog"][0]["LastModifiedDate"] = now.date().isoformat()

    fams = vendors["fams"]
    fams["classes"][0]["status"] = (
        "INPROGRESS" if fams["classes"][0]["status"] == "CLOSED" else "CLOSED"
    )

    validate_dataset(incremental)
    return incremental


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
    validate_training_data(
        {
            "success": True,
            "message": "Generated FAMS training data",
            "error_code": None,
            "data": {
                "classList": fams["classes"],
                "studentList": fams["students"],
            },
        }
    )


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


def generate_data(
    records: int,
    output_directory: Path,
    *,
    variant: str = "initial",
    new_records: int = 10,
) -> Path:
    output_directory = output_directory.resolve()
    output_directory.mkdir(parents=True, exist_ok=True)

    if variant == "pair":
        updated_at = datetime.now(UTC)
        dataset = build_dataset(
            records,
            generated_at=updated_at - timedelta(days=1),
        )
        incremental_dataset = build_incremental_dataset(
            records,
            new_records,
            updated_at=updated_at,
        )
        validate_dataset(dataset)
        validate_dataset(incremental_dataset)
        _write_vendor_files(dataset, output_directory)
        _write_vendor_files(incremental_dataset, output_directory / "incremental")
        return output_directory

    dataset = (
        build_incremental_dataset(records, new_records)
        if variant == "incremental"
        else build_dataset(records)
    )
    validate_dataset(dataset)
    _write_vendor_files(dataset, output_directory)
    return output_directory


def _write_vendor_files(dataset: dict[str, Any], output_directory: Path) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)
    for vendor, data in dataset["vendors"].items():
        (output_directory / f"{vendor}.json").write_text(
            json.dumps(data, ensure_ascii=False),
            encoding="utf-8",
        )


def activate_incremental_snapshot(output_directory: Path) -> Path:
    output_directory = output_directory.resolve()
    incremental_directory = output_directory / "incremental"
    missing = [
        vendor
        for vendor in PERFORMANCE_VENDORS
        if not (incremental_directory / f"{vendor}.json").is_file()
    ]
    if missing:
        raise ValueError(
            "Generate the matched scenario pair before activation; missing: "
            + ", ".join(missing)
        )
    for vendor in PERFORMANCE_VENDORS:
        shutil.copyfile(
            incremental_directory / f"{vendor}.json",
            output_directory / f"{vendor}.json",
        )
    return output_directory


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate validated performance data for all 8 mock vendors."
    )
    parser.add_argument("--records", type=int, default=1_000)
    parser.add_argument(
        "--variant",
        choices=("initial", "incremental", "pair"),
        default="initial",
    )
    parser.add_argument("--new-records", type=int, default=10)
    parser.add_argument(
        "--activate-incremental",
        action="store_true",
        help="Replace the active vendor files with the generated incremental snapshot.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BACKEND_ROOT / "data/mock/performance",
    )
    args = parser.parse_args()

    if args.activate_incremental:
        output = activate_incremental_snapshot(args.output_dir)
        print(f"Activated incremental source data in {output}", flush=True)
        return

    output = generate_data(
        args.records,
        args.output_dir,
        variant=args.variant,
        new_records=args.new_records,
    )
    size_mb = sum(path.stat().st_size for path in output.glob("*.json")) / 1024 / 1024
    if args.variant == "pair":
        incremental_size_mb = sum(
            path.stat().st_size for path in (output / "incremental").glob("*.json")
        ) / 1024 / 1024
        print(
            f"Generated matched snapshots: 8 x {args.records:,} initial records "
            f"({size_mb:.2f} MB) and 8 x {args.records + args.new_records:,} "
            f"incremental records ({incremental_size_mb:.2f} MB) in {output}",
            flush=True,
        )
    else:
        output_records = args.records + (
            args.new_records if args.variant == "incremental" else 0
        )
        print(
            f"Generated 8 x {output_records:,} {args.variant} records "
            f"in {output} ({size_mb:.2f} MB)",
            flush=True,
        )


if __name__ == "__main__":
    main()
