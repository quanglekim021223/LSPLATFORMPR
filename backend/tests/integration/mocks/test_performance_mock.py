from pathlib import Path

from scripts.generate_performance_mock_data import (
    PERFORMANCE_VENDORS,
    build_dataset,
    generate_data,
    validate_dataset,
)


def test_performance_dataset_matches_all_active_contracts() -> None:
    dataset = build_dataset(1_000)

    validate_dataset(dataset)

    vendors = dataset["vendors"]
    assert len(vendors["levelup"]["courses"]) + sum(
        len(records) for records in vendors["levelup"]["enrollments"].values()
    ) == 1_000
    assert (
        len(vendors["skillup"]["taxonomy"])
        + len(vendors["skillup"]["skill_profiles"])
        + len(vendors["skillup"]["reports"])
    ) == 1_000
    assert (
        len(vendors["datacamp"]["live_courses"])
        + len(vendors["datacamp"]["archived_courses"])
        + len(vendors["datacamp"]["events"])
    ) == 1_000
    assert len(vendors["coursera"]["contents"]) + len(
        vendors["coursera"]["enrollments"]
    ) == 1_000
    assert len(vendors["linkedin"]["assets"]) + len(
        vendors["linkedin"]["activity_reports"]
    ) == 1_000
    assert len(vendors["fams"]["classes"]) + len(vendors["fams"]["students"]) == 1_000

    for vendor in ("harvard_hmm", "harvard_spark"):
        assert len(vendors[vendor]["catalog"]) + len(
            vendors[vendor]["history_rows"]
        ) == 1_000


def test_performance_dataset_validates_datacamp_across_multiple_pages() -> None:
    validate_dataset(build_dataset(3_004))


def test_performance_dataset_is_written_as_one_file_per_vendor(tmp_path: Path) -> None:
    output_directory = generate_data(10, tmp_path)

    assert output_directory == tmp_path.resolve()
    assert sorted(path.stem for path in output_directory.glob("*.json")) == sorted(
        PERFORMANCE_VENDORS
    )
