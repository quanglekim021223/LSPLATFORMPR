from __future__ import annotations

import hashlib
import json

import pyarrow.parquet as pq
import pytest

from app.fabric_contract import BUSINESS_KEYS, DATASETS, TABLES, records_from_bytes
from app.fabric_runtime import live_settings
from app.fabric_tables import build_batch


def test_function_settings_ignore_local_env_and_map_aliases(monkeypatch):
    monkeypatch.setenv("SKILLUP_KEY", "wrong-local-key")
    monkeypatch.setenv("SKILLUP_INTELLIGENCE_BASE_URL", "http://localhost:8001")
    monkeypatch.setenv("SCHEDULER_ENABLED", "true")
    s = live_settings(
        {
            "LEVELUP_USER_NAME": "levelup-user",
            "SKILLUP_KEY": "azure-key",
            "COURSERA_ORGID": "org",
            "COURSERA_USER_NAME": "user",
            "HARVARD_ORGID": "hbp",
        }
    )
    assert s.skillup_api_key.get_secret_value() == "azure-key"
    assert s.levelup_username.get_secret_value() == "levelup-user"
    assert s.coursera_username.get_secret_value() == "user"
    assert s.coursera_org_id == "org"
    assert s.harvard_hmm_org_key == s.harvard_spark_org_key == "hbp"
    assert s.skillup_intelligence_base_url.startswith("https://")
    assert not s.scheduler_enabled


@pytest.mark.parametrize(
    "values",
    [
        {"SKILLUP_KEY": "@Microsoft.KeyVault(SecretUri=test)"},
        {"DATACAMP_BASE_URL": "http://localhost:8001"},
        {"COURSERA_TOKEN_URL": "https://attacker.example/token"},
    ],
)
def test_unsafe_configuration_refused(values):
    with pytest.raises(ValueError):
        live_settings(values)


def test_mapping_includes_all_twenty_one_bronze_tables():
    assert len(DATASETS) == 20
    assert len(TABLES) == 21
    assert "coursera_course_detail" in TABLES
    assert "skillup_certificates" in TABLES
    assert "skillup_learning_resources" in TABLES
    assert set(BUSINESS_KEYS) == TABLES


def test_skillup_snapshot_pages_build_both_bronze_tables(tmp_path):
    run_id = "skillup-snapshot-run"
    raw_root = tmp_path / "raw"
    table_root = tmp_path / "tables"
    datasets = {
        "learning_resources": [
            {
                "learningMaterialId": 1,
                "title": "Resource",
                "skills": [{"taxonomySkillId": 10, "skillName": "Python"}],
            }
        ],
        "certificates": [
            {
                "certificateId": 2,
                "title": "Certificate",
                "skills": [{"taxonomySkillId": 10, "skillName": "Python"}],
            }
        ],
    }
    for domain, records in datasets.items():
        directory = raw_root / "skillup" / domain / f"run_id={run_id}"
        directory.mkdir(parents=True)
        payload = json.dumps({"items": records}).encode()
        (directory / "offset=000001.json").write_bytes(payload)
        (directory / "manifest.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "data_domain": domain,
                    "pages": [
                        {
                            "file": "offset=000001.json",
                            "sha256": hashlib.sha256(payload).hexdigest(),
                            "records_count": 1,
                            "fetched_at": "2026-09-12T00:00:00+00:00",
                        }
                    ],
                }
            )
        )

    assert build_batch(raw_root, table_root, "skillup", run_id) == {
        "skillup_certificates": 1,
        "skillup_learning_resources": 1,
    }
    assert pq.read_table(table_root / "skillup_certificates").num_rows == 1
    assert pq.read_table(table_root / "skillup_learning_resources").num_rows == 1
    certificate_columns = pq.read_table(table_root / "skillup_certificates").column_names
    resource_columns = pq.read_table(table_root / "skillup_learning_resources").column_names
    assert "certificate_id" in certificate_columns
    assert "learning_material_id" in resource_columns
    assert "fetched_at" in certificate_columns
    assert "skill_ids" in resource_columns
    assert "certificateid" not in certificate_columns
    assert "learningmaterialid" not in resource_columns


def test_raw_catalog_paging_rejects_silent_truncation():
    from app.fabric_catalog import next_catalog_page

    assert next_catalog_page("coursera", {"paging": {"next": "100", "total": 200}}, 0, 100) == 100
    assert next_catalog_page("linkedin", {"paging": {"links": [], "total": 2}}, 0, 2) is None
    for payload in ({}, {"paging": {"total": 10}}):
        with pytest.raises(ValueError):
            next_catalog_page("coursera", payload, 0, 2)
    with pytest.raises(ValueError):
        next_catalog_page("coursera", {"paging": {"next": "100"}}, 0, 0)


def test_verified_live_nullable_fields_and_harvard_header():
    from app.schemas.datacamp.responses import DataCampEventUser
    from app.schemas.harvard import validate_history_csv
    from app.schemas.linkedin.responses import LinkedInUrls
    from app.schemas.skillup.responses import TaxonomySkillDefinition
    from tests.support.mocks.harvard import history_csv

    assert DataCampEventUser(email="example@test.invalid", lmsUsername=None).nameid is None
    assert LinkedInUrls().sso_launch is None
    assert TaxonomySkillDefinition(id=1, name="skill", description=None).description is None
    assert (
        validate_history_csv(
            history_csv("harvard_hmm").replace(b"Product\r", b"Product ID\r"), "harvard_hmm"
        )
        == 1
    )


def test_empty_missing_and_fams_paths():
    assert records_from_bytes(b'{"items":[]}', "items") == []
    with pytest.raises(ValueError, match="Missing record path"):
        records_from_bytes(b"{}", "items")
    raw = b'{"data":{"classList":[{"id":1}],"studentList":[{"account":"a"}]}}'
    assert records_from_bytes(raw, "data.classList") == [{"id": 1}]
    assert records_from_bytes(raw, "data.studentList") == [{"account": "a"}]


@pytest.mark.parametrize("raw", [b"a,a\nx,y\n", b"a,b\nx,y,z\n", b"a,b\nx\n"])
def test_invalid_csv_refused(raw):
    with pytest.raises(ValueError):
        records_from_bytes(raw, "csv")


def test_harvard_unquoted_title_commas_are_repaired():
    raw = (
        b"EventDate,Username,FirstName,LastName,Email,EventName,Title,Product ID\n"
        b"20260910,user,First,Last,user@example.test,Completed,Leading teams, "
        b"strategy and change,product-1\n"
    )
    records = records_from_bytes(raw, "csv")
    assert records[0]["Title"] == "Leading teams, strategy and change"
    assert records[0]["Product ID"] == "product-1"

    spark_raw = (
        b"Event Date,Username,Event Name,Title,Asset Type,Product ID,Skills,Duration\n"
        b"2026-09-10,user,Views,Measure growth, not only market share,Article,p1,Data,4\n"
    )
    spark_records = records_from_bytes(spark_raw, "csv")
    assert spark_records[0]["Title"] == "Measure growth, not only market share"
    assert spark_records[0]["Asset Type"] == "Article"
    assert spark_records[0]["Duration"] == "4"
