"""SkillUp ingestion for flattened tables in schema private.

Run from backend: python scripts/skillup_ingestion.py
Resume a bounded batch: python scripts/skillup_ingestion.py learning --max-pages 1
Completed checkpoints are skipped; this is a resumable full load, not a delta sync.
Requires psycopg[binary], requests, python-dotenv and the existing backend/.env.
Uses Session pooler/direct PostgreSQL (session advisory locks).
"""
import argparse
import os
import time
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv
from psycopg import sql
from psycopg.types.json import Jsonb
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BACKEND_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BACKEND_DIR / ".env", override=False, interpolate=False)

ENDPOINTS = {
    "learning": {
        "path": "/learning/materials",
        "table": "skillup_learning_resources",
        "column": "learning_material_id",
        "id_field": "learningMaterialId",
        "params": {"IncludeSkills": "true"},
    },
    "certificates": {
        "path": "/certificates",
        "table": "skillup_certificates",
        "column": "certificate_id",
        "id_field": "certificateId",
        "params": {"IncludeSkills": "true", "activeOnly": "false"},
    },
}


# Column order is shared by mapping and SQL parameter generation.
SKILL_FIELDS = (
    ("skill_ids", "taxonomySkillId", int),
    ("skill_names", "skillName", str),
    ("skill_descriptions", "description", str),
    ("skill_explanations", "explanation", str),
    ("skill_proficiencies", "proficiency", int),
)
LEARNING_FIELDS = (
    "learning_material_id", "external_material_id", "title", "url",
    "recommendation_type", "issuer_id", "issuer_name",
    "material_type_id", "material_type_name",
)
CERTIFICATE_FIELDS = ("certificate_id", "title", "issuer", "certificate_status")


def optional_value(obj, key, expected):
    value = obj.get(key)
    if value is not None and type(value) is not expected:
        raise ValueError(f"Invalid type for {key}; expected {expected.__name__} or null")
    return value


def optional_object(item, key):
    value = item.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object or null")
    return value


def data_columns(config):
    learning = config["id_field"] == "learningMaterialId"
    scalar = LEARNING_FIELDS if learning else CERTIFICATE_FIELDS
    arrays = tuple(field[0] for field in SKILL_FIELDS)
    return scalar + arrays + (("skill_sources",) if learning else ())


def map_item(item, config):
    """One API record -> one DB row; preserve NULLs and skill order."""
    item_id = item.get(config["id_field"])
    if type(item_id) is not int:
        raise ValueError(f"Missing or invalid {config['id_field']}")
    learning = config["id_field"] == "learningMaterialId"
    if learning:
        issuer = optional_object(item, "issuer")
        material_type = optional_object(item, "learningMaterialType")
        values = [
            item_id,
            optional_value(item, "externalMaterialId", str),
            optional_value(item, "title", str),
            optional_value(item, "url", str),
            optional_value(item, "recommendationType", int),
            optional_value(issuer, "id", int),
            optional_value(issuer, "name", str),
            optional_value(material_type, "id", int),
            optional_value(material_type, "name", str),
        ]
    else:
        values = [item_id, optional_value(item, "title", str),
                  optional_value(item, "issuer", str),
                  optional_value(item, "certificateStatus", int)]

    # Missing skills is suspicious when IncludeSkills=true: do not erase old skills.
    if "skills" not in item:
        raise ValueError(f"Missing skills for record {item_id}")
    skills = item["skills"]
    if skills is None:
        skills = []
    if not isinstance(skills, list) or any(not isinstance(s, dict) for s in skills):
        raise ValueError(f"Invalid skills for record {item_id}")
    fields = SKILL_FIELDS + ((("skill_sources", "source", int),) if learning else ())
    for _, key, expected in fields:
        values.append([optional_value(skill, key, expected) for skill in skills])
    return tuple(values)


def make_upsert(config):
    columns = data_columns(config)
    assignments = [
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in columns if c != config["column"]
    ]
    assignments.append(sql.SQL("fetched_at = EXCLUDED.fetched_at"))
    return sql.SQL(
        "INSERT INTO private.{} ({}, fetched_at) VALUES ({}, now()) "
        "ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(config["table"]),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        sql.Identifier(config["column"]),
        sql.SQL(", ").join(assignments),
    )


def required_env(name):
    value = os.environ.get(name)
    if not value or not value.strip():
        raise ValueError(f"Missing environment variable: {name}")
    return value


def fetch_page(session, url, params, id_field, page_size):
    response = session.get(
        url,
        params=params,
        timeout=(15, 120),
        allow_redirects=False,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Vendor returned HTTP {response.status_code}")

    body = response.json()
    if not isinstance(body, dict):
        raise ValueError("Response must be a JSON object")

    items = body.get("items")
    has_next = body.get("hasNextPage")
    total = body.get("totalCount")

    if not isinstance(items, list):
        raise ValueError("Response items must be a list")
    if type(has_next) is not bool:
        raise ValueError("Missing or invalid hasNextPage")
    if type(total) is not int or total < 0:
        raise ValueError("Missing or invalid totalCount")
    if len(items) > page_size:
        raise ValueError("Response exceeds requested PageSize")

    if "pageNumber" in body:
        if body["pageNumber"] != params["PageNumber"]:
            raise ValueError("Response pageNumber does not match request")

    if not items and (has_next or total > 0):
        raise ValueError("Unexpected empty page; checkpoint not advanced")

    ids = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Invalid item")
        item_id = item.get(id_field)
        if type(item_id) is not int:
            raise ValueError(f"Missing or invalid {id_field}")
        ids.append(item_id)

    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate IDs within the same page")

    return items, has_next, total


def ingest(conn, session, config, base_url, page_size, max_pages):
    endpoint = config["path"]
    filters = config["params"]

    conn.execute(
        """
        INSERT INTO private.ingestion_checkpoints
            (vendor, endpoint, page_size, query_params)
        VALUES ('skillup', %s, %s, %s)
        ON CONFLICT (vendor, endpoint) DO NOTHING
        """,
        (endpoint, page_size, Jsonb(filters)),
    )

    checkpoint = conn.execute(
        """
        SELECT next_page, page_size, query_params, status
        FROM private.ingestion_checkpoints
        WHERE vendor = 'skillup' AND endpoint = %s
        """,
        (endpoint,),
    ).fetchone()

    page, saved_size, saved_filters, status = checkpoint

    if saved_size != page_size or saved_filters != filters:
        raise ValueError(
            "PageSize or filters differ from saved checkpoint. "
            "Restore the original settings before resuming."
        )

    if status == "completed":
        print(f"{endpoint}: previous full sync already completed.")
        return

    upsert = make_upsert(config)

    pages_done = 0
    while max_pages == 0 or pages_done < max_pages:
        print(f"Fetching {endpoint}, page={page}, size={page_size}")

        params = {
            **filters,
            "PageNumber": page,
            "PageSize": page_size,
        }
        items, has_next, total = fetch_page(
            session,
            base_url + endpoint,
            params,
            config["id_field"],
            page_size,
        )

        # Validate/map the entire page before starting the DB transaction.
        rows = [map_item(item, config) for item in items]

        # Save the entire page and its checkpoint atomically.
        with conn.transaction():
            with conn.cursor() as cursor:
                if items:
                    cursor.executemany(
                        upsert,
                        rows,
                    )

            conn.execute(
                """
                UPDATE private.ingestion_checkpoints
                SET next_page = %s,
                    status = %s,
                    records_processed = records_processed + %s,
                    total_count_reported = %s,
                    last_error = NULL,
                    updated_at = now()
                WHERE vendor = 'skillup' AND endpoint = %s
                """,
                (
                    page + 1,
                    "paused" if has_next else "completed",
                    len(items),
                    total,
                    endpoint,
                ),
            )

        print(
            f"Saved page {page}: {len(items)} records; "
            f"vendor totalCount={total}"
        )

        if not has_next:
            count = conn.execute(
                sql.SQL("SELECT count(*) FROM private.{}").format(
                    sql.Identifier(config["table"])
                )
            ).fetchone()[0]
            print(f"Pagination finished. Stored unique IDs: {count}")
            if count != total:
                print("WARNING: Stored count differs from vendor totalCount.")
            return

        page += 1
        pages_done += 1

        if max_pages == 0 or pages_done < max_pages:
            time.sleep(0.5)

    print(f"Batch finished. Next run starts at page {page}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "endpoint",
        nargs="?",
        choices=["all", *ENDPOINTS],
        default="all",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Maximum pages per endpoint; 0 means run until finished.",
    )
    args = parser.parse_args()

    page_size = int(required_env("SKILLUP_PAGE_SIZE"))
    if page_size < 1 or args.max_pages < 0:
        raise ValueError("Invalid page size or max pages")

    base_url = required_env("SKILLUP_API_BASE_URL").rstrip("/")
    if base_url != "https://api.skillsintelligence.imocha.io":
        raise ValueError("Unexpected SkillUp API base URL")

    api_key = required_env("SKILLUP_API_KEY")

    selected = (
        list(ENDPOINTS)
        if args.endpoint == "all"
        else [args.endpoint]
    )

    db_config = {
        "host": required_env("SKILLUP_DB_HOST"),
        "port": int(required_env("SKILLUP_DB_PORT")),
        "dbname": required_env("SKILLUP_DB_NAME"),
        "user": required_env("SKILLUP_DB_USER"),
        "password": required_env("SKILLUP_DB_PASSWORD"),
        "sslmode": "require",
        "connect_timeout": 15,
        "autocommit": True,
    }

    failed = []
    started = time.monotonic()

    with requests.Session() as session:
        session.headers.update({"x-api-key": api_key})

        retry = Retry(
            total=4,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))

        for name in selected:
            config = ENDPOINTS[name]
            lock_name = "skillup:" + config["path"]

            print(f"\n=== Starting {name} ===", flush=True)

            try:
                # Reuse this connection for all pages of the endpoint.
                with psycopg.connect(**db_config) as conn:
                    locked = conn.execute(
                        "SELECT pg_try_advisory_lock(hashtext(%s)::bigint)",
                        (lock_name,),
                    ).fetchone()[0]

                    if not locked:
                        raise RuntimeError(
                            "Another process is running this endpoint"
                        )

                    try:
                        ingest(
                            conn,
                            session,
                            config,
                            base_url,
                            page_size,
                            args.max_pages,
                        )
                    finally:
                        # Release the session lock before returning
                        # the connection to the Session pooler.
                        if not conn.closed:
                            conn.execute(
                                "SELECT pg_advisory_unlock("
                                "hashtext(%s)::bigint)",
                                (lock_name,),
                            )

            except Exception as exc:
                failed.append(name)
                print(
                    f"FAILED {name}: {type(exc).__name__}: {exc}",
                    flush=True,
                )
                print(
                    "Checkpoint retained. Rerun to resume.",
                    flush=True,
                )

    elapsed = time.monotonic() - started
    print(f"\nRun finished in {elapsed:.1f} seconds.")

    if failed:
        print("Endpoints needing retry:", ", ".join(failed))
        raise SystemExit(1)

    print("Selected endpoints processed without execution errors.")


if __name__ == "__main__":
    main()
    