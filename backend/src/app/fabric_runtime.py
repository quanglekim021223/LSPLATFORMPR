"""Load approved live settings for the local Fabric bootstrap runner."""

from __future__ import annotations

import json
import subprocess
from typing import Any
from urllib.parse import urlsplit

from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from app.core.config import Settings

SETTING_PREFIXES = (
    "levelup_",
    "skillup_",
    "datacamp_",
    "coursera_",
    "linkedin_",
    "harvard_",
    "fams_",
    "http_",
)
SETTING_ALIASES = {
    "LEVELUP_KEY": ("levelup_api_key",),
    "LEVELUP_USER_NAME": ("levelup_username",),
    "SKILLUP_KEY": ("skillup_api_key",),
    "COURSERA_ORGID": ("coursera_org_id",),
    "COURSERA_USER_NAME": ("coursera_username",),
    "HARVARD_ORGID": ("harvard_hmm_org_key", "harvard_spark_org_key"),
    "HARVARD_API_USER_NAME": ("harvard_hmm_client_id", "harvard_spark_client_id"),
    "HARVARD_API_PASSWORD": ("harvard_hmm_client_secret", "harvard_spark_client_secret"),
    "HARVARD_SFTP_USER_NAME": ("harvard_sftp_username",),
}
PUBLIC_ENDPOINTS = {
    "levelup_base_url": "https://rest.myabsorb.eu",
    "datacamp_base_url": "https://lms-catalog-api.datacamp.com",
    "coursera_base_url": "https://api.coursera.com/ent/api/businesses.v1",
    "coursera_token_url": "https://api.coursera.com/oauth2/client_credentials/token",
    "linkedin_base_url": "https://api.linkedin.com/v2",
    "linkedin_token_url": "https://www.linkedin.com/oauth/v2/accessToken",
}
ALLOWED_HOSTS = {
    "rest.myabsorb.eu",
    "lms-catalog-api.datacamp.com",
    "api.coursera.com",
    "api.linkedin.com",
    "www.linkedin.com",
    "catalog-api.myhbp.org",
    "api.skillsintelligence.imocha.io",
    "apiv3.imocha.io",
    "fams.fa.edu.vn",
}


class FunctionSettings(Settings):
    """Only explicit Function App values; never local env, dotenv or secret files."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


def read_function_settings() -> dict[str, str]:
    result = subprocess.run(
        [
            "az",
            "functionapp",
            "config",
            "appsettings",
            "list",
            "--subscription",
            "65080acc-f372-4439-b513-dcf565f52913",
            "--resource-group",
            "FSA-Data-Ingest-dev-01_group",
            "--name",
            "fsa-data-ingest-function-app-dev-001",
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode:
        raise RuntimeError("Cannot read Function App settings; check Azure login/RBAC")
    return {
        item["name"]: item["value"]
        for item in json.loads(result.stdout)
        if isinstance(item.get("value"), str)
    }


def _setting_values(values: dict[str, str]) -> dict[str, Any]:
    accepted = set(Settings.model_fields)
    kwargs: dict[str, Any] = {
        k.lower(): v
        for k, v in values.items()
        if k.lower() in accepted and k.lower().startswith(SETTING_PREFIXES)
    }
    for source, targets in SETTING_ALIASES.items():
        value = values.get(source)
        if value:
            for target in targets:
                kwargs.setdefault(target, value)
    # Public endpoint URLs already verified in the repository's sample scripts.
    for key, value in PUBLIC_ENDPOINTS.items():
        kwargs.setdefault(key, value)
    for key, value in kwargs.items():
        if isinstance(value, str) and value.startswith("@Microsoft.KeyVault("):
            raise ValueError(f"Unresolved Key Vault reference: {key}")
    return kwargs


def _validate_endpoints(settings: FunctionSettings) -> None:
    for key in Settings.model_fields:
        if not key.endswith(("base_url", "token_url")):
            continue
        value = getattr(settings, key)
        if value:
            parsed = urlsplit(value)
            if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
                raise ValueError(f"Unapproved live endpoint: {key}")
    if settings.harvard_sftp_host != "transfer.hbsp.harvard.edu":
        raise ValueError("Unapproved SFTP host")


def live_settings(values: dict[str, str]) -> FunctionSettings:
    kwargs = _setting_values(values)
    kwargs.update(
        scheduler_enabled=False,
        bronze_storage_type="local",
        app_env="fabric",
        fams_load_mode="full",
    )
    # The API requires a start for each <=14-day window. Starting well before
    # LinkedIn Learning existed makes the bootstrap cover all retained history.
    kwargs.setdefault("linkedin_history_start_time", "2000-01-01T00:00:00Z")
    # Live taxonomy endpoint rejects pageSize > 50 (HTTP 400, verified 2026-09-10).
    kwargs["skillup_page_size"] = min(int(kwargs.get("skillup_page_size", 50)), 50)
    settings = FunctionSettings(**kwargs)
    _validate_endpoints(settings)
    return settings
