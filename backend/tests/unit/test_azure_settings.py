from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from pydantic import SecretStr, ValidationError

from app.core.config import Settings


def test_function_app_key_names() -> None:
    values = {
        "COURSERA_USER_NAME": "fake-coursera-user",
        "COURSERA_PASSWORD": "fake-coursera-password",
        "DATACAMP_TOKEN": "fake-datacamp-token",
        "FAMS_TOKEN": "fake-fams-token",
        "HARVARD_API_USER_NAME": "fake-harvard-user",
        "HARVARD_API_PASSWORD": "fake-harvard-password",
        "HARVARD_SFTP_USER_NAME": "fake-sftp-user",
        "HARVARD_SFTP_PASSWORD": "fake-sftp-password",
        "HARVARD_SFTP_HOST": "sftp.example.test",
        "HARVARD_SFTP_PORT": "2222",
        "LEVELUP_KEY": "fake-levelup-key",
        "SKILLUP_KEY": "fake-skillup-key",
        "LINKEDIN_CLIENT_ID": "fake-linkedin-id",
        "LINKEDIN_CLIENT_SECRET": "fake-linkedin-secret",
        "LINKEDIN_GRANT_TYPE": "client_credentials",
    }
    mapping = {
        "coursera_username": "COURSERA_USER_NAME",
        "coursera_password": "COURSERA_PASSWORD",
        "datacamp_token": "DATACAMP_TOKEN",
        "fams_token": "FAMS_TOKEN",
        "harvard_hmm_client_id": "HARVARD_API_USER_NAME",
        "harvard_spark_client_id": "HARVARD_API_USER_NAME",
        "harvard_hmm_client_secret": "HARVARD_API_PASSWORD",
        "harvard_spark_client_secret": "HARVARD_API_PASSWORD",
        "harvard_sftp_username": "HARVARD_SFTP_USER_NAME",
        "harvard_sftp_password": "HARVARD_SFTP_PASSWORD",
        "levelup_api_key": "LEVELUP_KEY",
        "skillup_api_key": "SKILLUP_KEY",
        "linkedin_client_id": "LINKEDIN_CLIENT_ID",
        "linkedin_client_secret": "LINKEDIN_CLIENT_SECRET",
    }
    with patch.dict(os.environ, values, clear=True):
        settings = Settings(_env_file=None)
    for field, key in mapping.items():
        secret = getattr(settings, field)
        assert isinstance(secret, SecretStr)
        assert secret.get_secret_value() == values[key]
        assert values[key] not in repr(settings)
    assert settings.harvard_sftp_host == values["HARVARD_SFTP_HOST"]
    assert settings.harvard_sftp_port == 2222
    assert settings.linkedin_grant_type == "client_credentials"


@pytest.mark.parametrize("field", [
    "levelup_api_key", "skillup_api_key", "coursera_username",
    "harvard_sftp_username", "harvard_hmm_client_id", "harvard_hmm_client_secret",
    "harvard_spark_client_id", "harvard_spark_client_secret",
])
def test_legacy_names_and_constructor_still_work(field: str) -> None:
    with patch.dict(os.environ, {field.upper(): "legacy-test-value"}, clear=True):
        settings = Settings(_env_file=None)
        assert getattr(settings, field).get_secret_value() == "legacy-test-value"
    with patch.dict(os.environ, {}, clear=True):
        settings = Settings(_env_file=None, **{field: "constructor-test-value"})
        assert getattr(settings, field).get_secret_value() == "constructor-test-value"


@pytest.mark.parametrize("token", ["new-fams-token", ""])
def test_fams_token_runtime_configuration(token: str) -> None:
    with patch.dict(os.environ, {
        "FAMS_TOKEN": token,
    }, clear=True):
        settings = Settings(_env_file=None)
    assert settings.fams_token.get_secret_value() == token
    assert settings.fams_configured == bool(token)
    assert settings.fams_secrets() == ((token,) if token else ())
    if not token:
        with pytest.raises(ValueError, match="Missing FAMS configuration: FAMS_TOKEN"):
            settings.validate_fams_runtime()
    else:
        settings.validate_fams_runtime()


def test_explicit_harvard_credentials_override_shared_keys() -> None:
    with patch.dict(os.environ, {
        "HARVARD_API_USER_NAME": "shared-user",
        "HARVARD_API_PASSWORD": "shared-password",
        "HARVARD_HMM_CLIENT_ID": "hmm-user",
        "HARVARD_HMM_CLIENT_SECRET": "hmm-password",
    }, clear=True):
        settings = Settings(_env_file=None)
    assert settings.harvard_hmm_client_id.get_secret_value() == "hmm-user"
    assert settings.harvard_hmm_client_secret.get_secret_value() == "hmm-password"
    assert settings.harvard_spark_client_id.get_secret_value() == "shared-user"
    assert settings.harvard_spark_client_secret.get_secret_value() == "shared-password"


def test_unsupported_linkedin_grant_is_rejected() -> None:
    with patch.dict(os.environ, {"LINKEDIN_GRANT_TYPE": "password"}, clear=True):
        with pytest.raises(ValidationError):
            Settings(_env_file=None)
