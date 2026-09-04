from __future__ import annotations

import io
import logging
from collections.abc import Callable

import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.main import _configure_application_logging


def test_log_level_is_normalized(
    settings_factory: Callable[..., Settings],
) -> None:
    assert settings_factory(log_level="debug").log_level == "DEBUG"


def test_invalid_log_level_is_rejected(
    settings_factory: Callable[..., Settings],
) -> None:
    with pytest.raises(ValidationError, match="LOG_LEVEL"):
        settings_factory(log_level="verbose")


def test_auth_runtime_configuration_is_required(
    settings_factory: Callable[..., Settings],
) -> None:
    missing_username = settings_factory(auth_admin_username="")
    with pytest.raises(ValueError, match="AUTH_ADMIN_USERNAME"):
        missing_username.validate_auth_runtime()

    missing_password_hash = settings_factory(auth_admin_password_hash="")
    with pytest.raises(ValueError, match="AUTH_ADMIN_PASSWORD_HASH"):
        missing_password_hash.validate_auth_runtime()

    short_jwt_secret = settings_factory(auth_jwt_secret="short")
    with pytest.raises(ValueError, match="at least 32 characters"):
        short_jwt_secret.validate_auth_runtime()


def test_ingestion_time_and_storage_validators(
    settings_factory: Callable[..., Settings],
) -> None:
    with pytest.raises(ValidationError, match="INGESTION_TIME must use HH:MM"):
        settings_factory(ingestion_time="not-a-time")

    with pytest.raises(ValidationError, match="valid 24-hour time"):
        settings_factory(ingestion_time="25:00")

    with pytest.raises(ValidationError, match="valid 24-hour time"):
        settings_factory(ingestion_time="12:65")

    with pytest.raises(ValidationError, match="BRONZE_STORAGE_TYPE"):
        settings_factory(bronze_storage_type="s3")


def test_runtime_vendor_validation_missing_configs(
    settings_factory: Callable[..., Settings],
) -> None:
    missing_levelup_config = settings_factory(levelup_base_url="")
    with pytest.raises(ValueError, match="Missing LevelUP configuration"):
        missing_levelup_config.validate_levelup_runtime()

    missing_skillup_config = settings_factory(skillup_intelligence_base_url="")
    with pytest.raises(ValueError, match="Missing SkillUp configuration"):
        missing_skillup_config.validate_skillup_runtime()

    missing_datacamp_config = settings_factory(datacamp_base_url="")
    with pytest.raises(ValueError, match="Missing DataCamp configuration"):
        missing_datacamp_config.validate_datacamp_runtime()

    missing_harvard_config = settings_factory(harvard_catalog_base_url="")
    with pytest.raises(ValueError, match="Missing Harvard HMM configuration"):
        missing_harvard_config.validate_harvard_runtime("harvard_hmm")

    missing_fams_config = settings_factory(fams_base_url="")
    with pytest.raises(ValueError, match="Missing FAMS configuration"):
        missing_fams_config.validate_fams_runtime()


def test_application_logs_use_uvicorn_handler() -> None:
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    app_logger = logging.getLogger("app")
    root_logger = logging.getLogger()
    uvicorn_logger = logging.getLogger("uvicorn")
    previous = (
        list(app_logger.handlers),
        app_logger.level,
        app_logger.propagate,
        list(root_logger.handlers),
        list(uvicorn_logger.handlers),
    )
    try:
        app_logger.handlers = []
        app_logger.propagate = True
        root_logger.handlers = []
        uvicorn_logger.handlers = [handler]

        _configure_application_logging("INFO")
        logging.getLogger("app.vendor").info("vendor request visible")

        assert "vendor request visible" in output.getvalue()
        assert app_logger.propagate is False
    finally:
        (
            app_logger.handlers,
            app_logger.level,
            app_logger.propagate,
            root_logger.handlers,
            uvicorn_logger.handlers,
        ) = previous
