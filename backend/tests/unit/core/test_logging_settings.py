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
