from __future__ import annotations

import logging


def configure_application_logging(level: str) -> None:
    app_logger = logging.getLogger("app")
    app_logger.setLevel(level)
    if app_logger.handlers or logging.getLogger().handlers:
        return

    uvicorn_handlers = (
        logging.getLogger("uvicorn.error").handlers
        or logging.getLogger("uvicorn").handlers
    )
    if uvicorn_handlers:
        app_logger.handlers = list(uvicorn_handlers)
        app_logger.propagate = False
