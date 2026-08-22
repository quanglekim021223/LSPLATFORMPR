from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

_SENSITIVE_PATTERN = re.compile(
    r"(?i)(authorization|x-api-key|api[_-]?key|private[_-]?key|password|secret)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)


def sanitize_text(value: object, secrets: Iterable[str] = ()) -> str:
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    text = _SENSITIVE_PATTERN.sub(r"\1\2[REDACTED]", text)
    return text[:1000]


def sanitize_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, item in value.items():
        if re.search(r"(?i)authorization|token|key|password|secret", key):
            sanitized[key] = "[REDACTED]"
        elif isinstance(item, Mapping):
            sanitized[key] = sanitize_mapping(item)
        else:
            sanitized[key] = item
    return sanitized

