from __future__ import annotations

from datetime import UTC, datetime, timedelta
from secrets import compare_digest
from typing import cast

import bcrypt
import jwt
from jwt.exceptions import InvalidTokenError

ALGORITHM = "HS256"


class AuthenticationError(Exception):
    """Raised when a bearer token cannot be authenticated."""


class AuthorizationError(Exception):
    """Raised when an authenticated token is not the configured admin."""


def create_access_token(
    *,
    user_id: str,
    secret_key: str,
    expire_minutes: int,
) -> str:
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": user_id,
            "role": "admin",
            "iat": now,
            "exp": now + timedelta(minutes=expire_minutes),
        },
        secret_key,
        algorithm=ALGORITHM,
    )
    return token


def verify_admin_token(
    *,
    token: str,
    secret_key: str,
    admin_username: str,
) -> str:
    try:
        payload = cast(
            dict[str, object],
            jwt.decode(token, secret_key, algorithms=[ALGORITHM]),
        )
    except InvalidTokenError as exc:
        raise AuthenticationError("Invalid or expired token") from exc

    subject = payload.get("sub")
    role = payload.get("role")
    if (
        not isinstance(subject, str)
        or not compare_digest(subject.encode(), admin_username.encode())
        or role != "admin"
    ):
        raise AuthorizationError("Admin access required")
    return subject


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode(), password_hash.encode())
    except ValueError:
        return False
