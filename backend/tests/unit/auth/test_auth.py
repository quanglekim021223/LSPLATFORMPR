from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest

from app.auth.auth import (
    AuthenticationError,
    AuthorizationError,
    create_access_token,
    hash_password,
    verify_admin_token,
    verify_password,
)

SECRET = "test-only-jwt-secret-with-at-least-32-characters"
ADMIN = "test-admin"


def test_password_hash_verification() -> None:
    password_hash = hash_password("correct-password")

    assert verify_password("correct-password", password_hash) is True
    assert verify_password("wrong-password", password_hash) is False
    assert verify_password("correct-password", "invalid-hash") is False


def test_admin_token_round_trip() -> None:
    token = create_access_token(
        user_id=ADMIN,
        secret_key=SECRET,
        expire_minutes=10,
    )

    assert (
        verify_admin_token(
            token=token,
            secret_key=SECRET,
            admin_username=ADMIN,
        )
        == ADMIN
    )


def test_expired_token_is_rejected() -> None:
    token = jwt.encode(
        {
            "sub": ADMIN,
            "role": "admin",
            "exp": datetime.now(UTC) - timedelta(seconds=1),
        },
        SECRET,
        algorithm="HS256",
    )

    with pytest.raises(AuthenticationError):
        verify_admin_token(
            token=token,
            secret_key=SECRET,
            admin_username=ADMIN,
        )


def test_non_admin_token_is_rejected() -> None:
    token = jwt.encode(
        {
            "sub": "another-user",
            "role": "user",
            "exp": datetime.now(UTC) + timedelta(minutes=10),
        },
        SECRET,
        algorithm="HS256",
    )

    with pytest.raises(AuthorizationError):
        verify_admin_token(
            token=token,
            secret_key=SECRET,
            admin_username=ADMIN,
        )


def test_malformed_token_is_rejected() -> None:
    with pytest.raises(AuthenticationError):
        verify_admin_token(
            token="not.a.valid.jwt.token",
            secret_key=SECRET,
            admin_username=ADMIN,
        )


def test_token_missing_subject_is_rejected() -> None:
    token = jwt.encode(
        {
            "role": "admin",
            "exp": datetime.now(UTC) + timedelta(minutes=10),
        },
        SECRET,
        algorithm="HS256",
    )
    with pytest.raises(AuthorizationError):
        verify_admin_token(
            token=token,
            secret_key=SECRET,
            admin_username=ADMIN,
        )


def test_admin_dependency_requires_bearer_credentials() -> None:
    from fastapi import HTTPException
    from pydantic import SecretStr

    from app.auth.dependencies import build_admin_dependency
    from app.core.config import Settings

    settings = Settings(
        auth_admin_username=ADMIN,
        auth_jwt_secret=SecretStr(SECRET),
    )
    require_admin = build_admin_dependency(settings)

    with pytest.raises(HTTPException) as exc_info:
        require_admin(None)
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Bearer token required"


def test_admin_dependency_validates_admin_success_and_failures() -> None:
    from fastapi import HTTPException
    from fastapi.security import HTTPAuthorizationCredentials
    from pydantic import SecretStr

    from app.auth.dependencies import build_admin_dependency
    from app.core.config import Settings

    settings = Settings(
        auth_admin_username=ADMIN,
        auth_jwt_secret=SecretStr(SECRET),
    )
    require_admin = build_admin_dependency(settings)

    # Success
    valid_token = create_access_token(user_id=ADMIN, secret_key=SECRET, expire_minutes=5)
    valid_credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials=valid_token,
    )
    result = require_admin(valid_credentials)
    assert result == ADMIN

    # Forbidden (non-admin)
    non_admin_token = jwt.encode(
        {"sub": "user2", "role": "user", "exp": datetime.now(UTC) + timedelta(minutes=5)},
        SECRET,
        algorithm="HS256",
    )
    non_admin_credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials=non_admin_token,
    )
    with pytest.raises(HTTPException) as forbidden_exc:
        require_admin(non_admin_credentials)
    assert forbidden_exc.value.status_code == 403

    # Unauthorized (expired/invalid)
    invalid_credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials="invalid-token",
    )
    with pytest.raises(HTTPException) as invalid_exc:
        require_admin(invalid_credentials)
    assert invalid_exc.value.status_code == 401
    assert invalid_exc.value.detail == "Invalid or expired token"
