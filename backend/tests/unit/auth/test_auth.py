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
