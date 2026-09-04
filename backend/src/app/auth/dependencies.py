from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.auth.auth import (
    AuthenticationError,
    AuthorizationError,
    verify_admin_token,
)
from app.core.config import Settings

AdminDependency = Callable[..., Awaitable[str]]
_BEARER_SCHEME = HTTPBearer(auto_error=False)


def build_admin_dependency(settings: Settings) -> AdminDependency:
    async def require_admin(
        credentials: Annotated[
            HTTPAuthorizationCredentials | None,
            Depends(_BEARER_SCHEME),
        ] = None,
    ) -> str:
        if credentials is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Bearer token required",
                headers={"WWW-Authenticate": "Bearer"},
            )

        try:
            return verify_admin_token(
                token=credentials.credentials,
                secret_key=settings.auth_jwt_secret.get_secret_value(),
                admin_username=settings.auth_admin_username,
            )
        except AuthorizationError as exc:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required",
            ) from exc
        except AuthenticationError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc

    return require_admin
