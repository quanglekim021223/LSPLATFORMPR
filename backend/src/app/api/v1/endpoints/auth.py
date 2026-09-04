from __future__ import annotations

from secrets import compare_digest

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.auth.auth import create_access_token, verify_password
from app.core.config import Settings


class LoginRequest(BaseModel):
    userid: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str


def build_auth_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/auth", tags=["auth"])

    @router.post("/login", response_model=TokenResponse)
    async def login(request: LoginRequest) -> TokenResponse:
        username_matches = compare_digest(
            request.userid.encode(),
            settings.auth_admin_username.encode(),
        )
        password_matches = verify_password(
            request.password,
            settings.auth_admin_password_hash.get_secret_value(),
        )
        if not username_matches or not password_matches:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

        token = create_access_token(
            user_id=request.userid,
            secret_key=settings.auth_jwt_secret.get_secret_value(),
            expire_minutes=settings.auth_token_expire_minutes,
        )
        return TokenResponse(access_token=token, token_type="Bearer")

    return router
