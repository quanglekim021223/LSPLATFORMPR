from fastapi import APIRouter

from app.auth.auth import create_access_token


def build_auth_router() -> APIRouter:

    router = APIRouter(prefix="/auth", tags=["auth"])

    @router.post("/login")
    async def login():

        token = create_access_token(
            subject="minhtc35"
        )

        return {
            "access_token": token,
            "token_type": "Bearer",
        }

    return router