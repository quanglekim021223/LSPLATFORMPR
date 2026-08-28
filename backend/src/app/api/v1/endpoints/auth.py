from fastapi import APIRouter
from app.auth.auth import create_access_token, get_user_password_hash, verify_password, hash_password, save_user_password_hash
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, status

class RegisterRequest(BaseModel):
    userid: str
    password: str


class LoginRequest(BaseModel):
    userid: str
    password: str

@router.post("/login")
async def login(
    request: LoginRequest,
):

    password_hash = await get_user_password_hash(
        keyvault_client,
        request.userid,
    )

    if not password_hash:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    is_valid = verify_password(
        request.password,
        password_hash,
    )

    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    token = create_access_token(
        subject=request.userid,
    )

    return {
        "access_token": token,
        "token_type": "Bearer",
    }
    
def build_auth_router(
    keyvault_client,
) -> APIRouter:

    router = APIRouter(
        prefix="/auth",
        tags=["auth"],
    )

    @router.post("/register")
    async def register(
        request: RegisterRequest,
    ):

        existing = await get_user_password_hash(
            keyvault_client,
            request.userid,
        )

        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="User already exists",
            )

        password_hash = hash_password(
            request.password
        )

        await save_user_password_hash(
            keyvault_client,
            request.userid,
            password_hash,
        )

        return {
            "message": "User registered"
        }

    return router