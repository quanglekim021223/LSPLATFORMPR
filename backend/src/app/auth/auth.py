# auth.py

from datetime import datetime, timedelta, timezone
import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError
import bcrypt
from azure.keyvault.secrets import SecretClient

SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"


def create_access_token(user_id: str) -> str:
    payload = {
        "sub": user_id,
        "exp": datetime.now(timezone.utc) + timedelta(hours=1)
    }

    token = jwt.encode(
        payload,
        SECRET_KEY,
        algorithm=ALGORITHM
    )

    return token



def verify_token(token: str) -> dict:

    try:
        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )

        return payload

    except ExpiredSignatureError:
        raise Exception("Token expired")

    except InvalidTokenError:
        raise Exception("Invalid token")
    

def hash_password(password: str) -> str:
    return bcrypt.hashpw(
        password.encode("utf-8"),
        bcrypt.gensalt(),
    ).decode("utf-8")


def verify_password(
    password: str,
    password_hash: str,
) -> bool:

    return bcrypt.checkpw(
        password.encode("utf-8"),
        password_hash.encode("utf-8"),
    )    
    
    

async def get_user_password_hash(
    client: SecretClient,
    userid: str,
) -> str | None:

    try:
        secret = client.get_secret(
            f"user-{userid}"
        )

        return secret.value

    except Exception:
        return None


async def save_user_password_hash(
    client: SecretClient,
    userid: str,
    password_hash: str,
) -> None:

    client.set_secret(
        f"user-{userid}",
        password_hash,
    )    