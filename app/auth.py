from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
import bcrypt
from app.config.dotenv import get_env
from typing import Any, Optional

ACCESS_TOKEN_EXPIRE_MINUTES = int(get_env("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
REFRESH_TOKEN_EXPIRE_DAYS = int(get_env("REFRESH_TOKEN_EXPIRE_DAYS", 7))

SECRET_KEY = get_env("SECRET_KEY")
ALGORITHM = get_env("ALGORITHM")

if not SECRET_KEY or not ALGORITHM:
    raise ValueError("SECRET_KEY e ALGORITHM devem estar definidos no .env")


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def create_token(data: dict, expires_delta: timedelta) -> str:
    to_encode = data.copy()
    # usa UTC consistente
    to_encode.update({"exp": datetime.now(timezone.utc) + expires_delta})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    delta = expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    # opcional, mas MUITO útil: marcar tipo
    data = {**data, "typ": "access"}
    return create_token(data, delta)


def create_refresh_token(data: dict) -> str:
    data = {**data, "typ": "refresh"}
    return create_token(data, timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS))


# ✅ AGORA retorna o payload inteiro
def decode_token(token: str) -> Optional[dict[str, Any]]:
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload if isinstance(payload, dict) else None
    except JWTError:
        return None


# ✅ helper para quando tu só queres o sub
def decode_subject(token: str) -> Optional[str]:
    payload = decode_token(token)
    if not payload:
        return None
    sub = payload.get("sub")
    return str(sub) if sub is not None else None

