from datetime import datetime, timedelta
from jose import JWTError, jwt
import bcrypt
from app.config.dotenv import get_env
from app.token_storage import DIA_VALIDO_REFRESH_TOKEN, MEMINUTO_VALIDO_ACCESS_TOKEN

# Configurações do .env
SECRET_KEY = get_env("SECRET_KEY")
ALGORITHM = get_env("ALGORITHM")

if not SECRET_KEY or not ALGORITHM:
    raise ValueError("SECRET_KEY e ALGORITHM devem estar definidos no .env")


# Funções de hash
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


# Criação de tokens JWT
def create_token(data: dict, expires_delta: timedelta) -> str:
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + expires_delta})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    delta = expires_delta or timedelta(minutes=MEMINUTO_VALIDO_ACCESS_TOKEN)
    return create_token(data, delta)


def create_refresh_token(data: dict) -> str:
    return create_token(data, timedelta(days=DIA_VALIDO_REFRESH_TOKEN))


# Decodificação de token
def decode_token(token: str) -> str | None:
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None


