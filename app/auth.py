from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt, ExpiredSignatureError
import bcrypt
from app.config.dotenv import get_env
from typing import Any, Optional

ACCESS_TOKEN_EXPIRE_MINUTES = int(get_env("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
REFRESH_TOKEN_EXPIRE_DAYS = int(get_env("REFRESH_TOKEN_EXPIRE_DAYS", 7))
EMAIL_VERIFICATION_EXPIRE_HOURS = int(get_env("EMAIL_VERIFICATION_EXPIRE_HOURS", 24))

SECRET_KEY = get_env("SECRET_KEY")
ALGORITHM = get_env("ALGORITHM")

if not SECRET_KEY or not ALGORITHM:
    raise ValueError("SECRET_KEY e ALGORITHM devem estar definidos no .env")


class TokenExpiredError(Exception):
    """Lançado quando o token de confirmação de e-mail expirou a sua validade."""
    pass


class TokenInvalidError(Exception):
    """Lançado quando o token de confirmação é inválido ou corrompido."""
    pass


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


def create_email_verification_token(email: str, expires_delta: Optional[timedelta] = None) -> str:
    """Cria um token JWT assinado para verificação de e-mail com validade temporal explícita."""
    delta = expires_delta or timedelta(hours=EMAIL_VERIFICATION_EXPIRE_HOURS)
    data = {"sub": email.strip().lower(), "typ": "email_verification"}
    return create_token(data, delta)


def verify_email_token(token: str) -> str:
    """
    Valida rigorosamente o token de confirmação de e-mail.
    Lança TokenExpiredError se o token tiver ultrapassado a sua validade.
    Lança TokenInvalidError se o token for inválido, malformado ou adulterado.
    Retorna o e-mail validado.
    """
    if not token or not token.strip():
        raise TokenInvalidError("Nenhum código de confirmação fornecido.")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except ExpiredSignatureError:
        raise TokenExpiredError("A ligação de confirmação expirou. O prazo de validade terminou.")
    except JWTError:
        raise TokenInvalidError("A ligação de confirmação é inválida ou foi corrompida.")

    if not isinstance(payload, dict) or payload.get("typ") != "email_verification":
        raise TokenInvalidError("Tipo de token inválido para confirmação de e-mail.")

    sub = payload.get("sub")
    if not sub:
        raise TokenInvalidError("Token não contém utilizador associado.")

    return str(sub).strip().lower()

