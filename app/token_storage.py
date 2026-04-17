from __future__ import annotations

from typing import Optional, Tuple
from datetime import datetime, timedelta, timezone
import hashlib

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.config.dotenv import get_env, get_env_int
from app.models.user_model import RefreshToken
from app.ultils.logger import log_message


# =========================
# Config
# =========================
ACCESS_TOKEN_EXPIRE_MINUTES = get_env_int("ACCESS_TOKEN_EXPIRE_MINUTES", 30)
REFRESH_TOKEN_EXPIRE_DAYS = get_env_int("REFRESH_TOKEN_EXPIRE_DAYS", 7)

BIND_IP = get_env("BIND_IP", "true").lower() == "true"
BIND_UA = get_env("BIND_UA", "true").lower() == "true"


# =========================
# Helpers
# =========================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_user_agent(ua: str) -> str:
    return " ".join((ua or "").strip().lower().split())


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def ensure_utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _validate_fp(fp: Optional[dict]) -> dict:
    if not isinstance(fp, dict):
        raise ValueError("Fingerprint inválido")

    if not fp.get("user_ip_prefix") or not fp.get("user_agent"):
        raise ValueError("Fingerprint incompleto")

    return fp


# =========================
# CORE FUNCTIONS
# =========================
def _get_token(db: Session, token: str) -> Optional[RefreshToken]:
    hashed = sha256_hex(token)
    return db.query(RefreshToken).filter_by(token=hashed).first()


def store_refresh_token(
    db: Session,
    token: str,
    user_id: int,
    days_valid: int = REFRESH_TOKEN_EXPIRE_DAYS,
    fp: Optional[dict] = None,
):
    try:
        fp = _validate_fp(fp)

        db_token = RefreshToken(
            token=sha256_hex(token),  # 🔐 guarda hash
            user_id=user_id,
            user_IP=(fp["user_ip_prefix"] or "").strip(),
            user_agent=sha256_hex(normalize_user_agent(fp["user_agent"])),
            expires_at=utcnow() + timedelta(days=days_valid),
            revoked=False,
        )

        db.add(db_token)
        db.commit()

        log_message("✅ Refresh token armazenado com sucesso", "info")

    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            message=f"❌ Erro ao salvar refresh token: {e}",
            level="error",
            source="token_storage.py",
            user=user_id,
        )
        raise RuntimeError("Erro interno ao salvar token")


def is_refresh_token_valid(db: Session, token: str) -> bool:
    try:
        db_token = _get_token(db, token)

        if not db_token:
            log_message("❌ Token não encontrado", "warning")
            return False

        if db_token.revoked:
            log_message("❌ Token revogado", "warning")
            return False

        if ensure_utc(db_token.expires_at) <= utcnow():
            log_message("❌ Token expirado", "warning")
            return False

        return True

    except Exception as e:
        log_message(
            message=f"❌ Erro ao validar token: {e}",
            level="error",
            source="token_storage.py",
            db=db,
        )
        return False


def assert_refresh_token_binding(db: Session, token: str, fp: dict) -> None:
    fp = _validate_fp(fp)
    db_token = _get_token(db, token)

    if not db_token:
        raise ValueError("Token não encontrado")

    if db_token.revoked:
        raise ValueError("Sessão inválida")

    if ensure_utc(db_token.expires_at) <= utcnow():
        raise ValueError("Sessão expirada")

    if BIND_IP:
        if db_token.user_IP != (fp["user_ip_prefix"] or "").strip():
            raise ValueError("Sessão inválida (IP diferente)")

    if BIND_UA:
        ua_hash = sha256_hex(normalize_user_agent(fp["user_agent"]))
        if db_token.user_agent != ua_hash:
            raise ValueError("Sessão inválida (dispositivo diferente)")


def rotate_refresh_token(
    db: Session,
    old_token: str,
    new_token: str,
    user_id: int,
    fp: dict,
):
    try:
        revoke_token(db, old_token)
        store_refresh_token(db, new_token, user_id, fp=fp)

        log_message("🔄 Refresh token rotacionado", "info")

    except Exception as e:
        log_message(f"❌ Erro ao rotacionar token: {e}", "error")
        raise RuntimeError("Erro ao renovar sessão")


def refresh_token_time_left(
    db: Session, token: str
) -> Tuple[Optional[timedelta], bool]:
    db_token = _get_token(db, token)

    if not db_token:
        return None, False

    delta = ensure_utc(db_token.expires_at) - utcnow()
    return delta, delta <= timedelta(days=1)


def revoke_token(db: Session, token: str):
    try:
        db_token = _get_token(db, token)

        if db_token:
            db_token.revoked = True
            db.commit()
            log_message("🚫 Token revogado", "info")

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao revogar token: {e}", "error")


def revoke_all_user_tokens(db: Session, user_id: int):
    try:
        db.query(RefreshToken).filter(
            RefreshToken.user_id == user_id, RefreshToken.revoked == False
        ).update({"revoked": True})

        db.commit()
        log_message(f"🚫 Todos tokens do usuário {user_id} revogados", "info")

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao revogar tokens: {e}", "error")
