from __future__ import annotations

from typing import Optional, Tuple
from datetime import datetime, timedelta, timezone
import hashlib

from sqlalchemy.orm import Session

from app.config.dotenv import get_env
from app.models.user_model import RefreshToken
from app.ultils.logger import log_message


# =========================
# Config
# =========================
ACCESS_TOKEN_EXPIRE_MINUTES = int(get_env("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
REFRESH_TOKEN_EXPIRE_DAYS = int(get_env("REFRESH_TOKEN_EXPIRE_DAYS", 7))

ACCESS_TOKEN_EXPIRE_SECONDS = ACCESS_TOKEN_EXPIRE_MINUTES * 60
REFRESH_TOKEN_EXPIRE_SECONDS = REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60

# “hardening” do binding
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
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _validate_fp(fp: Optional[dict]) -> dict:
    """
    fp esperado:
      {
        "user_ip_prefix": "...",
        "user_agent": "...",
        "fp": "sha256..."
      }
    """
    if not fp or not isinstance(fp, dict):
        raise ValueError("Fingerprint (fp) ausente ou inválido")

    if "user_ip_prefix" not in fp or "user_agent" not in fp:
        raise ValueError("Fingerprint (fp) incompleto")

    return fp


# =========================
# CRUD Refresh Token
# =========================
def store_refresh_token(
    db: Session,
    token: str,
    user_id: int,
    days_valid: int = REFRESH_TOKEN_EXPIRE_DAYS,
    fp: Optional[dict] = None,
):
    """
    Armazena refresh token com binding (IP/UA).
    Sugestão: salvar UA como hash por privacidade; IP prefix pode ser salvo (ou hash também).
    """
    fp = _validate_fp(fp)

    user_ip_prefix = (fp.get("user_ip_prefix") or "").strip()
    user_agent_norm = normalize_user_agent(fp.get("user_agent") or "")
    user_agent_hash = sha256_hex(user_agent_norm)

    db_token = RefreshToken(
        token=token,
        user_id=user_id,
        user_IP=user_ip_prefix,              # podes mudar para hash se quiseres
        user_agent=user_agent_hash,          # guarda hash em vez do UA cru
        expires_at=utcnow() + timedelta(days=days_valid),
        revoked=False,
    )
    db.add(db_token)
    db.commit()


def is_refresh_token_valid(db: Session, token: str) -> bool:
    db_token = db.query(RefreshToken).filter(RefreshToken.token == token).first()

    if not db_token:
        log_message("❌ refresh token não encontrado no BD", "error")
        return False

    if getattr(db_token, "revoked", False):
        log_message("❌ refresh token está revogado", "error")
        return False

    exp = ensure_utc(db_token.expires_at)
    now = utcnow()

    if exp <= now:
        log_message(f"❌ refresh token expirado. exp={exp} now={now}", "error")
        return False

    return True


def assert_refresh_token_binding(db: Session, token: str, fp: dict) -> None:
    """
    Valida severamente se IP/UA do refresh token no BD batem com o request atual.
    Chama isto no /refresh ANTES de gerar token novo.
    """
    fp = _validate_fp(fp)

    db_token = db.query(RefreshToken).filter(RefreshToken.token == token).first()
    if not db_token:
        raise ValueError("Refresh token não encontrado")

    if getattr(db_token, "revoked", False):
        raise ValueError("Refresh token revogado")

    # checa expiração
    exp = ensure_utc(db_token.expires_at)
    if exp <= utcnow():
        raise ValueError("Refresh token expirado")

    # binding
    if BIND_IP:
        ip_now = (fp.get("user_ip_prefix") or "").strip()
        if not ip_now or db_token.user_IP != ip_now:
            raise ValueError("Binding inválido: IP divergente")

    if BIND_UA:
        ua_now = normalize_user_agent(fp.get("user_agent") or "")
        ua_hash_now = sha256_hex(ua_now)
        if not ua_now or db_token.user_agent != ua_hash_now:
            raise ValueError("Binding inválido: User-Agent divergente")


def refresh_token_time_left(db: Session, token: str) -> Tuple[Optional[timedelta], bool]:
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    if not db_token or not db_token.expires_at:
        return None, False

    expires_at = ensure_utc(db_token.expires_at)
    now = utcnow()
    delta = expires_at - now
    return delta, delta <= timedelta(days=1)


def update_refresh_token(db: Session, token: str, new_token: str):
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    if db_token:
        db_token.token = new_token
        db.commit()


def revoke_token(db: Session, token: str):
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    if db_token:
        db_token.revoked = True
        db.commit()


def revoke_all_user_tokens(db: Session, user_id: int):
    db.query(RefreshToken).filter(RefreshToken.user_id == user_id, RefreshToken.revoked == False).update(
        {"revoked": True}
    )
    db.commit()