from __future__ import annotations

from typing import Optional, Tuple
from datetime import datetime, timedelta, timezone
import hashlib

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.config.dotenv import get_env, get_env_bool, get_env_int
from app.models.user_model import RefreshToken
from app.ultils.logger import log_message

# 🚀 IMPORTAÇÃO CRÍTICA PARA A CONSISTÊNCIA DE SEGURANÇA
from app.services.crypto_utils import aes_encrypt, aes_decrypt


# =========================
# Config
# =========================
ACCESS_TOKEN_EXPIRE_MINUTES = get_env_int("ACCESS_TOKEN_EXPIRE_MINUTES", 30)
REFRESH_TOKEN_EXPIRE_DAYS = get_env_int("REFRESH_TOKEN_EXPIRE_DAYS", 7)

# Enforcement ESTRITO do binding do refresh token por IP / User-Agent.
BIND_IP = get_env_bool("BIND_IP", False)
BIND_UA = get_env_bool("BIND_UA", False)


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

        # 🚀 IP e Hash guardados de forma totalmente ilegível na BD
        db_token = RefreshToken(
            token=sha256_hex(token),  # 🔐 guarda hash
            user_id=user_id,
            user_IP=(fp["user_ip_prefix"] or "").strip(), # 🔐 guarda IP encriptado
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

    # 1. Prepara os valores atuais para comparação
    current_ip = (fp["user_ip_prefix"] or "").strip()
    current_ua_raw = fp.get("user_agent", "")
    current_ua_hash = sha256_hex(normalize_user_agent(current_ua_raw))

    # 🚀 2. Desencripta o IP da BD de forma segura (Fallback para compatibilidade)
    try:
        # Se for um bloco AES válido, desencripta
        db_ip = aes_decrypt(db_token.user_IP) if db_token.user_IP else ""
    except Exception:
        # Se falhar (ex: tokens antigos que ainda estavam em texto limpo), usa o valor cru
        db_ip = db_token.user_IP

    # 3. LOG CRÍTICO DE DEPURAÇÃO: Mostra tudo o que vai ser comparado
    log_message(
        f"🔍 DEPURAÇÃO DE BINDING (REFRESH TOKEN):\n"
        f"  -> IP Guardado na BD : '{db_ip}' (Desencriptado)\n"
        f"  -> IP Atual (Request): '{current_ip}'\n"
        f"  -> UA Hash (BD)      : '{db_token.user_agent}'\n"
        f"  -> UA Hash (Atual)   : '{current_ua_hash}'\n"
        f"  -> UA Raw (Texto)    : '{current_ua_raw[:60]}...'\n"
        f"  -> BIND_IP={BIND_IP} | BIND_UA={BIND_UA}",
        "info"
    )

    # IP: divergência é apenas avisada (só falha se BIND_IP estiver ativo).
    if db_ip != current_ip:
        log_message(
            f"⚠️ refresh: IP divergente (guardado={db_ip} "
            f"atual={current_ip})",
            "warning",
        )
        if BIND_IP:
            raise ValueError("Sessão inválida (IP diferente)")

    # User-Agent: divergência é apenas avisada (só falha se BIND_UA ativo).
    if db_token.user_agent != current_ua_hash:
        log_message(
            "⚠️ refresh: User-Agent divergente (browser atualizado?) — "
            "sessão mantida por o token ser válido/não revogado.",
            "warning",
        )
        if BIND_UA:
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