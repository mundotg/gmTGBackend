from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from app.config.dotenv import get_env
from app.models.user_model import RefreshToken
from app.ultils.logger import log_message

MEMINUTO_VALIDO_ACCESS_TOKEN = int(get_env("ACCESS_TOKEN_EXPIRE_MINUTES", 30)) 
DIA_VALIDO_REFRESH_TOKEN = int(get_env("REFRESH_TOKEN_EXPIRE_DAYS", 7))
ACCESS_TOKEN_EXPIRE_MINUTES = MEMINUTO_VALIDO_ACCESS_TOKEN * 60
REFRESH_TOKEN_EXPIRE_DAYS = DIA_VALIDO_REFRESH_TOKEN * 24 * 60 * 60

# Salvar novo refresh token
def store_refresh_token(db: Session, token: str, user_id: int, days_valid: int = DIA_VALIDO_REFRESH_TOKEN):
    db_token = RefreshToken(
        token=token,
        user_id=user_id,
        expires_at=datetime.utcnow() + timedelta(days=days_valid)
    )
    db.add(db_token)
    db.commit()

# Verifica se o token é válido (não revogado e não expirado)
def is_refresh_token_valid(db: Session, token: str) -> bool:
    db_token = db.query(RefreshToken).filter(RefreshToken.token == token).first()

    if not db_token:
        log_message(f"❌ refresh token não encontrado no BD token={token}", "error")
        return False

    if db_token.revoked:
        log_message("❌ refresh token está revogado", "error")
        return False

    now = datetime.now(timezone.utc)
    exp = db_token.expires_at

    # Se exp vier "naive", normaliza para UTC
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)

    if exp <= now:
        log_message(f"❌ refresh token expirado. exp={exp} now={now}", "error")
        return False

    return True


def refresh_token_time_left(db: Session, token: str):
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    if not db_token or not db_token.expires_at:
        return None, False
    
    expires_at = db_token.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    
    now = datetime.now(timezone.utc)
    delta = expires_at - now
    return delta, delta <= timedelta(days=1)

def refresh_token_time_left_update(db: Session, token: str):
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    if not db_token or not db_token.expires_at:
        return None, False
    
    now = datetime.now(timezone.utc)

    # Corrige naive -> UTC
    expires_at = db_token.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    delta = expires_at - now
    near_expiration = delta <= timedelta(days=1)

    if near_expiration:
        new_expiration = now + timedelta(days=7)
        db_token.expires_at = new_expiration
        db.commit()
        db.refresh(db_token)
        delta = new_expiration - now
    
    return delta, near_expiration

def update_refresh_token(db: Session, token: str, new_token: str):
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    if db_token:
        db_token.token = new_token
        db.commit()

# Revoga o token
def revoke_token(db: Session, token: str):
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    if db_token:
        db_token.revoked = True
        db.commit()
