from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from app.config.dotenv import get_env
from app.models.user_model import RefreshToken

ACCESS_TOKEN_EXPIRE_MINUTES = int(get_env("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
REFRESH_TOKEN_EXPIRE_DAYS = int(get_env("REFRESH_TOKEN_EXPIRE_DAYS", 7))
# Salvar novo refresh token
def store_refresh_token(db: Session, token: str, user_id: int, days_valid: int = REFRESH_TOKEN_EXPIRE_DAYS):
    db_token = RefreshToken(
        token=token,
        user_id=user_id,
        expires_at=datetime.utcnow() + timedelta(days=days_valid)
    )
    db.add(db_token)
    db.commit()

# Verifica se o token é válido (não revogado e não expirado)
def is_refresh_token_valid(db: Session, token: str) -> bool:
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    return db_token is not None and not db_token.revoked and db_token.expires_at > datetime.utcnow()

# Revoga o token
def revoke_token(db: Session, token: str):
    db_token = db.query(RefreshToken).filter_by(token=token).first()
    if db_token:
        db_token.revoked = True
        db.commit()
