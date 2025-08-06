from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.models.geral_model import  Settings
from app.schemas.geral_schema import  SettingsCreate

from app.ultils.logger import log_message

# === Settings ===
def create_settings(db: Session, user_id: int, settings_data: SettingsCreate):
    settings = Settings(**settings_data.model_dump(), user_id=user_id)
    db.add(settings)
    db.commit()
    db.refresh(settings)
    log_message(f"⚙️ Configurações criadas para o usuário {user_id}", "info")
    return settings

def get_settings(db: Session, user_id: int):
    log_message(f"⚙️ Recuperando configurações do usuário {user_id}", "info")
    return db.query(Settings).filter(Settings.user_id == user_id).first()
