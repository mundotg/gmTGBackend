
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

from typing import List, Optional, Any, Dict, Type
from sqlalchemy import func, select, or_
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import SQLAlchemyError
from app.ultils.logger import log_message
    
def get_paginated_query(
    db: Session,
    model: Type,
    search: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    page: int = 1,
    limit: int = 10,
    relationships: Optional[List[str]] = None,
):
    """Retorna resultados paginados de qualquer modelo com suporte a relações e schemas."""
    try:
        query = select(model)

        # 🔗 Carregar relações se especificadas
        if relationships:
            for relation in relationships:
                if hasattr(model, relation):
                    query = query.options(selectinload(getattr(model, relation)))
                else:
                    log_message(f"Relação '{relation}' não encontrada no modelo {model.__name__}", "warning")

        # 🔍 Busca textual
        if search:
            or_conditions = [
                col.ilike(f"%{search}%")
                for col in model.__table__.columns
                if hasattr(col.type, "python_type") and col.type.python_type == str
            ]
            if or_conditions:
                query = query.filter(or_(*or_conditions))

        # ⚙️ Filtros dinâmicos
        if filters:
            for key, value in filters.items():
                if hasattr(model, key):
                    if value is not None:
                        query = query.filter(getattr(model, key) == value)

        total = db.scalar(select(func.count()).select_from(query.subquery()))
        offset = (page - 1) * limit
        items = db.scalars(query.offset(offset).limit(limit)).all()

        return {
            "items": items,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit,
        }

    except SQLAlchemyError as e:
        log_message(f"Erro ao executar consulta paginada: {e}", "error")
        return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}
