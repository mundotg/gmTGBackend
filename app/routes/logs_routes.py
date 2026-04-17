# app/api/logs/logs_routes.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.database import get_db
from app.models.log_models import Log
from app.routes.connection_routes import get_current_user_id
from app.schemas.responsehttp_schema import ResponseWrapper
from app.ultils.logger import log_message


router = APIRouter()


@router.get("/logs", response_model=ResponseWrapper[list])
async def get_logs(
    level: Optional[str] = Query(
        None, description="Filtrar por nível (info, error, warning, success)"
    ),
    limit: int = Query(100, le=500, description="Quantidade máxima de logs"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    📋 Retorna logs do sistema com opção de filtro por nível.
    """
    try:
        query = db.query(Log)

        if level:
            query = query.filter(Log.level == level)

        logs = query.order_by(Log.created_at.desc()).limit(limit).all()

        return ResponseWrapper(success=True, data=logs)

    except Exception as e:
        log_message(
            db,
            f"❌ Erro em get_logs: {e}",
            level="error",
            source="get_logs",
            user=user_id,
        )
        raise HTTPException(status_code=500, detail="Erro ao buscar logs.")


from sqlalchemy import func


@router.get("/logs/stats", response_model=ResponseWrapper[list])
async def get_logs_stats(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    📊 Retorna estatísticas de logs agrupadas por nível.
    """
    try:
        stats = db.query(Log.level, func.count(Log.id)).group_by(Log.level).all()

        return ResponseWrapper(
            success=True,
            data=[{"level": level, "count": count} for level, count in stats],
        )

    except Exception as e:
        log_message(
            db,
            f"❌ Erro em get_logs_stats: {e}",
            level="error",
            source="get_logs_stats",
            user=user_id,
        )
        raise HTTPException(
            status_code=500, detail="Erro ao gerar estatísticas de logs."
        )


@router.get("/logs/{log_id}", response_model=ResponseWrapper[dict])
async def get_log_by_id(
    log_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    🔍 Retorna um log específico por ID.
    """
    try:
        log = db.query(Log).filter(Log.id == log_id).first()

        if not log:
            raise HTTPException(status_code=404, detail="Log não encontrado.")

        return ResponseWrapper(success=True, data=log)

    except HTTPException:
        raise
    except Exception as e:
        log_message(
            db,
            f"❌ Erro em get_log_by_id: {e}",
            level="error",
            source="get_log_by_id",
            user=user_id,
        )
        raise HTTPException(status_code=500, detail="Erro ao buscar log.")
