import traceback
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional

from app.config.cache_manager import cache_result
from app.cruds.connection_cruds import (
    get_connection_logs,
    get_connection_logs_pagination,
)
from app.database import get_db
from app.schemas.connetion_schema import (
    ConnectionLogBase,
    ConnectionLogPaginationOutput,
)
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

router = APIRouter(prefix="/log", tags=["connections_log"])

# -----------------------------
# Funções com Cache
# -----------------------------


@cache_result(ttl=600, user_id="user_logs_{user_id}")
def get_connection_logs_cached(db: Session, user_id: int):
    """Obtém logs de conexão com cache (10 min)"""
    return get_connection_logs(db, user_id)


# @cache_result(ttl=600, user_id="user_logs_pagination_{user_id}")
async def get_connection_logs_pagination_cached(
    db: Session, user_id: int, connection_id: Optional[int], page: int, limit: int
):
    """Obtém logs paginados com cache (5 min)"""
    return get_connection_logs_pagination(db, user_id, connection_id, page, limit)


# -----------------------------
# Endpoints
# -----------------------------


@router.get("/connection_history/", response_model=List[ConnectionLogBase])
async def list_connection_history(
    db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)
):
    """Lista histórico de conexões do usuário."""
    try:
        logs = get_connection_logs_cached(db, user_id)
        return [
            ConnectionLogBase.model_validate(
                {
                    "connection": str(log.connection_id),
                    "action": log.action,
                    "timestamp": log.timestamp,
                    "status": log.status,
                }
            )
            for log in logs
        ]
    except Exception as e:
        log_message(
            f"❌ Erro ao listar histórico: {str(e)}\n{traceback.format_exc()}", "error"
        )
        raise HTTPException(status_code=500, detail="Erro interno ao listar histórico.")


@router.get("/connection_logs/", response_model=ConnectionLogPaginationOutput)
async def list_connection_logs(
    connection_id: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(10, le=100),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:

        if page < 1 or limit < 1:
            raise HTTPException(status_code=400, detail="Parâmetros inválidos.")

        return await get_connection_logs_pagination_cached(
            db,
            user_id,
            connection_id,
            page,
            limit,
        )

    except Exception as e:
        log_message(f"❌ Erro ao listar logs: {e}", "error")

        raise HTTPException(status_code=500, detail="Erro interno.")
