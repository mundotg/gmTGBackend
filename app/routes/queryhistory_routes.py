"""
Rotas exclusivas para o histórico de execução de queries (Audit Log).
Versão corrigida: sem leak de conexão + cache seguro.
"""

import traceback
from typing import Optional
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, load_only
from sqlalchemy import desc, or_

from app.config.cache_manager import cache_result
from app.database import get_db
from app.models.queryhistory_models import QueryHistory
from app.routes.connection_routes import get_current_user_id
from app.schemas.queryhistory_schemas import QueryType
from app.ultils.logger import log_message

router = APIRouter(prefix="/history", tags=["AuditLog"])


# =========================
# CACHE (APENAS DADOS SERIALIZADOS)
# =========================
@cache_result(
    ttl=300,
    user_id="user_history_{user_id}_conn_{conn_id}_limit_{limit}_offset_{offset}"
)
def get_cached_logs(
    *,
    user_id: int,
    limit: int,
    offset: int,
    search: Optional[str],
    query_type: Optional[str],
    conn_id: Optional[int],
    db: Session,
):
    return fetch_audit_logs(
        db=db,
        limit=limit,
        offset=offset,
        search=search,
        query_type=query_type,
        conn_id=conn_id,
    )


# =========================
# DB QUERY (SEM CACHE)
# =========================
def fetch_audit_logs(
    db: Session,
    limit: int,
    offset: int,
    search: Optional[str],
    query_type: Optional[str],
    conn_id: Optional[int],
):
    query = db.query(QueryHistory).options(
        load_only(
            QueryHistory.id,
            QueryHistory.executed_by,
            QueryHistory.app_source,
            QueryHistory.query_type,
            QueryHistory.executed_at,
            QueryHistory.is_favorite,
            QueryHistory.duration_ms,
            QueryHistory.error_message,
            QueryHistory.tags,
            QueryHistory.db_connection_id,
        )
    )

    # 🔍 Search leve
    if search and search.strip():
        term = f"%{search.strip()}%"
        query = query.filter(
            or_(
                QueryHistory.executed_by.ilike(term),
                QueryHistory.app_source.ilike(term),
            )
        )

    # 🎯 Tipo
    if query_type and query_type != "Todos":
        try:
            valid_type = QueryType(query_type)
            query = query.filter(QueryHistory.query_type == valid_type.value)
        except ValueError:
            return []

    # 🔥 Conn ID
    if conn_id is not None:
        query = query.filter(QueryHistory.db_connection_id == conn_id)

    logs = (
        query.order_by(desc(QueryHistory.executed_at))
        .offset(offset)
        .limit(limit)
        .all()
    )

    # 🔥 SERIALIZA (ESSENCIAL)
    return [
        {
            "id": l.id,
            "executed_by": l.executed_by,
            "app_source": l.app_source,
            "query_type": l.query_type,
            "executed_at": l.executed_at,
            "is_favorite": l.is_favorite,
            "duration_ms": l.duration_ms,
            "error_message": l.error_message,
            "tags": l.tags,
            "db_connection_id": l.db_connection_id,
        }
        for l in logs
    ]


# =========================
# LISTAGEM
# =========================
@router.get("/")
async def get_audit_logs(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    search: Optional[str] = Query(default=None),
    query_type: Optional[str] = Query(default=None),
    conn_id: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        return get_cached_logs(
            user_id=user_id,
            limit=limit,
            offset=offset,
            search=search,
            query_type=query_type,
            conn_id=conn_id,
            db=db,  # ⚠️ importante
        )

    except Exception as e:
        log_message(
            f"[AUDIT][LIST] user={user_id} error={str(e)}\n{traceback.format_exc()}",
            "error",
        )
        raise HTTPException(
            status_code=500,
            detail="Erro ao carregar rastro de auditoria",
        )

# =========================
# DETALHE
# =========================
@router.get("/{history_id}")
async def get_audit_detail(
    history_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        log = db.query(QueryHistory).filter(QueryHistory.id == history_id).first()

        if not log:
            raise HTTPException(status_code=404, detail="Registro não encontrado")

        return log

    except HTTPException:
        raise
    except Exception as e:
        log_message(
            f"[AUDIT][DETAIL] user={user_id} error={str(e)}\n{traceback.format_exc()}",
            "error",
        )
        raise HTTPException(status_code=500, detail="Erro ao carregar detalhe")


# =========================
# FAVORITO
# =========================
@router.post("/{history_id}/favorite")
async def toggle_favorite_query(
    history_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        log = db.query(QueryHistory).filter(QueryHistory.id == history_id).first()

        if not log:
            raise HTTPException(status_code=404, detail="Registro não encontrado")

        log.is_favorite = not log.is_favorite
        log.modified_by = f"user_id_{user_id}"

        db.commit()
        db.refresh(log)

        return {
            "id": log.id,
            "is_favorite": log.is_favorite,
        }

    except Exception as e:
        db.rollback()
        log_message(
            f"[AUDIT][FAVORITE] user={user_id} error={str(e)}\n{traceback.format_exc()}",
            "error",
        )
        raise HTTPException(status_code=500, detail="Erro ao atualizar favorito")


# =========================
# CLEANUP
# =========================
@router.delete("/clear-old")
async def cleanup_history(
    days: int = Query(default=30, ge=1, le=3650),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        threshold = datetime.now(timezone.utc) - timedelta(days=days)

        deleted_count = (
            db.query(QueryHistory)
            .filter(
                QueryHistory.executed_at < threshold,
                QueryHistory.is_favorite.is_(False),
            )
            .delete(synchronize_session=False)
        )

        db.commit()

        log_message(
            f"[AUDIT][CLEANUP] user={user_id} removed={deleted_count}",
            "info",
        )

        return {
            "message": f"Limpeza concluída. {deleted_count} registros removidos.",
            "deleted": deleted_count,
        }

    except Exception as e:
        db.rollback()
        log_message(
            f"[AUDIT][CLEANUP] user={user_id} error={str(e)}\n{traceback.format_exc()}",
            "error",
        )
        raise HTTPException(status_code=500, detail="Erro ao limpar histórico")