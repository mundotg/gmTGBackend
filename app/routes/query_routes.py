"""
Rotas para execução de queries e operações no banco de dados.
Versão melhorada com melhor estrutura, segurança e tratamento de erros.
"""

import hashlib
import json
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.redis import read_cache, write_cache
from app.database import get_db, get_db_async
from app.routes.connection_routes import get_current_user_id
from app.schemas.query_select_upAndInsert_schema import (
    AutoCreateRequest,
    CondicaoFiltro,
    InsertRequest,
    OrderByOption,
    QueryPayload,
    UpdateRequest,
)

# from app.services.cloudeAi_execute_query import executar_query_e_salvar_stream
from app.services.execute_query_select_stream import executar_query_e_salvar_stream
from app.services.insert_row_service import insert_row_service
from app.services.insert_service_auto import insert_row_service_auto
from app.ultils.QueryExecutionService import QueryExecutionService
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message
from app.ultils.update_line_build_exe import update_row_service


class QueryChannelManager:
    """Gerenciador de canais de query com limpeza automática."""

    def __init__(self, ttl_minutes: int = 30):
        self.channels: Dict[str, Dict[str, Any]] = {}
        self.ttl_minutes = ttl_minutes

    def create_channel(self, user_id: int, query: QueryPayload) -> str:
        """Cria um novo canal para a query."""
        channel_id = str(uuid.uuid4())
        self.channels[channel_id] = {
            "user_id": user_id,
            "query": query,
            "created_at": datetime.utcnow(),
        }
        return channel_id

    def get_channel(self, channel_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        """Recupera um canal validando ownership."""
        channel = self.channels.get(channel_id)
        if not channel:
            return None

        # Verifica se o canal expirou
        if self._is_expired(channel):
            self.remove_channel(channel_id)
            return None

        # Verifica ownership
        if channel["user_id"] != user_id:
            return None

        return channel

    def remove_channel(self, channel_id: str) -> bool:
        """Remove um canal."""
        return self.channels.pop(channel_id, None) is not None

    def cleanup_expired_channels(self) -> int:
        """Remove canais expirados."""
        expired_channels = []
        for channel_id, channel in self.channels.items():
            if self._is_expired(channel):
                expired_channels.append(channel_id)

        for channel_id in expired_channels:
            self.remove_channel(channel_id)

        return len(expired_channels)

    def _is_expired(self, channel: Dict[str, Any]) -> bool:
        """Verifica se um canal expirou."""
        created_at = channel.get("created_at")
        if not created_at:
            return True

        expiry_time = created_at + timedelta(minutes=self.ttl_minutes)
        return datetime.utcnow() > expiry_time


# Instâncias globais
channel_manager = QueryChannelManager()

# Router configuration
router = APIRouter(prefix="/exe", tags=["executeQuery"])


def cleanup_expired_channels():
    """Task em background para limpeza de canais expirados."""
    cleaned = channel_manager.cleanup_expired_channels()
    if cleaned > 0:
        log_message(f"Removidos {cleaned} canais expirados", "info")


@asynccontextmanager
async def handle_db_transaction(db: Session):
    """Context manager para transações do banco."""
    try:
        yield db
    except Exception:
        db.rollback()
        raise


@router.post("/update_row")
async def update_row_endpoint(
    data: UpdateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Atualiza uma linha na tabela."""
    background_tasks.add_task(cleanup_expired_channels)

    async with handle_db_transaction(db):
        try:
            engine, connection = ConnectionManager.ensure_connection(db, user_id)

            log_message(f"Atualizando linha para usuário {user_id}", "info")
            result = update_row_service(
                data,
                engine,
                user_id,
                connection.type,
                connection.id,
                db,  # pyright: ignore[reportArgumentType]
            )

            log_message("Linha atualizada com sucesso", "success")
            return result

        except HTTPException:
            raise
        except Exception as e:
            error_msg = f"Erro ao atualizar linha: {str(e)}"
            log_message(f"{error_msg}\n{traceback.format_exc()}", "error")
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/insert_row")
async def insert_row_endpoint(
    data: InsertRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Insere uma nova linha na tabela."""
    background_tasks.add_task(cleanup_expired_channels)

    async with handle_db_transaction(db):
        try:
            engine, connection = ConnectionManager.ensure_connection(db, user_id)

            log_message(f"Inserindo linha para usuário {user_id}", "info")
            result = insert_row_service(
                data, engine, user_id, connection.type, connection.id, db  # type: ignore
            )

            log_message("Linha inserida com sucesso", "success")
            return result

        except HTTPException:
            raise
        except Exception as e:
            error_msg = f"Erro ao inserir linha: {str(e)}"
            log_message(f"{error_msg}\n{traceback.format_exc()}", "error")
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/auto-create")
async def auto_create_endpoint(
    data: AutoCreateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Cria múltiplas linhas automaticamente."""
    background_tasks.add_task(cleanup_expired_channels)

    async with handle_db_transaction(db):
        try:
            engine, connection = ConnectionManager.ensure_connection(db, user_id)

            log_message(f"Auto-criando linhas para usuário {user_id}", "info")
            result = insert_row_service_auto(data, engine, user_id, connection, db)

            log_message("Auto-criação concluída com sucesso", "success")
            return result

        except HTTPException:
            raise
        except Exception as e:
            error_msg = f"Erro no auto-create: {str(e)}"
            log_message(f"{error_msg}\n{traceback.format_exc()}", "error")
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute_query")
async def execute_query_endpoint(
    body: QueryPayload,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Executa uma query sincronamente."""
    query_service = QueryExecutionService()
    return await query_service.execute_query(body, db, user_id)


@router.post("/query-scroll")
async def execute_query_scroll_endpoint(
    body: QueryPayload,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Executa uma query com scroll (paginação)."""
    query_service = QueryExecutionService()
    return await query_service.execute_query(body, db, user_id)


# ============================================================
# 🚀 Paginação por CURSOR (keyset) + cache Redis
# ============================================================
class QueryMoreRequest(BaseModel):
    payload: QueryPayload
    cursor: Optional[str] = None            # último valor da coluna de ordem
    order_column: Optional[str] = None      # coluna de ordem (o cliente indica)
    direction: Optional[str] = "ASC"
    limit: int = 50


def _infer_value_type(v: Optional[str]) -> str:
    if v is None:
        return "string"
    s = str(v).strip()
    try:
        float(s)
        return "number"
    except (TypeError, ValueError):
        pass
    if s.lower() in ("true", "false"):
        return "boolean"
    return "string"


def _derive_order(payload: QueryPayload, order_column: Optional[str]) -> tuple[Optional[str], str]:
    """Coluna + direção de ordem. Prioriza a indicada pelo cliente, senão o
    orderBy do payload, senão a 1ª coluna do select/aliases."""
    if order_column:
        return order_column, "ASC"
    ob = getattr(payload, "orderBy", None) or []
    if ob:
        return ob[0].column, (ob[0].direction or "ASC").upper()
    if payload.select:
        return payload.select[0], "ASC"
    if payload.aliaisTables:
        return next(iter(payload.aliaisTables.keys())), "ASC"
    return None, "ASC"


def _extract_col_value(row: Dict[str, Any], col: str) -> Any:
    """Valor da coluna de ordem numa linha (chaves podem ser qualificadas)."""
    if col in row:
        return row[col]
    leaf = col.split(".")[-1]
    if leaf in row:
        return row[leaf]
    for k in row:
        if str(k).split(".")[-1] == leaf:
            return row[k]
    return None


@router.post("/query-more")
async def query_more_endpoint(
    body: QueryMoreRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Carrega a PÁGINA SEGUINTE por CURSOR (keyset): em vez de `OFFSET n` (que
    re-varre a tabela e fica caro sob carga), aplica `WHERE ordem > cursor`
    (usa índice, custo ~constante). Reutiliza o motor de query (SQL e MongoDB).
    Cacheado em Redis por página → sob muita carga, pedidos idênticos não
    voltam à BD.
    """
    payload = body.payload
    limit = max(1, min(body.limit or 50, 500))
    direction = (body.direction or "ASC").upper()
    order_col, derived_dir = _derive_order(payload, body.order_column)
    if not body.order_column and not body.direction:
        direction = derived_dir

    # 🔑 Cache Redis (página) — chave por payload + cursor + ordem + limite.
    cache_key = None
    try:
        h = hashlib.sha1(
            json.dumps(payload.model_dump(exclude_none=True), sort_keys=True, default=str).encode()
        ).hexdigest()[:16]
        cache_key = f"qmore:{user_id}:{h}:{order_col}:{direction}:{body.cursor}:{limit}"
        cached = read_cache(cache_key)
        if cached:
            return cached
    except Exception:  # noqa: BLE001
        cache_key = None

    # Prepara o payload da página seguinte (sem offset).
    payload.limit = limit
    payload.offset = None
    payload.isCountQuery = False

    # Filtro keyset: só a partir do cursor (a 1ª página não leva cursor).
    if order_col and body.cursor is not None and str(body.cursor) != "":
        op = ">" if direction == "ASC" else "<"
        parts = order_col.split(".")
        table_ref = ".".join(parts[:-1]) if len(parts) >= 2 else (payload.baseTable or "")
        vt = _infer_value_type(body.cursor)
        payload.where = [
            *(payload.where or []),
            CondicaoFiltro(
                table_name_fil=table_ref,
                column=order_col,
                operator=op,
                value=str(body.cursor),
                column_type="number" if vt == "number" else "string",
                value_type=vt,  # type: ignore[arg-type]
                logicalOperator="AND",
            ),
        ]

    # Garante ordenação determinística pela coluna de cursor.
    if order_col and not (getattr(payload, "orderBy", None) or []):
        payload.orderBy = [OrderByOption(column=order_col, direction=direction)]

    result = await QueryExecutionService().execute_query(payload, db, user_id)
    preview: List[Dict[str, Any]] = result.get("preview", []) if isinstance(result, dict) else []

    next_cursor = None
    if preview and order_col:
        val = _extract_col_value(preview[-1], order_col)
        next_cursor = None if val is None else str(val)

    resp = {
        "success": True,
        "preview": preview,
        "columns": result.get("columns", []) if isinstance(result, dict) else [],
        "next_cursor": next_cursor,
        "has_more": len(preview) >= limit,
        "order_column": order_col,
        "direction": direction,
    }

    if cache_key:
        try:
            write_cache(cache_key, resp, ttl=45)  # curto: dados podem mudar
        except Exception:  # noqa: BLE001
            pass

    return resp


@router.post("/start-query")
async def start_query_endpoint(
    query: QueryPayload,
    background_tasks: BackgroundTasks,
    # db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    """
    Inicia uma query assíncrona e retorna um channel ID para SSE.
    """
    background_tasks.add_task(cleanup_expired_channels)

    try:
        # Cria canal
        channel_id = channel_manager.create_channel(user_id, query)

        # log_message(f"Canal criado: {channel_id} para usuário {user_id}", "info")

        return {
            "channelId": channel_id,
            "message": "Canal criado com sucesso",
            "expiresIn": f"{channel_manager.ttl_minutes} minutos",
        }

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Erro ao criar canal: {str(e)}"
        log_message(f"{error_msg}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/query-sse/{channel_id}")
async def execute_query_sse_endpoint(
    channel_id: str,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    """
    Executa query via Server-Sent Events usando o channel ID.
    """
    try:
        # Valida formato do channel_id
        try:
            uuid.UUID(channel_id)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Channel ID deve ser um UUID válido"
            )

        # Recupera canal
        channel = channel_manager.get_channel(channel_id, user_id)
        if not channel:
            raise HTTPException(
                status_code=404, detail="Canal não encontrado ou expirado"
            )

        query_payload: QueryPayload = channel["query"]
        if not query_payload:
            raise HTTPException(
                status_code=400, detail="Query payload não encontrado no canal"
            )

        # log_message(f"query_payload:{query_payload} ", "info")
        # print(query_payload.aliaisTables)
        # Remove o canal após uso (one-time use)
        channel_manager.remove_channel(channel_id)

        return await executar_query_e_salvar_stream(db, user_id, query_payload)

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Erro no SSE: {str(e)}"
        log_message(f"{error_msg}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/channel/{channel_id}/status")
async def get_channel_status(
    channel_id: str, user_id: int = Depends(get_current_user_id)
):
    """Verifica o status de um canal."""
    try:
        uuid.UUID(channel_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Channel ID deve ser um UUID válido"
        )

    channel = channel_manager.get_channel(channel_id, user_id)
    if not channel:
        return {"exists": False, "status": "not_found_or_expired"}

    created_at = channel["created_at"]
    expires_at = created_at + timedelta(minutes=channel_manager.ttl_minutes)

    return {
        "exists": True,
        "status": "active",
        "createdAt": created_at.isoformat(),
        "expiresAt": expires_at.isoformat(),
        "remainingTime": str(expires_at - datetime.utcnow()),
    }


@router.delete("/channel/{channel_id}")
async def delete_channel(channel_id: str, user_id: int = Depends(get_current_user_id)):
    """Remove um canal manualmente."""
    try:
        uuid.UUID(channel_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Channel ID deve ser um UUID válido"
        )

    # Verifica se o canal existe e pertence ao usuário
    channel = channel_manager.get_channel(channel_id, user_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Canal não encontrado")

    # Remove o canal
    removed = channel_manager.remove_channel(channel_id)

    return {
        "success": removed,
        "message": (
            "Canal removido com sucesso" if removed else "Falha ao remover canal"
        ),
    }


@router.get("/channels/cleanup")
async def cleanup_channels_endpoint():
    """Endpoint para limpeza manual de canais expirados (admin)."""
    cleaned = channel_manager.cleanup_expired_channels()
    return {
        "cleaned": cleaned,
        "active_channels": len(channel_manager.channels),
        "message": f"Removidos {cleaned} canais expirados",
    }
