"""
Rotas para execução de queries e operações no banco de dados.
Versão melhorada com melhor estrutura, segurança e tratamento de erros.
"""

import traceback
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict,Optional
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db, get_db_async
from app.routes.connection_routes import get_current_user_id
from app.schemas.query_select_upAndInsert_schema import (
    AutoCreateRequest,
    InsertRequest,
    QueryPayload,
    UpdateRequest,
)

from app.services.cloudeAi_execute_query import executar_query_e_salvar_stream
from app.services.insert_row_service import insert_row_service
from app.services.insert_service_auto import insert_row_service_auto
from app.ultils.QueryExecutionService import QueryExecutionService
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.build_query import update_row_service
from app.ultils.logger import log_message

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
                data, engine, user_id, connection.type, connection.id, db
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
                data, engine, user_id, connection.type, connection.id, db
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

        log_message(f"query_payload:{query_payload} ", "info")
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

