"""
Rotas otimizadas para execução de queries, backup, restore e transferência com SSE.
Versão 100% GET — compatível com EventSource (SSE).
"""

import asyncio
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
)
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db_async
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

# ============================================================
# 🔧 GERENCIADOR DE CANAIS DE QUERY
# ============================================================

class QueryChannelManager:
    """Gerencia canais de execução de query com expiração automática."""

    def __init__(self, ttl_minutes: int = 30):
        self.channels: Dict[str, Dict[str, Any]] = {}
        self.ttl_minutes = ttl_minutes

    def create_channel(self, user_id: int, query: Any) -> str:
        channel_id = str(uuid.uuid4())
        self.channels[channel_id] = {
            "user_id": user_id,
            "query": query,
            "created_at": datetime.utcnow(),
        }
        return channel_id

    def get_channel(self, channel_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        """Recupera um canal, validando o dono e a expiração."""
        channel = self.channels.get(channel_id)
        if not channel or channel["user_id"] != user_id:
            return None

        if self._is_expired(channel):
            self.remove_channel(channel_id)
            return None

        return channel

    def remove_channel(self, channel_id: str) -> bool:
        return self.channels.pop(channel_id, None) is not None

    def cleanup_expired_channels(self) -> int:
        expired = [
            cid for cid, ch in self.channels.items() if self._is_expired(ch)
        ]
        for cid in expired:
            self.remove_channel(cid)
        return len(expired)

    def _is_expired(self, channel: Dict[str, Any]) -> bool:
        created_at = channel.get("created_at")
        if not created_at:
            return True
        return datetime.utcnow() > created_at + timedelta(minutes=self.ttl_minutes)


channel_manager = QueryChannelManager()

# ============================================================
# 🧹 HELPERS
# ============================================================

@asynccontextmanager
async def handle_db_transaction(db: AsyncSession):
    """Contexto seguro para rollback automático."""
    try:
        yield db
    except Exception:
        db.rollback()
        raise


async def sse_stream(generator: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
    """Converte logs em formato Server-Sent Events."""
    try:
        async for message in generator:
            yield f"data: {message}\n\n"
        yield "data: ✅ Operação concluída\n\n"
    except Exception as e:
        yield f"data: ❌ Erro: {str(e)}\n\n"
    finally:
        yield "event: close\ndata: done\n\n"

# ============================================================
# 🚀 ROTEADOR PRINCIPAL
# ============================================================

router = APIRouter(prefix="/database", tags=["Database Operations"])

# ============================================================
# 💾 BACKUP STREAM
# ============================================================

@router.get("/backup/{connection_id}/stream")
async def backup_stream(
    connection_id: int, 
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id)
):
    """Executa backup com streaming SSE."""
    
    async def generator():
        try:
            yield "🚀 Iniciando backup..."
            await asyncio.sleep(0.3)
            await log_message(f"[User {user_id}] Iniciando backup para conexão {connection_id}")
            
            from importantConfig.db_backup_restore import backup_database
            
            yield "📦 Criando arquivo de backup..."
            path = backup_database(db, user_id, connection_id)
            
            yield f"✅ Backup concluído: {path}"
            
        except Exception as e:
            error_msg = f"❌ Erro no backup: {str(e)}"
            await log_message(f"[User {user_id}] {error_msg}{traceback.format_exc()}")
            yield error_msg

    return StreamingResponse(sse_stream(generator()), media_type="text/event-stream")


# ============================================================
# ♻️ RESTORE STREAM (convertido para GET)
# ============================================================

@router.get("/restore/{connection_id}/stream")
async def restore_stream(
    connection_id: int,
    filepath: str,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id)
):
    """Executa restauração via SSE usando caminho de arquivo GET param."""
    
    async def generator():
        try:
            yield "🚀 Iniciando restauração..."
            await asyncio.sleep(0.3)
            await log_message(f"[User {user_id}] Restauração iniciada para conexão {connection_id}")
            
            from importantConfig.db_backup_restore import restore_backup
            
            yield "🔧 Executando restauração..."
            restore_backup(db, user_id, connection_id, filepath)
            
            yield "✅ Restauração concluída!"
            await log_message(f"[User {user_id}] Restauração concluída")
            
        except Exception as e:
            error_msg = f"❌ Erro na restauração: {str(e)}"
            await log_message(f"[User {user_id}] {error_msg}{traceback.format_exc()}")
            yield error_msg

    return StreamingResponse(sse_stream(generator()), media_type="text/event-stream")


# ============================================================
# 🔁 TRANSFER STREAM (convertido para GET)
# ============================================================

@router.get("/transfer/stream")
async def transfer_stream(
    id_connectio_origen: int,
    id_connectio_distino: int,
    tables_origen: str,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    """Monitora a transferência de dados via SSE."""

    async def event_stream():
        start_time = datetime.now()
        try:
            yield "data: 👤 Iniciando transferência...\n\n"
            log_message(f"[User {user_id}] Iniciando transferência")
            await asyncio.sleep(0.3)

            yield "data: 🔍 Validando conexões origem/destino...\n\n"
            await asyncio.sleep(0.3)

            from importantConfig.db_transfer import transfer_data,converter_tables_origen
            # print("tables_origen:",tables_origen)

            async for progress_msg in transfer_data(
                id_user=user_id,
                db=db,
                id_connectio_origen=id_connectio_origen,
                id_connectio_distino=id_connectio_distino,
                tables_origen=converter_tables_origen(tables_origen),
            ):
                yield f"data: {progress_msg}\n\n"
                await asyncio.sleep(0.1)

            yield "data: ✅ Transferência concluída!\n\n"
            log_message(f"[User {user_id}] Transferência concluída com sucesso")

        except Exception as e:
            error_msg = f"❌ Erro: {str(e)}"
            log_message(f"[User {user_id}] {error_msg}\n{traceback.format_exc()}")
            yield f"data: {error_msg}\n\n"

        finally:
            # ⚠️ Fecha a sessão mesmo se erro ocorrer
            try:
                await db.close()
                log_message(f"[User {user_id}] Sessão encerrada corretamente.")
            except Exception as close_err:
                log_message(f"[User {user_id}] Erro ao encerrar sessão: {close_err}")

            duration = (datetime.now() - start_time).total_seconds()
            yield f"data: ⏱️ Finalizado em {duration:.2f}s\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")



# ============================================================
# 🧹 CONTROLE DE CANAIS E HEALTH
# ============================================================

@router.get("/channel/{channel_id}/status")
async def channel_status(channel_id: str, user_id: int = Depends(get_current_user_id)):
    try:
        uuid.UUID(channel_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="UUID inválido")
    
    channel = channel_manager.get_channel(channel_id, user_id)
    if not channel:
        return {"exists": False, "status": "expired_or_not_found"}

    created = channel["created_at"]
    expires = created + timedelta(minutes=channel_manager.ttl_minutes)
    remaining = expires - datetime.utcnow()

    return {
        "exists": True,
        "status": "active",
        "createdAt": created.isoformat(),
        "expiresAt": expires.isoformat(),
        "remainingTime": str(remaining),
    }


@router.get("/channels/cleanup")
async def cleanup_channels():
    cleaned = channel_manager.cleanup_expired_channels()
    return {
        "cleaned": cleaned,
        "remaining": len(channel_manager.channels),
        "message": f"{cleaned} canais expirados removidos",
    }


@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "active_channels": len(channel_manager.channels),
        "service": "database_operations"
    }
