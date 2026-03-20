
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Body
)
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

# ============================================================
# 📦 SCHEMAS PARA CRIAÇÃO/EDIÇÃO DE COLUNAS
# ============================================================

class FieldDefinition(BaseModel):
    connection_id: int
    table_name: str
    original_name: Optional[str] = Field(None, description="Se preenchido, indica edição. Se None, é criação.")
    nome: str
    tipo: str
    length: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True
    is_unique: bool = False
    is_primary_key: bool = False
    is_auto_increment: bool = False
    default_value: Optional[str] = None
    comentario: Optional[str] = None
    enum_values: List[str] = []
    referenced_table: Optional[str] = None
    field_references: Optional[str] = None
    on_delete_action: str = "NO ACTION"
    on_update_action: str = "NO ACTION"


# ============================================================
# 🔧 GERENCIADOR DE CANAIS DE TASK (STATEFUL)
# ============================================================

class QueryChannelManager:
    """Gerencia canais de execução de tasks pesadas com expiração automática."""

    def __init__(self, ttl_minutes: int = 30):
        self.channels: Dict[str, Dict[str, Any]] = {}
        self.ttl_minutes = ttl_minutes

    def create_channel(self, user_id: int, payload: Any) -> str:
        channel_id = str(uuid.uuid4())
        self.channels[channel_id] = {
            "user_id": user_id,
            "payload": payload,
            "created_at": datetime.utcnow(),
            "status": "pending"
        }
        return channel_id

    def get_channel(self, channel_id: str, user_id: int) -> Optional[Dict[str, Any]]:
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
# 🧹 HELPERS DE STREAMING
# ============================================================

@asynccontextmanager
async def handle_db_transaction(db: AsyncSession):
    """Contexto seguro para rollback automático em caso de falha DDL."""
    try:
        yield db
        await db.commit()
    except Exception:
        await db.rollback()
        raise

async def sse_stream(generator: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
    """Padroniza a saída para o formato Server-Sent Events (SSE)."""
    try:
        async for message in generator:
            yield f"data: {message}\n\n"
        yield "data: ✅ Operação concluída\n\n"
    except Exception as e:
        yield f"data: ❌ Erro Crítico: {str(e)}\n\n"
    finally:
        yield "event: close\ndata: done\n\n"

# ============================================================
# 🚀 ROTEADOR PRINCIPAL
# ============================================================

router = APIRouter(prefix="/database", tags=["Database Operations"])


# ============================================================
# 🛠️ ENDPOINTS DE FIELD (CREATE / EDIT)
# ============================================================

@router.post("/field/task")
async def register_field_task(
    field_data: FieldDefinition = Body(...),
    user_id: int = Depends(get_current_user_id)
):
    """
    Passo 1: Registra a intenção de criar/editar uma coluna e retorna um channel_id.
    Isso contorna o limite de enviar JSONs complexos via GET no SSE.
    """
    try:
        channel_id = channel_manager.create_channel(user_id=user_id, payload=field_data.dict())
        return {"success": True, "channel_id": channel_id, "message": "Task registrada. Inicie o stream."}
    except Exception as e:
        log_message(f"Erro ao registrar task de field: {e}", level="error")
        raise HTTPException(status_code=500, detail="Não foi possível registrar a operação.")



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
        "status": channel.get("status", "active"),
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
        "message": f"{cleaned} canais inativos foram expurgados da memória."
    }

@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "active_channels": len(channel_manager.channels),
        "service": "OrionForgeNexus_DB_Operations"
    }