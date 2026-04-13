# app/routers/db_backup_restore_controller.py
from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncGenerator, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db_async
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

# NOTA: A correção do ProactorEventLoop deve ficar no main.py, não aqui.

BACKUP_DIR = "backups"

router = APIRouter(prefix="/database", tags=["Backup & Restore (SSE)"])


# ============================================================
# 🧠 Helpers
# ============================================================

def _http_error(status: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status, detail=detail)


def _ensure_backup_dir() -> None:
    os.makedirs(BACKUP_DIR, exist_ok=True)


def _is_safe_backup_path(filepath: str) -> bool:
    if not filepath:
        return False
    base = os.path.abspath(BACKUP_DIR)
    target = os.path.abspath(filepath)
    return os.path.commonpath([base, target]) == base


def _normalize_backup_path(filepath: str) -> str:
    fp = (filepath or "").strip()
    if not fp:
        raise ValueError("filepath é obrigatório.")

    # se vier só "arquivo.sql.gz", joga pra backups/arquivo.sql.gz
    if not os.path.isabs(fp) and not fp.startswith(BACKUP_DIR + os.sep):
        fp = os.path.join(BACKUP_DIR, fp)

    if not _is_safe_backup_path(fp):
        raise ValueError("Caminho de backup inválido (fora do diretório permitido).")

    return fp


def sse(event: str, data: str) -> str:
    """
    Formata um evento SSE com suporte a multilinha.
    IMPORTANTE: precisa terminar com \n\n (ou no mínimo uma linha vazia).
    """
    if data is None:
        data = ""
    data = str(data).replace("\r", "")
    lines = data.split("\n")
    payload = f"event: {event}\n" + "".join([f"data: {line}\n" for line in lines]) + "\n"
    return payload


# ============================================================
# 📡 Channel manager
# ============================================================

@dataclass
class ChannelState:
    user_id: int
    created_at: datetime
    status: str = "active"   # active | done | error
    last_message: Optional[str] = None


class ChannelManager:
    def __init__(self, *, ttl_minutes: int = 30):
        self.ttl_minutes = ttl_minutes
        self.channels: Dict[str, ChannelState] = {}

    def create_channel(self, user_id: int) -> str:
        channel_id = str(uuid.uuid4())
        self.channels[channel_id] = ChannelState(user_id=user_id, created_at=datetime.utcnow())
        return channel_id

    def get_channel(self, channel_id: str, user_id: int) -> Optional[ChannelState]:
        ch = self.channels.get(channel_id)
        if not ch:
            return None
        if ch.user_id != user_id:
            return None
        if self.is_expired(ch):
            self.channels.pop(channel_id, None)
            return None
        return ch

    def set_status(self, channel_id: str, user_id: int, status: str) -> None:
        ch = self.get_channel(channel_id, user_id)
        if ch:
            ch.status = status

    def push(self, channel_id: str, user_id: int, message: str) -> None:
        ch = self.get_channel(channel_id, user_id)
        if ch:
            ch.last_message = message

    def is_expired(self, ch: ChannelState) -> bool:
        return datetime.utcnow() > (ch.created_at + timedelta(minutes=self.ttl_minutes))

    def cleanup_expired_channels(self) -> int:
        now = datetime.utcnow()
        expired = [
            cid for cid, ch in self.channels.items()
            if now > (ch.created_at + timedelta(minutes=self.ttl_minutes))
        ]
        for cid in expired:
            self.channels.pop(cid, None)
        return len(expired)


channel_manager = ChannelManager(ttl_minutes=30)


# ============================================================
# 💾 BACKUP STREAM
# ============================================================

@router.get("/backup/{connection_id}/stream")
async def backup_stream(
    connection_id: int,
    compress: bool = Query(True),
    channel_id: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    _ensure_backup_dir()

    # cria channel se não vier
    ch_id = channel_id or channel_manager.create_channel(user_id)

    async def generator() -> AsyncGenerator[str, None]:
        try:
            msg = f"🚀 Preparando instâncias de backup... (channel={ch_id})"
            channel_manager.push(ch_id, user_id, msg)
            yield sse("log", msg)
            await asyncio.sleep(0.1)

            from importantConfig.db_backup_restore import backup_database

            msg = "📦 Compilando dados e gerando arquivo de dump..."
            channel_manager.push(ch_id, user_id, msg)
            yield sse("log", msg)

            # ✅ precisa await
            path = await backup_database(db, user_id, connection_id, compress=compress)

            msg = f"✅ Backup finalizado. Arquivo salvo em: {path}"
            channel_manager.push(ch_id, user_id, msg)
            channel_manager.set_status(ch_id, user_id, "done")
            yield sse("done", msg)
            yield sse("final", "✅ Operação concluída")

        except Exception as e:
            msg = f"❌ Falha crítica no backup: {str(e)}"
            log_message(f"[User {user_id}] {msg}", level="error")
            channel_manager.push(ch_id, user_id, msg)
            channel_manager.set_status(ch_id, user_id, "error")
            yield sse("error", msg)
            yield sse("final", "Operação abortada")

    return StreamingResponse(
        generator(),
        media_type="text/event-stream; charset=utf-8",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ============================================================
# 🔁 RESTORE STREAM
# ============================================================

@router.get("/restore/{connection_id}/stream")
async def restore_stream(
    connection_id: int,
    filepath: str = Query(...),
    channel_id: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    _ensure_backup_dir()
    ch_id = channel_id or channel_manager.create_channel(user_id)

    async def generator() -> AsyncGenerator[str, None]:
        try:
            # ✅ valida e normaliza path (bloqueia path traversal)
            safe_path = _normalize_backup_path(filepath)

            if not os.path.exists(safe_path):
                raise ValueError(f"Arquivo não encontrado: {os.path.basename(safe_path)}")

            msg = f"🚀 Iniciando sequência de restauração... (channel={ch_id})"
            channel_manager.push(ch_id, user_id, msg)
            yield sse("log", msg)
            await asyncio.sleep(0.1)

            from importantConfig.db_backup_restore import restore_backup

            msg = f"🔧 Restaurando a partir do arquivo: {os.path.basename(safe_path)} ..."
            channel_manager.push(ch_id, user_id, msg)
            yield sse("log", msg)

            await restore_backup(db, user_id, connection_id, safe_path)

            msg = "✅ Restauração estrutural e de dados concluída!"
            channel_manager.push(ch_id, user_id, msg)
            channel_manager.set_status(ch_id, user_id, "done")
            yield sse("done", msg)
            yield sse("final", "✅ Operação concluída")

        except Exception as e:
            msg = f"❌ Falha na restauração: {str(e)}"
            log_message(f"[User {user_id}] {msg}", level="error")
            channel_manager.push(ch_id, user_id, msg)
            channel_manager.set_status(ch_id, user_id, "error")
            yield sse("error", msg)
            yield sse("final", "Operação abortada")

    return StreamingResponse(
        generator(),
        media_type="text/event-stream; charset=utf-8",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ============================================================
# 📊 STATUS & HEALTH
# ============================================================

@router.get("/channel/{channel_id}/status")
async def channel_status(
    channel_id: str,
    user_id: int = Depends(get_current_user_id),
):
    try:
        uuid.UUID(channel_id)
    except ValueError:
        raise _http_error(400, "UUID inválido")

    channel = channel_manager.get_channel(channel_id, user_id)
    if not channel:
        return {"exists": False, "status": "expired_or_not_found"}

    created = channel.created_at
    expires = created + timedelta(minutes=channel_manager.ttl_minutes)
    remaining = expires - datetime.utcnow()

    return {
        "exists": True,
        "status": channel.status,
        "lastMessage": channel.last_message,
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
        "message": f"{cleaned} canais inativos foram expurgados da memória.",
    }


@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "active_channels": len(channel_manager.channels),
        "service": "OrionForgeNexus_DB_Operations",
    }