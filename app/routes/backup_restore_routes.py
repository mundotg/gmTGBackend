# app/routers/db_backup_restore_controller.py
from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncGenerator, Dict, Optional

from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    HTTPException,
    Query,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import decode_token
from app.database import get_db_async
from app.services import backup_jobs
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

# NOTA: A correção do ProactorEventLoop deve ficar no main.py, não aqui.

BACKUP_DIR = "backups"

# Extensões aceites no upload de restore (SQL + Mongo/NoSQL).
RESTORE_ALLOWED_EXT = (".sql", ".backup", ".dump", ".gz", ".archive", ".bson", ".db", ".bak")

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
#
# GET /database/channel/{channel_id}/status
# GET /database/channels/cleanup
# GET /database/health
#
# Estas três rotas viviam aqui em duplicado. Como database_intro_routes é
# registado primeiro em main.py, as versões deste ficheiro nunca chegavam a
# ser atingidas — só produziam "Duplicate Operation ID" no arranque e entradas
# repetidas no Swagger. A implementação válida (idêntica, sobre o mesmo
# channel_manager) está em app/routes/database_intro_routes.py.

# ============================================================
# 🧰 Upload de ficheiro de restore  (o endpoint que faltava)
# ============================================================
def _safe_join_backup(filename: str) -> str:
    """Junta ao BACKUP_DIR de forma segura (bloqueia path traversal)."""
    base = os.path.abspath(BACKUP_DIR)
    # Só o nome-base, sem componentes de caminho.
    name = os.path.basename(filename or "")
    target = os.path.abspath(os.path.join(base, name))
    if os.path.commonpath([base, target]) != base:
        raise HTTPException(status_code=400, detail="Nome de ficheiro inválido.")
    return target


@router.post("/restore/{connection_id}/upload")
async def upload_restore_file(
    connection_id: int,
    file: UploadFile = File(...),
    user_id: int = Depends(get_current_user_id),
):
    """
    Recebe o ficheiro de backup para restauro e grava-o em `backups/`.
    Devolve `{filepath, filename}` para depois iniciar o job de restore.
    """
    _ensure_backup_dir()

    name = (file.filename or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Ficheiro sem nome.")

    if not name.lower().endswith(RESTORE_ALLOWED_EXT):
        raise HTTPException(
            status_code=400,
            detail=f"Extensão não permitida. Aceites: {', '.join(RESTORE_ALLOWED_EXT)}",
        )

    # Prefixo único para não colidir com uploads anteriores.
    unique = f"{uuid.uuid4().hex[:8]}_{os.path.basename(name)}"
    dest = _safe_join_backup(unique)

    try:
        size = 0
        with open(dest, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)  # 1 MB
                if not chunk:
                    break
                size += len(chunk)
                out.write(chunk)
    except Exception as e:
        log_message(f"[UPLOAD] user={user_id} erro={e}", level="error")
        if os.path.exists(dest):
            try:
                os.remove(dest)
            except OSError:
                pass
        raise HTTPException(status_code=500, detail="Falha ao gravar o ficheiro.")
    finally:
        await file.close()

    log_message(
        f"[UPLOAD] user={user_id} conn={connection_id} -> {unique} "
        f"({round(size / (1024*1024), 2)} MB)",
        level="info",
    )
    return {"filepath": dest, "filename": unique}


# ============================================================
# 🚀 Iniciar jobs (backup / restore) — corre em background
# ============================================================
@router.post("/jobs/backup")
async def start_backup_job(
    connection_id: int = Body(..., embed=True),
    compress: bool = Body(True, embed=True),
    user_id: int = Depends(get_current_user_id),
):
    """Cria um job de backup e devolve o `job_id` para seguir por WebSocket."""
    _ensure_backup_dir()
    job = backup_jobs.create_job(kind="backup", user_id=user_id, connection_id=connection_id)
    asyncio.create_task(
        backup_jobs.run_backup_job(job["id"], user_id, connection_id, compress)
    )
    return {"job_id": job["id"], "status": job["status"]}


@router.post("/jobs/restore")
async def start_restore_job(
    connection_id: int = Body(..., embed=True),
    filepath: str = Body(..., embed=True),
    user_id: int = Depends(get_current_user_id),
):
    """Cria um job de restore a partir de um ficheiro já carregado."""
    safe_path = _normalize_backup_path(filepath)
    if not os.path.exists(safe_path):
        raise HTTPException(status_code=404, detail="Ficheiro de backup não encontrado.")

    job = backup_jobs.create_job(kind="restore", user_id=user_id, connection_id=connection_id)
    asyncio.create_task(
        backup_jobs.run_restore_job(job["id"], user_id, connection_id, safe_path)
    )
    return {"job_id": job["id"], "status": job["status"]}


@router.get("/jobs/{job_id}")
async def get_job_status(
    job_id: str,
    user_id: int = Depends(get_current_user_id),
):
    """Estado do job por REST (fallback quando o WebSocket não está disponível)."""
    job = backup_jobs.get_job(job_id)
    if not backup_jobs.owns_job(job, user_id):
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    return job


# ============================================================
# 📥 Download de um backup concluído
# ============================================================
@router.get("/backups/{filename}/download")
async def download_backup(
    filename: str,
    user_id: int = Depends(get_current_user_id),
):
    path = _safe_join_backup(filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Backup não encontrado.")
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=os.path.basename(path),
    )


# ============================================================
# 🔌 WebSocket de progresso do job
# ============================================================
def _ws_user_id(websocket: WebSocket) -> Optional[int]:
    """
    Autentica a WebSocket pelo cookie `access_token` (as WS enviam cookies do
    mesmo site automaticamente). Devolve o user_id ou None.
    """
    token = websocket.cookies.get("access_token")
    if not token:
        # Fallback: ?token=... na query string (útil fora do browser).
        token = websocket.query_params.get("token")
    if not token:
        return None
    try:
        payload = decode_token(token)
        sub = payload.get("sub") if isinstance(payload, dict) else None
        return int(sub) if sub is not None else None
    except Exception:
        return None


@router.websocket("/ws/jobs/{job_id}")
async def job_progress_ws(websocket: WebSocket, job_id: str):
    """
    Segue um job de backup/restore em tempo real. Envia o estado atual ao ligar
    e depois cada alteração (deteta pela `version`). Fecha quando o job termina.
    """
    await websocket.accept()

    user_id = _ws_user_id(websocket)
    if user_id is None:
        await websocket.send_json({"event": "error", "message": "Não autenticado."})
        await websocket.close(code=4401)
        return

    job = backup_jobs.get_job(job_id)
    if not backup_jobs.owns_job(job, user_id):
        await websocket.send_json({"event": "error", "message": "Job não encontrado."})
        await websocket.close(code=4404)
        return

    last_version = -1
    try:
        while True:
            job = backup_jobs.get_job(job_id)
            if not job:
                await websocket.send_json({"event": "error", "message": "Job expirou."})
                break

            version = int(job.get("version", 0))
            if version != last_version:
                last_version = version
                await websocket.send_json({"event": "update", "job": job})

                if job.get("status") in ("done", "error"):
                    await websocket.send_json({"event": "final", "status": job["status"]})
                    break

            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        return
    except Exception as e:  # noqa: BLE001
        log_message(f"[WS_JOB {job_id}] erro: {e}", level="warning")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
