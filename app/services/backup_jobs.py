"""
💾 Jobs de backup/restore com estado em Redis.

O trabalho pesado (mongodump/pg_dump/…) corre num background task e escreve o
progresso no Redis. Os clientes seguem por WebSocket — se caírem e voltarem,
recuperam o estado do Redis (não se perde nada). Um único escritor (o runner)
por job; a WS só lê.

Estrutura do job em Redis (key `backupjob:{id}`):
  { id, kind, connection_id, user_id, status, progress, version,
    logs: [str], result: {filename, size_mb} | None, error, created_at, updated_at }
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.config.redis import read_cache, write_cache, delete_cache
from app.ultils.logger import log_message

JOB_TTL_SECONDS = 60 * 60  # 1 hora
MAX_LOG_LINES = 500

# ⚠️ Fallback em memória.
# O cliente Redis da app pode apontar para um Redis remoto (Redis Cloud) que
# fica instável/indisponível — nesse caso `write_cache` falha em silêncio e o
# job "desaparece" (dá 404). Mantemos sempre uma cópia local do job para o
# fluxo funcionar mesmo com o Redis em baixo. Com Redis a funcionar, o estado
# fica nos dois sítios (o Redis permite seguir o job de outro worker/reconexão).
_LOCAL: Dict[str, Dict[str, Any]] = {}


def _key(job_id: str) -> str:
    return f"backupjob:{job_id}"


def _remember(job: Dict[str, Any]) -> None:
    _LOCAL[job["id"]] = job
    # Evita crescer sem limite (mantém os últimos ~200 jobs em memória).
    if len(_LOCAL) > 200:
        for old in list(_LOCAL.keys())[:-200]:
            _LOCAL.pop(old, None)


def create_job(*, kind: str, user_id: int, connection_id: int, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    job_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    job: Dict[str, Any] = {
        "id": job_id,
        "kind": kind,  # "backup" | "restore"
        "connection_id": connection_id,
        "user_id": user_id,
        "status": "queued",  # queued | running | done | error
        "progress": 0,
        "version": 1,
        "logs": [],
        "result": None,
        "error": None,
        "created_at": now,
        "updated_at": now,
        **(extra or {}),
    }
    _remember(job)
    write_cache(_key(job_id), job, ttl=JOB_TTL_SECONDS)
    return job


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    # Redis primeiro (fonte partilhada); memória local como fallback fiável.
    job = read_cache(_key(job_id))
    if job is None:
        job = _LOCAL.get(job_id)
    else:
        _LOCAL[job_id] = job  # mantém a cópia local fresca
    return job


def _save(job: Dict[str, Any]) -> None:
    job["version"] = int(job.get("version", 0)) + 1
    job["updated_at"] = datetime.utcnow().isoformat()
    _remember(job)
    write_cache(_key(job["id"]), job, ttl=JOB_TTL_SECONDS)


def update_job(
    job_id: str,
    *,
    status: Optional[str] = None,
    progress: Optional[int] = None,
    log: Optional[str] = None,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    job = get_job(job_id)
    if not job:
        return None

    if status is not None:
        job["status"] = status
    if progress is not None:
        job["progress"] = max(0, min(100, int(progress)))
    if log is not None:
        logs: List[str] = job.get("logs", [])
        logs.append(log)
        # Mantém só as últimas N linhas para o job não crescer sem limite.
        job["logs"] = logs[-MAX_LOG_LINES:]
    if result is not None:
        job["result"] = result
    if error is not None:
        job["error"] = error

    _save(job)
    return job


def delete_job(job_id: str) -> None:
    _LOCAL.pop(job_id, None)
    delete_cache(_key(job_id))


def owns_job(job: Optional[Dict[str, Any]], user_id: int) -> bool:
    return bool(job and job.get("user_id") == user_id)


# ============================================================
# 🏃 Runners (correm em background)
# ============================================================
async def run_backup_job(job_id: str, user_id: int, connection_id: int, compress: bool) -> None:
    """Executa o backup e regista o progresso no job."""
    from app.database import AsyncSessionLocal
    from importantConfig.db_backup_restore import backup_database

    update_job(job_id, status="running", progress=5, log="🚀 A preparar o backup…")

    try:
        async with AsyncSessionLocal() as db:
            update_job(job_id, progress=25, log="📦 A gerar o dump da base de dados…")
            path = await backup_database(db, user_id, connection_id, compress=compress)

        import os

        filename = os.path.basename(path)
        try:
            size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
        except OSError:
            size_mb = 0.0

        update_job(
            job_id,
            status="done",
            progress=100,
            log=f"✅ Backup concluído: {filename} ({size_mb} MB)",
            result={"filename": filename, "size_mb": size_mb},
        )
    except Exception as e:  # noqa: BLE001 — reportar ao cliente
        log_message(f"[BACKUP_JOB {job_id}] erro: {e}", "error")
        update_job(job_id, status="error", log=f"❌ Falha no backup: {e}", error=str(e))


async def run_restore_job(job_id: str, user_id: int, connection_id: int, filepath: str) -> None:
    """Executa o restore a partir de um ficheiro já carregado."""
    from app.database import AsyncSessionLocal
    from importantConfig.db_backup_restore import restore_backup

    import os

    update_job(
        job_id,
        status="running",
        progress=10,
        log=f"🚀 A iniciar o restauro de {os.path.basename(filepath)}…",
    )

    try:
        async with AsyncSessionLocal() as db:
            update_job(job_id, progress=40, log="♻️ A restaurar estrutura e dados…")
            await restore_backup(db, user_id, connection_id, filepath)

        update_job(job_id, status="done", progress=100, log="✅ Restauro concluído com sucesso.")
    except Exception as e:  # noqa: BLE001
        log_message(f"[RESTORE_JOB {job_id}] erro: {e}", "error")
        update_job(job_id, status="error", log=f"❌ Falha no restauro: {e}", error=str(e))
