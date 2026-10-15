"""
Gerenciador de Jobs Assíncronos para OCR (FinOps - Desacoplamento de Compute).

Permite que requisições pesadas de OCR sejam processadas em background
sem bloquear o Event Loop da API FastAPI, armazenando estado, tempo de execução
e resultado com expiração automática.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pydantic import BaseModel

class OCRJobStatus:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class OCRJob(BaseModel):
    job_id: str
    status: str
    filename: str
    created_at: str
    completed_at: Optional[str] = None
    duration_s: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class OCRJobManager:
    """
    Gerenciador de tarefas assíncronas em memória com limite de retenção.
    Para infraestruturas distribuídas com múltiplos nós, pode ser facilmente
    substituído por um backend Redis / Celery.
    """

    def __init__(self, max_history: int = 500, ttl_seconds: int = 3600):
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self.max_history = max_history
        self.ttl_seconds = ttl_seconds

    def create_job(self, filename: str) -> str:
        self._cleanup_expired()
        job_id = f"ocr_{uuid.uuid4().hex[:16]}"
        now = datetime.now(timezone.utc).isoformat()
        self._jobs[job_id] = {
            "job_id": job_id,
            "status": OCRJobStatus.PROCESSING,
            "filename": filename,
            "created_at": now,
            "created_timestamp": time.time(),
            "completed_at": None,
            "duration_s": None,
            "result": None,
            "error": None,
        }
        return job_id

    def update_job_success(self, job_id: str, result: Dict[str, Any]) -> None:
        if job_id in self._jobs:
            start_t = self._jobs[job_id].get("created_timestamp", time.time())
            now_t = time.time()
            self._jobs[job_id].update({
                "status": OCRJobStatus.COMPLETED,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "duration_s": round(now_t - start_t, 2),
                "result": result,
                "error": None,
            })

    def update_job_failure(self, job_id: str, error_message: str) -> None:
        if job_id in self._jobs:
            start_t = self._jobs[job_id].get("created_timestamp", time.time())
            now_t = time.time()
            self._jobs[job_id].update({
                "status": OCRJobStatus.FAILED,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "duration_s": round(now_t - start_t, 2),
                "result": None,
                "error": error_message,
            })

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        self._cleanup_expired()
        job = self._jobs.get(job_id)
        if not job:
            return None
        # Retorna cópia sem timestamp interno
        dados = dict(job)
        dados.pop("created_timestamp", None)
        return dados

    def _cleanup_expired(self) -> None:
        """Remove jobs antigos que ultrapassaram o TTL."""
        now = time.time()
        expired = [
            jid for jid, info in self._jobs.items()
            if now - info.get("created_timestamp", 0) > self.ttl_seconds
        ]
        for jid in expired:
            self._jobs.pop(jid, None)

        # Se ultrapassar o limite máximo, remove os mais antigos
        if len(self._jobs) > self.max_history:
            ordenados = sorted(
                self._jobs.items(),
                key=lambda x: x[1].get("created_timestamp", 0)
            )
            excesso = len(self._jobs) - self.max_history
            for jid, _ in ordenados[:excesso]:
                self._jobs.pop(jid, None)


# Instância Singleton do gerenciador de jobs
ocr_job_manager = OCRJobManager()
