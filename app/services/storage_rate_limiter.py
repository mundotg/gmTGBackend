"""Controlo de taxa (Rate Limiting) e concorrência de transferências (estilo MEGA Drive)
para o módulo MustaInf Cloud Storage (`/storage`).

Implementa três mecanismos complementares:
1. **Sliding-Window Rate Limiter (Pedidos por Minuto)**:
   - Limita rajadas de chamadas por utilizador e categoria (`api`, `upload`, `download`, `share`).
2. **Semáforo de Concorrência por Utilizador (MEGA Transfer Slots)**:
   - Permite transferências paralelas (ex.: até 6 streams simultâneos de upload/download por utilizador)
     protegendo o servidor contra exaustão de workers.
3. **Bandwidth Rate Tracker (Bytes/minuto)**:
   - Monitoriza a taxa de transferência em tempo real por utilizador.

Usa Redis quando disponível para coordenação entre múltiplos workers e mantém fallback
em memória thread-safe (`threading.Lock`) para funcionamento garantido em qualquer ambiente.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException, Response, status

from app.config.dotenv import get_env_int
from app.config.redis import read_cache, write_cache

# ──────────────────────────── Limites configuráveis (.env) ────────────────────────────

# Pedidos de metadados / listagem por minuto por utilizador
RATE_LIMIT_API_PER_MIN: int = get_env_int("STORAGE_RATE_LIMIT_API_PER_MIN", 120)

# Operações de transferência (upload/download) iniciadas por minuto por utilizador
RATE_LIMIT_TRANSFER_PER_MIN: int = get_env_int("STORAGE_RATE_LIMIT_TRANSFER_PER_MIN", 40)

# Número máximo de transferências paralelas (slots estilo MEGA Drive) por utilizador
MAX_CONCURRENT_TRANSFERS: int = get_env_int("STORAGE_MAX_CONCURRENT_TRANSFERS", 6)

# Limite de largura de banda por minuto por utilizador (em MB, 0 = sem limite rígido)
MAX_BANDWIDTH_MB_PER_MIN: int = get_env_int("STORAGE_MAX_BANDWIDTH_MB_PER_MIN", 512)

WINDOW_SECONDS = 60
SLOT_TTL_SECONDS = 300  # Expiração automática de segurança caso um stream caia abruptamente


@dataclass
class RateLimitStatus:
    allowed: bool
    bucket: str
    limit: int
    remaining: int
    reset_seconds: int
    retry_after: int
    concurrent_active: int
    concurrent_max: int
    bandwidth_bytes_min: int
    bandwidth_limit_bytes_min: int

    def to_headers(self) -> Dict[str, str]:
        headers = {
            "X-RateLimit-Bucket": self.bucket,
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Reset": str(self.reset_seconds),
            "X-Concurrent-Slots-Used": str(self.concurrent_active),
            "X-Concurrent-Slots-Max": str(self.concurrent_max),
        }
        if not self.allowed and self.retry_after > 0:
            headers["Retry-After"] = str(self.retry_after)
        return headers


# ──────────────────────────── Estado em memória (fallback + precisão local) ────────────────────────────

_lock = threading.Lock()
# {(user_id, bucket): [timestamp1, timestamp2, ...]}
_mem_windows: Dict[Tuple[str, str], List[float]] = {}
# {user_id: {slot_id: timestamp_started}}
_mem_slots: Dict[str, Dict[str, float]] = {}
# {user_id: [(timestamp, num_bytes), ...]}
_mem_bandwidth: Dict[str, List[Tuple[float, int]]] = {}


def _prune_timestamps(timestamps: List[float], now: float, window: float = WINDOW_SECONDS) -> List[float]:
    cutoff = now - window
    return [ts for ts in timestamps if ts > cutoff]


def _get_window_list(user_key: str, bucket: str, now: float) -> List[float]:
    cache_key = f"storage:rl:win:{user_key}:{bucket}"
    cached = read_cache(cache_key)
    if isinstance(cached, list):
        pruned = _prune_timestamps(cached, now)
        _mem_windows[(user_key, bucket)] = pruned
        return pruned

    pruned = _prune_timestamps(_mem_windows.get((user_key, bucket), []), now)
    _mem_windows[(user_key, bucket)] = pruned
    return pruned


def _save_window_list(user_key: str, bucket: str, timestamps: List[float]) -> None:
    _mem_windows[(user_key, bucket)] = timestamps
    cache_key = f"storage:rl:win:{user_key}:{bucket}"
    write_cache(cache_key, timestamps, ttl=WINDOW_SECONDS + 5)


def _get_active_slots(user_key: str, now: float) -> Dict[str, float]:
    cache_key = f"storage:rl:slots:{user_key}"
    cached = read_cache(cache_key)
    source = cached if isinstance(cached, dict) else _mem_slots.get(user_key, {})
    cutoff = now - SLOT_TTL_SECONDS
    active = {sid: ts for sid, ts in source.items() if ts > cutoff}
    _mem_slots[user_key] = active
    return active


def _save_active_slots(user_key: str, slots: Dict[str, float]) -> None:
    _mem_slots[user_key] = slots
    cache_key = f"storage:rl:slots:{user_key}"
    write_cache(cache_key, slots, ttl=SLOT_TTL_SECONDS)


def _get_bandwidth_entries(user_key: str, now: float) -> List[Tuple[float, int]]:
    cache_key = f"storage:rl:bw:{user_key}"
    cached = read_cache(cache_key)
    source = cached if isinstance(cached, list) else _mem_bandwidth.get(user_key, [])
    cutoff = now - WINDOW_SECONDS
    active = [(ts, b) for ts, b in source if ts > cutoff]
    _mem_bandwidth[user_key] = active
    return active


def _save_bandwidth_entries(user_key: str, entries: List[Tuple[float, int]]) -> None:
    _mem_bandwidth[user_key] = entries
    cache_key = f"storage:rl:bw:{user_key}"
    write_cache(cache_key, entries, ttl=WINDOW_SECONDS + 5)


# ──────────────────────────── API Pública do Rate Limiter ────────────────────────────


def check_and_consume_rate_limit(
    user_id: object,
    bucket: str = "api",
    response: Optional[Response] = None,
    custom_limit: Optional[int] = None,
) -> RateLimitStatus:
    """Valida e consome 1 unidade de taxa na janela deslizante de 60s.
    Lança HTTP 429 com cabeçalhos `Retry-After` e `X-RateLimit-*` se excedido.
    """
    user_key = str(user_id)
    limit = custom_limit or (
        RATE_LIMIT_TRANSFER_PER_MIN if bucket in ("upload", "download", "transfer") else RATE_LIMIT_API_PER_MIN
    )
    now = time.time()

    with _lock:
        timestamps = _get_window_list(user_key, bucket, now)
        slots = _get_active_slots(user_key, now)
        bw_entries = _get_bandwidth_entries(user_key, now)
        bw_used = sum(b for _, b in bw_entries)
        bw_limit = MAX_BANDWIDTH_MB_PER_MIN * 1024 * 1024

        if len(timestamps) >= limit:
            oldest = min(timestamps) if timestamps else now
            retry_after = max(1, int(WINDOW_SECONDS - (now - oldest)) + 1)
            status_obj = RateLimitStatus(
                allowed=False,
                bucket=bucket,
                limit=limit,
                remaining=0,
                reset_seconds=retry_after,
                retry_after=retry_after,
                concurrent_active=len(slots),
                concurrent_max=MAX_CONCURRENT_TRANSFERS,
                bandwidth_bytes_min=bw_used,
                bandwidth_limit_bytes_min=bw_limit,
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "message": (
                        f"Limite de taxa atingido ({limit} operações/minuto em '{bucket}'). "
                        f"Aguarde {retry_after}s — o gestor de concorrência retomará automaticamente."
                    ),
                    "bucket": bucket,
                    "limit": limit,
                    "retry_after": retry_after,
                },
                headers=status_obj.to_headers(),
            )

        if bucket in ("upload", "download", "transfer") and bw_limit > 0 and bw_used >= bw_limit:
            oldest_bw = min((ts for ts, _ in bw_entries), default=now)
            retry_after = max(1, int(WINDOW_SECONDS - (now - oldest_bw)) + 1)
            status_obj = RateLimitStatus(
                allowed=False,
                bucket=bucket,
                limit=limit,
                remaining=max(0, limit - len(timestamps)),
                reset_seconds=retry_after,
                retry_after=retry_after,
                concurrent_active=len(slots),
                concurrent_max=MAX_CONCURRENT_TRANSFERS,
                bandwidth_bytes_min=bw_used,
                bandwidth_limit_bytes_min=bw_limit,
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "message": (
                        f"Limite de largura de banda por minuto atingido ({MAX_BANDWIDTH_MB_PER_MIN} MB/min). "
                        f"Tente novamente em {retry_after}s."
                    ),
                    "bucket": "bandwidth",
                    "retry_after": retry_after,
                },
                headers=status_obj.to_headers(),
            )

        timestamps.append(now)
        _save_window_list(user_key, bucket, timestamps)

        remaining = max(0, limit - len(timestamps))
        oldest = min(timestamps) if timestamps else now
        reset_seconds = max(1, int(WINDOW_SECONDS - (now - oldest)))

        status_obj = RateLimitStatus(
            allowed=True,
            bucket=bucket,
            limit=limit,
            remaining=remaining,
            reset_seconds=reset_seconds,
            retry_after=0,
            concurrent_active=len(slots),
            concurrent_max=MAX_CONCURRENT_TRANSFERS,
            bandwidth_bytes_min=bw_used,
            bandwidth_limit_bytes_min=bw_limit,
        )

    if response is not None:
        for k, v in status_obj.to_headers().items():
            response.headers[k] = v

    return status_obj


def record_bandwidth(user_id: object, num_bytes: int) -> None:
    """Regista bytes transferidos na janela atual de 60 segundos."""
    if num_bytes <= 0:
        return
    user_key = str(user_id)
    now = time.time()
    with _lock:
        entries = _get_bandwidth_entries(user_key, now)
        entries.append((now, int(num_bytes)))
        _save_bandwidth_entries(user_key, entries)


@contextmanager
def transfer_concurrency_slot(user_id: object, slot_id: str):
    """Context manager que reserva um slot de concorrência (estilo MEGA Drive)
    para um upload ou download em curso e liberta-o sempre no final.
    """
    user_key = str(user_id)
    now = time.time()

    with _lock:
        slots = _get_active_slots(user_key, now)
        if len(slots) >= MAX_CONCURRENT_TRANSFERS and slot_id not in slots:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "message": (
                        f"Limite de transferências simultâneas atingido "
                        f"({MAX_CONCURRENT_TRANSFERS} canais paralelos ativos). "
                        "A fila MEGA aguardará a libertação de um canal."
                    ),
                    "bucket": "concurrency",
                    "concurrent_active": len(slots),
                    "concurrent_max": MAX_CONCURRENT_TRANSFERS,
                    "retry_after": 2,
                },
                headers={
                    "Retry-After": "2",
                    "X-Concurrent-Slots-Used": str(len(slots)),
                    "X-Concurrent-Slots-Max": str(MAX_CONCURRENT_TRANSFERS),
                },
            )
        slots[slot_id] = now
        _save_active_slots(user_key, slots)

    try:
        yield
    finally:
        with _lock:
            slots_after = _get_active_slots(user_key, time.time())
            slots_after.pop(slot_id, None)
            _save_active_slots(user_key, slots_after)


def get_user_rate_snapshot(user_id: object) -> dict:
    """Devolve o estado atual de rate limit, slots concorrentes e largura de banda do utilizador."""
    user_key = str(user_id)
    now = time.time()
    with _lock:
        api_ts = _get_window_list(user_key, "api", now)
        transfer_ts = _get_window_list(user_key, "transfer", now)
        slots = _get_active_slots(user_key, now)
        bw_entries = _get_bandwidth_entries(user_key, now)
        bw_used = sum(b for _, b in bw_entries)

    return {
        "api_requests_per_min": {
            "used": len(api_ts),
            "limit": RATE_LIMIT_API_PER_MIN,
            "remaining": max(0, RATE_LIMIT_API_PER_MIN - len(api_ts)),
        },
        "transfers_per_min": {
            "used": len(transfer_ts),
            "limit": RATE_LIMIT_TRANSFER_PER_MIN,
            "remaining": max(0, RATE_LIMIT_TRANSFER_PER_MIN - len(transfer_ts)),
        },
        "concurrency": {
            "active_slots": len(slots),
            "max_slots": MAX_CONCURRENT_TRANSFERS,
            "recommended_client_parallelism": min(4, MAX_CONCURRENT_TRANSFERS),
        },
        "bandwidth_per_min": {
            "used_bytes": bw_used,
            "limit_bytes": MAX_BANDWIDTH_MB_PER_MIN * 1024 * 1024,
            "limit_mb": MAX_BANDWIDTH_MB_PER_MIN,
        },
        "window_seconds": WINDOW_SECONDS,
    }


def reset_rate_limits_for_user(user_id: object) -> None:
    """Utilitário para testes e administração: limpa contadores em memória e Redis do utilizador."""
    user_key = str(user_id)
    with _lock:
        for bucket in ("api", "transfer", "upload", "download"):
            _mem_windows.pop((user_key, bucket), None)
            write_cache(f"storage:rl:win:{user_key}:{bucket}", [], ttl=1)
        _mem_slots.pop(user_key, None)
        write_cache(f"storage:rl:slots:{user_key}", {}, ttl=1)
        _mem_bandwidth.pop(user_key, None)
        write_cache(f"storage:rl:bw:{user_key}", [], ttl=1)

