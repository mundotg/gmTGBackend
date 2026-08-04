"""
Rate limiting para endpoints sensíveis (login, refresh, registo).

Sem isto, `/auth/login` aceita tentativas ilimitadas: um atacante com uma
lista de passwords comuns testa milhares por minuto contra qualquer email
conhecido. O bcrypt torna cada tentativa cara para o servidor, o que agrava
o problema — o brute-force passa também a ser um vetor de exaustão de CPU.

Estratégia: janela deslizante por chave (IP + email), com contador em Redis
quando disponível e fallback em memória. O fallback é por processo: com
vários workers o limite efetivo multiplica-se pelo número de workers, por
isso em produção convém ter o Redis configurado.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request, status

from app.config.dotenv import get_env, get_env_int
from app.request_fingerprint import get_client_ip
from app.ultils.logger import log_message

# Limites por omissão: 5 tentativas por 5 minutos.
LOGIN_MAX_ATTEMPTS = get_env_int("LOGIN_RATE_LIMIT_ATTEMPTS", 5) or 5
LOGIN_WINDOW_SECONDS = get_env_int("LOGIN_RATE_LIMIT_WINDOW", 300) or 300

_KEY_PREFIX = "ratelimit:"

# ------------------------------------------------------------
# Backend Redis (opcional)
# ------------------------------------------------------------

_redis_client = None
_redis_checked = False


def _get_redis():
    """
    Cliente Redis dedicado, criado à primeira utilização.

    Não reutiliza `app.config.redis` de propósito: esse módulo arrasta a
    cadeia de imports do OCR. Se o Redis não estiver disponível, devolve
    None e o limitador cai para o backend em memória.
    """
    global _redis_client, _redis_checked

    if _redis_checked:
        return _redis_client

    _redis_checked = True

    try:
        import redis

        url = get_env("app_cache_REDIS_URL") or (
            f"redis://{get_env('REDIS_HOST', 'localhost')}:"
            f"{get_env_int('REDIS_PORT', 6379)}/"
            f"{get_env_int('REDIS_DB', 0)}"
        )

        client = redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
        client.ping()

        _redis_client = client
        log_message("🛡️ Rate limiting a usar Redis", "info", withBd=True)

    except Exception as exc:
        log_message(
            f"⚠️ Rate limiting sem Redis ({exc}); a usar contador em memória. "
            "Com vários workers o limite é aplicado por processo.",
            "warning",
            withBd=True,
        )
        _redis_client = None

    return _redis_client


# ------------------------------------------------------------
# Backend em memória (fallback)
# ------------------------------------------------------------

_memory_hits: dict[str, deque[float]] = defaultdict(deque)
_memory_lock = threading.Lock()


def _hit_memory(key: str, limit: int, window: int) -> tuple[bool, int]:
    """Devolve (permitido, segundos_para_retry)."""
    now = time.monotonic()
    cutoff = now - window

    with _memory_lock:
        hits = _memory_hits[key]

        # Descarta tentativas que já saíram da janela.
        while hits and hits[0] < cutoff:
            hits.popleft()

        if len(hits) >= limit:
            retry_after = int(hits[0] + window - now) + 1
            return False, max(retry_after, 1)

        hits.append(now)

        # Evita crescimento indefinido do dicionário em processos longos.
        if len(_memory_hits) > 10_000:
            for stale_key in [k for k, v in _memory_hits.items() if not v]:
                del _memory_hits[stale_key]

        return True, 0


def _hit_redis(client, key: str, limit: int, window: int) -> tuple[bool, int]:
    """INCR + EXPIRE atómicos via pipeline."""
    redis_key = f"{_KEY_PREFIX}{key}"

    pipe = client.pipeline()
    pipe.incr(redis_key)
    pipe.ttl(redis_key)
    count, ttl = pipe.execute()

    # Primeira tentativa da janela: define o TTL.
    if ttl is None or ttl < 0:
        client.expire(redis_key, window)
        ttl = window

    if count > limit:
        return False, max(int(ttl), 1)

    return True, 0


# ------------------------------------------------------------
# API pública
# ------------------------------------------------------------

def check_rate_limit(
    key: str,
    limit: int = LOGIN_MAX_ATTEMPTS,
    window: int = LOGIN_WINDOW_SECONDS,
) -> None:
    """
    Regista uma tentativa e levanta 429 se o limite foi excedido.

    Falha em modo aberto: se o backend de contagem rebentar, o pedido passa.
    Bloquear logins legítimos por causa de um Redis em baixo seria pior do
    que perder temporariamente a proteção.
    """
    client = _get_redis()

    try:
        if client is not None:
            allowed, retry_after = _hit_redis(client, key, limit, window)
        else:
            allowed, retry_after = _hit_memory(key, limit, window)

    except Exception as exc:
        log_message(f"⚠️ Rate limiter indisponível: {exc}", "warning", withBd=True)
        return

    if not allowed:
        log_message(f"🚫 Rate limit excedido: {key}", "warning")

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Demasiadas tentativas. Tenta novamente mais tarde.",
            headers={"Retry-After": str(retry_after)},
        )


def limit_login_attempts(request: Request, email: str | None = None) -> None:
    """
    Aplica o limite ao login, contando por IP e por email em separado.

    Duas chaves porque protegem de coisas diferentes:
    - por IP: trava um atacante que percorre muitas contas a partir de uma máquina
    - por email: trava um ataque distribuído contra uma conta específica
    """
    ip = get_client_ip(request)

    check_rate_limit(f"login:ip:{ip}")

    if email:
        check_rate_limit(f"login:email:{email.strip().lower()}")
