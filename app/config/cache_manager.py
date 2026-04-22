import asyncio
import json
import time
import hashlib
import traceback
import functools
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Callable, Optional, Any, Dict, TypeVar, ParamSpec, cast


from app.config.redis import write_cache, read_cache, redis_client

# <-- Certifica-te de importar o cliente Redis aqui para o clear_cache!
from app.ultils.logger import log_message
from app.config.dotenv import get_env, get_env_bool, get_env_int

# -------------------------
# Preservação de Tipos para o IDE (Autocomplete)
# -------------------------
P = ParamSpec("P")
R = TypeVar("R")

# -------------------------
# Configurações do .env
# -------------------------
CACHE_ENABLED = get_env_bool("CACHE_ENABLED", True)
CACHE_DISABLE_FLAG = get_env_bool("CACHE_DISABLE_FLAG", False)
CACHE_LOG_HITS = get_env_bool("CACHE_LOG_HITS", False)
CACHE_LOG_MISSES = get_env_bool("CACHE_LOG_MISSES", False)
CACHE_PREFIX = get_env("CACHE_PREFIX", "cache:")
# -------------------------------
# Cache em memória (Camada L1)
# -------------------------------
# Estrutura: { key: {"ts": timestamp, "val": value, "ttl": ttl_or_none} }
MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}
MEMORY_CACHE_TTL = get_env_int(
    "CACHE_MEMORY_TTL_SECONDS", 60
)  # TTL super curto para RAM


def _to_cacheable(value: Any) -> Any:
    """Converte valores complexos em tipos seguros para json/redis."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): _to_cacheable(v) for k, v in value.items()}

    # SQLAlchemy Row
    if hasattr(value, "_mapping"):
        return {str(k): _to_cacheable(v) for k, v in value._mapping.items()}
    # Iteráveis
    if isinstance(value, (list, tuple, set)):
        return [_to_cacheable(v) for v in value]
    # Pydantic v2 / v1
    if hasattr(value, "model_dump"):
        return _to_cacheable(value.model_dump())
    if hasattr(value, "dict"):
        return _to_cacheable(value.dict())

    # SQLAlchemy ORM fallback robusto
    try:
        from sqlalchemy.inspection import inspect
        from sqlalchemy.orm.state import InstanceState

        state = inspect(value, raiseerr=False)
        if state is not None and isinstance(state, InstanceState):
            return {
                attr.key: _to_cacheable(getattr(value, attr.key))
                for attr in state.mapper.column_attrs
            }
    except ImportError:
        pass

    return str(value)


def _safe_serialize(value: Any) -> Any:
    """Usado EXCLUSIVAMENTE para gerar a chave do cache de forma segura."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple, set)):
        return [_safe_serialize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _safe_serialize(v) for k, v in value.items()}

    # Detetar objetos gigantes do SQLAlchemy (Session, Engine, etc) para não tentar serializar
    type_str = str(type(value)).lower()
    if "sqlalchemy" in type_str or hasattr(value, "__tablename__"):
        return f"<SQLObject:{type(value).__name__}>"

    return f"<obj:{type(value).__name__}>"


def _make_key(func_name: str, user_id: Optional[str] = None, *args, **kwargs) -> str:
    """Gera chave SHA-256 estável excluindo dependências injetadas (db, session)."""
    clean_kwargs = {
        k: _safe_serialize(v)
        for k, v in kwargs.items()
        if not k.startswith("_")
        and k not in {"self", "cls", "session", "db", "engine", "conn"}
    }
    clean_args = [_safe_serialize(a) for a in args]

    key_data = {
        "f": func_name,
        "user": str(user_id) if user_id is not None else "global",
        "args": clean_args,
        "kwargs": clean_kwargs,
    }
    raw = json.dumps(key_data, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ============================================================
# Decorador Principal
# ============================================================
# 🔥 IMPORTS CORRIGIDOS


def cache_result(ttl: Optional[int] = None, user_id: Optional[str] = None):

    def decorator(func: Callable[P, R]) -> Callable[P, R]:

        is_async = asyncio.iscoroutinefunction(func)

        # -------------------------
        # ASYNC WRAPPER
        # -------------------------
        @functools.wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not CACHE_ENABLED or CACHE_DISABLE_FLAG:
                return await func(*args, **kwargs)  # type: ignore

            actual_user_id = user_id if user_id is not None else kwargs.get("user_id")
            key = _make_key(func.__name__, actual_user_id, *args, **kwargs)

            # L1
            mem = MEMORY_CACHE.get(key)
            if mem:
                if mem["ttl"] is None or (time.time() - mem["ts"] < mem["ttl"]):
                    if CACHE_LOG_HITS:
                        log_message(f"[L1 HIT] {func.__name__}", "debug")
                    return mem["val"]
                MEMORY_CACHE.pop(key, None)

            # L2
            cache_key = f"{CACHE_PREFIX}{func.__name__}:{key}"

            try:
                redis_data = read_cache(cache_key)
                if redis_data and "value" in redis_data:
                    if CACHE_LOG_HITS:
                        log_message(f"[L2 HIT] {func.__name__}", "debug")

                    MEMORY_CACHE[key] = {
                        "ts": time.time(),
                        "val": redis_data["value"],
                        "ttl": MEMORY_CACHE_TTL,
                    }
                    return redis_data["value"]
            except Exception as e:
                log_message(f"[REDIS_READ_ERROR] {e}", "error")

            if CACHE_LOG_MISSES:
                log_message(f"[MISS] {func.__name__}", "debug")

            # execução
            result = await func(*args, **kwargs)
            cacheable_result = _to_cacheable(result)

            # L1
            MEMORY_CACHE[key] = {
                "ts": time.time(),
                "val": cacheable_result,
                "ttl": MEMORY_CACHE_TTL,
            }

            # L2
            try:
                entry = {
                    "timestamp": time.time(),
                    "value": cacheable_result,
                    "function": func.__name__,
                }
                write_cache(cache_key, entry, ttl)
            except Exception as e:
                log_message(f"[REDIS_WRITE_ERROR] {e}", "error")

            return cast(R, result)

        # -------------------------
        # SYNC WRAPPER (🔥 agora funcional)
        # -------------------------
        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not CACHE_ENABLED or CACHE_DISABLE_FLAG:
                return func(*args, **kwargs)

            loop = asyncio.get_event_loop()
            return loop.run_until_complete(async_wrapper(*args, **kwargs))

        return cast(Callable[P, R], async_wrapper if is_async else sync_wrapper)

    return decorator


def clear_cache(pattern: str = f"{CACHE_PREFIX}*") -> int:
    total_removed = 0
    redis_removed = 0

    try:
        if redis_client:
            cursor = 0
            while True:
                cursor, keys = redis_client.scan(
                    cursor=cursor, match=pattern, count=200
                )

                if keys:
                    redis_client.delete(*keys)
                    redis_removed += len(keys)

                if cursor == 0:
                    break

        memory_removed = len(MEMORY_CACHE)
        MEMORY_CACHE.clear()

        total_removed = redis_removed + memory_removed

        log_message(
            f"🧹 Cache limpo -> Redis: {redis_removed}, RAM: {memory_removed}",
            "warning",
        )

        return total_removed

    except Exception as e:
        log_message(f"[CACHE_CLEAR_ERROR] {e}{traceback.format_exc()}", "error")
        return 0


def get_function_cache_info(func_name: str) -> dict:
    """
    Retorna informações de cache para uma função específica:
    - Redis (L2)
    - Memória (L1)
    """

    redis_keys = []
    memory_keys = []
    total = 0

    try:
        # -------------------------
        # 🔴 REDIS (L2)
        # -------------------------
        if redis_client:
            cursor = 0

            pattern = f"{CACHE_PREFIX}{func_name}:*"

            while True:
                cursor, keys = redis_client.scan(
                    cursor=cursor, match=pattern, count=200
                )

                if keys:
                    redis_keys.extend(keys)

                if cursor == 0:
                    break

        # -------------------------
        # 🧠 MEMORY (L1)
        # -------------------------
        for key in MEMORY_CACHE.keys():
            if func_name in key:
                memory_keys.append(key)

        total = len(redis_keys) + len(memory_keys)

        return {
            "function": func_name,
            "redis_keys": len(redis_keys),
            "memory_keys": len(memory_keys),
            "total_keys": total,
            "cache_enabled": CACHE_ENABLED,
            "cache_prefix": CACHE_PREFIX,
        }

    except Exception as e:
        log_message(
            f"[CACHE][INFO_ERROR] {e}{traceback.format_exc()}",
            "error",
        )
        return {
            "function": func_name,
            "error": str(e),
        }
