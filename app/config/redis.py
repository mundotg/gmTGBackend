import redis
import pickle
import gzip
from typing import Optional, Any, List

from app.config.dotenv import get_env, get_env_int, get_env_bool
from app.services.ocr._OCR_CACHE import CACHE_USE_GZIP
from app.ultils.logger import log_message

# -------------------------
# CONFIG
# -------------------------
REDIS_PREFIX = get_env("REDIS_CACHE_PREFIX", "cache:")
host = (get_env("REDIS_HOST", "localhost"),)
port = (get_env_int("REDIS_PORT", 6379),)
db = (get_env_int("REDIS_DB", 0),)
password = (get_env("REDIS_PASSWORD") or None,)
r = redis.Redis.from_url(
    get_env("app_cache_REDIS_URL") or f"redis://{host}:{port}/{db}",
    socket_timeout=get_env_int("REDIS_SOCKET_TIMEOUT", 5),
    socket_connect_timeout=get_env_int("REDIS_CONNECT_TIMEOUT", 5),
    retry_on_timeout=get_env_bool("REDIS_RETRY_ON_TIMEOUT", True),
    decode_responses=False,  # sempre bytes
)

redis_client = (
    r  # Exporta o cliente Redis para uso em outros módulos (ex: cache_scheduler)
)


# -------------------------
# HELPERS
# -------------------------
def _build_key(key: str) -> str:
    """Aplica prefixo global para isolamento."""
    return f"{REDIS_PREFIX}{key}"


def _serialize(data: Any) -> bytes:
    try:
        raw = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        if CACHE_USE_GZIP:
            raw = gzip.compress(raw)
        return raw
    except Exception as e:
        log_message(f"[REDIS][SERIALIZE_ERROR] {e}", "error")
        raise


def _deserialize(raw: bytes) -> Any:
    try:
        if CACHE_USE_GZIP:
            raw = gzip.decompress(raw)
        return pickle.loads(raw)
    except Exception as e:
        log_message(f"[REDIS][DESERIALIZE_ERROR] {e}", "error")
        return None


# -------------------------
# WRITE
# -------------------------
def write_cache(key: str, data: Any, ttl: Optional[int] = None):
    try:
        cache_key = _build_key(key)
        raw = _serialize(data)

        if ttl:
            r.setex(cache_key, ttl, raw)
        else:
            r.set(cache_key, raw)

    except Exception as e:
        log_message(f"[REDIS][WRITE] key={key} error={e}", "error")


# -------------------------
# READ
# -------------------------
def read_cache(key: str) -> Optional[Any]:
    try:
        cache_key = _build_key(key)
        raw = r.get(cache_key)

        if raw is None:
            return None

        return _deserialize(raw)

    except Exception as e:
        log_message(f"[REDIS][READ] key={key} error={e}", "error")
        return None


# -------------------------
# DELETE
# -------------------------
def delete_cache(key: str):
    try:
        r.delete(_build_key(key))
    except Exception as e:
        log_message(f"[REDIS][DELETE] key={key} error={e}", "error")


# -------------------------
# BULK DELETE (🔥 rápido)
# -------------------------
def delete_many(keys: List[str]):
    try:
        if not keys:
            return
        redis_keys = [_build_key(k) for k in keys]
        r.delete(*redis_keys)
    except Exception as e:
        log_message(f"[REDIS][DELETE_MANY] error={e}", "error")


# -------------------------
# EXISTS
# -------------------------
def cache_exists(key: str) -> bool:
    try:
        return r.exists(_build_key(key)) == 1
    except Exception as e:
        log_message(f"[REDIS][EXISTS] key={key} error={e}", "error")
        return False


# -------------------------
# TTL
# -------------------------
def get_cache_ttl(key: str) -> Optional[int]:
    try:
        ttl = r.ttl(_build_key(key))
        return ttl if ttl >= 0 else None
    except Exception as e:
        log_message(f"[REDIS][TTL] key={key} error={e}", "error")
        return None


# -------------------------
# CLEAR BY PREFIX (SAFE)
# -------------------------
def clear_cache_by_prefix(prefix: str = "") -> int:
    """
    Remove apenas caches com prefixo (NUNCA usa flushdb).
    """
    try:
        pattern = f"{REDIS_PREFIX}{prefix}*"
        cursor = 0
        total_deleted = 0

        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=200)

            if keys:
                r.delete(*keys)
                total_deleted += len(keys)

            if cursor == 0:
                break

        log_message(
            f"[REDIS] {total_deleted} caches removidos ({pattern})",
            "warning",
        )

        return total_deleted

    except Exception as e:
        log_message(f"[REDIS][CLEAR_PREFIX] {e}", "error")
        return 0


# -------------------------
# CLEAR ALL CACHE (SAFE)
# -------------------------
def clear_all_cache() -> int:
    """
    Limpa apenas o namespace de cache (seguro para produção).
    """
    return clear_cache_by_prefix("")


# -------------------------
# STATS
# -------------------------
def get_cache_info():
    try:
        info = r.info()

        return {
            "used_memory": info.get("used_memory_human"),
            "clients": info.get("connected_clients"),
            "commands": info.get("total_commands_processed"),
            "hits": info.get("keyspace_hits"),
            "misses": info.get("keyspace_misses"),
            "hit_rate": _calc_hit_rate(info),
        }

    except Exception as e:
        log_message(f"[REDIS][INFO] {e}", "error")
        return {}


def _calc_hit_rate(info: dict) -> float:
    try:
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        total = hits + misses
        return round((hits / total) * 100, 2) if total > 0 else 0.0
    except Exception:
        return 0.0
