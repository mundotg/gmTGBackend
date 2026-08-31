import redis
import pickle
import gzip
from typing import Optional, Any, List
from urllib.parse import quote

from app.config.dotenv import get_env, get_env_int, get_env_bool
from app.services.ocr._OCR_CACHE import CACHE_USE_GZIP
from app.ultils.logger import log_message

# -------------------------
# CONFIG
# -------------------------
REDIS_PREFIX = get_env("REDIS_CACHE_PREFIX", "cache:")

# ⚠️ Sem os parênteses e a vírgula final.
#
# Estas quatro linhas eram `host = (get_env(...),)` — uma vírgula final dentro
# de parênteses cria um TUPLO, não um valor. A URL de reserva ficava
# `redis://('localhost',):(6379,)/(0,)` e nunca teria ligado a nada. Só
# funcionava porque `app_cache_REDIS_URL` está definida no .env e ganha; bastava
# alguém apagá-la para o cache deixar de subir, sem explicação óbvia.
REDIS_HOST = get_env("REDIS_HOST", "localhost")
REDIS_PORT = get_env_int("REDIS_PORT", 6379)
REDIS_DB = get_env_int("REDIS_DB", 0)
REDIS_PASSWORD = get_env("REDIS_PASSWORD") or None
REDIS_USERNAME = get_env("REDIS_USERNAME") or None


def _build_url() -> str:
    """
    URL de ligação, com credenciais quando existem.

    A password era lida do ambiente e nunca chegava ao cliente: não ia na URL
    nem em `password=`. Um Redis protegido respondia NOAUTH e o cache ficava
    em baixo com a configuração aparentemente correta.

    `app_cache_REDIS_URL` continua a ganhar, para quem já tem a ligação inteira
    numa variável (Redis Cloud, Upstash).
    """
    completa = get_env("app_cache_REDIS_URL")
    if completa:
        return completa

    if REDIS_PASSWORD:
        # `quote` porque uma password com @ / : partia a URL.
        credenciais = f"{quote(REDIS_USERNAME or '', safe='')}:{quote(REDIS_PASSWORD, safe='')}@"
    else:
        credenciais = ""

    return f"redis://{credenciais}{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"


r = redis.Redis.from_url(
    _build_url(),
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
