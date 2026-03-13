import gzip
import json
import os
import pickle
import time
import hashlib
import random
import traceback
import tempfile
import shutil
from typing import Callable, Optional, Any, Dict
import functools

from app.ultils.logger import log_message
from app.config.dotenv import get_env, get_env_bool, get_env_int, get_env_float

# -------------------------
# Carrega configurações do .env
# -------------------------
CACHE_ENABLED = get_env_bool('CACHE_ENABLED', True)
CACHE_TTL_DEFAULT = get_env_int('CACHE_TTL_SECONDS', 3600)
CACHE_MAX_SIZE_MB = get_env_int('CACHE_MAX_SIZE_MB', 100)
CACHE_CLEANUP_INTERVAL = get_env_int('CACHE_CLEANUP_INTERVAL', 3600)
CACHE_CLEANUP_PROBABILITY = get_env_float('CACHE_CLEANUP_PROBABILITY', 0.01)
CACHE_MAX_OLD_FILES_REMOVE = get_env_int('CACHE_MAX_OLD_FILES_REMOVE', 10)
CACHE_DISABLE_FLAG = get_env_bool('CACHE_DISABLE_FLAG', False)
CACHE_DIR_NAME = get_env('CACHE_DIR_NAME', 'cache')
CACHE_PICKLE_PROTOCOL = get_env_int('CACHE_PICKLE_PROTOCOL', pickle.HIGHEST_PROTOCOL)
CACHE_LOG_HITS = get_env_bool('CACHE_LOG_HITS', False)
CACHE_LOG_MISSES = get_env_bool('CACHE_LOG_MISSES', False)
CACHE_USE_GZIP = get_env_bool('CACHE_USE_GZIP', True)
CACHE_GZIP_COMPRESSION_LEVEL = get_env_int('CACHE_GZIP_COMPRESSION_LEVEL', 6)

# Diretório base do cache (cria se não existir)
CACHE_DIR = os.path.join(os.path.dirname(__file__), CACHE_DIR_NAME)
os.makedirs(CACHE_DIR, exist_ok=True)


from datetime import datetime, date
from decimal import Decimal
from enum import Enum

def _to_cacheable(value):
    """
    Converte valores complexos em tipos seguros para pickle/json/cache.
    Suporta SQLAlchemy 2.0 (Rows) e ORM Models de forma robusta.
    """
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

    # 1. SQLAlchemy Row (session.execute(...).all())
    # IMPORTANTE: Tem de vir ANTES da verificação de tuple/list, pois o Row herda de tuple!
    # Usamos _mapping para extrair como dicionário (nome_da_coluna: valor).
    if hasattr(value, "_mapping"):
        return {str(k): _to_cacheable(v) for k, v in value._mapping.items()}

    # 2. Iteráveis comuns (agora é seguro, pois o Row já foi tratado acima)
    if isinstance(value, (list, tuple, set)):
        return [_to_cacheable(v) for v in value]

    # 3. Pydantic v2
    if hasattr(value, "model_dump"):
        return _to_cacheable(value.model_dump())

    # 4. Pydantic v1
    if hasattr(value, "dict"):
        return _to_cacheable(value.dict())

    # 5. SQLAlchemy ORM object (A abordagem mais robusta usando inspect)
    try:
        from sqlalchemy.inspection import inspect
        from sqlalchemy.orm.state import InstanceState
        
        # raiseerr=False evita excepções se o objeto não for do SQLAlchemy
        state = inspect(value, raiseerr=False) 
        if state is not None and isinstance(state, InstanceState):
            data = {}
            # Itera apenas sobre os atributos que são mapeados como colunas
            for attr in state.mapper.column_attrs:
                data[attr.key] = _to_cacheable(getattr(value, attr.key))
            return data
    except ImportError:
        pass # Caso o SQLAlchemy não esteja instalado no ambiente (segurança)

    # 6. Fallback original SQLAlchemy (caso o inspect falhe por algum motivo raro)
    if hasattr(value, "__table__"):
        data = {}
        for column in value.__table__.columns:
            data[column.name] = _to_cacheable(getattr(value, column.name))
        return data

    # Fallback final
    return str(value)
# -------------------------------
# Cache em memória (camada rápida)
# -------------------------------
# Estrutura: { key: {"ts": timestamp, "val": value, "ttl": ttl_or_none} }
MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}
MEMORY_CACHE_TTL = get_env_int('CACHE_MEMORY_TTL_SECONDS', 60)  # tempo padrão em RAM

# -------------------------------
# Utilitários de I/O seguros
# -------------------------------
def _cache_extension() -> str:
    return ".pkl.gz" if CACHE_USE_GZIP else ".pkl"

def _get_cache_path(key: str) -> str:
    return os.path.join(CACHE_DIR, f"{key}{_cache_extension()}")

def _atomic_write(path: str, data_bytes: bytes) -> None:
    """Grava de forma atômica para evitar ficheiros corrompidos."""
    dir_name = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name)
    os.close(fd)
    try:
        with open(tmp_path, "wb") as f:
            f.write(data_bytes)
        # substitui de forma atômica
        shutil.move(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

def _serialize_to_bytes(data: dict) -> bytes:
    """Serializa usando pickle e possivelmente gzip, retornando bytes."""
    try:
        if CACHE_USE_GZIP:
            # usa um buffer temporário
            with tempfile.SpooledTemporaryFile() as tf:
                with gzip.GzipFile(fileobj=tf, mode="wb", compresslevel=CACHE_GZIP_COMPRESSION_LEVEL) as gz:
                    pickle.dump(data, gz, protocol=CACHE_PICKLE_PROTOCOL)
                tf.seek(0)
                return tf.read()
        else:
            return pickle.dumps(data, protocol=CACHE_PICKLE_PROTOCOL)
    except Exception as e:
        log_message(f"Erro na serialização para bytes: {e}{traceback.format_exc()}", "error")
        raise

def _write_cache(path: str, data: dict) -> None:
    """Escreve dados no cache de forma atômica (gzip opcional)."""
    try:
        data_bytes = _serialize_to_bytes(data)
        _atomic_write(path, data_bytes)
    except Exception as e:
        log_message(f"Erro ao escrever cache em {path}: {e}{traceback.format_exc()}", "error")
        # não propaga (para evitar quebrar a app), mas loga
        # raise
@functools.lru_cache(maxsize=128)
def _read_cache(path: str) -> Optional[dict]:
    """Lê dados do cache (suporta gzip). Retorna None em caso de erro/corrompido."""
    try:
        if not os.path.exists(path):
            return None
        if CACHE_USE_GZIP:
            with gzip.open(path, "rb") as f:
                return pickle.load(f)
        else:
            with open(path, "rb") as f:
                return pickle.load(f)
    except Exception as e:
        # ficheiro possivelmente corrompido -> tenta remover
        log_message(f"Erro ao ler cache de {path}: {e}{traceback.format_exc()}", "error")
        try:
            os.remove(path)
            log_message(f"Arquivo de cache corrompido removido: {os.path.basename(path)}", "warning")
        except Exception:
            pass
        return None

# -------------------------------
# Estatísticas e limpeza de disco
# -------------------------------
def _get_cache_size() -> int:
    """Retorna o total do diretório de cache em bytes."""
    total_size = 0
    try:
        for name in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, name)
            if os.path.isfile(path):
                total_size += os.path.getsize(path)
    except Exception as e:
        log_message(f"Erro ao calcular tamanho do cache: {e}{traceback.format_exc()}", "error")
    return total_size

def _enforce_cache_size(max_size_mb: int) -> None:
    """Se o cache exceder o limite, remove os arquivos mais antigos até ficar abaixo do limite."""
    try:
        if max_size_mb is None:
            return
        max_bytes = max_size_mb * 1024 * 1024
        current = _get_cache_size()
        if current <= max_bytes:
            return

        # lista arquivos com ctime e tamanho
        entries = []
        for name in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, name)
            if os.path.isfile(path):
                try:
                    ctime = os.path.getctime(path)
                    size = os.path.getsize(path)
                    entries.append((path, ctime, size))
                except Exception:
                    continue
        # ordena do mais antigo para o mais novo
        entries.sort(key=lambda e: e[1])
        removed = 0
        for path, _, size in entries:
            if current <= max_bytes or removed >= CACHE_MAX_OLD_FILES_REMOVE:
                break
            try:
                os.remove(path)
                current -= size
                removed += 1
            except Exception as e:
                log_message(f"Erro ao remover arquivo de cache para liberar espaço: {e}", "error")
        if removed > 0:
            log_message(f"Enforced cache size: removidos {removed} arquivos para ficar abaixo de {max_size_mb}MB", "warning")
    except Exception as e:
        log_message(f"Erro em _enforce_cache_size: {e}{traceback.format_exc()}", "error")

def _clean_old_cache_files(max_age_seconds: Optional[int] = None) -> None:
    """Remove arquivos antigos baseado no tempo de criação (ou max_age_seconds)."""
    if max_age_seconds is None:
        max_age_seconds = CACHE_CLEANUP_INTERVAL
    current_time = time.time()
    files_removed = 0
    try:
        for fname in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, fname)
            if not os.path.isfile(path):
                continue
            try:
                age = current_time - os.path.getctime(path)
                if age > max_age_seconds:
                    os.remove(path)
                    files_removed += 1
            except Exception as e:
                log_message(f"Erro ao remover cache antigo {fname}: {e}", "error")
        if files_removed > 0:
            log_message(f"Removidos {files_removed} arquivos de cache antigos", "warning")
    except Exception as e:
        log_message(f"Erro durante limpeza do cache: {e}{traceback.format_exc()}", "error")

def clear_expired_cache_files() -> int:
    """
    Remove ficheiros de cache expirados (baseado no timestamp + ttl).
    Retorna a contagem de ficheiros removidos.
    """
    current_time = time.time()
    files_removed = 0
    total_files = 0
    try:
        for fname in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, fname)
            if not os.path.isfile(path):
                continue
            total_files += 1
            data = _read_cache(path)
            if not data:
                # ficheiro corrompido ou não legível
                continue
            timestamp = data.get("timestamp")
            ttl = data.get("ttl", CACHE_TTL_DEFAULT)
            # None TTL significa nunca expira
            if ttl is None or timestamp is None:
                continue
            if (current_time - timestamp) >= ttl:
                try:
                    os.remove(path)
                    files_removed += 1
                    log_message(f"🧹 Cache expirado removido: {fname}")
                except Exception as e:
                    log_message(f"Erro ao remover cache expirado {fname}: {e}{traceback.format_exc()}")
        if files_removed > 0:
            log_message(f"🧾 Total de {files_removed}/{total_files} caches expirados removidos.")
        else:
            log_message("Nenhum cache expirado encontrado.", "debug")
        return files_removed
    except Exception as e:
        log_message(f"Erro ao limpar caches expirados: {e}{traceback.format_exc()}", "error")
        return 0

# -------------------------------
# Serialização segura para gerar chaves
# -------------------------------
def _is_sqlalchemy_obj(obj: Any) -> bool:
    """Detecção heurística de objetos SQLAlchemy (Session, Engine, Model, etc.)."""
    try:
        t = type(obj)
        module = getattr(t, "__module__", "") or ""
        name = getattr(t, "__name__", "") or ""
        if "sqlalchemy" in module.lower():
            return True
        # heurística: objetos do ORM costumam ter __tablename__ / metadata
        if hasattr(obj, "__tablename__") or hasattr(obj, "metadata"):
            return True
        return False
    except Exception:
        return False

def _safe_serialize(value: Any) -> Any:
    """
    Converte valores para formas serializáveis simples (primitivos, listas, dicts),
    substituindo objetos complexos (ex: Session, Engine, Model) por marcadores.
    """
    try:
        if value is None:
            return None
        if _is_sqlalchemy_obj(value):
            return f"<SQLObject:{type(value).__name__}>"
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (list, tuple, set)):
            return [_safe_serialize(v) for v in value]
        if isinstance(value, dict):
            return {str(k): _safe_serialize(v) for k, v in value.items()}
        # objetos com representação simples
        if isinstance(value, (bytes, bytearray)):
            # hash curto para bytes grandes
            return f"<bytes:{hashlib.sha256(bytes(value)).hexdigest()[:8]}>"
        # tenta json-serializar; se falhar, usa repr curta
        try:
            json.dumps(value, default=str)
            return value
        except Exception:
            return f"<obj:{type(value).__name__}:{str(value)[:200]}>"
    except Exception as e:
        return f"<unserializable:{type(value).__name__}:{str(e)}>"

def _make_key(func_name: str, user_id: Optional[str] = None, *args, **kwargs) -> str:
    """
    Gera uma chave estável baseada em func_name, user_id, args e kwargs "limpos".
    Exclui argumentos irrelevantes como 'self', 'cls', 'session', 'db', 'engine'.
    """
    try:
        # copia e filtra kwargs
        clean_kwargs = {}
        for k, v in kwargs.items():
            if k.startswith("_"):
                continue
            if k in {"self", "cls", "session", "db", "engine", "conn"}:
                # substitui por marcador em vez de tentar serializar o objecto
                clean_kwargs[k] = f"<ignored:{k}>"
                continue
            clean_kwargs[k] = _safe_serialize(v)

        # filtra args (substitui objetos SQLAlchemy por marcador)
        clean_args = []
        for a in args:
            if _is_sqlalchemy_obj(a):
                clean_args.append(f"<SQLObject:{type(a).__name__}>")
            else:
                clean_args.append(_safe_serialize(a))

        key_data = {
            "f": func_name,
            "user": str(user_id) if user_id is not None else "global",
            "args": clean_args,
            "kwargs": clean_kwargs,
        }
        raw = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
    except Exception as e:
        # fallback robusto
        try:
            fallback = f"{func_name}-{str(user_id)}-{time.time()}"
            return hashlib.sha256(fallback.encode()).hexdigest()
        except Exception:
            return hashlib.sha256(func_name.encode()).hexdigest()

# -------------------------------
# Decorador principal
# -------------------------------
def cache_result(ttl: Optional[int] = None, max_cache_size_mb: Optional[int] = None, user_id: Optional[str] = None):
    """
    Decorador de cache com:
      - camada em memória (rápida)
      - fallback em disco (gzip opcional)
      - limitação de espaço (remoção dos mais antigos)
      - serialização segura para gerar chaves
    Args:
      ttl: tempo em segundos (None = infinito)
      max_cache_size_mb: limite (None = usar CACHE_MAX_SIZE_MB)
      user_id: forçar user_id fixo (opcional)
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Se cache globalmente desabilitado -> executa direto
            if not CACHE_ENABLED or CACHE_DISABLE_FLAG:
                return func(*args, **kwargs)

            # Determina o user_id real (param do decorador tem prioridade)
            actual_user_id = user_id if user_id is not None else kwargs.get("user_id")
            # if user_id:
            #     kwargs["user_id"] = user_id

            # Gera chave limpa (não altera kwargs originais)
            key = _make_key(func.__name__, *args, **kwargs)

            # 1) verifica em memória
            mem = MEMORY_CACHE.get(key)
            if mem is not None:
                mem_ttl = mem.get("ttl")
                if mem_ttl is None:
                    # sem ttl em memória -> usa MEMORY_CACHE_TTL para expiração local
                    if time.time() - mem["ts"] < MEMORY_CACHE_TTL:
                        if CACHE_LOG_HITS:
                            log_message(f"Cache RAM hit para {func.__name__}", "debug")
                        return mem["val"]
                    else:
                        MEMORY_CACHE.pop(key, None)
                else:
                    # se houve ttl específico guardado
                    if mem_ttl is None or (time.time() - mem["ts"] < mem_ttl):
                        if CACHE_LOG_HITS:
                            log_message(f"Cache RAM hit para {func.__name__}", "debug")
                        return mem["val"]
                    else:
                        MEMORY_CACHE.pop(key, None)

            # 2) verifica em disco
            path = _get_cache_path(key)
            if os.path.exists(path):
                data = _read_cache(path)
                if data is not None:
                    entry_ttl = data.get("ttl", CACHE_TTL_DEFAULT)
                    timestamp = data.get("timestamp", 0)
                    # ttl None = nunca expira
                    if entry_ttl is None or (time.time() - timestamp < (entry_ttl if entry_ttl is not None else float("inf"))):
                        if CACHE_LOG_HITS:
                            log_message(f"Cache disco hit para {func.__name__}", "debug")
                        # coloca em memória para acessos futuros
                        MEMORY_CACHE[key] = {"ts": time.time(), "val": data.get("value"), "ttl": entry_ttl}
                        return data.get("value")
                    else:
                        # expirado -> remove
                        try:
                            os.remove(path)
                            if CACHE_LOG_MISSES:
                                log_message(f"Cache expirado removido para {func.__name__}", "debug")
                        except Exception:
                            pass

            # 3) cache miss -> executa função
            if CACHE_LOG_MISSES:
                log_message(f"Cache miss para {func.__name__}", "debug")
            result = func(*args, **kwargs)
            cacheable_result = _to_cacheable(result)
            # 4) grava cache (memória + disco)
            # grava em memória (usa ttl do parâmetro do decorador, se fornecido)
            # 4) grava cache (memória + disco)
            MEMORY_CACHE[key] = {"ts": time.time(), "val": cacheable_result, "ttl": ttl}

            try:
                entry = {
                    "timestamp": time.time(),
                    "value": cacheable_result, # <--- CORREÇÃO: Usar o resultado convertido
                    "ttl": ttl,
                    "function": func.__name__,
                    "user_id": actual_user_id
                }
                _write_cache(path, entry)
            except Exception as e:
                # já foi logado em _write_cache, mas registra aqui para contexto
                log_message(f"Falha ao gravar cache para {func.__name__}: {e}", "error")

            # 5) verifica e aplica política de tamanho (probabilisticamente leve)
            max_size = max_cache_size_mb if max_cache_size_mb is not None else CACHE_MAX_SIZE_MB
            if max_size:
                # faz enforcement se ultrapassar
                try:
                    if _get_cache_size() > (max_size * 1024 * 1024):
                        _enforce_cache_size(max_size)
                except Exception as e:
                    log_message(f"Erro ao aplicar limite de cache: {e}", "error")
            # limpeza probabilística de arquivos antigos
            try:
                if random.random() < CACHE_CLEANUP_PROBABILITY: # type: ignore
                    _clean_old_cache_files()
            except Exception as e:
                log_message(f"Erro em limpeza probabilística do cache: {e}", "error")

            return result
        # adiciona utilitários à função decorada (opcional)
        def clear_cache_for_this_function():
            clear_cache_for_function(func.__name__)
        def get_cache_info_for_this_function():
            return get_function_cache_info(func.__name__)
        wrapper.clear_cache = clear_cache_for_this_function  # type: ignore
        wrapper.get_cache_info = get_cache_info_for_this_function  # type: ignore
        return wrapper
    return decorator

# -------------------------------
# Utilitários extras (manutenção)
# -------------------------------
def clear_cache() -> int:
    """Remove todos os ficheiros de cache do disco e limpa cache em memória."""
    files_removed = 0
    try:
        for fname in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, fname)
            if os.path.isfile(path):
                try:
                    os.remove(path)
                    files_removed += 1
                except Exception as e:
                    log_message(f"Erro ao remover cache {fname}: {e}", "error")
        MEMORY_CACHE.clear()
        log_message(f"Cache limpo: {files_removed} arquivos removidos", "warning")
        return files_removed
    except Exception as e:
        log_message(f"Erro ao limpar cache: {e}{traceback.format_exc()}", "error")
        return 0

def clear_cache_for_function(func_name: str) -> int:
    """Remove apenas os caches relacionados a uma função (baseado no campo 'function' no arquivo)."""
    files_removed = 0
    try:
        for fname in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, fname)
            if not os.path.isfile(path):
                continue
            data = _read_cache(path)
            if data and data.get("function") == func_name:
                try:
                    os.remove(path)
                    files_removed += 1
                except Exception as e:
                    log_message(f"Erro ao remover cache {fname} para função {func_name}: {e}", "error")
            else:
                # fallback heuristic: verifica nome do ficheiro
                if func_name.replace("_", "").lower() in fname.lower():
                    try:
                        os.remove(path)
                        files_removed += 1
                    except Exception:
                        pass
        # limpa memoria relevante (simples: remove todas entradas onde function == func_name)
        keys_to_remove = []
        for k, v in MEMORY_CACHE.items():
            # v não tem function; só limpamos por segurança global (apenas se contain function no disco)
            # para manter simples, vamos limpar toda a memória (opcional)
            pass
        # (não limpamos MEMORY_CACHE por função, porque as chaves não guardam função facilmente aqui)
        log_message(f"Cache para {func_name} limpo: {files_removed} arquivos removidos", "warning")
        return files_removed
    except Exception as e:
        log_message(f"Erro ao limpar cache da função {func_name}: {e}{traceback.format_exc()}", "error")
        return 0

def get_cache_info() -> dict:
    """Retorna informações gerais sobre o cache em disco e na memória."""
    total_size = 0
    file_count = 0
    oldest_file = None
    newest_file = None
    try:
        for fname in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, fname)
            if os.path.isfile(path):
                file_count += 1
                size = os.path.getsize(path)
                total_size += size
                ctime = os.path.getctime(path)
                if oldest_file is None or ctime < oldest_file[1]:
                    oldest_file = (fname, ctime)
                if newest_file is None or ctime > newest_file[1]:
                    newest_file = (fname, ctime)
    except Exception as e:
        log_message(f"Erro ao obter informações do cache: {e}{traceback.format_exc()}", "error")
    return {
        "total_files": file_count,
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "oldest_file": oldest_file[0] if oldest_file else None,
        "newest_file": newest_file[0] if newest_file else None,
        "cache_dir": CACHE_DIR,
        "cache_enabled": CACHE_ENABLED,
        "max_size_mb": CACHE_MAX_SIZE_MB,
        "default_ttl_seconds": CACHE_TTL_DEFAULT,
        "compression_enabled": CACHE_USE_GZIP,
        "memory_cache_entries": len(MEMORY_CACHE)
    }

def get_function_cache_info(func_name: str) -> dict:
    """Retorna informações sobre o cache de uma função específica (lê os arquivos correspondentes)."""
    files_info = []
    total_size = 0
    file_count = 0
    try:
        for fname in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, fname)
            if not os.path.isfile(path):
                continue
            data = _read_cache(path)
            if data and data.get("function") == func_name:
                try:
                    size = os.path.getsize(path)
                    total_size += size
                    file_count += 1
                    timestamp = data.get("timestamp", os.path.getctime(path))
                    age = time.time() - timestamp
                    files_info.append({
                        "file": fname,
                        "size_bytes": size,
                        "age_seconds": round(age, 2),
                        "timestamp": timestamp,
                        "ttl": data.get("ttl", "N/A"),
                        "user_id": data.get("user_id", "N/A")
                    })
                except Exception:
                    continue
    except Exception as e:
        log_message(f"Erro ao obter informações do cache da função {func_name}: {e}{traceback.format_exc()}", "error")
    return {
        "function": func_name,
        "cache_files_count": file_count,
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "cache_enabled": CACHE_ENABLED,
        "files": files_info
    }

def enable_cache() -> None:
    global CACHE_ENABLED
    CACHE_ENABLED = True
    log_message("Cache habilitado globalmente", "info")

def disable_cache() -> None:
    global CACHE_ENABLED
    CACHE_ENABLED = False
    log_message("Cache desabilitado globalmente", "info")

def get_cache_config() -> dict:
    return {
        'enabled': CACHE_ENABLED,
        'ttl_default': CACHE_TTL_DEFAULT,
        'max_size_mb': CACHE_MAX_SIZE_MB,
        'cleanup_interval': CACHE_CLEANUP_INTERVAL,
        'cleanup_probability': CACHE_CLEANUP_PROBABILITY,
        'max_old_files_remove': CACHE_MAX_OLD_FILES_REMOVE,
        'disable_flag': CACHE_DISABLE_FLAG,
        'directory': CACHE_DIR,
        'pickle_protocol': CACHE_PICKLE_PROTOCOL,
        'log_hits': CACHE_LOG_HITS,
        'log_misses': CACHE_LOG_MISSES,
        'use_gzip': CACHE_USE_GZIP,
        'gzip_compression_level': CACHE_GZIP_COMPRESSION_LEVEL,
        'memory_cache_ttl': MEMORY_CACHE_TTL
    }

# Inicialização do sistema de cache (log)
log_message(f"Sistema de cache inicializado: {CACHE_DIR}", "info")
log_message(f"Configurações: {get_cache_config()}", "debug")
