import gzip
import json
import os
import pickle
import time
import hashlib
import random
from typing import Callable, Optional
import functools
import logging
from app.config.dotenv import get_env, get_env_bool, get_env_int, get_env_float

# Configuração de logging
logger = logging.getLogger(__name__)

# Carrega configurações do .env
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
CACHE_USE_GZIP = get_env_bool('CACHE_USE_GZIP', True)  # Nova configuração para gzip
CACHE_GZIP_COMPRESSION_LEVEL = get_env_int('CACHE_GZIP_COMPRESSION_LEVEL', 6)

# Diretório base do cache
CACHE_DIR = os.path.join(os.path.dirname(__file__), CACHE_DIR_NAME)
os.makedirs(CACHE_DIR, exist_ok=True)

# -------------------------------
# Funções utilitárias
# -------------------------------

def _write_cache(path: str, data: dict):
    """Escreve dados no cache com compressão gzip se habilitado."""
    try:
        if CACHE_USE_GZIP:
            with gzip.open(path, "wb", compresslevel=CACHE_GZIP_COMPRESSION_LEVEL) as f:
                pickle.dump(data, f, protocol=CACHE_PICKLE_PROTOCOL)
        else:
            with open(path, "wb") as f:
                pickle.dump(data, f, protocol=CACHE_PICKLE_PROTOCOL)
    except Exception as e:
        logger.error(f"Erro ao escrever cache em {path}: {e}")
        raise

def _read_cache(path: str) -> Optional[dict]:
    """Lê dados do cache, com suporte a compressão gzip."""
    try:
        if CACHE_USE_GZIP:
            with gzip.open(path, "rb") as f:
                return pickle.load(f)
        else:
            with open(path, "rb") as f:
                return pickle.load(f)
    except Exception as e:
        logger.warning(f"Erro ao ler cache de {path}: {e}")
        return None

def _make_key(func_name: str, user_id: Optional[str] = None, *args, **kwargs) -> str:
    """Gera uma chave única baseada no nome da função + args/kwargs."""
    try:
        # Filtra kwargs que não afetam o resultado
        filtered_kwargs = {k: v for k, v in kwargs.items() 
                          if not k.startswith('_') and k not in ['self', 'cls']}
        
        key_data = {
            "f": func_name, 
            "id_user": user_id, 
            "a": args, 
            "kw": filtered_kwargs
        }
        key_raw = json.dumps(key_data, sort_keys=True, default=str)
    except Exception as e:
        logger.warning(f"Erro ao serializar chave do cache: {e}")
        key_raw = f"{func_name}-{user_id}-{args}-{kwargs}"
    
    return hashlib.sha256(key_raw.encode()).hexdigest()

def _get_cache_path(key: str) -> str:
    """Retorna o caminho completo do arquivo de cache."""
    extension = ".pkl.gz" if CACHE_USE_GZIP else ".pkl"
    return os.path.join(CACHE_DIR, f"{key}{extension}")

def _get_cache_size() -> int:
    """Retorna o tamanho total do cache em bytes."""
    total_size = 0
    try:
        for file in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, file)
            if os.path.isfile(path):
                total_size += os.path.getsize(path)
    except Exception as e:
        logger.error(f"Erro ao calcular tamanho do cache: {e}")
    
    return total_size

def _clean_old_cache_files(max_age_seconds: Optional[int] = None):
    """Remove arquivos de cache antigos."""
    if max_age_seconds is None:
        max_age_seconds = CACHE_CLEANUP_INTERVAL
        
    current_time = time.time()
    files_removed = 0
    
    try:
        for file in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, file)
            if os.path.isfile(path):
                file_age = current_time - os.path.getctime(path)
                if file_age > max_age_seconds:
                    try:
                        os.remove(path)
                        files_removed += 1
                    except Exception as e:
                        logger.warning(f"Erro ao remover cache antigo {file}: {e}")
        
        if files_removed > 0:
            logger.info(f"Removidos {files_removed} arquivos de cache antigos")
            
    except Exception as e:
        logger.error(f"Erro durante limpeza do cache: {e}")

def clear_expired_cache_files():
    """
    Remove todos os ficheiros de cache expirados (baseado no timestamp + ttl).
    Retorna a contagem de ficheiros removidos.
    """
    current_time = time.time()
    files_removed = 0
    total_files = 0

    try:
        for file in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, file)
            if not os.path.isfile(path):
                continue

            total_files += 1
            data = _read_cache(path)
            if not data:
                # Se não conseguiu ler, ignora (pode estar corrompido)
                continue

            timestamp = data.get("timestamp")
            ttl = data.get("ttl", CACHE_TTL_DEFAULT)

            # Se TTL for None, nunca expira
            if ttl is None or timestamp is None:
                continue

            # Verifica se o cache expirou
            if (current_time - timestamp) >= ttl:
                try:
                    os.remove(path)
                    files_removed += 1
                    logger.info(f"🧹 Cache expirado removido: {file}")
                except Exception as e:
                    logger.warning(f"Erro ao remover cache expirado {file}: {e}")

        if files_removed > 0:
            logger.info(f"🧾 Total de {files_removed}/{total_files} caches expirados removidos.")
        else:
            logger.debug("Nenhum cache expirado encontrado.")

        return files_removed

    except Exception as e:
        logger.error(f"Erro ao limpar caches expirados: {e}")
        return 0


# -------------------------------
# Função principal genérica
# -------------------------------
def cache_result(ttl: Optional[int] = None, max_cache_size_mb: Optional[int] = None, user_id: Optional[str] = None):
    """
    Decorador genérico de cache em disco.
    
    Args:
        ttl: Tempo em segundos antes de expirar o cache (None = infinito)
        max_cache_size_mb: Tamanho máximo do cache em MB (None = ilimitado)
        user_id: ID do usuário para cache específico
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Verifica se o cache está desabilitado globalmente
            if not CACHE_ENABLED or CACHE_DISABLE_FLAG:
                return func(*args, **kwargs)
                
            # Gera chave única incluindo user_id se fornecido
            key = _make_key(func.__name__, user_id, *args, **kwargs)
            path = _get_cache_path(key)

            # Limpeza periódica de cache antigo baseada na probabilidade configurada
            if random.random() < CACHE_CLEANUP_PROBABILITY:
                _clean_old_cache_files()

            # Verifica tamanho do cache se houver limite
            current_max_size = max_cache_size_mb if max_cache_size_mb is not None else CACHE_MAX_SIZE_MB
            if current_max_size:
                cache_size_mb = _get_cache_size() / (1024 * 1024)
                if cache_size_mb > current_max_size:
                    logger.warning(f"Cache excedeu limite de {current_max_size}MB")
                    # clear_oldest_cache_files(CACHE_MAX_OLD_FILES_REMOVE)

            # Tenta ler do disco
            cache_hit = False
            current_ttl = ttl if ttl is not None else CACHE_TTL_DEFAULT
            
            if os.path.exists(path):
                data = _read_cache(path)
                
                if data is not None:
                    # Se tiver TTL, verifica expiração
                    if current_ttl is None or (time.time() - data.get("timestamp", 0) < current_ttl):
                        if CACHE_LOG_HITS:
                            logger.debug(f"Cache hit para {func.__name__}")
                        cache_hit = True
                        return data["value"]
                    else:
                        # Cache expirado, remove o arquivo
                        try:
                            os.remove(path)
                            if CACHE_LOG_MISSES:
                                logger.debug(f"Cache expirado para {func.__name__}")
                        except OSError as e:
                            logger.warning(f"Erro ao remover cache expirado: {e}")

            # Se não houver cache válido → executa e grava
            if CACHE_LOG_MISSES and not cache_hit:
                logger.debug(f"Cache miss para {func.__name__}")
                
            result = func(*args, **kwargs)
            
            try:
                cache_data = {
                    "timestamp": time.time(), 
                    "value": result,
                    "function": func.__name__,
                    "ttl": current_ttl,
                    "user_id": user_id
                }
                _write_cache(path, cache_data)
                    
                if CACHE_LOG_HITS:
                    logger.debug(f"Cache gravado para {func.__name__}")
                    
            except Exception as e:
                logger.error(f"Erro ao gravar cache para {func.__name__}: {e}")
            
            return result
        
        # Adiciona métodos utilitários à função wrapper
        def clear_cache_for_this_function():
            """Limpa o cache apenas para esta função específica"""
            clear_cache_for_function(func.__name__)
        
        def get_cache_info() -> dict:
            """Retorna informações sobre o cache desta função"""
            return get_function_cache_info(func.__name__)
        
        def is_cache_enabled() -> bool:
            """Verifica se o cache está habilitado para esta função"""
            return CACHE_ENABLED and not CACHE_DISABLE_FLAG
        
        wrapper.clear_cache = clear_cache_for_this_function
        wrapper.get_cache_info = get_cache_info
        wrapper.is_cache_enabled = is_cache_enabled
        
        return wrapper
    return decorator

# -------------------------------
# Utilitários extras
# -------------------------------

def clear_cache():
    """Remove todos os ficheiros de cache."""
    files_removed = 0
    try:
        for file in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, file)
            if os.path.isfile(path):
                try:
                    os.remove(path)
                    files_removed += 1
                except Exception as e:
                    logger.error(f"Erro ao remover cache {file}: {e}")
        
        logger.info(f"Cache limpo: {files_removed} arquivos removidos")
        return files_removed
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {e}")
        return 0

def clear_cache_for_function(func_name: str):
    """Remove apenas os caches relacionados a uma função."""
    files_removed = 0
    
    try:
        for file in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, file)
            if os.path.isfile(path):
                # Tenta verificar se o arquivo pertence à função
                try:
                    data = _read_cache(path)
                    if data and data.get("function") == func_name:
                        os.remove(path)
                        files_removed += 1
                except:
                    # Se não conseguir ler, verifica pelo padrão do nome
                    if func_name.replace('_', '').lower() in file.lower():
                        os.remove(path)
                        files_removed += 1
                        
        logger.info(f"Cache para {func_name} limpo: {files_removed} arquivos removidos")
        return files_removed
    except Exception as e:
        logger.error(f"Erro ao limpar cache da função {func_name}: {e}")
        return 0

def get_cache_info() -> dict:
    """Retorna informações gerais sobre o cache"""
    total_size = 0
    file_count = 0
    oldest_file = None
    newest_file = None
    
    try:
        for file in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, file)
            if os.path.isfile(path):
                file_count += 1
                size = os.path.getsize(path)
                total_size += size
                ctime = os.path.getctime(path)
                
                if oldest_file is None or ctime < oldest_file[1]:
                    oldest_file = (file, ctime)
                if newest_file is None or ctime > newest_file[1]:
                    newest_file = (file, ctime)
    except Exception as e:
        logger.error(f"Erro ao obter informações do cache: {e}")
    
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
        "compression_enabled": CACHE_USE_GZIP
    }

def get_function_cache_info(func_name: str) -> dict:
    """Retorna informações sobre o cache de uma função específica"""
    files_info = []
    total_size = 0
    file_count = 0
    
    try:
        for file in os.listdir(CACHE_DIR):
            path = os.path.join(CACHE_DIR, file)
            if os.path.isfile(path):
                data = _read_cache(path)
                if data and data.get("function") == func_name:
                    size = os.path.getsize(path)
                    total_size += size
                    file_count += 1
                    ctime = os.path.getctime(path)
                    
                    age = time.time() - data.get("timestamp", ctime)
                    files_info.append({
                        "file": file,
                        "size_bytes": size,
                        "age_seconds": round(age, 2),
                        "timestamp": data.get("timestamp", ctime),
                        "ttl": data.get("ttl", "N/A"),
                        "user_id": data.get("user_id", "N/A")
                    })
    except Exception as e:
        logger.error(f"Erro ao obter informações do cache da função {func_name}: {e}")
    
    return {
        "function": func_name,
        "cache_files_count": file_count,
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "cache_enabled": CACHE_ENABLED,
        "files": files_info
    }

def enable_cache():
    """Habilita o cache globalmente"""
    global CACHE_ENABLED
    CACHE_ENABLED = True
    logger.info("Cache habilitado globalmente")

def disable_cache():
    """Desabilita o cache globalmente"""
    global CACHE_ENABLED
    CACHE_ENABLED = False
    logger.info("Cache desabilitado globalmente")

def get_cache_config() -> dict:
    """Retorna a configuração atual do cache"""
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
        'gzip_compression_level': CACHE_GZIP_COMPRESSION_LEVEL
    }

# Inicialização do sistema de cache
logger.info(f"Sistema de cache inicializado: {CACHE_DIR}")
logger.info(f"Configurações: {get_cache_config()}")


