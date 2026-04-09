import os
from datetime import datetime
import pickle
import gzip
import shutil
import tempfile
import hashlib
import traceback
from typing import Dict, Any

from app.config.dotenv import get_env, get_env_int
from app.ultils.logger import log_message

# =====================================================================
# CONFIGURAÇÕES E DEPENDÊNCIAS DO CACHE
# =====================================================================

CACHE_DIR = get_env("CACHE_DIR", "./ocr_cache_data")
CACHE_USE_GZIP = get_env("CACHE_USE_GZIP", "True").lower() == "true"
CACHE_GZIP_COMPRESSION_LEVEL = get_env_int("CACHE_GZIP_COMPRESSION_LEVEL", 3)
CACHE_PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL

os.makedirs(CACHE_DIR, exist_ok=True)

# =====================================================================
# CONTROLO DE MEMÓRIA (LIMITE 10GB) E LRU
# =====================================================================
# 10 GB representados em bytes
MAX_RAM_CACHE_SIZE = 10 * 1024 * 1024 * 1024
current_ram_usage = 0

# Estrutura RAM: { "nome_tamanho_hash": {"val": payload, "size": bytes_size} }
MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}


def _evict_lru_if_needed(new_size: int):
    """Remove os itens menos usados se o limite de 10GB for ultrapassado."""
    global current_ram_usage
    while MEMORY_CACHE and (current_ram_usage + new_size > MAX_RAM_CACHE_SIZE):
        # O primeiro item do dicionário é o mais antigo (Least Recently Used)
        oldest_key = next(iter(MEMORY_CACHE))
        oldest_item = MEMORY_CACHE.pop(oldest_key)
        current_ram_usage -= oldest_item["size"]
        print(
            f"🧹 [CACHE EVICTION] Limite de 10GB atingido. Removendo da RAM: {oldest_key} ({oldest_item['size']} bytes libertados)."
        )


def _mark_as_recently_used(key: str):
    """Move a chave para o fim do dicionário para a marcar como a mais usada."""
    if key in MEMORY_CACHE:
        item = MEMORY_CACHE.pop(key)
        MEMORY_CACHE[key] = item


# =====================================================================
# UTILITÁRIOS DE I/O SEGUROS
# =====================================================================
def _cache_extension() -> str:
    return ".pkl.gz" if CACHE_USE_GZIP else ".pkl"


def _get_cache_path(key: str) -> str:
    return os.path.join(CACHE_DIR, f"{key}{_cache_extension()}")


def _atomic_write(path: str, data_bytes: bytes) -> None:
    dir_name = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name)
    os.close(fd)
    try:
        with open(tmp_path, "wb") as f:
            f.write(data_bytes)
        shutil.move(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def _serialize_to_bytes(data: dict) -> bytes:
    try:
        if CACHE_USE_GZIP:
            with tempfile.SpooledTemporaryFile() as tf:
                with gzip.GzipFile(
                    fileobj=tf, mode="wb", compresslevel=CACHE_GZIP_COMPRESSION_LEVEL
                ) as gz:
                    pickle.dump(data, gz, protocol=CACHE_PICKLE_PROTOCOL)
                tf.seek(0)
                return tf.read()
        else:
            return pickle.dumps(data, protocol=CACHE_PICKLE_PROTOCOL)
    except Exception as e:
        log_message(f"Erro na serialização: {e}\n{traceback.format_exc()}", "error")
        raise


def _read_cache(path: str) -> dict:
    try:
        if not os.path.exists(path):
            return None
        if CACHE_USE_GZIP:
            with gzip.open(path, "rb") as gz:
                return pickle.load(gz)
        else:
            with open(path, "rb") as f:
                return pickle.load(f)
    except Exception as e:
        log_message(f"Erro ao ler cache: {e}", "error")
        return None


# =====================================================================
# INTERFACE PÚBLICA PARA O OCR
# =====================================================================
def generate_cache_key(file_bytes: bytes, filename: str) -> str:
    """Gera uma chave no formato: name_file_size_hash"""
    if not file_bytes:
        return ""

    size = len(file_bytes)
    file_hash = hashlib.md5(file_bytes).hexdigest()

    # Limpa caracteres estranhos do nome do ficheiro
    safe_name = (
        "".join(c for c in filename if c.isalnum() or c in "._-").strip()
        or "arquivo_desconhecido"
    )

    return f"{safe_name}_{size}_{file_hash}"


def get_cached_result(file_bytes: bytes, filename: str):
    """Retorna os dados se já existirem (RAM ou Disco)."""
    global current_ram_usage
    key = generate_cache_key(file_bytes, filename)
    if not key:
        return None

    # 1. Buscar na RAM
    if key in MEMORY_CACHE:
        _mark_as_recently_used(key)  # Marca como recém-usado
        return MEMORY_CACHE[key]["val"]

    # 2. Buscar no Disco
    cache_path = _get_cache_path(key)
    disk_data = _read_cache(cache_path)

    if disk_data is not None:

        # Serializamos para saber o tamanho exato dos bytes que vai ocupar
        data_bytes = _serialize_to_bytes(disk_data)
        item_size = len(data_bytes)

        # Garante que há espaço na RAM antes de puxar do disco
        _evict_lru_if_needed(item_size)

        # Coloca na RAM para os próximos acessos serem instantâneos
        MEMORY_CACHE[key] = {"val": disk_data, "size": item_size}
        current_ram_usage += item_size

        return disk_data

    return None


def set_cached_result(file_bytes: bytes, filename: str, result: dict):
    """Guarda a extração de forma permanente e gere a RAM limite."""
    global current_ram_usage
    key = generate_cache_key(file_bytes, filename)
    if not key:
        return

    payload_formatado = {
        "data_incerido": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        "text_gerado": result.get("text", ""),
        "text": result.get("text", ""),
        "lines": result.get("lines", []),
        "file": result.get("file", None),
    }

    try:
        # Serializa para gravar no disco e também para saber o peso na RAM
        data_bytes = _serialize_to_bytes(payload_formatado)
        item_size = len(data_bytes)

        # 1. Guardar no DISCO (Permanente)
        cache_path = _get_cache_path(key)
        _atomic_write(cache_path, data_bytes)

        # 2. Guardar na RAM (Se houver espaço)
        # Se por acaso já existir (ex: reescrita), libertamos o tamanho antigo
        if key in MEMORY_CACHE:
            current_ram_usage -= MEMORY_CACHE[key]["size"]
            MEMORY_CACHE.pop(key)

        _evict_lru_if_needed(item_size)

        MEMORY_CACHE[key] = {"val": payload_formatado, "size": item_size}
        current_ram_usage += item_size

    except Exception as e:
        log_message(
            f"Erro ao tentar guardar no cache: {e}\n{traceback.format_exc()}", "error"
        )
