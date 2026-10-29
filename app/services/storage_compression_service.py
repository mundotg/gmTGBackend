"""Serviço de compressão inteligente (Gzip transparente + pacotes ZIP em lote)
para o módulo MustaInf Cloud Storage (`/storage`).

Suporta:
1. **Compressão Transparente (Cliente ou Servidor)**:
   - Se o cliente comprimir no browser via `CompressionStream('gzip')` (`client_compressed=True`)
     ou se pedir `compress=True` no upload de um ficheiro comprimível (ex.: `.csv`, `.sql`, `.json`,
     `.txt`, `.log`, `.xml`, `.py`, `.ts`, `.pdf`), os bytes são armazenados comprimidos com Gzip
     no MinIO/cache (reduzindo o consumo de quota de disco e tráfego de rede).
   - No download (`GET /storage/download/{filename}`), se o ficheiro original não era `.gz` mas o
     objeto no storage está comprimido em Gzip, descomprime automaticamente em streaming (ou
     entrega comprimido se `compressed=true`).
2. **Download em Lote `.ZIP` (`build_zip_stream`)**:
   - Empacota múltiplos ficheiros selecionados ou uma pasta inteira num arquivo `.zip` (`ZIP_DEFLATED`).
"""

from __future__ import annotations

import gzip
import io
import os
import tempfile
import zipfile
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from app.config.redis import read_cache, write_cache

GZIP_MAGIC = b"\x1f\x8b"

# Extensões que já possuem compressão nativa forte (não ganham nada com Gzip extra)
ALREADY_COMPRESSED_EXTENSIONS = {
    ".zip",
    ".rar",
    ".7z",
    ".gz",
    ".tar",
    ".mp4",
    ".mp3",
    ".avi",
    ".mkv",
    ".jpg",
    ".jpeg",
    ".png",
}


def is_gzip_bytes(data: bytes) -> bool:
    return len(data) >= 2 and data[:2] == GZIP_MAGIC


def should_compress_filename(filename: str) -> bool:
    ext = os.path.splitext(filename or "")[1].lower()
    return ext not in ALREADY_COMPRESSED_EXTENSIONS


def record_compression_meta(
    object_path: str,
    *,
    compressed: bool,
    original_size: int,
    stored_size: int,
    method: str = "gzip",
) -> dict:
    saved_bytes = max(0, original_size - stored_size)
    ratio_pct = round((saved_bytes / original_size) * 100, 1) if original_size > 0 else 0.0
    meta = {
        "compressed": compressed,
        "method": method if compressed else "none",
        "original_size_bytes": original_size,
        "stored_size_bytes": stored_size,
        "saved_bytes": saved_bytes,
        "compression_ratio_percent": ratio_pct,
    }
    write_cache(f"storage:comp:{object_path}", meta, ttl=86400 * 90)
    return meta


def get_compression_meta(object_path: str, default_size: int = 0) -> dict:
    cached = read_cache(f"storage:comp:{object_path}")
    if isinstance(cached, dict):
        return cached
    return {
        "compressed": False,
        "method": "none",
        "original_size_bytes": default_size,
        "stored_size_bytes": default_size,
        "saved_bytes": 0,
        "compression_ratio_percent": 0.0,
    }


def prepare_upload_stream(
    file_obj,
    filename: str,
    *,
    compress_requested: bool = False,
    client_compressed: bool = False,
):
    """Prepara um `file_obj` (`UploadFile.file`) aplicando compressão Gzip se pedido
    ou validando Gzip enviado pelo cliente (`CompressionStream('gzip')`).

    Retorna:
        `(stream_obj, stored_size_bytes, original_size_bytes, is_compressed, cleanup_fn)`
    """
    file_obj.seek(0, os.SEEK_END)
    raw_size = file_obj.tell()
    file_obj.seek(0)

    if raw_size == 0:
        return file_obj, 0, 0, False, lambda: None

    ext = os.path.splitext(filename or "")[1].lower()
    head = file_obj.read(2)
    file_obj.seek(0)

    # Caso 1: O cliente já enviou o ficheiro comprimido com CompressionStream("gzip")
    if (client_compressed or (compress_requested and head == GZIP_MAGIC)) and ext != ".gz":
        if head == GZIP_MAGIC:
            original_size = 0
            try:
                with gzip.GzipFile(fileobj=file_obj, mode="rb") as gz:
                    while True:
                        chunk = gz.read(256 * 1024)
                        if not chunk:
                            break
                        original_size += len(chunk)
            except Exception:
                original_size = raw_size
            finally:
                file_obj.seek(0)
            return file_obj, raw_size, original_size or raw_size, True, lambda: None

    # Caso 2: Compressão no servidor (`compress=True`)
    if compress_requested and should_compress_filename(filename):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".gz")
        tmp_path = tmp.name
        tmp.close()

        def _cleanup():
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        try:
            file_obj.seek(0)
            with gzip.open(tmp_path, "wb", compresslevel=6) as gz_out:
                while True:
                    chunk = file_obj.read(256 * 1024)
                    if not chunk:
                        break
                    gz_out.write(chunk)
            file_obj.seek(0)

            gz_size = os.path.getsize(tmp_path)
            if gz_size < raw_size:
                gz_file = open(tmp_path, "rb")

                def _close_and_cleanup():
                    try:
                        gz_file.close()
                    except Exception:
                        pass
                    _cleanup()

                return gz_file, gz_size, raw_size, True, _close_and_cleanup
            else:
                _cleanup()
        except Exception:
            _cleanup()
            file_obj.seek(0)

    return file_obj, raw_size, raw_size, False, lambda: None


def compress_file_for_storage(
    source_path: str,
    filename: str,
    *,
    compress_requested: bool = False,
    client_compressed: bool = False,
) -> Tuple[str, int, int, bool]:
    """Processa o ficheiro temporário de upload para compressão opcional.

    Retorna:
        `(final_temp_path, stored_size_bytes, original_size_bytes, is_compressed)`
    """
    raw_size = os.path.getsize(source_path)
    if raw_size == 0:
        return source_path, 0, 0, False

    ext = os.path.splitext(filename or "")[1].lower()

    # Caso 1: O cliente já enviou o ficheiro comprimido com CompressionStream("gzip")
    if client_compressed and ext != ".gz":
        with open(source_path, "rb") as f:
            head = f.read(2)
        if head == GZIP_MAGIC:
            # Calcula o tamanho original descomprimido de forma segura em chunks
            original_size = 0
            try:
                with gzip.open(source_path, "rb") as gz:
                    while True:
                        chunk = gz.read(256 * 1024)
                        if not chunk:
                            break
                        original_size += len(chunk)
            except Exception:
                original_size = raw_size
            return source_path, raw_size, original_size, True

    # Caso 2: Compressão solicitada no servidor (`compress=True`)
    if compress_requested and should_compress_filename(filename):
        with open(source_path, "rb") as f:
            head = f.read(2)
        if head == GZIP_MAGIC and ext != ".gz":
            return source_path, raw_size, raw_size, True

        gz_path = f"{source_path}.gz_tmp"
        try:
            with open(source_path, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
                while True:
                    chunk = src.read(256 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

            gz_size = os.path.getsize(gz_path)
            # Só mantém a versão comprimida se realmente poupar espaço
            if gz_size < raw_size:
                os.replace(gz_path, source_path)
                return source_path, gz_size, raw_size, True
            else:
                os.remove(gz_path)
        except Exception:
            if os.path.exists(gz_path):
                try:
                    os.remove(gz_path)
                except OSError:
                    pass

    return source_path, raw_size, raw_size, False


def wrap_download_stream_with_decompression(
    raw_iterator: Iterable[bytes],
    filename: str,
    *,
    keep_compressed: bool = False,
) -> Iterator[bytes]:
    """Se o ficheiro original não termina em `.gz`, mas os bytes armazenados no bucket
    estão em formato Gzip (compressão transparente do MustaInf Cloud), descomprime
    em streaming para entregar o ficheiro original intacto ao utilizador.
    """
    ext = os.path.splitext(filename or "")[1].lower()
    iterator = iter(raw_iterator)

    try:
        first_chunk = next(iterator)
    except StopIteration:
        return

    if keep_compressed or ext == ".gz" or not is_gzip_bytes(first_chunk):
        yield first_chunk
        for chunk in iterator:
            yield chunk
        return

    # Descompressão incremental zlib/gzip (wbits = 16 + MAX_WBITS para formato gzip)
    import zlib

    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    out = decompressor.decompress(first_chunk)
    if out:
        yield out

    for chunk in iterator:
        out = decompressor.decompress(chunk)
        if out:
            yield out

    tail = decompressor.flush()
    if tail:
        yield tail


def build_zip_archive_stream(
    entries: List[Tuple[str, Iterable[bytes]]],
) -> Tuple[Iterator[bytes], int]:
    """Cria um arquivo `.zip` comprimido (`ZIP_DEFLATED`) a partir de uma lista de
    `(arcname, byte_iterator)` e devolve `(stream_iterator, zip_size_bytes)`.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    tmp_path = tmp.name
    tmp.close()

    used_names: Dict[str, int] = {}

    try:
        with zipfile.ZipFile(tmp_path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for arcname, byte_iter in entries:
                clean_name = arcname.lstrip("/\\") or "file"
                if clean_name in used_names:
                    used_names[clean_name] += 1
                    base, ext = os.path.splitext(clean_name)
                    clean_name = f"{base} ({used_names[clean_name]}){ext}"
                else:
                    used_names[clean_name] = 0

                with zf.open(clean_name, mode="w") as dest:
                    for chunk in byte_iter:
                        if chunk:
                            dest.write(chunk)

        total_size = os.path.getsize(tmp_path)

        def _reader() -> Iterator[bytes]:
            try:
                with open(tmp_path, "rb") as f:
                    while True:
                        block = f.read(256 * 1024)
                        if not block:
                            break
                        yield block
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        return _reader(), total_size
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
