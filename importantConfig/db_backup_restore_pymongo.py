# db_backup_restore.py
from __future__ import annotations

import asyncio
import gzip
import os
import re
import shutil
import time
import subprocess
import struct
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from pymongo import MongoClient, IndexModel
from bson import BSON

# Ajuste os imports conforme a estrutura do seu projeto
from app.config.dependencies import _maybe_remap_host
from app.models.connection_models import DBConnection
from app.services.crypto_utils import secret_decrypt
from app.ultils.logger import log_message

BACKUP_DIR = "backups"

# Limites de segurança
MAX_STDOUT_CHARS = 2_000_000  # 2M chars para logs
MAX_RESTORE_FILE_MB = 1024 * 5  # 5GB


# ===============================================================
# 🔧 Helpers de Sistema e Caminhos
# ===============================================================

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _is_local_host(host: str) -> bool:
    """Verifica se o host é a máquina local."""
    h = (host or "").strip().lower()
    return h in ("localhost", "127.0.0.1", "::1", ".", "(local)") or h.startswith("localhost")

# ... (Funções auxiliares _now_stamp, _driver_name, _safe_filename_part mantidas iguais) ...
def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _driver_name(url: Any) -> str:
    name = url.get_backend_name().lower()
    if "postgres" in name: return "postgresql"
    if "mysql" in name or "mariadb" in name: return "mysql"
    if "sqlite" in name: return "sqlite"
    if "oracle" in name: return "oracle"
    if "mssql" in name or "sqlserver" in name: return "mssql"
    return name

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_\-\.]+")

def _safe_filename_part(value: str, fallback: str = "default") -> str:
    v = (value or "").strip()
    if not v: return fallback
    v = v.replace(os.sep, "_").replace("/", "_").replace("\\", "_")
    v = _SAFE_NAME_RE.sub("_", v)
    return v.strip("._-") or fallback

def _build_backup_filename(db_name: str, ext: str) -> str:
    safe_db = _safe_filename_part(db_name, "default")
    safe_ext = _safe_filename_part(ext, "bin")
    return f"{safe_db}_backup_{_now_stamp()}.{safe_ext}"

def _mask_cmd(cmd: List[str]) -> str:
    masked: List[str] = []
    skip_next = False
    sensitive_flags = {"-p", "--password", "-pass", "--pass", "-pwd"}
    for arg in cmd:
        if skip_next:
            masked.append("***")
            skip_next = False
            continue
        low = arg.lower()
        if low in sensitive_flags:
            masked.append(arg)
            skip_next = True
            continue
        if "password=" in low or "pwd=" in low:
            key, _ = arg.split("=", 1)
            masked.append(f"{key}=***")
            continue
        if "/" in arg and "@" in arg: # Oracle
            try:
                creds, host = arg.split("@", 1)
                if "/" in creds:
                    u, _ = creds.split("/", 1)
                    masked.append(f"{u}/***@{host}")
                    continue
            except: pass
        masked.append(arg)
    return " ".join(masked)

def _require_int_positive(name: str, value: Any) -> None:
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} deve ser um inteiro > 0. Recebido: {value!r}")

def _file_exists_and_nonempty(path: str) -> None:
    if not os.path.exists(path):
        raise RuntimeError(f"Arquivo não foi gerado: {path}")
    if os.path.getsize(path) == 0:
        raise RuntimeError(f"Arquivo gerado está vazio (0 bytes): {path}")

def _write_text_file(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def _file_size_mb(path: str) -> float:
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except OSError:
        return 0.0

# ===============================================================
# ⚙️ Executor de Comandos (Async/Threaded)
# ===============================================================

async def _run_command_async(
    cmd: List[str],
    env: Dict[str, str],
    description: str,
    *,
    stdin_file: Optional[Any] = None,
    timeout_sec: int = 3600,
) -> str:
    """
    Executa comandos via subprocess em thread separada.
    """
    start = time.time()
    log_message(f"📤 Executando ({description}): {_mask_cmd(cmd)}", level="info")

    def _exec_sync():
        return subprocess.run(
            cmd,
            env=env,
            stdin=stdin_file,
            capture_output=True,
            timeout=timeout_sec
        )

    try:
        result = await asyncio.to_thread(_exec_sync)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Timeout ao {description} (>{timeout_sec}s).")
    except Exception as e:
        raise RuntimeError(f"Erro interno ao invocar comando: {e}")

    duration = round(time.time() - start, 2)
    out = result.stdout.decode("utf-8", errors="replace").strip()
    err = result.stderr.decode("utf-8", errors="replace").strip()
    full_output = f"STDOUT: {out}\nSTDERR: {err}"

    # Detecção de erros comuns
    if result.returncode != 0 or "Access is denied" in out or "Operating system error 5" in out:
        log_message(f"❌ Falha ao {description}: {full_output}", level="error")
        raise RuntimeError(f"Erro no banco de dados: {out} {err}")

    log_message(f"✅ {description.capitalize()} concluído em {duration}s", level="success")

    if len(out) > MAX_STDOUT_CHARS:
        return out[:MAX_STDOUT_CHARS] + "\n...[truncado]..."
    return out


# --- Compressão / Descompressão ---

def _compress_file_sync(filepath: str) -> str:
    gz_path = filepath + ".gz"
    with open(filepath, "rb") as f_in, gzip.open(gz_path, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(filepath)
    return gz_path

async def _compress_file_async(filepath: str) -> str:
    return await asyncio.to_thread(_compress_file_sync, filepath)

def _extract_file_sync(filepath: str) -> str:
    if not filepath.endswith(".gz"): return filepath
    out_path = filepath[:-3]
    with gzip.open(filepath, "rb") as f_in, open(out_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return out_path

async def _extract_file_async(filepath: str) -> str:
    return await asyncio.to_thread(_extract_file_sync, filepath)


@dataclass(frozen=True)
class _ConnParts:
    driver: str
    db_name: str
    user: str
    host: str
    port: str
    password: str


def _dec(value: Any) -> str:
    """Decifra um campo cifrado em repouso; devolve string vazia se vazio."""
    if not value:
        return ""
    try:
        return secret_decrypt(value)
    except Exception:
        # Já em claro (ex.: dados antigos) — devolve como está.
        return str(value)


def _driver_from_conn(_conn: DBConnection, engine: Any = None) -> str:
    """
    Deriva o driver a partir do tipo da conexão. Usa `_conn.type` (que existe
    para todos, incluindo MongoDB) em vez de `engine.url`, que rebenta num
    MongoClient. `engine` é opcional (fallback só para tipos desconhecidos).
    """
    t = (_conn.type or "").lower()
    aliases = {
        "postgres": "postgresql",
        "postgresql": "postgresql",
        "mysql": "mysql",
        "mariadb": "mysql",
        "sqlite": "sqlite",
        "oracle": "oracle",
        "sqlserver": "mssql",
        "mssql": "mssql",
        "mongodb": "mongodb",
        "mongo": "mongodb",
    }
    if t in aliases:
        return aliases[t]
    # Fallback para o driver do SQLAlchemy quando o tipo é desconhecido.
    try:
        return _driver_name(engine.url)
    except Exception:
        return t or "unknown"


def _conn_parts_from_engine(engine: Any, _conn: DBConnection) -> _ConnParts:
    # ⚠️ Os campos vêm cifrados em repouso — decifrar antes de os passar às
    # ferramentas de linha de comando. E remapear localhost→host.docker.internal
    # quando o backend corre em contentor (ver dependencies._maybe_remap_host).
    driver = _driver_from_conn(_conn, engine)
    db_name = _conn.database_name or "default"
    user = _dec(_conn.username)
    host = _maybe_remap_host(_dec(_conn.host) or "localhost")
    port = str(_conn.port) if _conn.port else ""
    password = _dec(_conn.password)
    return _ConnParts(driver, db_name, user, host, port, password)


# ===============================================================
# 🍃 MongoDB (PyMongo + BSON, sem mongodump/mongorestore)
# ===============================================================

# Formato interno do backup MongoDB.
#
# O arquivo continua com a extensão `.archive.gz`, mas NÃO é um arquivo
# compatível com mongorestore. É um formato próprio do gestor de bases de
# dados, desenhado para funcionar apenas com PyMongo/BSON.
#
# Estrutura:
#   MAGIC
#   M <tamanho:uint64> <metadata BSON>
#   C <tamanho:uint64> <collection metadata BSON>
#   D <tamanho:uint64> <document BSON>
#   ...
#   E
#
# Os documentos são escritos um a um, sem carregar uma collection inteira
# para memória.
_MONGO_BACKUP_MAGIC = b"GMONGO_BACKUP_V1\n"
_MONGO_RECORD_HEADER = struct.Struct("!BQ")  # tipo + tamanho unsigned 64-bit
_MONGO_TYPE_METADATA = b"M"
_MONGO_TYPE_COLLECTION = b"C"
_MONGO_TYPE_DOCUMENT = b"D"
_MONGO_TYPE_END = b"E"
_MONGO_BATCH_SIZE = 1000


def _mongo_default_port(port: str) -> int:
    try:
        return int(port or "27017")
    except (TypeError, ValueError):
        raise ValueError(f"Porta MongoDB inválida: {port!r}")


def _mongo_client(parts: _ConnParts) -> MongoClient:
    """Cria um MongoClient usando os dados já decifrados da conexão."""
    kwargs: Dict[str, Any] = {
        "host": parts.host or "localhost",
        "port": _mongo_default_port(parts.port),
        "serverSelectionTimeoutMS": int(os.environ.get("MONGO_SERVER_SELECTION_TIMEOUT_MS", "10000")),
        "connectTimeoutMS": int(os.environ.get("MONGO_CONNECT_TIMEOUT_MS", "10000")),
    }

    if parts.user:
        kwargs["username"] = parts.user
        kwargs["password"] = parts.password
        kwargs["authSource"] = os.environ.get("MONGO_AUTH_DB", "admin")

    # Mantém compatibilidade com deployments que precisam de TLS.
    # Não ativa TLS por padrão, preservando o comportamento anterior.
    tls = os.environ.get("MONGO_TLS", "").strip().lower()
    if tls in {"1", "true", "yes", "on"}:
        kwargs["tls"] = True

    return MongoClient(**kwargs)


def _mongo_write_record(stream: Any, record_type: bytes, payload: bytes = b"") -> None:
    if record_type not in {
        _MONGO_TYPE_METADATA,
        _MONGO_TYPE_COLLECTION,
        _MONGO_TYPE_DOCUMENT,
        _MONGO_TYPE_END,
    }:
        raise ValueError(f"Tipo de record Mongo inválido: {record_type!r}")

    if record_type == _MONGO_TYPE_END:
        stream.write(_MONGO_RECORD_HEADER.pack(record_type[0], 0))
        return

    stream.write(_MONGO_RECORD_HEADER.pack(record_type[0], len(payload)))
    stream.write(payload)


def _mongo_read_record(stream: Any) -> tuple[bytes, bytes]:
    header = stream.read(_MONGO_RECORD_HEADER.size)
    if not header:
        raise RuntimeError("Backup Mongo terminado inesperadamente.")
    if len(header) != _MONGO_RECORD_HEADER.size:
        raise RuntimeError("Cabeçalho de record Mongo corrompido.")

    type_byte, size = _MONGO_RECORD_HEADER.unpack(header)
    record_type = bytes([type_byte])

    if record_type == _MONGO_TYPE_END:
        if size != 0:
            raise RuntimeError("Record final Mongo inválido.")
        return record_type, b""

    # Evita alocações absurdas causadas por um backup adulterado.
    max_record_mb = int(os.environ.get("MONGO_MAX_RECORD_MB", "64"))
    max_record_bytes = max_record_mb * 1024 * 1024
    if size > max_record_bytes:
        raise RuntimeError(
            f"Record Mongo demasiado grande ({size / (1024 * 1024):.2f} MB)."
        )

    payload = stream.read(size)
    if len(payload) != size:
        raise RuntimeError("Record Mongo incompleto/corrompido.")
    return record_type, payload


def _mongo_sanitize_index_info(info: Dict[str, Any]) -> Dict[str, Any]:
    """Converte IndexInformation em metadata BSON reaproveitável no restore."""
    result = dict(info)
    # Esses campos são metadados do servidor e não argumentos de create_indexes.
    result.pop("v", None)
    result.pop("ns", None)
    return result


def _mongo_collection_metadata(db: Any, collection_name: str) -> Dict[str, Any]:
    """Obtém opções, tipo e índices de uma collection/view."""
    info = db.command("listCollections", filter={"name": collection_name})
    batch = info.get("cursor", {}).get("firstBatch", [])
    if not batch:
        raise RuntimeError(f"Collection Mongo não encontrada: {collection_name}")

    raw_info = batch[0]
    collection_type = raw_info.get("type", "collection")
    options = dict(raw_info.get("options") or {})

    metadata: Dict[str, Any] = {
        "name": collection_name,
        "type": collection_type,
        "options": options,
    }

    if collection_type == "view":
        metadata["viewOn"] = options.get("viewOn")
        metadata["pipeline"] = options.get("pipeline", [])
        metadata["indexes"] = []
        return metadata

    collection = db[collection_name]
    indexes: List[Dict[str, Any]] = []
    for index_info in collection.list_indexes():
        indexes.append(_mongo_sanitize_index_info(dict(index_info)))
    metadata["indexes"] = indexes
    return metadata


def _mongo_create_collection_from_metadata(db: Any, metadata: Dict[str, Any]) -> Any:
    """Cria collection/view a partir do metadata do backup."""
    name = metadata["name"]
    collection_type = metadata.get("type", "collection")

    # A restauração começa substituindo a collection/view existente.
    if name in db.list_collection_names():
        db.drop_collection(name)

    if collection_type == "view":
        view_on = metadata.get("viewOn")
        pipeline = metadata.get("pipeline", [])
        if not view_on:
            raise RuntimeError(f"View '{name}' sem viewOn no backup.")
        return db.create_collection(name, viewOn=view_on, pipeline=pipeline)

    options = dict(metadata.get("options") or {})

    # Alguns campos retornados por listCollections são informativos e não
    # devem ser enviados de volta para create_collection.
    for key in (
        "uuid",
        "idIndex",
        "ns",
    ):
        options.pop(key, None)

    try:
        return db.create_collection(name, **options)
    except Exception as first_error:
        # Algumas opções dependem da versão/configuração do servidor. Não
        # abortamos a restauração inteira só porque uma opção específica não
        # é aceite; tentamos criar a collection vazia e recriar os índices.
        log_message(
            f"⚠️ Não foi possível recriar todas as opções da collection "
            f"'{name}': {first_error}. Criando collection padrão.",
            level="warning",
        )
        try:
            return db.create_collection(name)
        except Exception:
            # Se outro processo criou a collection entre o drop/create, usa-a.
            if name in db.list_collection_names():
                return db[name]
            raise


def _mongo_recreate_indexes(collection: Any, indexes: List[Dict[str, Any]]) -> None:
    """Recria os índices gravados no backup, exceto o _id_ automático."""
    models: List[IndexModel] = []

    for info in indexes:
        name = info.get("name")
        if name == "_id_":
            continue

        key = info.get("key")
        if not key:
            log_message(
                f"⚠️ Índice '{name}' ignorado: definição 'key' ausente.",
                level="warning",
            )
            continue

        options = dict(info)
        options.pop("key", None)
        options.pop("name", None)
        options.pop("v", None)
        options.pop("ns", None)

        try:
            models.append(IndexModel(key, name=name, **options))
        except Exception as e:
            raise RuntimeError(
                f"Não foi possível preparar o índice '{name}' "
                f"da collection '{collection.name}': {e}"
            ) from e

    if models:
        collection.create_indexes(models)


def _mongo_backup_sync(parts: _ConnParts, filepath: str) -> str:
    """Backup MongoDB usando exclusivamente PyMongo + BSON + gzip."""
    client = _mongo_client(parts)
    try:
        db = client[parts.db_name]
        # Força a conexão antes de criar o arquivo, para não gerar um backup
        # vazio caso host/credenciais estejam errados.
        client.admin.command("ping")

        collection_names = db.list_collection_names()
        metadata = {
            "format": "gmongo-pymongo",
            "version": 1,
            "database": parts.db_name,
            "created_at": datetime.utcnow().isoformat() + "Z",
            "collections_count": len(collection_names),
        }

        _ensure_dir(os.path.dirname(os.path.abspath(filepath)) or ".")
        with gzip.open(filepath, "wb", compresslevel=6) as stream:
            stream.write(_MONGO_BACKUP_MAGIC)
            _mongo_write_record(stream, _MONGO_TYPE_METADATA, BSON.encode(metadata))

            for collection_name in collection_names:
                collection_meta = _mongo_collection_metadata(db, collection_name)
                _mongo_write_record(
                    stream,
                    _MONGO_TYPE_COLLECTION,
                    BSON.encode(collection_meta),
                )

                if collection_meta.get("type") == "view":
                    continue

                collection = db[collection_name]
                cursor = collection.find({}, no_cursor_timeout=False).batch_size(_MONGO_BATCH_SIZE)
                try:
                    for document in cursor:
                        _mongo_write_record(
                            stream,
                            _MONGO_TYPE_DOCUMENT,
                            BSON.encode(document),
                        )
                finally:
                    cursor.close()

            _mongo_write_record(stream, _MONGO_TYPE_END)

        return filepath
    finally:
        client.close()


async def _mongo_backup(parts: _ConnParts, filepath: str) -> str:
    """Backup MongoDB para `.archive.gz`, sem depender de mongodump."""
    await asyncio.to_thread(_mongo_backup_sync, parts, filepath)
    return filepath


def _mongo_restore_sync(parts: _ConnParts, filepath: str) -> None:
    """Restaura um backup criado por `_mongo_backup_sync`."""
    client = _mongo_client(parts)
    try:
        db = client[parts.db_name]
        client.admin.command("ping")

        with gzip.open(filepath, "rb") as stream:
            magic = stream.read(len(_MONGO_BACKUP_MAGIC))
            if magic != _MONGO_BACKUP_MAGIC:
                raise RuntimeError(
                    "Formato de backup Mongo inválido ou incompatível. "
                    "Este restore espera um backup criado pelo gestor com PyMongo."
                )

            record_type, payload = _mongo_read_record(stream)
            if record_type != _MONGO_TYPE_METADATA:
                raise RuntimeError("Backup Mongo sem metadata inicial.")

            archive_metadata = BSON(payload).decode()
            if archive_metadata.get("format") != "gmongo-pymongo":
                raise RuntimeError("Formato de backup Mongo não suportado.")
            if int(archive_metadata.get("version", 0)) != 1:
                raise RuntimeError(
                    f"Versão de backup Mongo não suportada: {archive_metadata.get('version')}"
                )

            current_collection: Optional[Any] = None
            current_metadata: Optional[Dict[str, Any]] = None
            batch: List[Dict[str, Any]] = []

            def flush_batch() -> None:
                nonlocal batch
                if batch and current_collection is not None:
                    current_collection.insert_many(batch, ordered=False)
                    batch = []

            while True:
                record_type, payload = _mongo_read_record(stream)

                if record_type == _MONGO_TYPE_END:
                    flush_batch()
                    break

                if record_type == _MONGO_TYPE_COLLECTION:
                    flush_batch()
                    current_metadata = BSON(payload).decode()
                    current_collection = _mongo_create_collection_from_metadata(
                        db, current_metadata
                    )

                    # Views não recebem documentos/índices.
                    if current_metadata.get("type") == "view":
                        current_collection = None
                    continue

                if record_type == _MONGO_TYPE_DOCUMENT:
                    if current_collection is None or current_metadata is None:
                        raise RuntimeError(
                            "Documento Mongo encontrado antes de uma collection."
                        )
                    if current_metadata.get("type") == "view":
                        raise RuntimeError("Backup inválido: view contém documentos.")

                    document = BSON(payload).decode()
                    batch.append(document)
                    if len(batch) >= _MONGO_BATCH_SIZE:
                        flush_batch()
                    continue

                raise RuntimeError(f"Record Mongo desconhecido: {record_type!r}")

            # Recria índices depois de todos os documentos da collection.
            # Como o formato é sequencial, precisamos reabrir o arquivo para
            # percorrer os headers novamente e aplicar os índices por collection.
            stream.seek(0)
            magic = stream.read(len(_MONGO_BACKUP_MAGIC))
            if magic != _MONGO_BACKUP_MAGIC:
                raise RuntimeError("Backup Mongo ficou inválido durante o restore.")
            _mongo_read_record(stream)  # metadata

            while True:
                record_type, payload = _mongo_read_record(stream)
                if record_type == _MONGO_TYPE_END:
                    break
                if record_type != _MONGO_TYPE_COLLECTION:
                    continue

                meta = BSON(payload).decode()
                if meta.get("type") == "view":
                    continue

                name = meta["name"]
                collection = db[name]
                _mongo_recreate_indexes(collection, meta.get("indexes", []))

    finally:
        client.close()


async def _mongo_restore(parts: _ConnParts, filepath: str) -> None:
    """Restore MongoDB a partir de `.archive.gz`, sem mongorestore."""
    await asyncio.to_thread(_mongo_restore_sync, parts, filepath)


def _build_env(password: str) -> Dict[str, str]:
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password
        env["MYSQL_PWD"] = password
    return env


def _backup_ext_for_driver(driver: str) -> str:
    mapping = { "postgresql": "backup", "mysql": "sql", "sqlite": "db", "oracle": "dmp", "mssql": "bak" }
    return mapping.get(driver, "bin")


def _validate_conn_parts(parts: _ConnParts) -> None:
    requires_host = ("postgresql", "mysql", "mssql", "oracle")
    if parts.driver in requires_host and not parts.host:
        raise ValueError("Host é obrigatório.")
    if not parts.db_name:
        raise ValueError("Nome do banco de dados é obrigatório.")


# ===============================================================
# 💾 BACKUP (Async)
# ===============================================================

# async def backup_database(
#     db: AsyncSession,
#     user_id: int,
#     connection_id: int,
#     compress: bool = True,
# ) -> str:
#     _require_int_positive("user_id", user_id)
#     _require_int_positive("connection_id", connection_id)
    
#     # Prepara diretório e permissões
#     await asyncio.to_thread(_ensure_dir, BACKUP_DIR)
#     await asyncio.to_thread(_fix_windows_permissions, BACKUP_DIR)

#     # ⚠️ Só precisamos dos metadados da conexão — NÃO de um motor ligado.
#     # pg_dump/mongodump ligam-se sozinhos. Construir o motor async aqui só
#     # trazia problemas (ex.: asyncpg a rejeitar SSL) que não têm nada a ver
#     # com o backup.
#     _conn = await get_connection_id_async(db, user_id, connection_id)
#     if not _conn:
#         raise ValueError("Conexão não encontrada.")

#     parts = _conn_parts_from_engine(None, _conn)

#     # 🍃 MongoDB tem um caminho próprio (PyMongo + BSON), sem SQL.
#     if parts.driver == "mongodb":
#         _validate_conn_parts(parts)
#         filename = _build_backup_filename(parts.db_name, "archive.gz")
#         filepath = os.path.join(BACKUP_DIR, filename)
#         log_message(f"💾 Backup Mongo iniciado: {parts.db_name}", level="info")
#         try:
#             await _mongo_backup(parts, filepath)
#             _file_exists_and_nonempty(filepath)
#             # O formato próprio já é `.gz`; não passa pela compressão genérica.
#             return filepath
#         except Exception as e:
#             log_message(f"🔥 Erro no backup Mongo: {e}\n{traceback.format_exc()}", level="error")
#             if os.path.exists(filepath):
#                 try:
#                     os.remove(filepath)
#                 except Exception:
#                     pass
#             raise

#     # SQL: dispatch por driver (o driver vem de _conn.type, não de um motor).
#     _validate_conn_parts(parts)

#     ext = _backup_ext_for_driver(parts.driver)
#     filename = _build_backup_filename(parts.db_name, ext)
#     filepath = os.path.join(BACKUP_DIR, filename)
#     env = _build_env(parts.password)

#     log_message(f"💾 Backup iniciado: {parts.db_name} [{parts.driver}]", level="info")

#     try:
#         if parts.driver == "postgresql":
#             # Busca binário pg_dump (aceita override via ENV 'PG_DUMP_PATH')
#             pg_bin = _resolve_binary("pg_dump", explicit_path=os.environ.get("PG_DUMP_PATH"))
#             cmd = [
#                 pg_bin, "-h", parts.host, "-p", parts.port or "5432", "-U", parts.user,
#                 "-F", "c", "-f", filepath, parts.db_name,
#             ]
#             await _run_command_async(cmd, env, "backup pg")

#         elif parts.driver == "mysql":
#             mysql_bin = _resolve_binary("mysqldump", explicit_path=os.environ.get("MYSQLDUMP_PATH"))
#             cmd = [
#                 mysql_bin, "-h", parts.host, "-P", parts.port or "3306", "-u", parts.user,
#                 "--single-transaction", "--quick", "--routines", "--triggers",
#                 "--databases", parts.db_name,
#             ]
#             dump_content = await _run_command_async(cmd, env, "backup mysql")
#             await asyncio.to_thread(_write_text_file, filepath, dump_content)

#         elif parts.driver == "sqlite":
#             # No SQLite o "host" guarda o caminho do ficheiro .db.
#             db_path = parts.host
#             if not db_path or not os.path.exists(db_path):
#                 raise FileNotFoundError(f"SQLite não encontrado: {db_path}")
#             await asyncio.to_thread(shutil.copy2, db_path, filepath)

#         elif parts.driver == "oracle":
#             # Oracle geralmente já está no PATH
#             _resolve_binary("exp")
#             conn_str = f"{parts.user}/{parts.password}@{parts.host}:{parts.port}/{parts.db_name}"
#             cmd = [
#                 "exp", conn_str, f"file={filepath}", f"log={filepath}.log",
#                 f"owner={parts.user}", "statistics=none"
#             ]
#             await _run_command_async(cmd, env, "backup oracle")

#         elif parts.driver == "mssql" or parts.driver == "sqlserver":
#             sqlcmd_bin = _resolve_binary("sqlcmd", explicit_path=os.environ.get("SQLCMD_PATH"))
            
#             # Determina o caminho correto para o servidor SQL escrever
#             target_path = _mssql_backup_target_path(parts, filepath)

#             cmd = [
#                 sqlcmd_bin, "-S", f"{parts.host},{parts.port or '1433'}",
#                 "-U", parts.user, "-P", parts.password,
#                 "-Q", f"BACKUP DATABASE [{parts.db_name}] TO DISK='{target_path}' WITH FORMAT",
#             ]
#             await _run_command_async(cmd, env, "backup mssql")

#         else:
#             raise ValueError(f"Driver '{parts.driver}' não suportado.")

#         _file_exists_and_nonempty(filepath)

#         if compress and not filepath.endswith(".gz"):
#             filepath = await _compress_file_async(filepath)
#             log_message(f"📦 Comprimido: {os.path.basename(filepath)}", level="info")

#         return filepath

#     except Exception as e:
#         log_message(f"🔥 Erro no backup: {e}\n{traceback.format_exc()}", level="error")
#         if os.path.exists(filepath):
#             try: os.remove(filepath)
#             except: pass
#         raise


# # ===============================================================
# # 🔁 RESTORE (Async)
# # ===============================================================

# async def restore_backup(
#     db: AsyncSession,
#     user_id: int,
#     connection_id: int,
#     filepath: str,
# ) -> None:
#     _require_int_positive("user_id", user_id)
#     _require_int_positive("connection_id", connection_id)
    
#     if not filepath or not os.path.exists(filepath):
#         raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

#     if _file_size_mb(filepath) > MAX_RESTORE_FILE_MB:
#         raise ValueError(f"Arquivo muito grande (> {MAX_RESTORE_FILE_MB}MB).")

#     _conn = await get_connection_id_async(db, user_id, connection_id)
#     if not _conn:
#         raise ValueError("Conexão não encontrada.")
#     parts = _conn_parts_from_engine(None, _conn)

#     # 🍃 MongoDB: o restore lê o `.archive.gz` próprio do gestor diretamente.
#     # Não usa mongorestore nem a descompressão genérica.
#     if parts.driver == "mongodb":
#         log_message(f"♻️ Restaurando Mongo em: {parts.db_name}", level="info")
#         await _mongo_restore(parts, filepath)
#         log_message("✅ Restauração Mongo concluída.", level="success")
#         return

#     extracted_path: Optional[str] = None
#     final_restore_path = filepath

#     if filepath.endswith(".gz"):
#         log_message("📦 Descompactando...", level="info")
#         extracted_path = await _extract_file_async(filepath)
#         final_restore_path = extracted_path

#     env = _build_env(parts.password)

#     log_message(f"♻️ Restaurando em: {parts.db_name} [{parts.driver}]", level="info")

#     try:
#         if parts.driver == "postgresql":
#             pg_restore_bin = _resolve_binary("pg_restore", explicit_path=os.environ.get("PG_RESTORE_PATH"))
#             cmd = [
#                 pg_restore_bin, "-h", parts.host, "-p", parts.port or "5432", "-U", parts.user,
#                 "-d", parts.db_name, "--clean", "--if-exists", "--no-owner", "--no-acl",
#                 final_restore_path,
#             ]
#             await _run_command_async(cmd, env, "restore pg")

#         elif parts.driver == "mysql":
#             mysql_bin = _resolve_binary("mysql", explicit_path=os.environ.get("MYSQL_PATH"))
#             cmd = [mysql_bin, "-h", parts.host, "-P", parts.port or "3306", "-u", parts.user, parts.db_name]
#             with open(final_restore_path, "rb") as f_stream:
#                 await _run_command_async(cmd, env, "restore mysql", stdin_file=f_stream)

#         elif parts.driver == "sqlite":
#             db_path = parts.host  # o "host" guarda o caminho do ficheiro .db
#             if not db_path: raise RuntimeError("Path SQLite inválido.")
#             await asyncio.to_thread(shutil.copy2, final_restore_path, db_path)

#         elif parts.driver == "oracle":
#             _resolve_binary("imp")
#             conn_str = f"{parts.user}/{parts.password}@{parts.host}:{parts.port}/{parts.db_name}"
#             cmd = ["imp", conn_str, f"file={final_restore_path}", "full=y", "ignore=y"]
#             await _run_command_async(cmd, env, "restore oracle")

#         elif parts.driver == "mssql" or parts.driver == "sqlserver":
#             sqlcmd_bin = _resolve_binary("sqlcmd", explicit_path=os.environ.get("SQLCMD_PATH"))
            
#             # Para SQL Server, o restore precisa ler do disco. 
#             # Se for local, abs_path resolve. Se remoto, precisaria ser UNC (não implementado full aqui).
#             abs_path = os.path.abspath(final_restore_path)
            
#             cmd = [
#                 sqlcmd_bin, "-S", f"{parts.host},{parts.port or '1433'}", "-U", parts.user, "-P", parts.password,
#                 "-Q", f"USE master; ALTER DATABASE [{parts.db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; RESTORE DATABASE [{parts.db_name}] FROM DISK='{abs_path}' WITH REPLACE; ALTER DATABASE [{parts.db_name}] SET MULTI_USER;",
#             ]
#             await _run_command_async(cmd, env, "restore mssql")

#         else:
#             raise ValueError(f"Driver '{parts.driver}' não suportado.")

#         log_message("✅ Restauração concluída.", level="success")

#     finally:
#         if extracted_path and os.path.exists(extracted_path):
#             try: os.remove(extracted_path)
#             except Exception as e:
#                 log_message(f"⚠️ Falha ao limpar temp: {e}", level="warning")