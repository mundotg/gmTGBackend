# db_backup_restore.py
from __future__ import annotations

import asyncio
import gzip
import os
import re
import shutil
import time
import traceback
import subprocess  # Importante para rodar o icacls e sqlcmd
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

# Ajuste os imports conforme a estrutura do seu projeto
from app.models.connection_models import DBConnection
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message

BACKUP_DIR = "backups"

# Limites de segurança
MAX_STDOUT_CHARS = 2_000_000  # 2M chars para logs
MAX_RESTORE_FILE_MB = 1024 * 5  # 5GB


# ===============================================================
# 🔧 Helpers
# ===============================================================

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

# 👇 NOVA FUNÇÃO: Corrige permissão no Windows via código
def _fix_windows_permissions(path: str) -> None:
    """
    Executa o comando icacls para dar permissão total ao grupo 'Todos' (Everyone).
    Usa o SID *S-1-1-0 que é universal, funcionando em qualquer idioma de Windows.
    Isso permite que o serviço do SQL Server escreva na pasta do usuário.
    """
    if os.name != 'nt':  # Só roda no Windows
        return

    try:
        abs_path = os.path.abspath(path)
        # Comando: icacls "C:\Path" /grant *S-1-1-0:(OI)(CI)F /T /Q
        # *S-1-1-0 = SID universal de 'Everyone/Todos'
        # (OI)(CI) = Herança para arquivos e subpastas
        # F = Full Control
        cmd = ["icacls", abs_path, "/grant", "*S-1-1-0:(OI)(CI)F", "/T", "/Q"]
        
        # Roda síncrono e silencia a saída (capture_output)
        subprocess.run(cmd, capture_output=True, check=False)
        log_message(f"🔓 Permissões Windows ajustadas para: {abs_path}", level="info")
    except Exception as e:
        # Não damos raise aqui para não bloquear o backup se já tiver permissão
        log_message(f"⚠️ Tentativa de ajustar permissões falhou (pode ser ignorado se já funcionar): {e}", level="warning")


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
        if "/" in arg and "@" in arg:
            try:
                creds, host = arg.split("@", 1)
                if "/" in creds:
                    u, _ = creds.split("/", 1)
                    masked.append(f"{u}/***@{host}")
                    continue
            except Exception: pass
        masked.append(arg)
    return " ".join(masked)


def _require_int_positive(name: str, value: Any) -> None:
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} deve ser um inteiro > 0. Recebido: {value!r}")


def _require_binary(bin_name: str) -> None:
    if shutil.which(bin_name) is None:
        raise RuntimeError(f"Dependência não encontrada: '{bin_name}'. Instale e adicione ao PATH.")


def _file_exists_and_nonempty(path: str) -> None:
    if not os.path.exists(path):
        raise RuntimeError(f"Arquivo não foi gerado: {path}")
    if os.path.getsize(path) == 0:
        raise RuntimeError(f"Arquivo gerado está vazio (0 bytes): {path}")


async def _run_command_async(
    cmd: List[str],
    env: Dict[str, str],
    description: str,
    *,
    stdin_file: Optional[Any] = None,
    timeout_sec: int = 3600,
) -> str:
    """
    Executa comandos via subprocess em thread separada (Fix Windows EventLoop).
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


def _conn_parts_from_engine(engine: Any, _conn: DBConnection) -> _ConnParts:
    driver = _driver_name(engine.url)
    db_name = _conn.database_name or "default"
    user = _conn.username or ""
    host = _conn.host or "localhost"
    port = str(_conn.port) if _conn.port else ""
    password = _conn.password or ""
    return _ConnParts(driver, db_name, user, host, port, password)


def _build_env(password: str) -> Dict[str, str]:
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password
        env["MYSQL_PWD"] = password
    return env


def _backup_ext_for_driver(driver: str) -> str:
    mapping = {
        "postgresql": "backup",
        "mysql": "sql",
        "sqlite": "db",
        "oracle": "dmp",
        "mssql": "bak"
    }
    return mapping.get(driver, "bin")


def _validate_conn_parts(parts: _ConnParts) -> None:
    requires_host = ("postgresql", "mysql", "mssql", "oracle")
    if parts.driver in requires_host and not parts.host:
        raise ValueError("Host é obrigatório.")
    if not parts.db_name:
        raise ValueError("Nome do banco de dados é obrigatório.")


def _write_text_file(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _file_size_mb(path: str) -> float:
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except OSError:
        return 0.0


# ===============================================================
# 💾 BACKUP (Async)
# ===============================================================

async def backup_database(
    db: AsyncSession,
    user_id: int,
    connection_id: int,
    compress: bool = True,
) -> str:
    # 1. Validações e Preparação
    _require_int_positive("user_id", user_id)
    _require_int_positive("connection_id", connection_id)
    
    # Cria diretório se não existir
    await asyncio.to_thread(_ensure_dir, BACKUP_DIR)
    
    # 👇 APLICA A CORREÇÃO DE PERMISSÃO AQUI
    await asyncio.to_thread(_fix_windows_permissions, BACKUP_DIR)

    engine, _conn = await ConnectionManager.get_engine_idconn_async(db, user_id, connection_id)
    parts = _conn_parts_from_engine(engine, _conn)
    _validate_conn_parts(parts)

    ext = _backup_ext_for_driver(parts.driver)
    filename = _build_backup_filename(parts.db_name, ext)
    filepath = os.path.join(BACKUP_DIR, filename)
    
    env = _build_env(parts.password)

    log_message(f"💾 Backup iniciado: {parts.db_name} [{parts.driver}]", level="info")

    try:
        if parts.driver == "postgresql":
            _require_binary("pg_dump")
            cmd = [
                "pg_dump", "-h", parts.host, "-p", parts.port or "5432", "-U", parts.user,
                "-F", "c", "-f", filepath, parts.db_name,
            ]
            await _run_command_async(cmd, env, "backup pg")

        elif parts.driver == "mysql":
            _require_binary("mysqldump")
            cmd = [
                "mysqldump", "-h", parts.host, "-P", parts.port or "3306", "-u", parts.user,
                "--single-transaction", "--quick", "--routines", "--triggers",
                "--databases", parts.db_name,
            ]
            dump_content = await _run_command_async(cmd, env, "backup mysql")
            await asyncio.to_thread(_write_text_file, filepath, dump_content)

        elif parts.driver == "sqlite":
            db_path = engine.url.database
            if not db_path or not os.path.exists(db_path):
                raise FileNotFoundError(f"SQLite não encontrado: {db_path}")
            await asyncio.to_thread(shutil.copy2, db_path, filepath)

        elif parts.driver == "oracle":
            _require_binary("exp")
            conn_str = f"{parts.user}/{parts.password}@{parts.host}:{parts.port}/{parts.db_name}"
            cmd = [
                "exp", conn_str, f"file={filepath}", f"log={filepath}.log",
                f"owner={parts.user}", "statistics=none"
            ]
            await _run_command_async(cmd, env, "backup oracle")

        elif parts.driver == "mssql":
            _require_binary("sqlcmd")
            abs_path = os.path.abspath(filepath)
            cmd = [
                "sqlcmd", "-S", f"{parts.host},{parts.port or '1433'}",
                "-U", parts.user, "-P", parts.password,
                "-Q", f"BACKUP DATABASE [{parts.db_name}] TO DISK='{abs_path}' WITH FORMAT",
            ]
            await _run_command_async(cmd, env, "backup mssql")

        else:
            raise ValueError(f"Driver '{parts.driver}' não suportado.")

        _file_exists_and_nonempty(filepath)

        if compress and not filepath.endswith(".gz"):
            filepath = await _compress_file_async(filepath)
            log_message(f"📦 Comprimido: {os.path.basename(filepath)}", level="info")

        return filepath

    except Exception as e:
        log_message(f"🔥 Erro no backup: {e}\n{traceback.format_exc()}", level="error")
        if os.path.exists(filepath):
            try: os.remove(filepath)
            except: pass
        raise


# ===============================================================
# 🔁 RESTORE (Async)
# ===============================================================

async def restore_backup(
    db: AsyncSession,
    user_id: int,
    connection_id: int,
    filepath: str,
) -> None:
    _require_int_positive("user_id", user_id)
    _require_int_positive("connection_id", connection_id)
    
    if not filepath or not os.path.exists(filepath):
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

    if _file_size_mb(filepath) > MAX_RESTORE_FILE_MB:
        raise ValueError(f"Arquivo muito grande (> {MAX_RESTORE_FILE_MB}MB).")

    extracted_path: Optional[str] = None
    final_restore_path = filepath

    if filepath.endswith(".gz"):
        log_message("📦 Descompactando...", level="info")
        extracted_path = await _extract_file_async(filepath)
        final_restore_path = extracted_path

    engine, _conn = await ConnectionManager.get_engine_idconn_async(db, user_id, connection_id)
    parts = _conn_parts_from_engine(engine, _conn)
    env = _build_env(parts.password)

    log_message(f"♻️ Restaurando em: {parts.db_name} [{parts.driver}]", level="info")

    try:
        if parts.driver == "postgresql":
            _require_binary("pg_restore")
            cmd = [
                "pg_restore", "-h", parts.host, "-p", parts.port or "5432", "-U", parts.user,
                "-d", parts.db_name, "--clean", "--if-exists", "--no-owner", "--no-acl",
                final_restore_path,
            ]
            await _run_command_async(cmd, env, "restore pg")

        elif parts.driver == "mysql":
            _require_binary("mysql")
            cmd = ["mysql", "-h", parts.host, "-P", parts.port or "3306", "-u", parts.user, parts.db_name]
            with open(final_restore_path, "rb") as f_stream:
                await _run_command_async(cmd, env, "restore mysql", stdin_file=f_stream)

        elif parts.driver == "sqlite":
            db_path = engine.url.database
            if not db_path: raise RuntimeError("Path SQLite inválido.")
            await asyncio.to_thread(shutil.copy2, final_restore_path, db_path)

        elif parts.driver == "oracle":
            _require_binary("imp")
            conn_str = f"{parts.user}/{parts.password}@{parts.host}:{parts.port}/{parts.db_name}"
            cmd = ["imp", conn_str, f"file={final_restore_path}", "full=y", "ignore=y"]
            await _run_command_async(cmd, env, "restore oracle")

        elif parts.driver == "mssql":
            _require_binary("sqlcmd")
            abs_path = os.path.abspath(final_restore_path)
            cmd = [
                "sqlcmd", "-S", f"{parts.host},{parts.port or '1433'}", "-U", parts.user, "-P", parts.password,
                "-Q", f"USE master; ALTER DATABASE [{parts.db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; RESTORE DATABASE [{parts.db_name}] FROM DISK='{abs_path}' WITH REPLACE; ALTER DATABASE [{parts.db_name}] SET MULTI_USER;",
            ]
            await _run_command_async(cmd, env, "restore mssql")

        else:
            raise ValueError(f"Driver '{parts.driver}' não suportado.")

        log_message("✅ Restauração concluída.", level="success")

    finally:
        if extracted_path and os.path.exists(extracted_path):
            try: os.remove(extracted_path)
            except Exception as e:
                log_message(f"⚠️ Falha ao limpar temp: {e}", level="warning")