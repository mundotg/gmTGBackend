# app/importantConfig/db_backup.py

import os
import subprocess
import gzip
import shutil
import time
from datetime import datetime
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from app.config.dependencies import get_session_by_connection
from app.cruds.connection_cruds import get_db_connection_by_id
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message

BACKUP_DIR = "backups"


# ===============================================================
# 🔧 FUNÇÕES AUXILIARES
# ===============================================================
def _ensure_dir(path: str):
    """Cria diretório de forma segura e performática."""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def _build_backup_filename(db_name: str, ext: str = "sql") -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{db_name}_backup_{timestamp}.{ext}"


def _run_command(cmd: list[str], env: dict, description: str):
    """Executa comandos externos de forma eficiente com captura de logs."""
    start = time.time()
    log_message(f"📤 Executando: {' '.join(cmd)}", "info")

    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=4096,
    )
    stdout, stderr = process.communicate()
    duration = round(time.time() - start, 2)

    if process.returncode != 0:
        log_message(f"❌ Falha ao {description}: {stderr.strip()}", "error")
        raise subprocess.CalledProcessError(process.returncode, cmd, stderr)

    log_message(f"✅ {description.capitalize()} concluído em {duration}s", "success")
    return stdout


def _compress_file(filepath: str) -> str:
    """Compacta o arquivo de backup para economizar espaço."""
    gz_path = filepath + ".gz"
    with open(filepath, "rb") as f_in, gzip.open(gz_path, "wb", compresslevel=5) as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(filepath)
    return gz_path


# ===============================================================
# 💾 BACKUP UNIVERSAL (OTIMIZADO)
# ===============================================================
def backup_database(db: Session,user_id:int, connection_id: int, compress: bool = True) -> str:
    """
    Cria um arquivo de backup (.sql ou .db) compatível com PostgreSQL, MySQL, SQLite e Oracle.
    """
    engine,conn = ConnectionManager.get_engine_idconn_async(db=db,user_id=user_id,id_connection=connection_id)
    url = engine.url
    driver = url.get_backend_name().lower().replace("+psycopg2", "")

    db_name = url.database or "default"
    user = url.username or ""
    host = url.host or "localhost"
    port = str(url.port or "")
    password = url.password or ""

    _ensure_dir(BACKUP_DIR)
    filepath = os.path.join(BACKUP_DIR, _build_backup_filename(db_name))
    env = os.environ.copy()
    if password:
        env.update({"PGPASSWORD": password, "MYSQL_PWD": password, "ORACLE_PWD": password})

    log_message(f"💾 Iniciando backup da base '{db_name}' ({driver}) → {filepath}", "info")

    try:
        # Seleciona comando conforme o driver
        if driver in ("postgresql", "postgres"):
            cmd = [
                "pg_dump", "-h", host, "-p", port or "5432", "-U", user,
                "-F", "p", "-f", filepath, db_name
            ]
        elif driver in ("mysql", "mariadb"):
            cmd = [
                "mysqldump", "-h", host, "-P", port or "3306", "-u", user,
                "--result-file", filepath, db_name
            ]
        elif driver == "sqlite":
            db_path = url.database
            if not db_path or not os.path.exists(db_path):
                raise FileNotFoundError(f"Arquivo SQLite não encontrado: {db_path}")
            shutil.copy2(db_path, filepath)
            log_message(f"✅ Backup SQLite copiado com sucesso → {filepath}", "success")
            return filepath
        elif driver == "oracle":
            dump_file = os.path.splitext(filepath)[0] + ".dmp"
            cmd = [
                "exp", f"{user}/{password}@{host}:{port}/{db_name}",
                f"file={dump_file}", f"log={dump_file}.log", f"owner={user}"
            ]
            filepath = dump_file
        else:
            raise ValueError(f"Banco de dados '{driver}' não é suportado para backup.")

        _run_command(cmd, env, "gerar backup")

        if compress and os.path.exists(filepath):
            filepath = _compress_file(filepath)
            log_message(f"📦 Backup comprimido → {filepath}", "info")

        return filepath

    except Exception as e:
        log_message(f"🔥 Erro durante o backup: {str(e)}", "error")
        raise


# ===============================================================
# 🔁 RESTAURAÇÃO UNIVERSAL (OTIMIZADA)
# ===============================================================
def restore_backup(db: Session,user_id:int, connection_id: int, filepath: str):
    """
    Restaura um backup (.sql, .db ou .dmp) compatível com PostgreSQL, MySQL, SQLite e Oracle.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Arquivo de backup não encontrado: {filepath}")

    if filepath.endswith(".gz"):
        log_message("📦 Descompactando backup...", "info")
        with gzip.open(filepath, "rb") as f_in:
            filepath = filepath[:-3]
            with open(filepath, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    engine,conn = ConnectionManager.get_engine_idconn_async(db=db,user_id=user_id,id_connection=connection_id)
    url = engine.url
    driver = url.get_backend_name().lower().replace("+psycopg2", "")

    db_name = url.database or "default"
    user = url.username or ""
    host = url.host or "localhost"
    port = str(url.port or "")
    password = url.password or ""

    env = os.environ.copy()
    if password:
        env.update({"PGPASSWORD": password, "MYSQL_PWD": password, "ORACLE_PWD": password})

    log_message(f"♻️ Iniciando restauração do backup '{filepath}' → '{db_name}' ({driver})", "info")

    try:
        if driver in ("postgresql", "postgres"):
            cmd = [
                "psql", "-h", host, "-p", port or "5432", "-U", user,
                "-d", db_name, "-f", filepath
            ]
        elif driver in ("mysql", "mariadb"):
            cmd = [
                "mysql", "-h", host, "-P", port or "3306", "-u", user,
                db_name, "-e", f"source {filepath}"
            ]
        elif driver == "sqlite":
            db_path = url.database
            shutil.copy2(filepath, db_path)
            log_message(f"✅ Banco SQLite restaurado com sucesso → {db_path}", "success")
            return
        elif driver == "oracle":
            dump_file = os.path.splitext(filepath)[0] + ".dmp"
            cmd = [
                "imp", f"{user}/{password}@{host}:{port}/{db_name}",
                f"file={dump_file}", f"log={dump_file}.log",
                f"fromuser={user}", f"touser={user}"
            ]
        else:
            raise ValueError(f"Banco '{driver}' não suportado para restauração.")

        _run_command(cmd, env, "restaurar backup")
        log_message("✅ Restauração concluída com sucesso.", "success")

    except Exception as e:
        log_message(f"🔥 Erro durante restauração: {str(e)}", "error")
        raise
