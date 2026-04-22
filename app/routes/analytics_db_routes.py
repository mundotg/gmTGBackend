"""
Controller de Analytics da Base de Dados
Versão final: multi-db, cache por conexão, sem leak e robusto.
"""

import traceback
from typing import Callable, Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.config.cache_manager import cache_result
from app.database import get_db
from app.models.connection_models import DBConnection
from app.routes.connection_routes import get_current_user_id
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message

router = APIRouter(prefix="/analytics/db", tags=["DatabaseAnalytics"])


# =========================
# HELPERS
# =========================
def format_size(bytes_size: int) -> str:
    try:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_size < 1024:
                return f"{bytes_size:.2f} {unit}"
            bytes_size /= 1024
        return f"{bytes_size:.2f} PB"
    except Exception:
        return "0 B"


def safe_scalar(conn, query, default=0, label="query"):
    try:
        result = conn.execute(query).scalar()
        return result if result is not None else default
    except SQLAlchemyError as e:
        log_message(f"[DB_METRIC_ERROR][{label}] {str(e)}", "error")
        return default
    except Exception as e:
        log_message(f"[DB_METRIC_UNKNOWN][{label}] {str(e)}", "error")
        return default


# =========================
# QUERY DEFINITIONS
# =========================
DB_QUERIES: Dict[str, Dict[str, Callable]] = {
    "postgresql": {
        "size": lambda conn: safe_scalar(
            conn, text("SELECT pg_database_size(current_database())"), 0
        ),
        "rows": lambda conn: safe_scalar(
            conn, text("SELECT COALESCE(SUM(n_live_tup),0) FROM pg_stat_user_tables"), 0
        ),
        "tx": lambda conn: safe_scalar(
            conn, text("SELECT COUNT(*) FROM pg_stat_activity WHERE state='active'"), 0
        ),
        "deadlocks": lambda conn: safe_scalar(
            conn, text("SELECT COALESCE(SUM(deadlocks),0) FROM pg_stat_database"), 0
        ),
    },
    "mysql": {
        "size": lambda conn: safe_scalar(
            conn,
            text(
                """
            SELECT SUM(data_length + index_length)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
        """
            ),
            0,
        ),
        "rows": lambda conn: safe_scalar(
            conn,
            text(
                """
            SELECT SUM(table_rows)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
        """
            ),
            0,
        ),
        "tx": lambda conn: safe_scalar(
            conn, text("SELECT COUNT(*) FROM information_schema.processlist"), 0
        ),
        "deadlocks": lambda conn: 0,
    },
    "sqlserver": {
        "size": lambda conn: safe_scalar(
            conn,
            text(
                """
            SELECT SUM(CAST(size AS BIGINT)) * 8 * 1024
            FROM sys.master_files
            WHERE database_id = DB_ID()
        """
            ),
            0,
        ),
        "rows": lambda conn: safe_scalar(
            conn,
            text(
                """
            SELECT SUM(row_count)
            FROM sys.dm_db_partition_stats
            WHERE index_id IN (0,1)
        """
            ),
            0,
        ),
        "tx": lambda conn: safe_scalar(
            conn, text("SELECT COUNT(*) FROM sys.dm_exec_requests"), 0
        ),
        "deadlocks": lambda conn: safe_scalar(
            conn,
            text(
                """
            SELECT cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name = 'Number of Deadlocks/sec'
        """
            ),
            0,
        ),
    },
    "sqlite": {
        "size": lambda conn: (
            safe_scalar(conn, text("PRAGMA page_count"), 0)
            * safe_scalar(conn, text("PRAGMA page_size"), 0)
        ),
        "rows": lambda conn: 0,
        "tx": lambda conn: 0,
        "deadlocks": lambda conn: 0,
    },
    "oracle": {
        "size": lambda conn: safe_scalar(
            conn, text("SELECT SUM(bytes) FROM dba_segments"), 0
        ),
        "rows": lambda conn: 0,
        "tx": lambda conn: 0,
        "deadlocks": lambda conn: 0,
    },
}


# =========================
# CORE
# =========================
def fetch_db_metrics(engine: Engine, conn_info: DBConnection):
    db_type = (conn_info.type or "").lower()

    if db_type in ["postgres"]:
        db_type = "postgresql"
    if db_type in ["mssql"]:
        db_type = "sqlserver"

    if db_type in ["mongodb", "mongo"]:
        return {
            "tableSizeTotal": "N/A",
            "rowCountTotal": 0,
            "activeTransactions": 0,
            "deadlocks": 0,
            "engine": "mongo",
        }

    if db_type not in DB_QUERIES:
        raise Exception(f"DB não suportada: {db_type}")

    queries = DB_QUERIES[db_type]

    with engine.connect() as conn:
        size = queries["size"](conn)
        rows = queries["rows"](conn)
        tx = queries["tx"](conn)
        deadlocks = queries["deadlocks"](conn)

        size = int(size) if size and size < 10**18 else 0

        return {
            "tableSizeTotal": format_size(size),
            "rowCountTotal": int(rows),
            "activeTransactions": int(tx),
            "deadlocks": int(deadlocks),
            "engine": db_type,
        }


# =========================
# CACHEABLE WRAPPER
# =========================
@cache_result(ttl=120, user_id="user_db_metrics_{user_id_}_conn_{conn_id}")
async def get_metrics_cached(
    *, user_id_: int, conn_id: int, engine: Engine, conn_info: DBConnection
):
    return fetch_db_metrics(engine, conn_info)


# =========================
# ENDPOINT
# =========================
@router.get("/")
async def get_database_metrics(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        engine, conn_info = ConnectionManager.ensure_connection(db, user_id)

        if not engine or not conn_info:
            raise HTTPException(400, "Nenhuma conexão ativa encontrada")

        # 🔥 CACHE AUTOMÁTICO (sem .set)
        return await get_metrics_cached(
            user_id_=user_id, conn_id=conn_info.id, engine=engine, conn_info=conn_info
        )

    except HTTPException:
        raise

    except Exception as e:
        log_message(
            f"[DB_ANALYTICS] user={user_id} error={str(e)}\n{traceback.format_exc()}",
            "error",
        )

        raise HTTPException(
            status_code=500,
            detail="Erro ao carregar métricas da base de dados",
        )
