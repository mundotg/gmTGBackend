# =========================
# SQL SERVER QUERIES
# =========================
POSTGRESQUERY = """
    SELECT
      bl.pid AS blocked_pid,
      a.usename AS blocked_user,
      a.application_name AS blocked_application,
      kl.pid AS blocking_pid,
      ka.usename AS blocking_user,
      ka.application_name AS blocking_application,
      a.query AS blocked_query,
      ka.query AS blocking_query,
      a.state AS blocked_state,
      ka.state AS blocking_state,
      a.backend_start AS connection_start,
      a.query_start AS query_start,
      NOW() - a.query_start AS query_duration
    FROM pg_catalog.pg_locks bl
    JOIN pg_catalog.pg_stat_activity a ON a.pid = bl.pid
    JOIN pg_catalog.pg_locks kl
      ON kl.locktype = bl.locktype
      AND kl.database IS NOT DISTINCT FROM bl.database
      AND kl.relation IS NOT DISTINCT FROM bl.relation
      AND kl.page IS NOT DISTINCT FROM bl.page
      AND kl.tuple IS NOT DISTINCT FROM bl.tuple
      AND kl.virtualxid IS NOT DISTINCT FROM bl.virtualxid
      AND kl.transactionid IS NOT DISTINCT FROM bl.transactionid
      AND kl.classid IS NOT DISTINCT FROM bl.classid
      AND kl.objid IS NOT DISTINCT FROM bl.objid
      AND kl.objsubid IS NOT DISTINCT FROM bl.objsubid
      AND kl.pid != bl.pid
    JOIN pg_catalog.pg_stat_activity ka ON ka.pid = kl.pid
    WHERE NOT bl.granted
      AND kl.granted;
    """

SQLSERVERQUERY = """
    SELECT
        r.blocking_session_id AS blocking_pid,
        s.session_id AS blocked_pid,
        s.login_name AS blocked_user,
        s.program_name AS blocked_application,
        s.host_name AS blocked_host,
        r.wait_type,
        r.wait_time,
        r.total_elapsed_time AS query_duration,
        t.text AS blocked_query,
        (
        SELECT text
        FROM sys.dm_exec_sql_text(
            (SELECT sql_handle FROM sys.dm_exec_requests WHERE session_id = r.blocking_session_id)
        )
        ) AS blocking_query
    FROM sys.dm_exec_requests r
    INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
    WHERE r.blocking_session_id <> 0;
    """

MYSQLQUERY = """
    SELECT
        b.trx_mysql_thread_id AS blocked_pid,
        b.trx_query AS blocked_query,
        b.trx_started AS query_start,
        TIMESTAMPDIFF(SECOND, b.trx_started, NOW()) AS query_duration,
        w.trx_mysql_thread_id AS blocking_pid,
        w.trx_query AS blocking_query,
        p.HOST AS blocked_host,
        p.USER AS blocked_user,
        p.DB AS blocked_database
    FROM information_schema.innodb_lock_waits lw
    JOIN information_schema.innodb_trx b ON b.trx_id = lw.requesting_trx_id
    JOIN information_schema.innodb_trx w ON w.trx_id = lw.blocking_trx_id
    LEFT JOIN information_schema.PROCESSLIST p ON p.ID = b.trx_mysql_thread_id;
    """

ORACLEQUERY = """
    SELECT
        w.session_id AS blocked_pid,
        w.serial_num AS blocked_serial,
        w.oracle_username AS blocked_user,
        w.os_user_name AS blocked_os_user,
        w.machine_name AS blocked_machine,
        h.session_id AS blocking_pid,
        h.serial_num AS blocking_serial,
        h.oracle_username AS blocking_user,
        h.os_user_name AS blocking_os_user,
        w.lockwait,
        w.seconds_in_wait
    FROM v$session w
    JOIN v$session h
        ON w.row_wait_obj# = h.row_wait_obj#
        AND w.row_wait_file# = h.row_wait_file#
        AND w.row_wait_block# = h.row_wait_block#
        AND w.row_wait_row# = h.row_wait_row#
    WHERE w.lockwait IS NOT NULL;
    """
SQLSERVER_BLOCKING = """
    SELECT
    r.blocking_session_id AS blocking_pid,
    r.session_id AS blocked_pid,
    s.login_name AS blocked_user,
    s.program_name AS blocked_application,
    s.host_name AS blocked_host,
    r.wait_type,
    r.wait_time,
    r.total_elapsed_time AS query_duration,
    blocked.text AS blocked_query,
    blocking.text AS blocking_query
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) blocked
    OUTER APPLY sys.dm_exec_sql_text(
    (SELECT sql_handle FROM sys.dm_exec_requests WHERE session_id = r.blocking_session_id)
    ) blocking
    JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id
    WHERE r.blocking_session_id <> 0;
    """

SQLSERVER_DEADLOCK_HISTORY = """
    SELECT
        XEventData.XMLData.value('(event/@timestamp)[1]', 'datetime2') AS event_time,
        XEventData.XMLData.value('(event/data/value)[1]', 'varchar(max)') AS deadlock_graph
    FROM (
        SELECT CAST(record AS xml) AS XMLData
        FROM sys.dm_os_ring_buffers
        WHERE ring_buffer_type = 'RING_BUFFER_DEADLOCK'
    ) XEventData
    ORDER BY event_time DESC;
    """

SQLSERVER_HEAVY_SESSIONS = """
    SELECT
    r.session_id,
    r.status,
    r.cpu_time,
    r.logical_reads,
    r.wait_type,
    r.wait_time,
    DB_NAME(r.database_id) AS database_name,
    t.text AS sql_text
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
    ORDER BY r.wait_time DESC;
    """


# =========================
# POSTGRESQL QUERIES
# =========================
POSTGRES_BLOCKING = """
    SELECT
    blocked.pid AS blocked_pid,
    blocked.usename AS blocked_user,
    blocked.query AS blocked_query,
    NOW() - blocked.query_start AS blocked_duration,
    blocker.pid AS blocking_pid,
    blocker.usename AS blocking_user,
    blocker.query AS blocking_query,
    blocker.state AS blocking_state
    FROM pg_catalog.pg_locks blocked_lock
    JOIN pg_catalog.pg_stat_activity blocked ON blocked_lock.pid = blocked.pid
    JOIN pg_catalog.pg_locks blocking_lock
    ON blocked_lock.locktype = blocking_lock.locktype
    AND blocked_lock.database IS NOT DISTINCT FROM blocking_lock.database
    AND blocked_lock.relation IS NOT DISTINCT FROM blocking_lock.relation
    AND blocked_lock.pid <> blocking_lock.pid
    JOIN pg_catalog.pg_stat_activity blocker ON blocker.pid = blocking_lock.pid
    WHERE NOT blocked_lock.granted
    AND blocking_lock.granted;
    """

POSTGRES_DEADLOCKS_EVENTS = """
    SELECT *
    FROM pg_stat_activity
    WHERE wait_event_type = 'Lock'
    AND wait_event = 'deadlock';
    """

POSTGRES_ACTIVE_QUERIES = """
    SELECT
    pid,
    usename,
    query_start,
    NOW() - query_start AS duration,
    state,
    query
    FROM pg_stat_activity
    WHERE state <> 'idle'
    ORDER BY duration DESC;
    """


# =========================
# MYSQL / MARIADB QUERIES
# =========================
MYSQL_BLOCKING = """
    SELECT
    b.trx_mysql_thread_id AS blocked_pid,
    b.trx_query AS blocked_query,
    TIMESTAMPDIFF(SECOND, b.trx_started, NOW()) AS blocked_duration,
    w.trx_mysql_thread_id AS blocking_pid,
    w.trx_query AS blocking_query,
    p.USER AS blocked_user,
    p.HOST AS blocked_host
    FROM information_schema.innodb_lock_waits lw
    JOIN information_schema.innodb_trx b ON b.trx_id = lw.requesting_trx_id
    JOIN information_schema.innodb_trx w ON w.trx_id = lw.blocking_trx_id
    LEFT JOIN information_schema.PROCESSLIST p ON p.ID = b.trx_mysql_thread_id;
    """

MYSQL_LONG_RUNNING_QUERIES = """
    SELECT
        id AS thread_id,
        user,
        host,
        db,
        command,
        time AS duration_seconds,
        state,
        info AS query_text
    FROM information_schema.PROCESSLIST
    WHERE command <> 'Sleep'
    ORDER BY time DESC;
    """


# =========================
# ORACLE QUERIES
# =========================
ORACLE_BLOCKING = """
    SELECT
    w.sid AS blocked_sid,
    w.serial# AS blocked_serial,
    w.username AS blocked_user,
    h.sid AS blocking_sid,
    h.serial# AS blocking_serial,
    w.SECONDS_IN_WAIT,
    w.event
    FROM v$session w
    JOIN v$session h ON w.blocking_session = h.sid
    WHERE w.blocking_session IS NOT NULL;
    """


# =========================
# COMBINED MAP FOR EASY ACCESS
# =========================
DATABASE_MONITORING_QUERIES = {
    "sqlserver": {
        "blocking": SQLSERVER_BLOCKING,
        "deadlocks": SQLSERVER_DEADLOCK_HISTORY,
        "sessions": SQLSERVER_HEAVY_SESSIONS,
    },
    "postgres": {
        "blocking": POSTGRES_BLOCKING,
        "deadlocks": POSTGRES_DEADLOCKS_EVENTS,
        "sessions": POSTGRES_ACTIVE_QUERIES,
    },
    "mysql": {
        "blocking": MYSQL_BLOCKING,
        "sessions": MYSQL_LONG_RUNNING_QUERIES,
    },
    "oracle": {
        "blocking": ORACLE_BLOCKING,
    }
}

import asyncio
import traceback
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from app.ultils.logger import log_message
from typing import List, Dict
from datetime import datetime


class DeadlockManager:
    SUPPORTED_POSTGRES = ("postgresql", "postgresql+psycopg2")
    SUPPORTED_MSSQL = ("mssql", "mssql+pyodbc")
    SUPPORTED_MYSQL = ("mysql", "mysql+pymysql", "mariadb")
    SUPPORTED_ORACLE = ("oracle", "oracle+cx_oracle")

    

    # ---------------------------------------------------------

    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self.driver = self.engine.dialect.name.lower()
        self._deadlock_history = []
        self.POSTGRESQUERY = POSTGRESQUERY
        self.MYSQLQUERY = MYSQLQUERY
        self.SQLSERVERQUERY = SQLSERVERQUERY
        self.ORACLEQUERY = ORACLEQUERY
        
    def _query_deadlocks(self):
        if self.driver in self.SUPPORTED_POSTGRES:
            return text(self.POSTGRESQUERY)
        if self.driver in self.SUPPORTED_MSSQL:
            return text(self.SQLSERVERQUERY)
        if self.driver in self.SUPPORTED_MYSQL:
            return text(self.MYSQLQUERY)
        if self.driver in self.SUPPORTED_ORACLE:
            return text(self.ORACLEQUERY)
        if self.driver == "sqlite":
            return None

        raise ValueError(f"Banco não suportado: {self.driver}")

    async def listar_processos_em_deadlock(self) -> List[Dict]:
        try:
            query = self._query_deadlocks()
            if query is None:
                return [{"info": "SQLite não possui deadlocks."}]

            async with self.engine.connect() as conn:
                result = await conn.execute(query)
                rows = result.mappings().all()

            processos = [dict(row) for row in rows]

            if processos:
                self._registrar_historico_deadlock(processos)

            return processos

        except Exception as e:
            erro_trace = traceback.format_exc()
            log_message(f"❌ Erro ao listar deadlocks: {e}\n{erro_trace}", level="error")
            return [{"erro": str(e), "driver": self.driver}]

    def _registrar_historico_deadlock(self, processos):
        self._deadlock_history.append({
            "timestamp": datetime.now().isoformat(),
            "processos": processos,
            "quantidade": len(processos)
        })
        self._deadlock_history = self._deadlock_history[-100:]

    def obter_historico_deadlocks(self):
        return self._deadlock_history

    def _get_kill_query(self, pid: int):
        if self.driver in (*self.SUPPORTED_POSTGRES, *self.SUPPORTED_MYSQL, *self.SUPPORTED_MSSQL):
            return text("KILL :pid"), {"pid": pid}

        if self.driver == "sqlite":
            raise ValueError("SQLite não possui processos para finalizar.")

        raise ValueError(f"KILL não suportado no driver {self.driver}")

    async def matar_processo(self, pid: int) -> Dict:
        try:
            query, params = self._get_kill_query(pid)

            async with self.engine.begin() as conn:
                await conn.execute(query, params)

            return {"status": "ok", "mensagem": f"PID {pid} finalizado"}

        except Exception as e:
            erro_trace = traceback.format_exc()
            log_message(f"❌ Erro ao matar PID {pid}: {e}\n{erro_trace}", level="error")
            return {"status": "erro", "pid": pid, "mensagem": str(e)}

    async def matar_todos_processos_bloqueadores(self):
        processos = await self.listar_processos_em_deadlock()
        resultados = []

        for p in processos:
            pid = p.get("blocking_pid")
            if pid:
                res = await self.matar_processo(pid)
                resultados.append({"pid": pid, "resultado": res})
                await asyncio.sleep(0.1)

        return {
            "status": "ok",
            "quantidade": len(resultados),
            "resultados": resultados
        }

    async def obter_estatisticas_gerais(self):
        processos = await self.listar_processos_em_deadlock()

        bloqueadores = {p.get("blocking_user") for p in processos if p.get("blocking_user")}
        bloqueados = {p.get("blocked_user") for p in processos if p.get("blocked_user")}

        return {
            "total": len(processos),
            "bloqueadores_unicos": list(bloqueadores),
            "bloqueados_unicos": list(bloqueados),
            "driver": self.driver,
            "timestamp": datetime.now().isoformat()
        }
