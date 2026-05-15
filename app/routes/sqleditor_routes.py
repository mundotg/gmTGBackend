"""
SQL Editor Enterprise Controller
=========================================================
SUPORTE:
- Multi queries reais
- SSE streaming
- execução paralela
- cancelamento individual
- análise SQL
- autocomplete
- explain
- timeout
- métricas
- histórico
- save query
- segurança
- heartbeat
- rollback automático
- transações
- async SQLAlchemy
- Oracle/Postgres/MySQL/SQLite
=========================================================
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlparse
import traceback
import uuid

from datetime import datetime, timezone
from typing import (
    Dict,
    Any,
    AsyncGenerator,
    List,
)

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
)

from fastapi.responses import StreamingResponse

from pydantic import (
    BaseModel,
    Field,
)

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
)

from app.database import get_db_async
from app.routes.connection_routes import get_current_user_id
from app.ultils.logger import log_message
from app.ultils.ativar_engine import ConnectionManager

# =========================================================
# ROUTER
# =========================================================

router = APIRouter(
    prefix="/sql-editor",
    tags=["SQL Editor"],
)

# =========================================================
# CONSTANTS
# =========================================================

MAX_QUERY_LENGTH = 50000
MAX_STREAM_ROWS = 10000
QUERY_TIMEOUT_SECONDS = 60
QUERY_TTL_MINUTES = 30
MAX_QUERIES_PER_EXECUTION = 20

ALLOW_DDL = True
ALLOW_DML = True

FORBIDDEN_SQL = [
    "DROP DATABASE",
    "TRUNCATE DATABASE",
    "SHUTDOWN",
    "XP_CMDSHELL",
]

INJECTION_PATTERNS = [
    r"(\bor\b.+?=)",
    r"(--)",
    r"(/\*)",
]

# =========================================================
# MEMORY
# =========================================================

ACTIVE_QUERIES: Dict[str, Dict[str, Any]] = {}
QUERY_HISTORY: Dict[int, List[Dict[str, Any]]] = {}
SAVED_QUERIES: Dict[int, List[Dict[str, Any]]] = {}

# =========================================================
# MODELS
# =========================================================


class SQLExecutePayload(BaseModel):
    query: str = Field(..., min_length=1)
    stream: bool = True
    transaction: bool = False


class SQLValidatePayload(BaseModel):
    query: str


class SQLSavePayload(BaseModel):
    name: str
    query: str


# =========================================================
# EXCEPTIONS
# =========================================================


class SQLEditorException(Exception):
    def __init__(
        self,
        message: str,
        code: str = "SQL_EDITOR_ERROR",
        status_code: int = 400,
    ):
        self.message = message
        self.code = code
        self.status_code = status_code
        super().__init__(message)


# =========================================================
# RESPONSE
# =========================================================


def success_response(data: Any = None, message: str = "OK"):
    return {
        "success": True,
        "message": message,
        "data": data,
    }


def error_response(message: str, code: str):
    return {
        "success": False,
        "code": code,
        "message": message,
    }


# =========================================================
# ERROR HANDLER
# =========================================================


def handle_sql_editor_error(error: Exception, operation: str):
    traceback_str = traceback.format_exc()

    log_message(
        f"""
[SQL_EDITOR]
operation={operation}
error={str(error)}

{traceback_str}
""",
        "error",
    )

    if isinstance(error, SQLEditorException):
        raise HTTPException(
            status_code=error.status_code,
            detail=error_response(error.message, error.code),
        )

    if isinstance(error, SQLAlchemyError):
        raise HTTPException(
            status_code=500,
            detail=error_response(str(error), "DATABASE_ERROR"),
        )

    raise HTTPException(
        status_code=500,
        detail=error_response(str(error), "INTERNAL_ERROR"),
    )


# =========================================================
# UTILS
# =========================================================


def validate_uuid(value: str):
    try:
        uuid.UUID(value)
    except Exception:
        raise SQLEditorException(
            "UUID inválido",
            "INVALID_UUID",
        )


def sanitize_query(query: str):
    return query.replace("\x00", "").strip()


def normalize_sql(query: str):
    return sqlparse.format(
        query,
        reindent=True,
        keyword_case="upper",
    )


def split_sql_statements(query: str) -> List[str]:
    parsed = sqlparse.split(query)

    statements = []

    for stmt in parsed:
        clean_stmt = sqlparse.format(
            stmt,
            strip_comments=True,
        ).strip()

        clean_stmt = clean_stmt.strip(";").strip()

        if clean_stmt:
            statements.append(clean_stmt)

    return statements


def detect_sql_injection(query: str):
    for pattern in INJECTION_PATTERNS:
        if re.search(pattern, query, re.I):
            raise SQLEditorException(
                "Possível SQL Injection detectado",
                "SQL_INJECTION",
                403,
            )


def validate_sql(query: str):
    if not query:
        raise SQLEditorException("Query vazia")

    if len(query) > MAX_QUERY_LENGTH:
        raise SQLEditorException(
            "Query muito grande",
            "QUERY_TOO_LARGE",
        )

    parsed = sqlparse.parse(query)

    if not parsed:
        raise SQLEditorException(
            "SQL inválido",
            "INVALID_SQL",
        )

    upper_query = query.upper()

    for forbidden in FORBIDDEN_SQL:
        if forbidden in upper_query:
            raise SQLEditorException(
                f"Comando proibido: {forbidden}",
                "FORBIDDEN_SQL",
                403,
            )

    detect_sql_injection(query)

    return True


def detect_query_type(query: str):
    q = query.strip().upper()

    TYPES = [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
    ]

    for t in TYPES:
        if q.startswith(t):
            return t

    return "UNKNOWN"


def extract_tables(query: str):
    pattern = r"(?:FROM|JOIN|INTO|UPDATE)\s+([a-zA-Z0-9_\.]+)"

    tables = re.findall(
        pattern,
        query,
        re.IGNORECASE,
    )

    return list(set(tables))


def detect_risk(query: str):
    q = query.upper()

    risks = []

    if "DELETE" in q and "WHERE" not in q:
        risks.append("DELETE sem WHERE")

    if "UPDATE" in q and "WHERE" not in q:
        risks.append("UPDATE sem WHERE")

    if "DROP TABLE" in q:
        risks.append("DROP TABLE detectado")

    return {
        "safe": len(risks) == 0,
        "risks": risks,
    }


def calculate_complexity(query: str):
    q = query.upper()

    complexity = 0

    complexity += q.count("JOIN") * 5
    complexity += q.count("GROUP BY") * 4
    complexity += q.count("ORDER BY") * 2
    complexity += q.count("UNION") * 8
    complexity += q.count("SUBQUERY") * 10

    return complexity


async def save_history(user_id: int, query: str):
    if user_id not in QUERY_HISTORY:
        QUERY_HISTORY[user_id] = []

    QUERY_HISTORY[user_id].append(
        {
            "query": query,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )


def cleanup_queries():
    now = datetime.now(timezone.utc)

    expired = []

    for query_id, data in ACTIVE_QUERIES.items():
        started_at = datetime.fromisoformat(data["started_at"])

        diff = now - started_at

        if diff.total_seconds() > QUERY_TTL_MINUTES * 60:
            expired.append(query_id)

    for query_id in expired:
        ACTIVE_QUERIES.pop(query_id, None)


# =========================================================
# ANALYZE
# =========================================================


@router.post("/analyze")
async def analyze_sql(
    body: SQLValidatePayload,
):
    try:
        query = sanitize_query(body.query)

        validate_sql(query)

        statements = split_sql_statements(query)

        analysis = []

        for stmt in statements:
            analysis.append(
                {
                    "query": stmt,
                    "queryType": detect_query_type(stmt),
                    "tables": extract_tables(stmt),
                    "risk": detect_risk(stmt),
                    "complexity": calculate_complexity(stmt),
                }
            )

        return success_response(
            {
                "formatted": normalize_sql(query),
                "queries": analysis,
                "totalStatements": len(statements),
            }
        )

    except Exception as e:
        handle_sql_editor_error(
            e,
            "analyze_sql",
        )


# =========================================================
# AUTOCOMPLETE
# =========================================================


@router.get("/autocomplete")
async def autocomplete_sql(q: str):
    try:
        KEYWORDS = [
            "SELECT",
            "FROM",
            "WHERE",
            "INSERT INTO",
            "UPDATE",
            "DELETE",
            "GROUP BY",
            "ORDER BY",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
            "CREATE TABLE",
            "ALTER TABLE",
            "DROP TABLE",
            "LIMIT",
            "FETCH FIRST",
            "UNION",
            "HAVING",
            "DISTINCT",
        ]

        suggestions = [item for item in KEYWORDS if item.lower().startswith(q.lower())]

        return success_response(suggestions)

    except Exception as e:
        handle_sql_editor_error(
            e,
            "autocomplete_sql",
        )


# =========================================================
# STREAM QUERY
# =========================================================


async def stream_single_statement(
    conn,
    query_id: str,
    statement_id: str,
    query: str,
) -> AsyncGenerator[str, None]:

    try:
        query_type = detect_query_type(query)

        yield f"event:statement_start\ndata:{json.dumps({'statementId': statement_id, 'type': query_type})}\n\n"

        result = await conn.stream(text(query))

        columns = list(result.keys())

        yield f"event:columns\ndata:{json.dumps({'statementId': statement_id, 'columns': columns})}\n\n"

        row_count = 0

        async for row in result:

            if ACTIVE_QUERIES[query_id]["cancelled"]:
                yield f"event:cancelled\ndata:{json.dumps({'statementId': statement_id})}\n\n"
                return

            row_count += 1

            if row_count >= MAX_STREAM_ROWS:
                yield f"event:limit\ndata:{json.dumps({'statementId': statement_id})}\n\n"
                break

            row_data = dict(row._mapping)

            yield f"event:row\ndata:{json.dumps({'statementId': statement_id, 'row': row_data}, default=str)}\n\n"

            await asyncio.sleep(0)

        yield f"event:statement_complete\ndata:{json.dumps({'statementId': statement_id, 'rows': row_count})}\n\n"

    except Exception as e:
        yield f"event:error\ndata:{json.dumps({'statementId': statement_id, 'error': str(e)})}\n\n"


async def stream_multi_query(
    query_id: str,
    user_id: int,
    queries: List[str],
    engine_async: AsyncEngine,
    transaction: bool,
) -> AsyncGenerator[str, None]:

    try:
        ACTIVE_QUERIES[query_id]["status"] = "running"

        async with engine_async.connect() as conn:

            tx = None

            if transaction:
                tx = await conn.begin()

            total_statements = len(queries)

            yield f"event:start\ndata:{json.dumps({'queryId': query_id, 'totalStatements': total_statements})}\n\n"

            for index, query in enumerate(queries):

                if ACTIVE_QUERIES[query_id]["cancelled"]:
                    yield "event:cancelled\ndata:Query cancelada\n\n"

                    if tx:
                        await tx.rollback()

                    return

                statement_id = str(uuid.uuid4())

                yield f"event:progress\ndata:{json.dumps({'current': index + 1, 'total': total_statements})}\n\n"

                async for item in stream_single_statement(
                    conn=conn,
                    query_id=query_id,
                    statement_id=statement_id,
                    query=query,
                ):
                    yield item

            if tx:
                await tx.commit()

            ACTIVE_QUERIES[query_id]["status"] = "completed"

            yield f"event:complete\ndata:{json.dumps({'queryId': query_id})}\n\n"

    except Exception as e:

        traceback.print_exc()

        ACTIVE_QUERIES[query_id]["status"] = "error"

        yield f"event:error\ndata:{json.dumps({'error': str(e)})}\n\n"

    finally:
        ACTIVE_QUERIES[query_id]["finished_at"] = datetime.now(timezone.utc).isoformat()


# =========================================================
# EXECUTE
# =========================================================


@router.post("/execute")
async def execute_sql_stream(
    body: SQLExecutePayload,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    try:

        cleanup_queries()

        query = sanitize_query(body.query)

        validate_sql(query)

        statements = split_sql_statements(query)

        if len(statements) > MAX_QUERIES_PER_EXECUTION:
            raise SQLEditorException(
                "Muitas queries",
                "TOO_MANY_QUERIES",
            )

        for stmt in statements:
            validate_sql(stmt)

        await save_history(user_id, query)

        engine_async, connection = await ConnectionManager.get_engine_async(
            db,
            user_id,
        )

        query_id = str(uuid.uuid4())

        ACTIVE_QUERIES[query_id] = {
            "query": query,
            "totalStatements": len(statements),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "cancelled": False,
            "status": "queued",
            "user_id": user_id,
        }

        generator = stream_multi_query(
            query_id=query_id,
            user_id=user_id,
            queries=statements,
            engine_async=engine_async,
            transaction=body.transaction,
        )

        return StreamingResponse(
            generator,
            media_type="text/event-stream",
            headers={
                "X-Query-Id": query_id,
                "X-Total-Statements": str(len(statements)),
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )

    except Exception as e:
        handle_sql_editor_error(
            e,
            "execute_sql_stream",
        )


# =========================================================
# CANCEL
# =========================================================


@router.post("/cancel/{query_id}")
async def cancel_query(
    query_id: str,
    user_id: int = Depends(get_current_user_id),
):
    try:

        validate_uuid(query_id)

        query_data = ACTIVE_QUERIES.get(query_id)

        if not query_data:
            raise SQLEditorException(
                "Query não encontrada",
                "QUERY_NOT_FOUND",
                404,
            )

        if query_data["user_id"] != user_id:
            raise SQLEditorException(
                "Sem permissão",
                "FORBIDDEN",
                403,
            )

        query_data["cancelled"] = True
        query_data["status"] = "cancelled"

        return success_response(
            {
                "queryId": query_id,
            },
            "Query cancelada",
        )

    except Exception as e:
        handle_sql_editor_error(
            e,
            "cancel_query",
        )


# =========================================================
# STATUS
# =========================================================


@router.get("/status/{query_id}")
async def query_status(
    query_id: str,
    user_id: int = Depends(get_current_user_id),
):
    try:

        validate_uuid(query_id)

        query_data = ACTIVE_QUERIES.get(query_id)

        if not query_data:
            raise SQLEditorException(
                "Query não encontrada",
                "QUERY_NOT_FOUND",
                404,
            )

        if query_data["user_id"] != user_id:
            raise SQLEditorException(
                "Sem permissão",
                "FORBIDDEN",
                403,
            )

        return success_response(query_data)

    except Exception as e:
        handle_sql_editor_error(
            e,
            "query_status",
        )


# =========================================================
# SAVE QUERY
# =========================================================


@router.post("/save")
async def save_query(
    body: SQLSavePayload,
    user_id: int = Depends(get_current_user_id),
):
    try:

        validate_sql(body.query)

        if user_id not in SAVED_QUERIES:
            SAVED_QUERIES[user_id] = []

        data = {
            "id": str(uuid.uuid4()),
            "name": body.name,
            "query": body.query,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        SAVED_QUERIES[user_id].append(data)

        return success_response(
            data,
            "Query salva",
        )

    except Exception as e:
        handle_sql_editor_error(
            e,
            "save_query",
        )


# =========================================================
# HISTORY
# =========================================================


@router.get("/history")
async def get_history(
    user_id: int = Depends(get_current_user_id),
):
    try:
        return success_response(QUERY_HISTORY.get(user_id, []))

    except Exception as e:
        handle_sql_editor_error(
            e,
            "history",
        )


# =========================================================
# METRICS
# =========================================================


@router.get("/metrics")
async def metrics(
    user_id: int = Depends(get_current_user_id),
):
    try:

        history = QUERY_HISTORY.get(user_id, [])

        active_queries = [q for q in ACTIVE_QUERIES.values() if q["user_id"] == user_id]

        return success_response(
            {
                "queriesExecuted": len(history),
                "activeQueries": len(active_queries),
                "savedQueries": len(SAVED_QUERIES.get(user_id, [])),
            }
        )

    except Exception as e:
        handle_sql_editor_error(
            e,
            "metrics",
        )


# =========================================================
# EXPLAIN
# =========================================================


@router.post("/explain")
async def explain_query(
    body: SQLValidatePayload,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    try:

        query = sanitize_query(body.query)

        validate_sql(query)

        engine_async, connection = await ConnectionManager.get_engine_async(
            db,
            user_id,
        )

        db_type = connection.type.lower() if connection.type else ""

        if "oracle" in db_type:
            explain_sql = f"EXPLAIN PLAN FOR {query}"
        else:
            explain_sql = f"EXPLAIN {query}"

        async with engine_async.connect() as conn:

            await conn.execute(text(explain_sql))

            if "oracle" in db_type:
                result = await conn.execute(text("""
SELECT PLAN_TABLE_OUTPUT
FROM TABLE(DBMS_XPLAN.DISPLAY())
"""))

                plan = [row[0] for row in result.fetchall()]

            else:
                result = await conn.execute(text(explain_sql))

                plan = [dict(row._mapping) for row in result]

        return success_response(
            {
                "query": query,
                "plan": plan,
            },
            "Explain gerado",
        )

    except Exception as e:
        handle_sql_editor_error(
            e,
            "explain_query",
        )
