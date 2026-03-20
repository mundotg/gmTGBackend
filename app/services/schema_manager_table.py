from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Optional, Tuple, Dict, Any, List, Union

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.models.connection_models import DBConnection
from app.schemas.dbstructure_schema import TableDDLRequest
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryType
from app.cruds.queryhistory_crud import create_query_history
from app.services.editar_linha import quote_identifier
from app.ultils.logger import log_message

from app.cruds.dbstructure_crud import (
    create_db_structure,
    update_db_structure_by_name,
    soft_delete_db_structure_by_name,
)

from app.ultils.Database_error_logger import DDLExecutionError, _lidar_com_erro_sql


# ============================================================
# ✅ CORE HELPERS (poucos métodos)
# ============================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _shorten_sql(sql: Optional[str], limit: int = 1200) -> str:
    s = " ".join((sql or "").split())
    return s if len(s) <= limit else s[:limit] + " ...[truncated]"


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"error": "json_encode_failed"}, ensure_ascii=False)


def _audit_tag(operation: str, status: str) -> str:
    op = re.sub(r"\s+", "_", (operation or "").strip().lower())
    st = re.sub(r"\s+", "_", (status or "").strip().lower())
    return f"ddl_{op}_{st}"


# ============================================================
# 🛡️ HELPERS
# ============================================================

def _validate_dialect(db_type: str) -> str:
    db_type = (db_type or "").lower().strip()
    allowed = {"postgresql", "postgres", "mysql", "mariadb", "oracle", "mssql", "sqlserver", "sqlite"}
    if db_type not in allowed:
        raise ValueError(f"Dialeto '{db_type}' não suportado para manipulação estrutural.")
    return db_type


def _split_schema_table(full: str) -> Tuple[Optional[str], str]:
    full = (full or "").strip()
    if "." in full:
        schema, table = full.split(".", 1)
        return schema.strip() or None, table.strip()
    return None, full


def _q_table(db_type: str, schema: Optional[str], table: str) -> str:
    return (
        f"{quote_identifier(db_type, schema)}.{quote_identifier(db_type, table)}"
        if schema
        else quote_identifier(db_type, table)
    )


def _normalize_schema(schema: Optional[str]) -> Optional[str]:
    s = (schema or "").strip()
    return s or None


def _normalize_description(payload: Any) -> Optional[str]:
    d = getattr(payload, "description", None)
    if d is None:
        d = getattr(payload, "comment", None)
    d = (d or "").strip()
    return d or None


# ============================================================
# 🧾 AUDITORIA + EXECUÇÃO (poucos métodos)
# ============================================================

@dataclass(frozen=True)
class AuditContext:
    user_id: Optional[int] = None
    client_ip: Optional[str] = None
    app_source: str = "API"
    request_id: Optional[str] = None
    executed_by: Optional[str] = None
    modified_by: str = "sistema"


def _audit_write(
    db: Session,
    *,
    ctx: AuditContext,
    connection_id: int,
    operation: str,
    query_type: QueryType,
    dialect: str,
    table: str,
    column: Optional[str],
    sql: Optional[str],
    status: str,  # attempt | success | error
    started_at: datetime,
    duration_ms: Optional[int] = None,
    error_message: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    meta_info: Dict[str, Any] = {
        "kind": "DDL_AUDIT",
        "status": status,
        "operation": operation,
        "dialect": dialect,
        "table": table,
        "column": column,
        "request_id": ctx.request_id,
        "app_source": ctx.app_source,
        "client_ip": ctx.client_ip,
    }
    if extra:
        meta_info.update(extra)

    historico = QueryHistoryCreate(
        user_id=ctx.user_id,
        db_connection_id=connection_id,
        query=_shorten_sql(sql) if sql else "Create table",
        query_type=query_type,
        executed_at=started_at,
        duration_ms=duration_ms,
        result_preview=_safe_json({"status": status, "table": table, "dialect": dialect}),
        error_message=error_message,
        is_favorite=False,
        tags=_audit_tag(operation, status),
        app_source=ctx.app_source,
        client_ip=ctx.client_ip,
        executed_by=ctx.executed_by or (f"user_{ctx.user_id}" if ctx.user_id is not None else None),
        modified_by=ctx.modified_by,
        meta_info=meta_info,
    )

    try:
        create_query_history(db=db, user_id=ctx.user_id, data=historico)
    except Exception as e:
        log_message(f"⚠️ Falha ao gravar auditoria DDL: {e}", level="warning")


def _run_ddl(
    db: Session,
    *,
    ctx: AuditContext,
    engine: Engine,
    connection_id: int,
    operation: str,
    query_type: QueryType,
    dialect: str,
    table: str,
    column: Optional[str],
    sql_or_queries: Union[str, List[str]],
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    started_at = _utcnow()
    t0 = perf_counter()

    compiled_sql = (
        sql_or_queries if isinstance(sql_or_queries, str) else " ; ".join(sql_or_queries)
    )

    _audit_write(
        db,
        ctx=ctx,
        connection_id=connection_id,
        operation=operation,
        query_type=query_type,
        dialect=dialect,
        table=table,
        column=column,
        sql=compiled_sql,
        status="attempt",
        started_at=started_at,
        extra=extra,
    )

    try:
        with engine.begin() as conn:
            if isinstance(sql_or_queries, str):
                conn.execute(text(sql_or_queries))
            else:
                for q in sql_or_queries:
                    conn.execute(text(q))

        dur = int((perf_counter() - t0) * 1000)
        _audit_write(
            db,
            ctx=ctx,
            connection_id=connection_id,
            operation=operation,
            query_type=query_type,
            dialect=dialect,
            table=table,
            column=column,
            sql=compiled_sql,
            status="success",
            started_at=started_at,
            duration_ms=dur,
            extra=extra,
        )

    except Exception as exc:
        dur = int((perf_counter() - t0) * 1000)
        friendly_msg, details = _lidar_com_erro_sql(
            exc,
            operation=operation,
            dialect=dialect,
            table=table,
            column=column,
            sql=compiled_sql,
        )

        _audit_write(
            db,
            ctx=ctx,
            connection_id=connection_id,
            operation=operation,
            query_type=query_type,
            dialect=dialect,
            table=table,
            column=column,
            sql=compiled_sql,
            status="error",
            started_at=started_at,
            duration_ms=dur,
            error_message=friendly_msg,
            extra={**(extra or {}), "error": details},
        )

        raise DDLExecutionError(
            f"Falha ao executar {operation} em {dialect} ({table}{'.' + column if column else ''}). {friendly_msg}",
            operation=operation,
            dialect=dialect,
            table=table,
            column=column,
            details=details,
        ) from exc


# ============================================================
# 📝 DESCRIÇÃO helper (por dialeto) + auditoria
# ============================================================

def _apply_table_description(
    db: Session,
    *,
    ctx: AuditContext,
    engine: Engine,
    connection_id: int,
    db_type: str,
    schema: Optional[str],
    table: str,
    full_table_name: str,
    description: str,
) -> None:
    description = (description or "").strip()
    if not description:
        return

    desc_escaped = description.replace("'", "''")
    safe_table = _q_table(db_type, schema, table)

    if db_type in ["postgresql", "postgres", "oracle"]:
        sql = f"COMMENT ON TABLE {safe_table} IS '{desc_escaped}';"
        _run_ddl(
            db,
            ctx=ctx,
            engine=engine,
            connection_id=connection_id,
            operation="TABLE DESCRIPTION",
            query_type=QueryType.ALTERTABLE,
            dialect=db_type,
            table=full_table_name,
            column=None,
            sql_or_queries=sql,
            extra={"description": description},
        )
        return

    if db_type in ["mysql", "mariadb"]:
        sql = f"ALTER TABLE {safe_table} COMMENT='{desc_escaped}';"
        _run_ddl(
            db,
            ctx=ctx,
            engine=engine,
            connection_id=connection_id,
            operation="TABLE DESCRIPTION",
            query_type=QueryType.ALTERTABLE,
            dialect=db_type,
            table=full_table_name,
            column=None,
            sql_or_queries=sql,
            extra={"description": description},
        )
        return

    if db_type in ["mssql", "sqlserver"]:
        schema_name = schema or "dbo"
        objname = table.replace("'", "''")
        schemaname = schema_name.replace("'", "''")
        desc_sql = description.replace("'", "''")

        sql = (
            "DECLARE @v sql_variant;\n"
            f"SET @v = N'{desc_sql}';\n"
            "IF EXISTS(\n"
            "  SELECT 1 FROM sys.extended_properties ep\n"
            "  JOIN sys.tables t ON ep.major_id = t.object_id\n"
            "  JOIN sys.schemas s ON t.schema_id = s.schema_id\n"
            "  WHERE ep.name = 'MS_Description' AND s.name = "
            f"'{schemaname}' AND t.name = '{objname}'\n"
            ")\n"
            "BEGIN\n"
            "  EXEC sp_updateextendedproperty\n"
            "    @name=N'MS_Description', @value=@v,\n"
            f"    @level0type=N'SCHEMA', @level0name='{schemaname}',\n"
            f"    @level1type=N'TABLE',  @level1name='{objname}';\n"
            "END\n"
            "ELSE\n"
            "BEGIN\n"
            "  EXEC sp_addextendedproperty\n"
            "    @name=N'MS_Description', @value=@v,\n"
            f"    @level0type=N'SCHEMA', @level0name='{schemaname}',\n"
            f"    @level1type=N'TABLE',  @level1name='{objname}';\n"
            "END\n"
        )

        _run_ddl(
            db,
            ctx=ctx,
            engine=engine,
            connection_id=connection_id,
            operation="TABLE DESCRIPTION",
            query_type=QueryType.ALTERTABLE,
            dialect=db_type,
            table=full_table_name,
            column=None,
            sql_or_queries=sql,
            extra={"description": description},
        )
        return

    # Dialetos sem comentário nativo: audita "skipped"
    _audit_write(
        db,
        ctx=ctx,
        connection_id=connection_id,
        operation="TABLE DESCRIPTION",
        query_type=QueryType.ALTERTABLE,
        dialect=db_type,
        table=full_table_name,
        column=None,
        sql=None,
        status="success",
        started_at=_utcnow(),
        extra={"skipped": True, "reason": "dialect_no_native_comment", "description": description},
    )


# ============================================================
# ⚙️ DDL PRINCIPAIS (TABELA) + METADATA
# ============================================================

def execute_create_table(
    db: Session,
    engine: Engine,
    connection_model: DBConnection,
    full_table_name: str,
    payload: TableDDLRequest,
    *,
    audit_ctx: AuditContext,
) -> None:
    db_type = _validate_dialect(connection_model.type)  # type: ignore
    schema, table = _split_schema_table(full_table_name)
    safe_table = _q_table(db_type, schema, table)

    if db_type in ["postgresql", "postgres"]:
        temp = "TEMP " if getattr(payload, "temporary", False) else ""
        ine = "IF NOT EXISTS " if getattr(payload, "if_not_exists", True) else ""
        sql = f"CREATE {temp}TABLE {ine}{safe_table} (id INTEGER);"
    elif db_type in ["mysql", "mariadb"]:
        ine = "IF NOT EXISTS " if getattr(payload, "if_not_exists", True) else ""
        engine_sql = f" ENGINE={payload.engine}" if getattr(payload, "engine", None) else ""
        charset_sql = f" DEFAULT CHARSET={payload.charset}" if getattr(payload, "charset", None) else ""
        coll_sql = f" COLLATE={payload.collation}" if getattr(payload, "collation", None) else ""
        temp = "TEMPORARY " if getattr(payload, "temporary", False) else ""
        sql = f"CREATE {temp}TABLE {ine}{safe_table} (id INT){engine_sql}{charset_sql}{coll_sql};"
    elif db_type in ["mssql", "sqlserver"]:
        if getattr(payload, "if_not_exists", True):
            schema_name = schema or "dbo"
            safe_schema = quote_identifier(db_type, schema_name)
            safe_table_only = quote_identifier(db_type, table)
            oid = f"{schema_name}.{table}".replace("'", "''")
            sql = f"IF OBJECT_ID(N'{oid}', N'U') IS NULL BEGIN CREATE TABLE {safe_schema}.{safe_table_only} (id INT); END;"
        else:
            sql = f"CREATE TABLE {safe_table} (id INT);"
    elif db_type == "oracle":
        sql = f"CREATE TABLE {safe_table} (id NUMBER);"
    elif db_type == "sqlite":
        ine = "IF NOT EXISTS " if getattr(payload, "if_not_exists", True) else ""
        sql = f"CREATE TABLE {ine}{safe_table} (id INTEGER);"
    else:
        raise ValueError(f"Dialeto '{db_type}' não suportado.")

    _run_ddl(
        db,
        ctx=audit_ctx,
        engine=engine,
        connection_id=connection_model.id,
        operation="CREATE TABLE",
        query_type=QueryType.CREATETABLE,
        dialect=db_type,
        table=full_table_name,
        column=None,
        sql_or_queries=sql,
        extra={"payload": getattr(payload, "model_dump", lambda **_: payload.__dict__)(exclude_none=True)},
    )

    description = _normalize_description(payload)
    if description:
        _apply_table_description(
            db,
            ctx=audit_ctx,
            engine=engine,
            connection_id=connection_model.id,
            db_type=db_type,
            schema=schema,
            table=table,
            full_table_name=full_table_name,
            description=description,
        )

    # ✅ metadata com tratamento de erro + auditoria
    try:
        create_db_structure(
            db,
            db_connection_id=connection_model.id,
            table_name=table,
            schema_name=_normalize_schema(schema),
            description=description,
            engine=getattr(payload, "engine", None),
            charset=getattr(payload, "charset", None),
            collation=getattr(payload, "collation", None),
        )
    except Exception as exc:
        friendly_msg, details = _lidar_com_erro_sql(
            exc,
            operation="METADATA::CREATE_TABLE",
            dialect=db_type,
            table=full_table_name,
            column=None,
            sql=None,
        )
        _audit_write(
            db,
            ctx=audit_ctx,
            connection_id=connection_model.id,
            operation="METADATA::CREATE_TABLE",
            query_type=QueryType.CREATETABLE,
            dialect=db_type,
            table=full_table_name,
            column=None,
            sql=None,
            status="error",
            started_at=_utcnow(),
            error_message=friendly_msg,
            extra={"error": details},
        )
        raise


def execute_alter_table(
    db: Session,
    engine: Engine,
    connection_model: DBConnection,
    old_full_table_name: str,
    new_full_table_name: str,
    payload: TableDDLRequest,
    *,
    audit_ctx: AuditContext,
) -> None:
    db_type = _validate_dialect(connection_model.type)  # type: ignore
    old_schema, old_table = _split_schema_table(old_full_table_name)
    new_schema, new_table = _split_schema_table(new_full_table_name)

    queries: List[str] = []
    rename_needed = (old_schema != new_schema) or (old_table != new_table)

    if rename_needed:
        if db_type in ["postgresql", "postgres"]:
            if old_table != new_table:
                queries.append(
                    f"ALTER TABLE {_q_table(db_type, old_schema, old_table)} "
                    f"RENAME TO {quote_identifier(db_type, new_table)};"
                )
            if old_schema != new_schema and new_schema:
                cur_table = new_table if old_table != new_table else old_table
                queries.append(
                    f"ALTER TABLE {_q_table(db_type, old_schema, cur_table)} "
                    f"SET SCHEMA {quote_identifier(db_type, new_schema)};"
                )
        elif db_type in ["mysql", "mariadb"]:
            queries.append(
                f"RENAME TABLE {_q_table(db_type, old_schema, old_table)} "
                f"TO {_q_table(db_type, new_schema, new_table)};"
            )
        elif db_type in ["mssql", "sqlserver"]:
            if old_schema != new_schema and new_schema:
                queries.append(
                    f"ALTER SCHEMA {quote_identifier(db_type, new_schema)} "
                    f"TRANSFER {_q_table(db_type, old_schema, old_table)};"
                )
            if old_table != new_table:
                schema_name = old_schema or "dbo"
                obj = f"{schema_name}.{old_table}".replace("'", "''")
                queries.append(f"EXEC sp_rename '{obj}', '{new_table}';")
        elif db_type == "oracle":
            if old_schema and new_schema and old_schema != new_schema:
                raise ValueError("Oracle: mover schema não suportado via ALTER simples.")
            if old_table != new_table:
                queries.append(
                    f"RENAME {quote_identifier(db_type, old_table)} "
                    f"TO {quote_identifier(db_type, new_table)}"
                )
        elif db_type == "sqlite":
            if old_schema or new_schema:
                raise ValueError("SQLite não suporta schema qualificado para rename.")
            if old_table != new_table:
                queries.append(
                    f"ALTER TABLE {quote_identifier(db_type, old_table)} "
                    f"RENAME TO {quote_identifier(db_type, new_table)};"
                )
        else:
            raise ValueError(f"Dialeto '{db_type}' não suportado para rename/move table.")

    if queries:
        _run_ddl(
            db,
            ctx=audit_ctx,
            engine=engine,
            connection_id=connection_model.id,
            operation="ALTER TABLE",
            query_type=QueryType.ALTERTABLE,
            dialect=db_type,
            table=old_full_table_name,
            column=None,
            sql_or_queries=queries,
            extra={
                "old_full": old_full_table_name,
                "new_full": new_full_table_name,
                "payload": getattr(payload, "model_dump", lambda **_: payload.__dict__)(exclude_none=True),
            },
        )

    description = _normalize_description(payload)
    if description is not None:
        final_schema, final_table = _split_schema_table(new_full_table_name)
        _apply_table_description(
            db,
            ctx=audit_ctx,
            engine=engine,
            connection_id=connection_model.id,
            db_type=db_type,
            schema=final_schema,
            table=final_table,
            full_table_name=new_full_table_name,
            description=description,
        )

    # ✅ metadata com tratamento de erro + auditoria
    try:
        update_db_structure_by_name(
            db,
            db_connection_id=connection_model.id,
            original_table_name=old_table,
            original_schema_name=_normalize_schema(old_schema),
            new_table_name=new_table,
            new_schema_name=_normalize_schema(new_schema),
            description=description,
            engine=getattr(payload, "engine", None),
            charset=getattr(payload, "charset", None),
            collation=getattr(payload, "collation", None),
        )
    except Exception as exc:
        friendly_msg, details = _lidar_com_erro_sql(
            exc,
            operation="METADATA::UPDATE_TABLE",
            dialect=db_type,
            table=new_full_table_name,
            column=None,
            sql=None,
        )
        _audit_write(
            db,
            ctx=audit_ctx,
            connection_id=connection_model.id,
            operation="METADATA::UPDATE_TABLE",
            query_type=QueryType.ALTERTABLE,
            dialect=db_type,
            table=new_full_table_name,
            column=None,
            sql=None,
            status="error",
            started_at=_utcnow(),
            error_message=friendly_msg,
            extra={"old_full": old_full_table_name, "new_full": new_full_table_name, "error": details},
        )
        raise


def execute_drop_table(
    db: Session,
    engine: Engine,
    connection_model: DBConnection,
    full_table_name: str,
    *,
    if_exists: bool = True,
    cascade: bool = False,
    audit_ctx: AuditContext,
) -> None:
    db_type = _validate_dialect(connection_model.type)  # type: ignore
    schema, table = _split_schema_table(full_table_name)
    safe_table = _q_table(db_type, schema, table)

    if db_type in ["postgresql", "postgres"]:
        sql = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{safe_table}{' CASCADE' if cascade else ''};"
    elif db_type in ["mysql", "mariadb"]:
        sql = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{safe_table};"
    elif db_type in ["mssql", "sqlserver"]:
        if if_exists:
            schema_name = schema or "dbo"
            oid = f"{schema_name}.{table}".replace("'", "''")
            sql = (
                f"IF OBJECT_ID(N'{oid}', N'U') IS NOT NULL "
                f"BEGIN DROP TABLE {quote_identifier(db_type, schema_name)}.{quote_identifier(db_type, table)}; END;"
            )
        else:
            sql = f"DROP TABLE {safe_table};"
    elif db_type == "oracle":
        sql = f"DROP TABLE {safe_table}{' CASCADE CONSTRAINTS' if cascade else ''}"
    elif db_type == "sqlite":
        sql = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{safe_table};"
    else:
        raise ValueError(f"Dialeto '{db_type}' não suportado.")

    _run_ddl(
        db,
        ctx=audit_ctx,
        engine=engine,
        connection_id=connection_model.id,
        operation="DROP TABLE",
        query_type=QueryType.DROPTABLE,
        dialect=db_type,
        table=full_table_name,
        column=None,
        sql_or_queries=sql,
        extra={"if_exists": if_exists, "cascade": cascade},
    )

    # ✅ metadata com tratamento de erro + auditoria
    try:
        soft_delete_db_structure_by_name(
            db,
            db_connection_id=connection_model.id,
            table_name=table,
            schema_name=_normalize_schema(schema),
        )
    except Exception as exc:
        friendly_msg, details = _lidar_com_erro_sql(
            exc,
            operation="METADATA::SOFT_DELETE_TABLE",
            dialect=db_type,
            table=full_table_name,
            column=None,
            sql=None,
        )
        _audit_write(
            db,
            ctx=audit_ctx,
            connection_id=connection_model.id,
            operation="METADATA::SOFT_DELETE_TABLE",
            query_type=QueryType.DROPTABLE,
            dialect=db_type,
            table=full_table_name,
            column=None,
            sql=None,
            status="error",
            started_at=_utcnow(),
            error_message=friendly_msg,
            extra={"error": details},
        )
        raise