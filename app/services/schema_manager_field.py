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

from app.cruds.dbstructure_crud import (
    create_db_field,
    soft_delete_field_name,
    update_fields_by_tablename,
)
from app.cruds.queryhistory_crud import create_query_history
from app.models.connection_models import DBConnection
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryType
from app.schemas.dbstructure_schema import DBFieldCreate, FieldDDLRequest
from app.services.editar_linha import quote_identifier
from app.services.field_info import map_column_type
from app.ultils.Database_error_logger import DDLExecutionError, _lidar_com_erro_sql
from app.ultils.logger import log_message


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
    status: str,
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
        query=_shorten_sql(sql) if sql else None,
        query_type=query_type,
        executed_at=started_at,
        duration_ms=duration_ms,
        result_preview=_safe_json(
            {"status": status, "table": table, "column": column, "dialect": dialect}
        ),
        error_message=error_message,
        is_favorite=False,
        tags=_audit_tag(operation, status),
        app_source=ctx.app_source,
        client_ip=ctx.client_ip,
        executed_by=ctx.executed_by
        or (f"user_{ctx.user_id}" if ctx.user_id is not None else None),
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
        sql_or_queries
        if isinstance(sql_or_queries, str)
        else " ; ".join(sql_or_queries)
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

        # ✅ AUDITA O ERRO (antes faltava)
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
# 🛡️ VALIDADORES/HELPERS
# ============================================================

FK_ACTIONS = {"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}
_NUM_RE = re.compile(r"^-?\d+(\.\d+)?$")
_SQL_KEYWORDS_DEFAULT = {
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "NOW()",
    "GETDATE()",
    "SYSDATE",
}


def _normalize_fk_action(v: Optional[str]) -> str:
    v = (v or "NO ACTION").strip().upper()
    if v not in FK_ACTIONS:
        raise ValueError(f"Ação FK inválida: {v}. Use: {sorted(FK_ACTIONS)}")
    return v


def _validate_dialect(db_type: str) -> str:
    db_type = (db_type or "").lower().strip()
    allowed = {
        "postgresql",
        "postgres",
        "mysql",
        "mariadb",
        "oracle",
        "mssql",
        "sqlserver",
        "sqlite",
    }
    if db_type not in allowed:
        raise ValueError(
            f"Dialeto '{db_type}' não suportado para manipulação estrutural."
        )
    return db_type


def _split_schema_table(full: str) -> Tuple[Optional[str], str]:
    full = (full or "").strip()
    if "." in full:
        schema, table = full.split(".", 1)
        return schema.strip() or None, table.strip()
    return None, full


def _q_table(db_type: str, schema: Optional[str], table: str) -> str:
    if schema:
        return f"{quote_identifier(db_type, schema)}.{quote_identifier(db_type, table)}"
    return quote_identifier(db_type, table)


def _sanitize_name(name: str, max_len: int = 60) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", name or "")
    name = re.sub(r"_+", "_", name).strip("_")
    return name[:max_len]


def _validate_field_for_add(field: FieldDDLRequest) -> Tuple[str, str]:
    if not (field.name and str(field.name).strip()):
        raise ValueError("Nome do campo é obrigatório.")
    if not (field.type and str(field.type).strip()):
        raise ValueError("Tipo do campo é obrigatório.")
    if (field.scale is not None) and (field.precision is None):
        raise ValueError(
            "A precisão (precision) é obrigatória quando a escala (scale) é informada."
        )

    fk_on_delete = _normalize_fk_action(getattr(field, "fk_on_delete", None))
    fk_on_update = _normalize_fk_action(getattr(field, "fk_on_update", None))

    if getattr(field, "is_foreign_key", False):
        if not getattr(field, "referenced_table", None) or not getattr(
            field, "referenced_field", None
        ):
            raise ValueError(
                "Tabela e campo referenciados são obrigatórios quando is_foreign_key=True."
            )

    return fk_on_delete, fk_on_update


def build_type_string(field: FieldDDLRequest, db_type: str) -> str:
    type_str = str(field.type).upper().strip()

    # 🔥 1. TRATAMENTO DE ENUM
    if type_str == "ENUM" and getattr(field, "enum_values", None):
        # Escapa aspas simples e formata: ENUM('valor1', 'valor2')
        vals = ", ".join(
            f"'{v.replace(chr(39), chr(39) + chr(39))}'" for v in field.enum_values
        )
        type_str = f"ENUM({vals})"

    # TRATAMENTO DE PRECISÃO E TAMANHO
    elif field.precision is not None:
        if field.scale is not None:
            type_str = f"{type_str}({field.precision},{field.scale})"
        else:
            type_str = f"{type_str}({field.precision})"
    elif field.length is not None:
        type_str = f"{type_str}({field.length})"

    # 🔥 2. TRATAMENTO DE UNSIGNED (Apenas MySQL/MariaDB suportam isto nativamente em tipos numéricos)
    if getattr(field, "is_unsigned", False) and db_type in ["mysql", "mariadb"]:
        if "INT" in type_str or type_str in ["FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"]:
            type_str += " UNSIGNED"

    return type_str


def _sql_default(field: FieldDDLRequest) -> str:
    if field.default_value is None:
        return ""
    raw = str(field.default_value).strip()
    if raw == "":
        return ""
    upper = raw.upper()

    if (raw.startswith("'") and raw.endswith("'")) or (
        raw.startswith('"') and raw.endswith('"')
    ):
        return f"DEFAULT {raw}"
    if upper in {"NULL", "TRUE", "FALSE"}:
        return f"DEFAULT {upper}"
    if _NUM_RE.match(raw):
        return f"DEFAULT {raw}"
    if upper in _SQL_KEYWORDS_DEFAULT or raw.endswith("()"):
        return f"DEFAULT {raw}"
    return f"DEFAULT '{raw.replace(chr(39), chr(39) + chr(39))}'"


# ============================================================
# ⚙️ OPERAÇÕES DDL PRINCIPAIS (3 funções)
# ============================================================


def execute_add_column(
    db: Session,
    engine: Engine,
    connection_model: DBConnection,
    table_name: str,
    field: FieldDDLRequest,
    *,
    audit_ctx: AuditContext,
) -> None:
    fk_on_delete, fk_on_update = _validate_field_for_add(field)
    db_type = _validate_dialect(connection_model.type)  # type: ignore

    schema, table = _split_schema_table(table_name)
    safe_table = _q_table(db_type, schema, table)
    safe_col = quote_identifier(db_type, field.name)

    # 🔥 MELHORIA: Agora passamos o db_type para o formatador do tipo
    type_str = build_type_string(field, db_type)
    null_str = "NULL" if field.is_nullable else "NOT NULL"
    default_str = _sql_default(field)
    unique_str = "UNIQUE" if field.is_unique else ""

    operation = "ADD COLUMN"

    if db_type in ["postgresql", "postgres"]:
        sql = f"ALTER TABLE {safe_table} ADD COLUMN {safe_col} {type_str} {default_str} {null_str} {unique_str};"
    elif db_type in ["mysql", "mariadb"]:
        ai_str = "AUTO_INCREMENT" if field.is_auto_increment else ""
        sql = f"ALTER TABLE {safe_table} ADD COLUMN {safe_col} {type_str} {null_str} {default_str} {unique_str} {ai_str};"
    elif db_type == "oracle":
        sql = f"ALTER TABLE {safe_table} ADD ({safe_col} {type_str} {default_str} {null_str} {unique_str})"
    elif db_type in ["mssql", "sqlserver"]:
        sql = f"ALTER TABLE {safe_table} ADD {safe_col} {type_str} {null_str} {default_str} {unique_str};"
    elif db_type == "sqlite":
        if (not field.is_nullable) and not field.default_value:
            raise ValueError(
                "SQLite exige default_value para colunas NOT NULL ao usar ADD COLUMN."
            )
        sql = f"ALTER TABLE {safe_table} ADD COLUMN {safe_col} {type_str} {default_str} {null_str};"
    else:
        raise ValueError(f"Dialeto '{db_type}' não suportado.")

    _run_ddl(
        db,
        ctx=audit_ctx,
        engine=engine,
        connection_id=connection_model.id,
        operation=operation,
        query_type=QueryType.ADD_COLUMN,
        dialect=db_type,
        table=table_name,
        column=field.name,
        sql_or_queries=sql,
        extra={
            "payload": getattr(field, "model_dump", lambda **_: field.__dict__)(
                exclude_none=True
            )
        },
    )

    # FK opcional (exceto sqlite)
    if (
        db_type != "sqlite"
        and getattr(field, "is_foreign_key", False)
        and getattr(field, "referenced_table", None)
    ):
        ref_schema, ref_table_name = _split_schema_table(str(field.referenced_table))
        ref_table = _q_table(db_type, ref_schema, ref_table_name)
        ref_col = quote_identifier(
            db_type, str(getattr(field, "referenced_field", "id"))
        )
        fk_name = quote_identifier(
            db_type, _sanitize_name(f"fk_{table}_{field.name}_{ref_table_name}")
        )

        fk_sql = (
            f"ALTER TABLE {safe_table} "
            f"ADD CONSTRAINT {fk_name} "
            f"FOREIGN KEY ({safe_col}) "
            f"REFERENCES {ref_table}({ref_col}) "
            f"ON DELETE {fk_on_delete} "
            f"ON UPDATE {fk_on_update};"
        )

        _run_ddl(
            db,
            ctx=audit_ctx,
            engine=engine,
            connection_id=connection_model.id,
            operation="ADD FK",
            query_type=QueryType.ADD_COLUMN,
            dialect=db_type,
            table=table_name,
            column=field.name,
            sql_or_queries=fk_sql,
            extra={
                "fk": {
                    "referenced_table": str(field.referenced_table),
                    "referenced_field": str(field.referenced_field),
                }
            },
        )

    # Persistência de Metadados
    structure_id = getattr(field, "table_id", None)
    if not structure_id:
        raise ValueError(
            "table_id não informado no FieldDDLRequest (necessário para salvar metadata)."
        )

    field_in = DBFieldCreate(
        name=field.name,
        status="active",
        type=map_column_type(field.type, db_type),
        is_nullable=field.is_nullable,
        default_value=field.default_value,
        is_primary_key=field.is_primary_key,
        comment=field.comment,
        referenced_table=field.referenced_table,
        referenced_field=field.referenced_field,
        fk_on_delete=fk_on_delete,
        fk_on_update=fk_on_update,
        is_unique=field.is_unique,
        is_foreign_key=field.is_foreign_key,
        is_auto_increment=field.is_auto_increment,
        length=field.length,
        precision=field.precision,
        scale=field.scale,
        is_unsigned=field.is_unsigned,  # 🔥 ADICIONADO AQUI NA CRIAÇÃO
    )

    try:
        create_db_field(db=db, field_in=field_in, structure_id=structure_id)
    except Exception as exc:
        friendly_msg, details = _lidar_com_erro_sql(
            exc,
            operation="METADATA::CREATE_FIELD",
            dialect=db_type,
            table=table_name,
            column=field.name,
            sql=None,
        )
        _audit_write(
            db,
            ctx=audit_ctx,
            connection_id=connection_model.id,
            operation="METADATA::CREATE_FIELD",
            query_type=QueryType.ADD_COLUMN,
            dialect=db_type,
            table=table_name,
            column=field.name,
            sql=None,
            status="error",
            started_at=_utcnow(),
            error_message=friendly_msg,
            extra={"structure_id": structure_id, "error": details},
        )
        raise


def execute_alter_column(
    db: Session,
    engine: Engine,
    connection_model: DBConnection,
    table_name: str,
    field: FieldDDLRequest,
    *,
    audit_ctx: AuditContext,
) -> None:
    db_type = _validate_dialect(connection_model.type)  # type: ignore
    schema, table = _split_schema_table(table_name)
    safe_table = _q_table(db_type, schema, table)

    original_name = field.original_name or field.name
    original_col = quote_identifier(db_type, original_name)
    new_col = quote_identifier(db_type, field.name)

    # 🔥 MELHORIA: Agora passamos o db_type para o formatador do tipo
    type_str = build_type_string(field, db_type)
    null_str = "NULL" if field.is_nullable else "NOT NULL"

    queries: List[str] = []

    if field.original_name and field.original_name != field.name:
        if db_type in ["postgresql", "postgres", "oracle", "sqlite"]:
            queries.append(
                f"ALTER TABLE {safe_table} RENAME COLUMN {original_col} TO {new_col};"
            )
        elif db_type in ["mssql", "sqlserver"]:
            queries.append(
                f"EXEC sp_rename '{table_name}.{field.original_name}', '{field.name}', 'COLUMN';"
            )
        elif db_type in ["mysql", "mariadb"]:
            queries.append(
                f"ALTER TABLE {safe_table} CHANGE COLUMN {original_col} {new_col} {type_str} {null_str};"
            )
        else:
            raise ValueError(f"Dialeto '{db_type}' não suportado para rename.")

    if not (db_type in ["mysql", "mariadb"] and queries):
        if db_type in ["postgresql", "postgres"]:
            queries.append(
                f"ALTER TABLE {safe_table} ALTER COLUMN {new_col} TYPE {type_str};"
            )
            queries.append(
                f"ALTER TABLE {safe_table} ALTER COLUMN {new_col} {'DROP' if field.is_nullable else 'SET'} NOT NULL;"
            )
        elif db_type == "oracle":
            queries.append(
                f"ALTER TABLE {safe_table} MODIFY ({new_col} {type_str} {null_str});"
            )
        elif db_type in ["mssql", "sqlserver"]:
            queries.append(
                f"ALTER TABLE {safe_table} ALTER COLUMN {new_col} {type_str} {null_str};"
            )
        elif db_type in ["mysql", "mariadb"]:
            queries.append(
                f"ALTER TABLE {safe_table} MODIFY COLUMN {new_col} {type_str} {null_str};"
            )
        elif db_type == "sqlite":
            if any(
                [
                    field.type,
                    field.length,
                    field.precision,
                    field.scale,
                    field.is_nullable is not None,
                ]
            ):
                raise ValueError(
                    "SQLite não suporta alterar tipo/nulidade sem recriar a tabela inteira."
                )

    if queries:
        _run_ddl(
            db,
            ctx=audit_ctx,
            engine=engine,
            connection_id=connection_model.id,
            operation="ALTER COLUMN",
            query_type=QueryType.ALTERCOLUMN,
            dialect=db_type,
            table=table_name,
            column=field.name,
            sql_or_queries=queries,
            extra={
                "original_name": original_name,
                "payload": getattr(field, "model_dump", lambda **_: field.__dict__)(
                    exclude_none=True
                ),
            },
        )

    structure_id = getattr(field, "table_id", None)
    if not structure_id:
        raise ValueError("table_id não informado para atualização de metadados.")

    field_in = DBFieldCreate(
        name=field.name,
        status="active",
        type=map_column_type(field.type, db_type),
        is_nullable=field.is_nullable,
        default_value=field.default_value,
        is_primary_key=field.is_primary_key,
        comment=field.comment,
        referenced_table=field.referenced_table,
        referenced_field=field.referenced_field,
        is_unique=field.is_unique,
        is_foreign_key=field.is_foreign_key,
        is_auto_increment=field.is_auto_increment,
        length=field.length,
        precision=field.precision,
        scale=field.scale,
        is_unsigned=field.is_unsigned,  # 🔥 ADICIONADO AQUI NA ALTERAÇÃO
    )

    try:
        update_fields_by_tablename(
            db=db,
            structure_id=structure_id,
            original_name=original_name,
            field_update=field_in,
        )
    except Exception as exc:
        friendly_msg, details = _lidar_com_erro_sql(
            exc,
            operation="METADATA::UPDATE_FIELD",
            dialect=db_type,
            table=table_name,
            column=field.name,
            sql=None,
        )
        _audit_write(
            db,
            ctx=audit_ctx,
            connection_id=connection_model.id,
            operation="METADATA::UPDATE_FIELD",
            query_type=QueryType.ALTERCOLUMN,
            dialect=db_type,
            table=table_name,
            column=field.name,
            sql=None,
            status="error",
            started_at=_utcnow(),
            error_message=friendly_msg,
            extra={
                "structure_id": structure_id,
                "original_name": original_name,
                "error": details,
            },
        )
        raise


def execute_drop_column(
    db: Session,
    engine: Engine,
    dialect: str,
    structure_name: str,
    structure_id: int,
    column_name: str,
    *,
    connection_id: int,
    audit_ctx: AuditContext,
) -> None:
    db_type = _validate_dialect(dialect)

    schema, table = _split_schema_table(structure_name)
    safe_table = _q_table(db_type, schema, table)
    safe_col = quote_identifier(db_type, column_name)

    sql = f"ALTER TABLE {safe_table} DROP COLUMN {safe_col};"

    _run_ddl(
        db,
        ctx=audit_ctx,
        engine=engine,
        connection_id=connection_id,
        operation="DROP COLUMN",
        query_type=QueryType.REMOVE_COLUMN,
        dialect=db_type,
        table=structure_name,
        column=column_name,
        sql_or_queries=sql,
        extra={"structure_id": structure_id},
    )

    try:
        soft_delete_field_name(db, column_name, structure_id)
    except Exception as exc:
        friendly_msg, details = _lidar_com_erro_sql(
            exc,
            operation="METADATA::SOFT_DELETE_FIELD",
            dialect=db_type,
            table=structure_name,
            column=column_name,
            sql=None,
        )
        _audit_write(
            db,
            ctx=audit_ctx,
            connection_id=connection_id,
            operation="METADATA::SOFT_DELETE_FIELD",
            query_type=QueryType.REMOVE_COLUMN,
            dialect=db_type,
            table=structure_name,
            column=column_name,
            sql=None,
            status="error",
            started_at=_utcnow(),
            error_message=friendly_msg,
            extra={"structure_id": structure_id, "error": details},
        )
        raise
