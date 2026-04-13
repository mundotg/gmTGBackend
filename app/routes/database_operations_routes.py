import asyncio
import re
import traceback
from typing import Optional, Callable, Any, List

from fastapi import APIRouter, Depends, HTTPException, Body, Path, Query, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.connection_models import DBConnection
from app.schemas.dbstructure_schema import (
    BulkDropTablesRequest,
    FieldDDLRequest,
    TableDDLRequest,
)
from app.schemas.responsehttp_schema import ResponseWrapper
from app.cruds.dbstructure_crud import get_structure_id_by_connection_and_table
from app.services.schema_manager_field import (
    execute_add_column,
    execute_drop_column,
    execute_alter_column,
    AuditContext,
)
from app.services.schema_manager_table import (
    execute_create_table,
    execute_alter_table,
    execute_drop_table,
)
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

# ✅ pega o erro padronizado num lugar só (field ou table tanto faz)
try:
    from app.ultils.Database_error_logger import DDLExecutionError
except Exception:
    DDLExecutionError = None

router = APIRouter(prefix="/database", tags=["Database Schema (DDL)"])

# ============================================================
# 🔧 HELPERS
# ============================================================

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _http_error(status: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status, detail=detail)


def _validate_identifier(value: str, label: str) -> str:
    v = (value or "").strip()
    if not v:
        raise ValueError(f"{label} não pode estar vazio.")
    if "." in v:
        raise ValueError(f"{label} não pode conter '.' (use schema_name separado).")
    if not IDENT_RE.match(v):
        raise ValueError(
            f"{label} inválido: '{v}'. Use letras/números/_ e não comece com número."
        )
    return v


def _build_full_table_name(schema: Optional[str], table: str) -> str:
    table = _validate_identifier(table, "table_name")
    if schema and schema.strip():
        schema = _validate_identifier(schema, "schema_name")
        return f"{schema}.{table}"
    return table


def _map_ddl_error_to_http(e: Exception) -> HTTPException:
    if isinstance(e, ValueError):
        return _http_error(400, str(e))

    if DDLExecutionError and isinstance(e, DDLExecutionError):
        msg = str(e)
        op = getattr(e, "operation", None)
        dialect = getattr(e, "dialect", None)
        table = getattr(e, "table", None)
        col = getattr(e, "column", None)

        log_message(
            f"❌ DDLExecutionError mapped: op={op} dialect={dialect} table={table} col={col} msg={msg}",
            level="error",
        )

        lower = msg.lower()
        if any(
            x in lower for x in ["already exists", "duplicate", "exists", "já existe"]
        ):
            return _http_error(409, msg)
        if any(x in lower for x in ["permission", "denied", "permiss", "not allowed"]):
            return _http_error(403, msg)
        if any(x in lower for x in ["not found", "does not exist", "inexistente"]):
            return _http_error(404, msg)
        return _http_error(400, msg)

    return _http_error(500, "Erro interno ao modificar a estrutura do banco de dados.")


async def _get_engine(db: Session, user_id: int):
    engine, connectionModel = await asyncio.to_thread(
        ConnectionManager.ensure_connection, db, user_id
    )
    if not engine:
        raise _http_error(503, "Não foi possível conectar ao motor do banco de dados.")
    return engine, connectionModel


async def _run_sync(func: Callable[..., Any], *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


def _ensure_connection_id(connectionModel: Any, connection_id: Optional[int]) -> None:
    if connection_id is None:
        return
    if getattr(connectionModel, "id", None) != connection_id:
        raise _http_error(403, "Permissão negada ou incompatibilidade de conexão.")


def _build_audit_context(request: Request, user_id: int) -> AuditContext:
    return AuditContext(
        user_id=user_id,
        client_ip=request.client.host if request.client else "unknown",
        app_source="API",
        executed_by=f"user_id:{user_id}",
    )


async def _handle_endpoint(
    *,
    action_name: str,
    db: Session,
    user_id: int,
    connectionModel_ref_getter: Callable[[], Optional[DBConnection]],
    engine_ref_getter: Callable[
        [], Any
    ] = lambda: None,  # ✅ Referência para fechar a engine
    success_message: str,
    runner: Callable[[], Any],
    log_context: str,
):
    try:
        await runner()
        cm = connectionModel_ref_getter()
        dialect_name = getattr(cm, "type", "unknown") if cm else "unknown"
        log_message(
            f"✅ {action_name}: {log_context} (dialect={dialect_name}, user={user_id})",
            level="success",
        )
        return ResponseWrapper(success=True, data=success_message)
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        log_message(
            f"❌ {action_name} failed: {log_context} user={user_id} err={repr(e)}\n{traceback.format_exc()}",
            level="error",
        )
        raise _map_ddl_error_to_http(e)
    finally:
        # ✅ FORÇA O ENCERRAMENTO DAS CONEXÕES PENDENTES NO FINAL DA ROTA
        engine = engine_ref_getter()
        if engine:
            try:
                if hasattr(engine, "dispose"):
                    # Verifica se é uma AsyncEngine (SQLAlchemy 2.0+)
                    if (
                        asyncio.iscoroutinefunction(engine.dispose)
                        or type(engine).__name__ == "AsyncEngine"
                    ):
                        await engine.dispose()
                    else:
                        await asyncio.to_thread(engine.dispose)
            except Exception as ex:
                log_message(f"⚠️ Erro ao fazer dispose da engine: {ex}", "warning")


# ============================================================
# ➕ FIELD: CREATE
# ============================================================
@router.post("/field/", response_model=ResponseWrapper, summary="Criar nova Coluna")
async def create_field(
    request: Request,
    payload: FieldDDLRequest = Body(...),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    # 🔥 MELHORIA: Usa o connection_id diretamente do payload (evita duplicação com Query params)
    connection_id = payload.connection_id

    full_table_name = _build_full_table_name(payload.schema_name, payload.table_name)
    field_name = _validate_identifier(payload.name, "field.name")
    audit_ctx = _build_audit_context(request, user_id)

    safe_payload = payload.model_copy(deep=True)
    safe_payload.name = field_name

    connectionModel_ref: Optional[DBConnection] = None
    engine_ref: Any = None

    async def runner():
        nonlocal connectionModel_ref, engine_ref

        # Pega a engine e valida
        engine, connectionModel = await _get_engine(db, user_id)
        connectionModel_ref = connectionModel
        engine_ref = engine
        _ensure_connection_id(connectionModel, connection_id)

        # Busca o ID da estrutura da tabela
        table_id = await _run_sync(
            get_structure_id_by_connection_and_table,
            db,
            connectionModel.id,
            payload.table_name,
        )

        if not table_id:
            raise HTTPException(
                status_code=404,
                detail=f"Tabela '{payload.table_name}' não encontrada nos metadados.",
            )

        safe_payload.table_id = table_id

        # Executa a alteração no banco
        await _run_sync(
            execute_add_column,
            db,
            engine,
            connectionModel,
            full_table_name,
            safe_payload,
            audit_ctx=audit_ctx,
        )

    return await _handle_endpoint(
        action_name="ADD COLUMN",
        db=db,
        user_id=user_id,
        connectionModel_ref_getter=lambda: connectionModel_ref,
        engine_ref_getter=lambda: engine_ref,
        success_message=f"A coluna '{field_name}' foi adicionada com sucesso na tabela '{payload.table_name}'.",
        runner=runner,
        log_context=f"col='{field_name}' table='{full_table_name}'",
    )


# ============================================================
# ✏️ FIELD: UPDATE
# ============================================================
@router.put(
    "/field/{original_column_name}",
    response_model=ResponseWrapper,
    summary="Editar Coluna existente",
)
async def update_field(
    request: Request,
    original_column_name: str = Path(..., description="Nome atual da coluna no banco"),
    payload: FieldDDLRequest = Body(...),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    # 🔥 MELHORIA: Usa o connection_id do payload
    connection_id = payload.connection_id

    original_column_name_safe = _validate_identifier(
        original_column_name, "original_column_name"
    )
    full_table_name = _build_full_table_name(payload.schema_name, payload.table_name)
    new_field_name = _validate_identifier(payload.name, "field.name")
    audit_ctx = _build_audit_context(request, user_id)

    safe_payload = payload.model_copy(deep=True)
    safe_payload.original_name = original_column_name_safe
    safe_payload.name = new_field_name

    connectionModel_ref: Optional[DBConnection] = None
    engine_ref: Any = None

    async def runner():
        nonlocal connectionModel_ref, engine_ref
        engine, connectionModel = await _get_engine(db, user_id)
        connectionModel_ref = connectionModel
        engine_ref = engine
        _ensure_connection_id(connectionModel, connection_id)

        table_id = await _run_sync(
            get_structure_id_by_connection_and_table,
            db,
            connectionModel.id,
            payload.table_name,
        )

        if not table_id:
            raise HTTPException(
                status_code=404,
                detail=f"Tabela '{payload.table_name}' não encontrada nos metadados.",
            )

        safe_payload.table_id = table_id

        await _run_sync(
            execute_alter_column,
            db,
            engine,
            connectionModel,
            full_table_name,
            safe_payload,
            audit_ctx=audit_ctx,
        )

    action = (
        "renomeada e alterada"
        if original_column_name_safe != new_field_name
        else "alterada"
    )

    return await _handle_endpoint(
        action_name="ALTER COLUMN",
        db=db,
        user_id=user_id,
        connectionModel_ref_getter=lambda: connectionModel_ref,
        engine_ref_getter=lambda: engine_ref,
        success_message=f"As propriedades da coluna '{new_field_name}' foram atualizadas com sucesso.",
        runner=runner,
        log_context=f"orig='{original_column_name_safe}' new='{new_field_name}' action={action} table='{full_table_name}'",
    )


# ============================================================
# 🗑️ FIELD: DELETE
# ============================================================


@router.delete(
    "/field/{table_name}/{column_name}",
    response_model=ResponseWrapper,
    summary="Excluir uma Coluna (DROP)",
)
async def delete_field(
    request: Request,
    table_name: str = Path(..., description="Nome da tabela alvo"),
    column_name: str = Path(..., description="Nome da coluna a ser eliminada"),
    schema_name: Optional[str] = Query(
        None, description="Schema do banco (ex: public, dbo)"
    ),
    connection_id: int = Query(
        ..., description="ID da conexão do banco de dados alvo"
    ),  # Aqui o Query faz sentido, pois DELETE não tem body
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    full_table_name = _build_full_table_name(schema_name, table_name)
    column_name_safe = _validate_identifier(column_name, "column_name")
    audit_ctx = _build_audit_context(request, user_id)

    connectionModel_ref: Optional[DBConnection] = None
    engine_ref: Any = None

    async def runner():
        nonlocal connectionModel_ref, engine_ref
        engine, connectionModel = await _get_engine(db, user_id)
        connectionModel_ref = connectionModel
        engine_ref = engine
        _ensure_connection_id(connectionModel, connection_id)

        table_id = await _run_sync(
            get_structure_id_by_connection_and_table,
            db,
            connection_id,
            table_name,
        )

        if not table_id:
            raise HTTPException(
                status_code=404,
                detail=f"Tabela '{table_name}' não encontrada nos metadados.",
            )

        await _run_sync(
            execute_drop_column,
            db,
            engine,
            connectionModel.type,
            full_table_name,
            table_id,
            column_name_safe,
            connection_id=connection_id,
            audit_ctx=audit_ctx,
        )

    return await _handle_endpoint(
        action_name="DROP COLUMN",
        db=db,
        user_id=user_id,
        connectionModel_ref_getter=lambda: connectionModel_ref,
        engine_ref_getter=lambda: engine_ref,
        success_message=f"A coluna '{column_name_safe}' foi removida permanentemente da tabela '{table_name}'.",
        runner=runner,
        log_context=f"col='{column_name_safe}' table='{full_table_name}'",
    )


# ============================================================
# 🧨 TABLE: BULK DROP
# ============================================================


@router.delete(
    "/table/bulk",
    response_model=ResponseWrapper,
    summary="Excluir várias Tabelas (Bulk DROP)",
)
async def delete_tables_bulk(
    request: Request,
    payload: BulkDropTablesRequest = Body(...),
    connection_id: Optional[int] = Query(
        None, description="ID da conexão do banco de dados alvo (opcional)"
    ),
    if_exists: bool = Query(True, description="Executa com IF EXISTS quando suportado"),
    cascade: bool = Query(False, description="Executa com CASCADE quando suportado"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    if not payload.tables:
        raise _http_error(400, "A lista de tabelas não pode estar vazia.")

    audit_ctx = _build_audit_context(request, user_id)

    bulk_schema = getattr(payload, "schema_name", []) or []
    tables_safe: List[str] = []
    for i, t in enumerate(payload.tables):
        schema_name = bulk_schema[i] if i < len(bulk_schema) else None
        tables_safe.append(
            _build_full_table_name(schema_name, _validate_identifier(t, "table_name"))
        )

    connectionModel_ref: Optional[DBConnection] = None
    engine_ref: Any = None

    async def runner():
        nonlocal connectionModel_ref, engine_ref
        engine, connectionModel = await _get_engine(db, user_id)
        connectionModel_ref = connectionModel
        engine_ref = engine
        _ensure_connection_id(connectionModel, connection_id)
        print(
            "🚀 Conexão garantida, iniciando drops em massa...",
            connectionModel_ref.database_name,
        )
        for full_table_name in tables_safe:
            await _run_sync(
                execute_drop_table,
                db,
                engine,
                connectionModel,
                full_table_name,
                if_exists=if_exists,
                cascade=cascade,
                audit_ctx=audit_ctx,
            )

    return await _handle_endpoint(
        action_name="BULK DROP TABLE",
        db=db,
        user_id=user_id,
        connectionModel_ref_getter=lambda: connectionModel_ref,
        engine_ref_getter=lambda: engine_ref,
        success_message=f"{len(tables_safe)} tabela(s) removida(s) com sucesso.",
        runner=runner,
        log_context=f"tables={tables_safe}",
    )


# ============================================================
# ➕ TABLE: CREATE
# ============================================================


@router.post("/table/", response_model=ResponseWrapper, summary="Criar nova Tabela")
async def create_table(
    request: Request,
    payload: TableDDLRequest = Body(...),
    connection_id: Optional[int] = Query(
        None, description="ID da conexão do banco de dados alvo (opcional)"
    ),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    table_raw = getattr(payload, "name", None) or getattr(payload, "table_name", None)
    table_name = _validate_identifier(table_raw, "table.name")

    full_table_name = _build_full_table_name(
        getattr(payload, "schema_name", None), table_name
    )
    audit_ctx = _build_audit_context(request, user_id)

    safe_payload = payload.model_copy(deep=True)
    if hasattr(safe_payload, "name"):
        safe_payload.name = table_name
    if hasattr(safe_payload, "table_name"):
        safe_payload.table_name = table_name

    connectionModel_ref: Optional[DBConnection] = None
    engine_ref: Any = None

    async def runner():
        nonlocal connectionModel_ref, engine_ref
        engine, connectionModel = await _get_engine(db, user_id)
        connectionModel_ref = connectionModel
        engine_ref = engine
        _ensure_connection_id(connectionModel, connection_id)

        await _run_sync(
            execute_create_table,
            db,
            engine,
            connectionModel,
            full_table_name,
            safe_payload,
            audit_ctx=audit_ctx,
        )

    return await _handle_endpoint(
        action_name="CREATE TABLE",
        db=db,
        user_id=user_id,
        connectionModel_ref_getter=lambda: connectionModel_ref,
        engine_ref_getter=lambda: engine_ref,
        success_message=f"A tabela '{full_table_name}' foi criada com sucesso.",
        runner=runner,
        log_context=f"table='{full_table_name}'",
    )


# ============================================================
# ✏️ TABLE: UPDATE / RENAME
# ============================================================


@router.put(
    "/table/{original_table_name}",
    response_model=ResponseWrapper,
    summary="Editar/Renomear Tabela existente",
)
async def update_table(
    request: Request,
    original_table_name: str = Path(..., description="Nome atual da tabela no banco"),
    payload: TableDDLRequest = Body(...),
    connection_id: Optional[int] = Query(
        None, description="ID da conexão do banco de dados alvo (opcional)"
    ),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    original_name_safe = _validate_identifier(
        original_table_name, "original_table_name"
    )

    table_raw = getattr(payload, "name", None) or getattr(payload, "table_name", None)
    new_name_safe = _validate_identifier(table_raw, "table.name")

    old_schema = getattr(payload, "original_schema_name", None) or getattr(
        payload, "schema_name", None
    )
    new_schema = getattr(payload, "schema_name", None)

    old_full = _build_full_table_name(old_schema, original_name_safe)
    new_full = _build_full_table_name(new_schema, new_name_safe)

    audit_ctx = _build_audit_context(request, user_id)

    safe_payload = payload.model_copy(deep=True)
    if hasattr(safe_payload, "original_name"):
        safe_payload.original_name = original_name_safe
    if hasattr(safe_payload, "name"):
        safe_payload.name = new_name_safe
    if hasattr(safe_payload, "table_name"):
        safe_payload.table_name = new_name_safe

    connectionModel_ref: Optional[DBConnection] = None
    engine_ref: Any = None

    async def runner():
        nonlocal connectionModel_ref, engine_ref
        engine, connectionModel = await _get_engine(db, user_id)
        connectionModel_ref = connectionModel
        engine_ref = engine
        _ensure_connection_id(connectionModel, connection_id)

        await _run_sync(
            execute_alter_table,
            db,
            engine,
            connectionModel,
            old_full,
            new_full,
            safe_payload,
            audit_ctx=audit_ctx,
        )

    action = "renomeada" if old_full != new_full else "alterada"

    return await _handle_endpoint(
        action_name="ALTER TABLE",
        db=db,
        user_id=user_id,
        connectionModel_ref_getter=lambda: connectionModel_ref,
        engine_ref_getter=lambda: engine_ref,
        success_message=f"A tabela '{old_full}' foi {action} com sucesso.",
        runner=runner,
        log_context=f"old='{old_full}' new='{new_full}' action={action}",
    )


# ============================================================
# 🗑️ TABLE: DELETE
# ============================================================


@router.delete(
    "/table/{table_name}",
    response_model=ResponseWrapper,
    summary="Excluir uma Tabela (DROP)",
)
async def delete_table(
    request: Request,
    table_name: str = Path(..., description="Nome da tabela alvo"),
    schema_name: Optional[str] = Query(
        None, description="Schema do banco (ex: public, dbo)"
    ),
    connection_id: Optional[int] = Query(
        None, description="ID da conexão do banco de dados alvo (opcional)"
    ),
    if_exists: bool = Query(True, description="Executa com IF EXISTS quando suportado"),
    cascade: bool = Query(
        False, description="Executa com CASCADE quando suportado (ex: Postgres)"
    ),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    table_name_safe = _validate_identifier(table_name, "table_name")
    full_table_name = _build_full_table_name(schema_name, table_name_safe)
    audit_ctx = _build_audit_context(request, user_id)

    connectionModel_ref: Optional[DBConnection] = None
    engine_ref: Any = None

    async def runner():
        nonlocal connectionModel_ref, engine_ref
        engine, connectionModel = await _get_engine(db, user_id)
        connectionModel_ref = connectionModel
        engine_ref = engine
        _ensure_connection_id(connectionModel, connection_id)

        await _run_sync(
            execute_drop_table,
            db,
            engine,
            connectionModel,
            full_table_name,
            if_exists=if_exists,
            cascade=cascade,
            audit_ctx=audit_ctx,
        )

    return await _handle_endpoint(
        action_name="DROP TABLE",
        db=db,
        user_id=user_id,
        connectionModel_ref_getter=lambda: connectionModel_ref,
        engine_ref_getter=lambda: engine_ref,
        success_message=f"A tabela '{full_table_name}' foi removida permanentemente.",
        runner=runner,
        log_context=f"table='{full_table_name}'",
    )
