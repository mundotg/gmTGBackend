import traceback
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.orm import Session

from app.config.cache_manager import cache_result
from app.database import get_db
from app.models.dbstructure_models import DBField
from app.routes.connection_routes import get_current_user_id
from app.schemas.dbstructure_schema import (
    DBFieldOut,
    DBStructureOut,
    FieldsBulkRequest,
)
from app.schemas.queryhistory_schemas import TableInfo
from app.schemas.responsehttp_schema import ResponseWrapper
from app.cruds.connection_cruds import (
    get_active_connection_by_userid,
    get_db_connection_by_id,
)
from app.services.stream_tables_counts_servvice import get_table_count_streams
from app.services.database_inspector import (
    get_fields_info_bulk_cached as service_get_fields_info_bulk_cached,
    get_fields_info_cached as service_get_fields_info_cached,
    get_strutures_names,
    get_strutures_names_only,
    get_table_count,
    get_table_names,
    get_table_names_with_count,
    sync_connection_statistics,
)
from app.ultils.logger import log_message

router = APIRouter(prefix="/consu", tags=["Consulta de Banco de Dados"])


# --------------------------------------------------
# Helpers
# --------------------------------------------------


def _get_active_connection_or_400(db: Session, user_id: int):
    active = get_active_connection_by_userid(db, user_id)
    if not active:
        raise HTTPException(
            status_code=400,
            detail="Nenhuma conexão ativa encontrada para este usuário.",
        )
    return active


def _validate_table_name(table_name: str) -> str:
    if not table_name or not table_name.strip():
        raise HTTPException(status_code=422, detail="O nome da tabela é obrigatório.")
    return table_name.strip()


def _validate_connection_access_or_404(db: Session, connection_id: int):
    connection = get_db_connection_by_id(db, connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="Conexão não encontrada.")
    return connection


def _log_and_raise_500(message: str, error: Exception, detail: str):
    log_message(f"{message}: {error}\n{traceback.format_exc()}", level="error")
    raise HTTPException(status_code=500, detail=detail)


# --------------------------------------------------
# Cache wrappers
# --------------------------------------------------


@cache_result(ttl=300, user_id="tables_with_count_{user_id}")
def get_tables_with_count_cached(
    connection_id: int,
    user_id: int,
    db: Session,
) -> List[Dict[str, Any]]:
    return get_table_names_with_count(connection_id, user_id, db)


# @cache_result(ttl=600, user_id="tables_names_{user_id}")
def get_tables_names_cached(
    connection_id: int,
    user_id: int,
    db: Session,
) -> List[str]:
    return get_table_names(connection_id, user_id, db)


@cache_result(ttl=600, user_id="fields_bulk_{user_id}")
def get_fields_info_bulk_cached_wrapper(
    connection_id: int,
    user_id: int,
    table_names: list[str],
    db: Session,
) -> Dict[str, List[DBField]]:
    return service_get_fields_info_bulk_cached(
        connection_id=connection_id,
        table_names=table_names,
        user_id=user_id,
        db=db,
    )


@cache_result(ttl=600, user_id="fields_{user_id}")
def get_fields_info_cached_wrapper(
    connection_id: int,
    user_id: int,
    table_name: str,
    db: Session,
) -> List[DBField]:
    return service_get_fields_info_cached(
        connection_id=connection_id,
        table_name=table_name,
        user_id=user_id,
        db=db,
    )


@cache_result(ttl=600, user_id="structures_only_{user_id}")
def get_structures_names_only_cached(
    connection_id: int,
    user_id: int,
    db: Session,
) -> List[DBStructureOut]:
    return get_strutures_names_only(connection_id, user_id, db)


@cache_result(ttl=600, user_id="structures_full_{user_id}")
def get_structures_names_cached(
    connection_id: int,
    user_id: int,
    db: Session,
) -> List[DBStructureOut]:
    return get_strutures_names(connection_id, user_id, db)


@cache_result(ttl=180, user_id="table_count_{user_id}")
def get_table_count_cached(
    connection_id: int,
    table_name: str,
    db: Session,
    user_id: int,
) -> int:
    return get_table_count(connection_id, table_name, db, user_id)


@cache_result(ttl=1800, user_id="stats_sync_{user_id}")
def sync_connection_stats_cached(user_id: int, db: Session) -> Any:
    stats = sync_connection_statistics(user_id, db)
    if hasattr(stats, "__dict__"):
        stats = {k: v for k, v in stats.__dict__.items() if not k.startswith("_")}
    return stats


# --------------------------------------------------
# Endpoints
# --------------------------------------------------


@router.get("/tables-with-count", response_model=ResponseWrapper[List[TableInfo]])
async def get_tables_with_count_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        active = _get_active_connection_or_400(db, user_id)
        tables = get_tables_with_count_cached(active.connection_id, user_id, db)

        return ResponseWrapper(
            success=True,
            data=[TableInfo(name=t["name"], rowcount=t["rowcount"]) for t in tables],
        )
    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao obter tabelas com contagem",
            e,
            "Erro interno ao buscar tabelas com contagem.",
        )


@router.get("/tables", response_model=ResponseWrapper[List[str]])
async def get_tables_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        active = _get_active_connection_or_400(db, user_id)
        names = get_tables_names_cached(active.connection_id, user_id, db)
        return ResponseWrapper(success=True, data=names)
    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao obter nomes de tabelas",
            e,
            "Erro interno ao buscar nomes das tabelas.",
        )


@router.get("/structures", response_model=ResponseWrapper[List[DBStructureOut]])
async def get_structures_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        active = _get_active_connection_or_400(db, user_id)
        structures = get_structures_names_only_cached(
            connection_id=active.connection_id,
            user_id=user_id,
            db=db,
        )
        return ResponseWrapper(success=True, data=structures)
    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao obter estruturas de tabelas",
            e,
            "Erro interno ao buscar estruturas das tabelas.",
        )


@router.get(
    "/all/structures/{connection_id}",
    response_model=ResponseWrapper[List[DBStructureOut]],
)
async def get_structures_by_connection_id_endpoint(
    connection_id: int = Path(..., description="ID da conexão"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        _validate_connection_access_or_404(db, connection_id)
        structures = get_structures_names_only_cached(connection_id, user_id, db)
        return ResponseWrapper(success=True, data=structures)
    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao obter estruturas por connection_id",
            e,
            "Erro interno ao buscar estruturas das tabelas.",
        )


@router.get(
    "/field/{connection_id}/{table_name}",
    response_model=ResponseWrapper[List[DBFieldOut]],
)
async def get_fields_by_connection_id_endpoint(
    connection_id: int = Path(..., description="ID da conexão"),
    table_name: str = Path(..., description="Nome da tabela"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        _validate_connection_access_or_404(db, connection_id)
        table_name = _validate_table_name(table_name)

        fields = get_fields_info_cached_wrapper(
            connection_id=connection_id,
            user_id=user_id,
            table_name=table_name,
            db=db,
        )
        return ResponseWrapper(success=True, data=fields)
    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao obter fields da tabela",
            e,
            "Erro interno ao buscar estruturas das tabelas.",
        )


@router.post(
    "/fields/{connection_id}",
    response_model=ResponseWrapper[Dict[str, List[DBFieldOut]]],
)
async def get_fields_by_connection_id_and_table_names_endpoint(
    payload: FieldsBulkRequest,
    connection_id: int = Path(..., description="ID da conexão"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        _validate_connection_access_or_404(db, connection_id)

        table_names = [
            t.strip() for t in (payload.table_names or []) if t and t.strip()
        ]
        if not table_names:
            return ResponseWrapper(success=True, data={})

        fields_by_table = get_fields_info_bulk_cached_wrapper(
            connection_id=connection_id,
            table_names=table_names,
            user_id=user_id,
            db=db,
        )

        return ResponseWrapper(success=True, data=fields_by_table)

    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao obter fields bulk",
            e,
            "Erro interno ao buscar fields das tabelas.",
        )


@router.get("/table/{table_name}/count", response_model=ResponseWrapper[int])
async def get_table_count_endpoint(
    table_name: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        table_name = _validate_table_name(table_name)
        active = _get_active_connection_or_400(db, user_id)

        count = get_table_count_cached(active.connection_id, table_name, db, user_id)
        return ResponseWrapper(success=True, data=count)

    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            f"❌ Erro ao contar registros da tabela '{table_name}'",
            e,
            f"Erro interno ao contar registros da tabela '{table_name}'.",
        )


@router.get("/sync", response_model=ResponseWrapper[Any])
async def sync_connection_stats_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        stats = sync_connection_stats_cached(user_id, db)
        return ResponseWrapper(success=True, data=stats)
    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao sincronizar estatísticas",
            e,
            "Erro interno ao sincronizar estatísticas da conexão.",
        )


@router.get("/stream/tables/counts")
async def stream_table_counts_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        return get_table_count_streams(db, user_id)
    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Erro no stream de contagem de tabelas",
            e,
            "Erro interno no stream de contagem de tabelas.",
        )


# --------------------------------------------------
# Cache management
# --------------------------------------------------


@router.post("/cache/clear")
async def clear_cache_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        from app.config.cache_manager import clear_cache_for_function

        functions_to_clear = [
            "get_tables_with_count_cached",
            "get_tables_names_cached",
            "get_fields_info_bulk_cached_wrapper",
            "get_fields_info_cached_wrapper",
            "get_structures_names_only_cached",
            "get_structures_names_cached",
            "get_table_count_cached",
            "sync_connection_stats_cached",
        ]

        cleared_count = 0
        for func_name in functions_to_clear:
            cleared_count += clear_cache_for_function(func_name)

        log_message(
            f"✅ Cache limpo para usuário {user_id}: {cleared_count} funções",
            level="info",
        )

        return ResponseWrapper(
            success=True,
            data={"cleared_functions": cleared_count},
            # message="Cache limpo com sucesso",
        )

    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao limpar cache",
            e,
            "Erro interno ao limpar cache.",
        )


@router.get("/cache/info")
async def get_cache_info_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        from app.config.cache_manager import get_function_cache_info

        cache_info = {}
        functions_to_check = [
            "get_tables_with_count_cached",
            "get_tables_names_cached",
            "get_fields_info_bulk_cached_wrapper",
            "get_fields_info_cached_wrapper",
            "get_structures_names_only_cached",
            "get_structures_names_cached",
            "get_table_count_cached",
            "sync_connection_stats_cached",
        ]

        for func_name in functions_to_check:
            try:
                cache_info[func_name] = get_function_cache_info(func_name)
            except Exception as e:
                cache_info[func_name] = {"error": str(e)}

        return ResponseWrapper(
            success=True,
            data=cache_info,
            # message="Informações do cache obtidas com sucesso",s
        )

    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao obter informações do cache",
            e,
            "Erro interno ao obter informações do cache.",
        )


# --------------------------------------------------
# Health
# --------------------------------------------------


@router.get("/health")
async def health_check_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        active = _get_active_connection_or_400(db, user_id)
        connection = get_db_connection_by_id(db, active.connection_id)

        return ResponseWrapper(
            success=True,
            data={
                "user_id": user_id,
                "active_connection": {
                    "connection_id": active.connection_id,
                    "database_name": connection.database_name if connection else "N/A",
                    "connection_type": connection.type if connection else "N/A",
                },
                "cache_enabled": True,
                "status": "healthy",
            },
            # message="Conexão ativa e saudável",
        )

    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise_500(
            "❌ Health check falhou",
            e,
            "Erro no health check da conexão.",
        )
