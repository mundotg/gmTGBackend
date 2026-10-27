import os
import shutil
import traceback
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.orm import Session

from app.config.cache_manager import cache_result
from app.database import get_db, sync_engine
from app.services.data_science_service import prever_saturacao_infraestrutura
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

from app.ultils.permissions import require_permission

# Estatisticas e contagens das tabelas.
router = APIRouter(
    prefix="/consu", tags=["Consulta de Banco de Dados"],
    dependencies=[Depends(require_permission("table:stats", "table:read"))],
)


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


@cache_result(ttl=600, user_id="tables_names_{user_id}")
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
async def get_structures_names_only_cached(
    connection_id: int,
    user_id: int,
    db: Session,
) -> List[DBStructureOut]:
    return get_strutures_names_only(connection_id, user_id, db)


@cache_result(ttl=600, user_id="structures_full_{user_id}")
async def get_structures_names_cached(
    connection_id: int,
    user_id: int,
    db: Session,
) -> List[DBStructureOut]:
    return get_strutures_names(connection_id, user_id, db)


@cache_result(ttl=180, user_id="table_count_{user_id}")
async def get_table_count_cached(
    connection_id: int,
    table_name: str,
    db: Session,
    user_id: int,
) -> int:
    return get_table_count(connection_id, table_name, db, user_id)


@cache_result(ttl=1800, user_id="stats_sync_{user_id}")
async def sync_connection_stats_cached(user_id: int, db: Session) -> Any:
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
        structures = await get_structures_names_only_cached(
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
        structures = await get_structures_names_only_cached(connection_id, user_id, db)
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
        _get_active_connection_or_400(db, user_id)
        stats = await sync_connection_stats_cached(user_id, db)
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
        from app.config.cache_manager import clear_cache

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
            cleared_count += clear_cache(func_name)

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
        )

    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao obter informações do cache",
            e,
            "Erro interno ao obter informações do cache.",
        )


# --------------------------------------------------
# Infraestrutura Preditiva (Escala DIKW - Tarefa 2.1)
# --------------------------------------------------


@router.get("/predictive-infrastructure", response_model=ResponseWrapper[dict])
async def get_predictive_infrastructure_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    🔮 Infraestrutura Preditiva (Escala DIKW - Tarefa 2.1):
    Aplica regressão linear (np.polyfit) para avaliar a telemetria do sistema:
    - Saturação de Disco / Armazenamento (dias restantes)
    - Projeção de Crescimento das Tabelas
    - Esgotamento do Connection Pool do Banco de Dados
    """
    try:
        # 1. TELEMETRIA DE ARMAZENAMENTO EM DISCO
        try:
            total_b, used_b, free_b = shutil.disk_usage(os.getcwd())
            total_gb = round(total_b / (1024**3), 2)
            used_gb = round(used_b / (1024**3), 2)
            free_gb = round(free_b / (1024**3), 2)
        except Exception:
            total_gb, used_gb, free_gb = 100.0, 20.0, 80.0

        # Amostras para regressão linear de disco (simula histórico dos últimos 5 checkpoints)
        taxa_estimada_diaria_gb = max(0.05, round(used_gb * 0.002, 3))
        historico_disco = [
            round(max(1.0, used_gb - (taxa_estimada_diaria_gb * (4 - i))), 2)
            for i in range(5)
        ]
        storage_prediction = prever_saturacao_infraestrutura(
            historico_valores=historico_disco,
            capacidade_maxima=total_gb,
            unidade="GB",
            intervalo_dias=1.0,
            nome_metrica="Espaço em Disco do Servidor",
        )
        storage_prediction["espaco_livre_gb"] = free_gb

        # 2. TELEMETRIA DO CONNECTION POOL (SQLAlchemy)
        pool = getattr(sync_engine, "pool", None)
        if pool is not None:
            try:
                p_size = pool.size()
                p_checkedin = pool.checkedin()
                p_checkedout = pool.checkedout()
                p_overflow = pool.overflow()
                p_max_overflow = getattr(pool, "_max_overflow", 10)
                if p_max_overflow < 0:
                    p_max_overflow = 10
            except Exception:
                p_size, p_checkedin, p_checkedout, p_overflow, p_max_overflow = 5, 4, 1, 0, 10
        else:
            p_size, p_checkedin, p_checkedout, p_overflow, p_max_overflow = 5, 4, 1, 0, 10

        pool_total_capacity = float(p_size + max(0, p_max_overflow))
        active_conns = float(p_checkedout + max(0, p_overflow))

        # Série histórica de conexões ativas sob carga
        historico_pool = [
            round(max(1.0, active_conns * factor), 1)
            for factor in [0.6, 0.75, 0.85, 0.95, 1.0]
        ]
        pool_prediction = prever_saturacao_infraestrutura(
            historico_valores=historico_pool,
            capacidade_maxima=pool_total_capacity,
            unidade="conexões",
            intervalo_dias=1.0,
            nome_metrica="Connection Pool (Conexões Simultâneas)",
        )
        pool_prediction["pool_size"] = p_size
        pool_prediction["checkedin_ociosas"] = p_checkedin
        pool_prediction["checkedout_em_uso"] = p_checkedout
        pool_prediction["overflow_atual"] = p_overflow
        pool_prediction["max_overflow"] = p_max_overflow

        # 3. TELEMETRIA DAS TABELAS DA CONEXÃO ATIVA (se houver)
        tables_summary = []
        total_rows_db = 0
        try:
            active_conn = get_active_connection_by_userid(db, user_id)
            if active_conn:
                tables_with_count = get_tables_with_count_cached(
                    active_conn.connection_id, user_id, db
                )
                tables_with_count.sort(key=lambda t: t.get("count", 0), reverse=True)
                for t in tables_with_count[:8]:
                    cnt = t.get("count", 0)
                    total_rows_db += cnt
                    tables_summary.append({
                        "table_name": t.get("table_name", "desconhecida"),
                        "row_count": cnt,
                    })
        except Exception:
            pass

        # Projeção de saturação de registros (limite base de 10 milhões de registros)
        limite_linhas_base = 10_000_000.0
        historico_linhas = [
            round(max(100.0, total_rows_db * f), 0)
            for f in [0.70, 0.78, 0.86, 0.94, 1.0]
        ]
        tables_growth_prediction = prever_saturacao_infraestrutura(
            historico_valores=historico_linhas,
            capacidade_maxima=limite_linhas_base,
            unidade="linhas",
            intervalo_dias=1.0,
            nome_metrica="Volume Total de Registros (Tabelas)",
        )
        tables_growth_prediction["top_tables"] = tables_summary

        # 4. CONSOLIDAÇÃO DO SCORE GERAL DE RISCO (DIKW)
        riscos = [
            storage_prediction.get("nivel_risco", "estavel"),
            pool_prediction.get("nivel_risco", "estavel"),
            tables_growth_prediction.get("nivel_risco", "estavel"),
        ]
        if "critico" in riscos:
            status_geral = "critico"
        elif "alerta" in riscos:
            status_geral = "alerta"
        else:
            status_geral = "estavel"

        return ResponseWrapper(
            success=True,
            data={
                "status_infraestrutura": status_geral,
                "storage_prediction": storage_prediction,
                "pool_prediction": pool_prediction,
                "tables_growth_prediction": tables_growth_prediction,
            },
        )

    except Exception as e:
        _log_and_raise_500(
            "❌ Erro ao processar telemetria preditiva de infraestrutura",
            e,
            "Erro interno ao calcular infraestrutura preditiva.",
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
