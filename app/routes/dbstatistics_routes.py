from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any, List, Dict
from app.config.cache_manager import cache_result
from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.dbstructure_schema import DBStructureOut
from app.schemas.responsehttp_schema import ResponseWrapper
from app.schemas.queryhistory_schemas import TableInfo
from app.cruds.connection_cruds import get_active_connection_by_userid, get_db_connection_by_id
from app.services.stream_tables_counts_servvice import get_table_count_streams
from app.ultils.logger import log_message
from app.services.database_inspector import (
    get_strutures_names,
    get_table_names_with_count,
    get_table_names,
    get_table_count,
    sync_connection_statistics,
)

router = APIRouter(prefix="/consu", tags=["Consulta de Banco de Dados"])


# -----------------------------
# Utilitários para validações e cache
# -----------------------------
def _get_active_connection_or_400(db: Session, user_id: int):
    """Retorna a conexão ativa ou lança exceção HTTP 400"""
    active = get_active_connection_by_userid(db, user_id)
    if not active:
        raise HTTPException(status_code=400, detail="Nenhuma conexão ativa encontrada para este usuário.")
    return active

def _validate_table_name(table_name: str):
    """Valida o nome da tabela"""
    if not table_name or not table_name.strip():
        raise HTTPException(status_code=422, detail="O nome da tabela é obrigatório.")
    return table_name.strip()

# -----------------------------
# Funções com Cache
# -----------------------------

@cache_result(ttl=300, user_id="user_{user_id}")  # 5 minutos de cache
def get_tables_with_count_cached(connection_id: int, user_id: int, db: Session) -> List[Dict]:
    """Obtém tabelas com contagem com cache"""
    return get_table_names_with_count(connection_id, user_id, db)

@cache_result(ttl=600, user_id="user_{user_id}")  # 10 minutos de cache
def get_tables_names_cached(connection_id: int, user_id: int, db: Session) -> List[str]:
    """Obtém nomes de tabelas com cache"""
    return get_table_names(connection_id, user_id, db)



@cache_result(ttl=600, user_id="user_{user_id}")  # 10 minutos de cache
def get_strutures_names_cached(connection_id: int, user_id: int, db: Session) -> List[DBStructureOut]:
    """Obtém nomes de tabelas com cache"""
    return get_strutures_names(connection_id, user_id, db)

@cache_result(ttl=180, user_id="user_{user_id}")  # 3 minutos de cache (contagens mudam frequentemente)
def get_table_count_cached(connection_id: int, table_name: str, db: Session, user_id: int) -> int:
    """Obtém contagem de tabela com cache"""
    return get_table_count(connection_id, table_name, db, user_id)

@cache_result(ttl=1800, user_id="user_{user_id}")  # 30 minutos de cache para estatísticas
def sync_connection_stats_cached(user_id: int, db: Session) -> Any:
    """Sincroniza estatísticas com cache"""
    stats = sync_connection_statistics(user_id, db)
    if hasattr(stats, "__dict__"):
        stats = {k: v for k, v in stats.__dict__.items() if not k.startswith("_")}
    return stats

# -----------------------------
# Endpoints
# -----------------------------

@router.get("/tables-with-count", response_model=ResponseWrapper[List[TableInfo]])
def get_tables_with_count(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna lista de tabelas da conexão ativa com a contagem de registros.
    Cache: 5 minutos
    """
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
        log_message(f"❌ Erro ao obter tabelas com contagem: {e}", level="error")
        raise HTTPException(
            status_code=500, 
            detail="Erro interno ao buscar tabelas com contagem."
        )


@router.get("/tables", response_model=ResponseWrapper[List[str]])
def get_tables(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna apenas os nomes das tabelas da conexão ativa.
    Cache: 10 minutos
    """
    try:
        active = _get_active_connection_or_400(db, user_id)
        names = get_tables_names_cached(active.connection_id, user_id, db)
        
        return ResponseWrapper(success=True, data=names)
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro ao obter nomes de tabelas: {e}", level="error")
        raise HTTPException(
            status_code=500, 
            detail="Erro interno ao buscar nomes das tabelas."
        )
        
@router.get("/structures", response_model=ResponseWrapper[List[DBStructureOut]])  # Corrigido: era "/strutures"
def get_structures(  # Corrigido: nome da função era "get_tables"
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna as estruturas das tabelas da conexão ativa.
    Cache: 10 minutos
    """
    try:
        active = _get_active_connection_or_400(db, user_id)
        structures = get_strutures_names_cached(active.connection_id, user_id, db)  # Corrigido: variável "names" para "structures"
        
        return ResponseWrapper(success=True, data=structures)  # Corrigido: "names" para "structures"
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro ao obter estruturas de tabelas: {e}", level="error")  # Corrigido mensagem
        raise HTTPException(
            status_code=500, 
            detail="Erro interno ao buscar estruturas das tabelas."  # Corrigido mensagem
        )


@router.get("/table/{table_name}/count", response_model=ResponseWrapper[int])
def get_table_count_endpoint(
    table_name: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna a contagem de registros de uma tabela específica da conexão ativa.
    Cache: 3 minutos
    """
    try:
        # Validação do nome da tabela
        table_name = _validate_table_name(table_name)
        active = _get_active_connection_or_400(db, user_id)

        count = get_table_count_cached(active.connection_id, table_name, db, user_id)
        return ResponseWrapper(success=True, data=count)
        
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro ao contar registros da tabela '{table_name}': {e}", level="error")
        raise HTTPException(
            status_code=500, 
            detail=f"Erro interno ao contar registros da tabela '{table_name}'."
        )


@router.get("/sync", response_model=ResponseWrapper[Any])
def sync_connection_stats(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Sincroniza e salva estatísticas da conexão ativa (tabelas, views, procedures, etc.).
    Cache: 30 minutos
    """
    try:
        stats = sync_connection_stats_cached(user_id, db)
        return ResponseWrapper(success=True, data=stats)
        
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro ao sincronizar estatísticas: {e}", level="error")
        raise HTTPException(
            status_code=500, 
            detail="Erro interno ao sincronizar estatísticas da conexão."
        )


@router.get("/stream/tables/counts")
async def stream_table_counts(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Stream em tempo real das contagens de tabelas.
    SEM CACHE - Dados em tempo real
    """
    try:
        print("Iniciando stream de contagem de tabelas...")
        return get_table_count_streams(db, user_id)
        
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro no stream de contagem de tabelas: {e}", level="error")
        raise HTTPException(
            status_code=500, 
            detail="Erro interno no stream de contagem de tabelas."
        )


# -----------------------------
# Endpoints de Gerenciamento de Cache
# -----------------------------

@router.post("/cache/clear")
def clear_cache_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Limpa o cache das consultas do usuário atual.
    Útil para desenvolvimento ou quando os dados mudaram.
    """
    try:
        from app.config.cache_manager import clear_cache_for_function
        
        # Limpa cache das principais funções
        functions_to_clear = [
            "get_tables_with_count_cached",
            "get_tables_names_cached", 
            "get_table_count_cached",
            "sync_connection_stats_cached"
        ]
        
        cleared_count = 0
        for func_name in functions_to_clear:
            cleared_count += clear_cache_for_function(func_name)
        
        log_message(f"✅ Cache limpo para usuário {user_id}: {cleared_count} funções", level="info")
        
        return ResponseWrapper(
            success=True,
            data={"cleared_functions": cleared_count},
            message="Cache limpo com sucesso"
        )
        
    except Exception as e:
        log_message(f"❌ Erro ao limpar cache: {e}", level="error")
        raise HTTPException(
            status_code=500, 
            detail="Erro interno ao limpar cache."
        )


@router.get("/cache/info")
def get_cache_info_endpoint(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna informações sobre o cache das consultas.
    """
    try:
        from app.config.cache_manager import get_function_cache_info
        
        cache_info = {}
        functions_to_check = [
            "get_tables_with_count_cached",
            "get_tables_names_cached",
            "get_table_count_cached", 
            "sync_connection_stats_cached"
        ]
        
        for func_name in functions_to_check:
            try:
                cache_info[func_name] = get_function_cache_info(func_name)
            except Exception as e:
                cache_info[func_name] = {"error": str(e)}
        
        return ResponseWrapper(
            success=True,
            data=cache_info,
            message="Informações do cache obtidas com sucesso"
        )
        
    except Exception as e:
        log_message(f"❌ Erro ao obter informações do cache: {e}", level="error")
        raise HTTPException(
            status_code=500, 
            detail="Erro interno ao obter informações do cache."
        )


# -----------------------------
# Health Check e Status
# -----------------------------

@router.get("/health")
def health_check(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Health check da conexão do usuário.
    """
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
                    "connection_type": connection.type if connection else "N/A"
                },
                "cache_enabled": True,
                "status": "healthy"
            },
            message="Conexão ativa e saudável"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Health check falhou: {e}", level="error")
        raise HTTPException(
            status_code=500, 
            detail="Erro no health check da conexão."
        )