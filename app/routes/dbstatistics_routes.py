from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any, List

from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.responsehttp_schema import ResponseWrapper
from app.schemas.queryhistory_schemas import TableInfo
from app.cruds.connection_cruds import get_active_connection_by_userid, get_db_connection_by_id
from app.services.stream_tables_counts_servvice import get_table_count_streams
from app.ultils.logger import log_message
from app.services.database_inspector import (
    get_table_names_with_count,
    get_table_names,
    get_table_count,
    sync_connection_statistics,
)

router = APIRouter(prefix="/consu", tags=["Consulta de Banco de Dados"])


# -----------------------------
# Utilitário para validações
# -----------------------------
def _get_active_connection_or_400(db: Session, user_id: int):
    """Retorna a conexão ativa ou lança exceção HTTP 400"""
    active = get_active_connection_by_userid(db, user_id)
    if not active:
        raise HTTPException(status_code=400, detail="Nenhuma conexão ativa encontrada para este usuário.")
    return active


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
    """
    active = _get_active_connection_or_400(db, user_id)

    try:
        tables = get_table_names_with_count(active.connection_id, user_id, db)
        return ResponseWrapper(
            success=True,
            data=[TableInfo(name=t["name"], rowcount=t["rowcount"]) for t in tables],
        )
    except Exception as e:
        log_message(f"❌ Erro ao obter tabelas com contagem: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno ao buscar tabelas com contagem.")


@router.get("/tables", response_model=ResponseWrapper[List[str]])
def get_tables(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna apenas os nomes das tabelas da conexão ativa.
    """
    active = _get_active_connection_or_400(db, user_id)

    try:
        names = get_table_names(active.connection_id, user_id, db)
        return ResponseWrapper(success=True, data=names)
    except Exception as e:
        log_message(f"❌ Erro ao obter nomes de tabelas: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno ao buscar nomes das tabelas.")


@router.get("/table/{table_name}/count", response_model=ResponseWrapper[int])
def get_table_count_endpoint(
    table_name: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna a contagem de registros de uma tabela específica da conexão ativa.
    """
    if not table_name or len(table_name.strip()) == 0:
        raise HTTPException(status_code=422, detail="O nome da tabela é obrigatório.")

    active = _get_active_connection_or_400(db, user_id)

    try:
        count = get_table_count(active.connection_id, table_name, db,user_id)
        return ResponseWrapper(success=True, data=count)
    except Exception as e:
        log_message(f"❌ Erro ao contar registros da tabela '{table_name}': {e}", level="error")
        raise HTTPException(status_code=500, detail=f"Erro interno ao contar registros da tabela '{table_name}'.")


@router.get("/sync", response_model=ResponseWrapper[Any])
def sync_connection_stats(
    # db_type: str = Query(..., description="Tipo do banco de dados (postgresql, mysql, sqlite, mssql)"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Sincroniza e salva estatísticas da conexão ativa (tabelas, views, procedures, etc.).
    """
    active = _get_active_connection_or_400(db, user_id)

    connection = get_db_connection_by_id(db, active.connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="Conexão ativa não encontrada no banco.")
    try:
        stats = sync_connection_statistics(active.connection_id, user_id, connection.type, db, connection.name)
        return ResponseWrapper(success=True, data=stats)
    except Exception as e:
        log_message(f"❌ Erro ao sincronizar estatísticas: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno ao sincronizar estatísticas da conexão.")
    


@router.get("/stream/tables/counts")
async def stream_table_counts(
    user_id: int,
    db: Session = Depends(get_db)
):
    active = _get_active_connection_or_400(db, user_id)
    
    print("Iniciando stream de contagem de tabelas...")
    return get_table_count_streams(active.connection_id, db, user_id)
    

