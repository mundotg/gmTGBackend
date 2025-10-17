# app/api/consu/metadata_db.py

import json
from typing import Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.config.cache_manager import cache_result
from app.config.dependencies import EngineManager
from app.cruds.dbstatistics_crud import converter_stats, get_statistics_by_connection_geral
from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.dbstructure_schema import MetadataTableResponse
from app.schemas.query_select_upAndInsert_schema import OrderByOption
from app.schemas.queryhistory_schemas import DatabaseMetadata, TableInfo
from app.schemas.responsehttp_schema import ResponseWrapper, TableRow
from app.services.database_inspector import get_table_names_with_count, sync_connection_statistics
from app.services.field_info import sincronizar_metadados_da_tabela
from app.services.pesquizar_index_linha_in_bd import pesquisar_in_db
from app.cruds.connection_cruds import get_active_connection_by_userid, get_db_connection_by_id
from app.ultils.ativar_session_bd import reativar_connection
from app.ultils.logger import log_message

router = APIRouter()

# -------------------------------
# Funções Auxiliares Reutilizáveis
# -------------------------------

def _get_user_connection_info(user_id: int, db: Session) -> Dict[str, Any]:
    """
    Obtém informações da conexão ativa do usuário.
    Centraliza a lógica repetitiva.
    """
    active = get_active_connection_by_userid(db, user_id)
    if not active:
        raise HTTPException(status_code=404, detail="Nenhuma conexão ativa encontrada")

    connection = get_db_connection_by_id(db, active.connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="Conexão não encontrada")
    
    return {
        "active": active,
        "connection": connection
    }

def _get_user_engine(user_id: int, db: Session):
    """
    Obtém ou reativa a engine do usuário.
    """
    engine = EngineManager.get(user_id)
    if not engine:
        res = reativar_connection(db=db, id_user=user_id)
        if not res["success"]:
            raise HTTPException(status_code=500, detail="Não foi possível reativar a conexão")
        engine = EngineManager.get(user_id)
    
    return engine

def _safe_json_loads(json_str: str, default: Any = None) -> Any:
    """
    Carrega JSON de forma segura com fallback.
    """
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default if default is not None else []

# -------------------------------
# Funções com Cache Corrigidas
# -------------------------------

@cache_result(ttl=900, user_id="user_{user_id}")  # 🔥 AGORA com user_id na chave
def get_metadata_db_cached(user_id: int, db: Session):
    """
    Obtém metadados do banco com cache específico por usuário.
    """
    connection_info = _get_user_connection_info(user_id, db)
    connection = connection_info["connection"]
    
    # Obtém estatísticas
    stats = converter_stats(get_statistics_by_connection_geral(db, connection_info["active"].connection_id))
    if not stats:
        stats = sync_connection_statistics(user_id, db)
        if not stats:
            raise HTTPException(status_code=500, detail="Falha ao obter estatísticas da conexão")

    # Obtém informações das tabelas
    table_info = get_table_names_with_count(connection_info["active"].connection_id, user_id, db)

    return {
        "connectionName": connection.name,
        "databaseName": connection.database_name,
        "serverVersion": stats["server_version"],
        "tableCount": stats["table_count"],
        "viewCount": stats["view_count"],
        "procedureCount": stats["procedure_count"],
        "functionCount": stats["function_count"],
        "triggerCount": stats["trigger_count"],
        "indexCount": stats["index_count"],
        "tableNames": table_info,
    }

@cache_result(ttl=900, user_id="user_{user_id}")
def get_table_metadata_cached(table_name: str, user_id: int, db: Session):
    """
    Obtém metadados de tabela com cache específico por usuário.
    """
    # Validação adicional do nome da tabela
    if not table_name or not table_name.strip():
        raise HTTPException(status_code=400, detail="Nome da tabela inválido")
    
    return sincronizar_metadados_da_tabela(db, table_name.strip(), user_id)

@cache_result(ttl=900, user_id="user_{user_id}")
def get_row_data_by_index_cached(
    index: int, 
    table_name: str, 
    primary_key_field: str, 
    primary_key_value: str, 
    col_type: str, 
    order_by: List[Dict], 
    user_id: int, 
    db: Session
):
    """
    Busca dados de linha com cache específico por usuário.
    """
    # Validação de parâmetros
    if not table_name.strip():
        raise HTTPException(status_code=400, detail="Nome da tabela inválido")
    
    if not primary_key_field.strip():
        raise HTTPException(status_code=400, detail="Campo de chave primária inválido")

    connection_info = _get_user_connection_info(user_id, db)
    connection = connection_info["connection"]
    engine = _get_user_engine(user_id, db)

    try:
        # Converte order_by para objetos OrderByOption
        order_by_list = [OrderByOption(**item) for item in order_by]
        
        row_data = pesquisar_in_db(
            engine=engine,
            db_type=connection.type,
            campo_primary_key=primary_key_field.strip(),
            table_name=table_name.strip(),
            selected_row_index=index if not primary_key_value else None,
            col_type=col_type,
            orderby=order_by_list,
            primary_key_value=primary_key_value,
        )

        if not row_data:
            raise HTTPException(status_code=404, detail="Nenhuma linha encontrada")

        return {"data": row_data}

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro ao buscar linha completa: {e}", level="error")
        raise HTTPException(status_code=500, detail=f"Erro interno ao buscar linha: {str(e)}")

# -------------------------------
# Endpoints Principais
# -------------------------------

@router.get("/consu/metadata_db/", response_model=ResponseWrapper[DatabaseMetadata])
def get_metadata_db(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    🔍 Retorna metadados do banco de dados ativo do usuário.
    """
    try:
        result = get_metadata_db_cached(user_id, db)
        
        return ResponseWrapper(
            success=True,
            data=DatabaseMetadata(
                connectionName=result["connectionName"],
                databaseName=result["databaseName"],
                serverVersion=result["serverVersion"],
                tableCount=result["tableCount"],
                viewCount=result["viewCount"],
                procedureCount=result["procedureCount"],
                functionCount=result["functionCount"],
                triggerCount=result["triggerCount"],
                indexCount=result["indexCount"],
                tableNames=[TableInfo(name=t["name"], rowcount=t["rowcount"]) for t in result["tableNames"]]
            )
        )
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro inesperado em get_metadata_db: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/consu/metadata_fieds/{table_name}", response_model=MetadataTableResponse)
def get_metadata_fields(
    table_name: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    📋 Retorna os metadados (colunas e tipos) de uma tabela específica.
    """
    try:
        return get_table_metadata_cached(table_name, user_id, db)
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro inesperado em get_metadata_fields: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get(
    "/consu/linha-completa/{index}",
    response_model=ResponseWrapper[TableRow],
    summary="Buscar linha completa da tabela",
    description="""
    🔎 Retorna uma linha completa da tabela com base em:
    - **Chave primária (`primary_key_value`)**, ou
    - **Índice (`index`) com ordenação (`orderby`)**.

    ⚠️ Obs.: Informe **ou** `primary_key_value` **ou** `index`.  
    """
)
def get_row_data_by_index(
    index: int,
    table_name: str = Query(..., description="Nome da tabela"),
    primary_key_field: str = Query(..., description="Campo de chave primária ou identificador"),
    primary_key_value: str = Query(None, description="Valor da chave primária"),  # 🔥 Tornado opcional
    col_type: str = Query(..., description="Tipo da chave"),
    order_by: str = Query("[]", description="Array de ordenação em formato JSON"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    🔍 Busca uma linha completa no banco de dados.
    """
    try:
        # Validação: primary_key_value ou index, mas não ambos
        if primary_key_value and index != 0:  # index 0 é o padrão quando não usado
            raise HTTPException(
                status_code=400, 
                detail="Informe apenas primary_key_value OU index, não ambos"
            )
            
        # Converte order_by de forma segura
        order_by_parsed = _safe_json_loads(order_by, [])
        
        result = get_row_data_by_index_cached(
            index=index,
            table_name=table_name,
            primary_key_field=primary_key_field,
            primary_key_value=primary_key_value or "",  # Garante string vazia se None
            col_type=col_type,
            order_by=order_by_parsed,
            user_id=user_id,
            db=db
        )
        
        # Verifica se houve erro na função cached
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return ResponseWrapper(success=True, data=result["data"])
        
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro inesperado em get_row_data_by_index: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")