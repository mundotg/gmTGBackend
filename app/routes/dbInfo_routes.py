# app/api/consu/metadata_db.py

import json
from typing import Dict, Any, Optional
from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from sqlalchemy.orm import Session
from pydantic import ValidationError

from app.config.cache_manager import cache_result # <- Assumindo que você crie isso
from app.config.engine_manager_cache import EngineManager
from app.cruds.dbstatistics_crud import converter_stats, get_statistics_by_connection_geral
from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.dbstructure_schema import MetadataTableResponse
from app.schemas.query_select_upAndInsert_schema import OrderByOption, QueryPayload
from app.schemas.queryhistory_schemas import DatabaseMetadata, TableInfo
from app.schemas.responsehttp_schema import ResponseWrapper, TableRow
from app.services.database_inspector import get_table_names_with_count, sync_connection_statistics
from app.services.field_info import sincronizar_metadados_da_tabela
from app.services.pesquizar_index_linha_in_bd import pesquisar_in_db
from app.cruds.connection_cruds import get_active_connection_by_userid, get_db_connection_by_id
from app.ultils.QueryExecutionService import QueryExecutionService
from app.ultils.ativar_session_bd import reativar_connection
from app.ultils.logger import log_message

router = APIRouter()

# -------------------------------
# Funções Auxiliares Reutilizáveis
# -------------------------------

def _get_user_connection_info(user_id: int, db: Session) -> Dict[str, Any]:
    active = get_active_connection_by_userid(db, user_id)
    if not active:
        raise HTTPException(status_code=404, detail="Nenhuma conexão ativa encontrada. Por favor, conecte-se a um banco de dados.")

    connection = get_db_connection_by_id(db, active.connection_id) # type: ignore
    if not connection:
        raise HTTPException(status_code=404, detail="As credenciais desta conexão não foram encontradas.")
    
    return {"active": active, "connection": connection}

def _get_user_engine(user_id: int, db: Session):
    engine = EngineManager.get(user_id)
    if not engine:
        log_message(f"Engine não encontrada para user {user_id}. Tentando reativar...", level="warning")
        res = reativar_connection(db=db, id_user=user_id)
        if not res.get("success"):
            raise HTTPException(status_code=503, detail="Não foi possível reestabelecer a conexão com o Banco de Dados. Verifique se o servidor está online.")
        engine = EngineManager.get(user_id)
    
    return engine

def _safe_json_loads(json_str: str, default: Any = None) -> Any:
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        log_message("Aviso: Falha ao decodificar JSON recebido na URL.", level="warning")
        return default if default is not None else []

# -------------------------------
# Funções com Cache
# -------------------------------

@cache_result(ttl=900, user_id="user_metadata_{user_id}")  
def get_metadata_db_cached(user_id: int, db: Session):
    connection_info = _get_user_connection_info(user_id, db)
    connection = connection_info["connection"]
    
    stats = converter_stats(get_statistics_by_connection_geral(connection_info["active"].connection_id))
    if not stats:
        stats = sync_connection_statistics(user_id, db)
        if not stats:
            raise HTTPException(status_code=500, detail="Falha ao obter estatísticas da conexão")

    # Passamos o schema para filtrar tabelas do sistema massivas
    table_info = get_table_names_with_count(connection_info["active"].connection_id, user_id, db)

    return {
        "connectionName": connection.name,
        "databaseName": connection.database_name,
        "serverVersion": stats.get("server_version", "Desconhecido"),
        "tableCount": stats.get("table_count", 0),
        "viewCount": stats.get("view_count", 0),
        "procedureCount": stats.get("procedure_count", 0),
        "functionCount": stats.get("function_count", 0),
        "triggerCount": stats.get("trigger_count", 0),
        "indexCount": stats.get("index_count", 0),
        "tableNames": table_info,
    }

# -------------------------------
# Endpoints Principais
# -------------------------------

@router.get("/consu/metadata_db/", response_model=ResponseWrapper[DatabaseMetadata])
def get_metadata_db(
    schema: Optional[str] = Query(None, description="Filtra tabelas por schema (ex: public, dbo)"),
    refresh_cache: bool = Query(False, description="Força a atualização dos metadados ignorando o cache"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    🔍 Retorna metadados gerais do banco de dados ativo do usuário.
    """
    try:
        # Mecanismo para bypassar cache se o usuário clicar no botão "Recarregar" na UI
        if refresh_cache:
            # invalidate_cache(func_name="get_metadata_db_cached", user_id=f"user_{user_id}")
            pass 

        result = get_metadata_db_cached(user_id, schema, db)
        
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
                tableNames=[TableInfo(name=t["name"], rowcount=t.get("rowcount", 0)) for t in result["tableNames"]]
            )
        )
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro inesperado em get_metadata_db: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno ao carregar metadados do banco.")


@router.get("/consu/metadata_fieds/{table_name}", response_model=MetadataTableResponse)
def get_metadata_fields(
    table_name: str,
    schema: Optional[str] = Query(None, description="Schema da tabela"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    📋 Retorna os metadados (colunas e tipos) de uma tabela específica.
    """
    if not table_name or not table_name.strip():
        raise HTTPException(status_code=400, detail="Nome da tabela não pode estar vazio.")
        
    try:
        # Monta o nome completo (ex: dbo.users) para segurança
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        return sincronizar_metadados_da_tabela(db, full_table_name.strip(), user_id)
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro em get_metadata_fields [{table_name}]: {e}", level="error")
        raise HTTPException(status_code=500, detail="Não foi possível ler as colunas desta tabela.")


@router.get(
    "/consu/linha-completa/{index}",
    response_model=ResponseWrapper[TableRow],
    summary="Buscar linha completa da tabela",
)
def get_row_data_by_index(
    index: int,
    table_name: str = Query(..., description="Nome da tabela"),
    schema: Optional[str] = Query(None, description="Schema da tabela"),
    primary_key_field: str = Query(..., description="Campo identificador (PK)"),
    primary_key_value: str = Query(None, description="Valor da chave primária para busca direta"),
    col_type: str = Query(..., description="Tipo de dado da chave primária"),
    order_by: str = Query("[]", description="JSON contendo a regra de ordenação"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    🔍 Busca uma linha completa no banco de dados. 
    Protegido contra ambiguidades na pesquisa.
    """
    try:
        if primary_key_value and index != 0:
            raise HTTPException(status_code=400, detail="Informe 'primary_key_value' OU 'index', não ambos simultaneamente.")
            
        order_by_parsed = _safe_json_loads(order_by, [])
        
        # Validar Pydantic models para garantir que o json não traz injeções
        try:
            order_by_list = [OrderByOption(**item) for item in order_by_parsed]
        except ValidationError:
            raise HTTPException(status_code=422, detail="Formato de 'order_by' inválido.")

        full_table_name = f"{schema}.{table_name}" if schema else table_name

        connection_info = _get_user_connection_info(user_id, db)
        engine = _get_user_engine(user_id, db)

        # A lógica de pesquisa foi extraída do cache na rota, pois dados mudam a cada segundo em bancos vivos
        row_data = pesquisar_in_db(
            engine=engine,
            db_type=connection_info["connection"].type,
            campo_primary_key=primary_key_field.strip(),
            table_name=full_table_name.strip(),
            selected_row_index=index if not primary_key_value else None,
            col_type=col_type,
            orderby=order_by_list,
            primary_key_value=primary_key_value,
        )

        if not row_data:
            raise HTTPException(status_code=404, detail="O registro solicitado não foi encontrado ou foi excluído.")

        return ResponseWrapper(success=True, data=row_data)

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro em get_row_data_by_index: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro ao buscar o registro detalhado no banco de dados.")
    

@router.post(
    "/consu/query_line/{index}",
    response_model=ResponseWrapper[TableRow],
    summary="Buscar linha completa de uma consulta por index",
)
async def get_row_data_query_by_index(
    index: int = Path(..., ge=0, description="O índice (offset) exato da linha desejada"),
    payload: QueryPayload = Body(..., description="As configurações da query original"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    🔍 Busca uma linha completa no banco de dados com base na query original e no índice. 
    Protegido contra ambiguidades na pesquisa e limites de paginação.
    """
    try:
        # 1. Blindagem no Backend: Forçamos o limit e offset para garantir apenas 1 linha
        payload.offset = index
        payload.limit = 1
        # Desativamos a contagem total, pois só queremos os dados desta linha
        payload.isCountQuery = False 

        query_service = QueryExecutionService()
        result = await query_service.execute_query(payload, db, user_id)

        # 2. Valida se retornou dados válidos
        if not result or not result.get("preview"):
            raise HTTPException(
                status_code=404, 
                detail="O registro solicitado não foi encontrado. Pode ter sido excluído ou a query foi alterada."
            )
        
        # 3. Extrai apenas o objeto (primeira e única linha da lista)
        row_data = result["preview"][0]
        # print(row_data)
        # print(result["columns"])

        return ResponseWrapper(success=True, data=row_data)

    except HTTPException:
        # Repassa as exceções HTTP que nós mesmos levantamos
        raise
    except Exception as e:
        # 4. Log detalhado para o backend
        import traceback
        log_message(f"❌ Erro em get_row_data_query_by_index: {e}\n{traceback.format_exc()}", level="error")
        raise HTTPException(
            status_code=500, 
            detail="Erro interno ao buscar o registro detalhado no banco de dados."
        )