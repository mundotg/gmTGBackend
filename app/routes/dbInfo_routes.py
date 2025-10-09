# app/api/consu/metadata_db.py

import json
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
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


@router.get("/consu/metadata_db/", response_model=ResponseWrapper[DatabaseMetadata])
def get_metadata_db(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna informações estatísticas da base de dados ativa do usuário.
    """
    active = get_active_connection_by_userid(db, user_id)
    if not active:
        return ResponseWrapper(success=False, error="Nenhuma conexão ativa encontrada")

    connection = get_db_connection_by_id(db, active.connection_id)
    if not connection:
        return ResponseWrapper(success=False, error="Conexão não encontrada")
    
    stats = converter_stats(get_statistics_by_connection_geral(db, active.connection_id))
    if not stats:
        stats = sync_connection_statistics( user_id, db)
        if not stats:
            return ResponseWrapper(success=False, error="Falha ao obter estatísticas da conexão")

    table_info = get_table_names_with_count(active.connection_id, user_id, db)

    return ResponseWrapper(
        success=True,
        data=DatabaseMetadata(
            connectionName=connection.name,
            databaseName=connection.database_name,
            serverVersion=stats["server_version"],
            tableCount=stats["table_count"],
            viewCount=stats["view_count"],
            procedureCount=stats["procedure_count"],
            functionCount=stats["function_count"],
            triggerCount=stats["trigger_count"],
            indexCount=stats["index_count"],
            tableNames=[
                TableInfo(name=t["name"], rowcount=t["rowcount"]) for t in table_info
            ]
        )
    )


@router.get("/consu/metadata_fieds/{table_name}", response_model=MetadataTableResponse)
def get_metadata_fields(
    table_name: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retorna os metadados (colunas e tipos) de uma tabela específica.
    """
    try:
        return sincronizar_metadados_da_tabela(db, table_name, user_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao obter metadados: {str(e)}"
        )

@router.get(
    "/consu/linha-completa/{index}",
    response_model=ResponseWrapper[TableRow],
    summary="Buscar linha completa da tabela",
    description="""
    Retorna uma linha completa da tabela com base em:
    - **Chave primária (`primary_key_value`)**, ou
    - **Índice (`index`) com ordenação (`orderby`)**.

    Obs.: Informe **ou** `primary_key_value` **ou** `index`.  
    """
)
def get_row_data_by_index(
    index: int,
    table_name: str = Query(..., description="Nome da tabela"),
    primary_key_field: str = Query(..., description="Campo de chave primária ou identificador"),
    primary_key_value: str = Query(..., description="Valor da chave primária"),
    col_type: str = Query(..., description="tipo da chave"),
    order_by: str = Query("[]", description="Array de ordenação em formato JSON"),  # vem do frontend como string
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    🔎 Busca uma< linha completa no banco de dados:
    - Se `primary_key_value` for informado → busca direta pela chave.
    - Caso contrário → busca pela posição (`index`) usando `ORDER BY`.
    """
    # -----------------------------#
    # 🔹 Conexão ativa do usuário  #
    # -----------------------------#
    active = get_active_connection_by_userid(db, user_id)
    if not active:
        return ResponseWrapper(success=False, error="Nenhuma conexão ativa encontrada")

    connection = get_db_connection_by_id(db, active.connection_id)
    if not connection:
        return ResponseWrapper(success=False, error="Conexão não encontrada")

    engine = EngineManager.get(user_id)
    if not engine:
        res = reativar_connection(db=db, id_user=user_id)
        if not res["success"]:
            raise HTTPException(status_code=400, detail="Conexão do banco de dados não encontrada")
        engine = EngineManager.get(user_id)

    # -------------------------
    # 🔹 Execução da consulta
    # -------------------------
    try:
        
        order_by_list = [OrderByOption(**item) for item in json.loads(order_by)]
        row_data = pesquisar_in_db(
            engine=engine,
            db_type=connection.type,
            campo_primary_key=primary_key_field,
            table_name=table_name,
            selected_row_index=index if not primary_key_value else None,
            col_type= col_type,
            orderby=order_by_list,
            primary_key_value=primary_key_value,
        )

        if not row_data:
            return ResponseWrapper(success=False, error="Nenhuma linha encontrada")

        return ResponseWrapper(success=True, data=TableRow(__root__=row_data))

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro ao buscar linha completa: {e}", level="error")
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno ao buscar linha completa: {str(e)}"
        )
