

import asyncio
import json
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session
from fastapi.responses import StreamingResponse
from app.cruds.dbstatistics_crud import get_cached_row_count_all_tupla, update_or_create_cache
from app.cruds.dbstructure_crud import get_db_structures_by_conn_id_and_table, get_fields_by_structure_pk
from app.services.editar_linha import quote_identifier
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message
from sqlalchemy.engine import Engine
def get_table_rowcount(
    db: Session,
    engine: Engine,
    table_name: str,
    connection_id: int,
    db_type: str,
    structure_id: int | None = None,
    schema: str | None = None
) -> dict:
    """
    Retorna a contagem de registros (real ou estimada) de uma tabela.
    Usa estatísticas específicas por SGBD quando disponível.

    Retorno:
        { "name": <nome_tabela>, "rowcount": <int> }
    """
    count = -1

    # Pega coluna principal para contagem, se existir
    column_obj = get_fields_by_structure_pk(db, structure_id) if structure_id else None
    column_name = quote_identifier(db_type,table_name+"."+column_obj.name) if column_obj else "*"

    full_table_name = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'

    try:
        with engine.connect() as conn:
            query = text(f'SELECT COUNT({column_name}) FROM {full_table_name}')
            count = conn.execute(query).scalar() or 0
    except Exception as e:
        log_message(f"❌ Erro ao contar registros da tabela '{table_name}': {e}", "error")
        count = -1

    # Atualiza cache
    update_or_create_cache(db, connection_id, table_name, count)
    # print("count:",count)

    return count

def get_table_count_streams(db: Session, id_user: int) -> int:
    """
    Retorna a contagem de registros de uma tabela de forma segura e eficiente.
    - Retorna 0 se for uma view.
    - Usa quote para evitar SQL injection.
    - Faz fallback para -1 em caso de erro.
    """
    engine,connection = ConnectionManager.ensure_connection(db, id_user)
    table_info = get_cached_row_count_all_tupla(db, connection.id)
    # log_message(f"{table_info}")
    if table_info :
        # Se já tiver cache, envia os dados do cache primeiro
        async def cached_event_generator():
            try:
                for table, count in table_info:
                    if count == -1 or count == '-1':
                        structure = get_db_structures_by_conn_id_and_table(db, connection.id, table)
                        count = get_table_rowcount(db,engine,table,connection.id,connection.type.lower() ,structure.id, structure.schema_name) 
                    data = json.dumps({"table": table, "count": count})
                    yield f"data: {data}\n\n"
                    await asyncio.sleep(0.05)  # Pequeno delay para não sobrecarregar o cliente
                yield "event: end\ndata: done\n\n"
            except Exception as e:
                yield f"event: error\ndata: {str(e)}\n\n"

        return StreamingResponse(cached_event_generator(), media_type="text/event-stream")
    
   

    async def event_generator():
        try:
            inspector = inspect(engine)
            tables = inspector.get_table_names(schema=inspector.default_schema_name)

            for table in tables:
                structure = get_db_structures_by_conn_id_and_table(db, connection.id, table)
                count = get_table_rowcount(db,engine,table,connection.id, connection.type.lower(), structure.id ,structure.schema_name) 
                # print(count)
                # 🔹 Formato SSE (JSON para o frontend entender melhor)
                data = json.dumps({"table": table, "count": count})
                yield f"data: {data}\n\n"
                
                # Delay pequeno para não sobrecarregar
                await asyncio.sleep(0.1)
                
            # 🔹 Finaliza stream
            yield "event: end\ndata: done\n\n"

        except Exception as e:
            log_message(f"{str(e)}", "error")
            yield f"event: error\ndata: {str(e)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")
