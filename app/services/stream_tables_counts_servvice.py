

import asyncio
import json
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from fastapi.responses import StreamingResponse
from app.config.dependencies import EngineManager, get_session_by_connection_id
from app.cruds.dbstatistics_crud import get_cached_row_count_all_tupla, update_or_create_cache
from app.ultils.logger import log_message


def get_table_count_streams(connection_id: int, db: Session, id_user: int) -> int:
    """
    Retorna a contagem de registros de uma tabela de forma segura e eficiente.
    - Retorna 0 se for uma view.
    - Usa quote para evitar SQL injection.
    - Faz fallback para -1 em caso de erro.
    """
    
    table_info = get_cached_row_count_all_tupla(db, connection_id)
    if table_info :
        # Se já tiver cache, envia os dados do cache primeiro
        async def cached_event_generator():
            try:
                for table, count in table_info:
                    data = json.dumps({"table": table, "count": count})
                    yield f"data: {data}\n\n"
                    await asyncio.sleep(0.05)  # Pequeno delay para não sobrecarregar o cliente
                yield "event: end\ndata: done\n\n"
            except Exception as e:
                yield f"event: error\ndata: {str(e)}\n\n"

        return StreamingResponse(cached_event_generator(), media_type="text/event-stream")
    
    engine = EngineManager.get(id_user) or get_session_by_connection_id(connection_id, db)

    async def event_generator():
        try:
            inspector = inspect(engine)
            tables = inspector.get_table_names(schema=inspector.default_schema_name)

            for table in tables:
                count = 0
                try:
                    with engine.connect() as conn:
                        query = text(f'SELECT COUNT(*) FROM "{table}"')
                        count = conn.execute(query).scalar() or 0
                except SQLAlchemyError as e:
                    log_message(f"⚠️ Erro ao contar registros da tabela {table}: {e}", "error")
                    count = -1

                # 🔹 Formato SSE (JSON para o frontend entender melhor)
                data = json.dumps({"table": table, "count": count})
                yield f"data: {data}\n\n"
                update_or_create_cache(db, connection_id, table, count)

                # Delay pequeno para não sobrecarregar
                await asyncio.sleep(0.1)

            # 🔹 Finaliza stream
            yield "event: end\ndata: done\n\n"

        except Exception as e:
            yield f"event: error\ndata: {str(e)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")
