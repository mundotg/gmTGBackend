import asyncio
import time
import json
from app.database import SessionLocal
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session
from fastapi.responses import StreamingResponse
from typing import List, Tuple, Optional
from app.config.cache_manager import cache_result
from app.cruds.dbstatistics_crud import get_cached_row_count_all_tupla, update_or_create_cache
from app.cruds.dbstructure_crud import get_db_structures_by_conn_id_and_table, get_fields_by_structure_pk
from app.services.database_inspector import verificar_ou_atualizar_estrutura
from app.services.editar_linha import quote_identifier
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message
from sqlalchemy.engine import Engine
# -----------------------------
# Funções com Cache
# -----------------------------

@cache_result(ttl=180, user_id="user_table_rowcount{user_id}")  # 3 minutos de cache para contagens
def get_table_rowcount_cached(
    db: Session,
    engine: Engine,
    table_name: str,
    connection_id: int,
    db_type: str,
    structure_id: Optional[int] = None,
    schema: Optional[str] = None
) -> dict | None:
    """
    Retorna a contagem de registros (real ou estimada) de uma tabela com cache.
    """
    return get_table_rowcount(db, engine, table_name, connection_id, db_type, structure_id, schema)

@cache_result(ttl=300, user_id="user_count_all_tupla_{user_id}")  # 5 minutos de cache para lista de tabelas
def get_cached_table_info(
    db: Session, 
    connection_id: int
) -> List[Tuple[str, int]]:
    """
    Obtém informações de tabelas em cache.
    """
    return get_cached_row_count_all_tupla( connection_id)


@cache_result(ttl=600, user_id="user_structures_by_conn_id_and_table_{user_id}") 
def get_db_structure_cached(
    db: Session,
    connection_id: int,
    table_name: str
):
    """
    Obtém estrutura da tabela com cache.
    """

    try:
        if connection_id <= 0:
            raise ValueError("connection_id inválido")

        if not table_name or not table_name.strip():
            raise ValueError("table_name inválido")

        return get_db_structures_by_conn_id_and_table(
            db,
            connection_id,
            table_name
        )

    except ValueError as e:
        log_message(
            message=f"Erro de validação em get_db_structure_cached | conn_id={connection_id} | table={table_name} | erro={str(e)}",
            level="warning",
        )
        raise

    except Exception as e:
        log_message(
            message=f"Erro ao obter estrutura em cache | conn_id={connection_id} | table={table_name} | erro={str(e)}",
            level="error",
        )
        raise

# -----------------------------
# Utilitários
# -----------------------------



def _get_table_count_query(db_type: str, table_name: str, schema: Optional[str], column_name: Optional[str]) -> str:
    """
    Gera query otimizada para contagem baseada no tipo de banco.
    """
    full_table_name = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    column = quote_identifier(db_type, f"{table_name}.{column_name}") if column_name else "*"
    
    # Query base para todos os bancos
    return f'SELECT COUNT({column}) FROM {full_table_name}'

def _estimate_table_count(engine: Engine, table_name: str, schema: Optional[str], db_type: str) -> int:
    """
    Tenta obter contagem estimada para tabelas muito grandes.
    """
    try:
        with engine.connect() as conn:
            if db_type == "postgresql":
                # PostgreSQL - usa estatísticas do sistema
                query = text("""
                    SELECT n_live_tup 
                    FROM pg_stat_user_tables 
                    WHERE schemaname = :schema AND relname = :table
                """)
                result = conn.execute(query, {"schema": schema or "public", "table": table_name}).scalar()
                return result or -1
            elif db_type == "mysql":
                # MySQL - usa INFORMATION_SCHEMA
                query = text("""
                    SELECT TABLE_ROWS 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table
                """)
                result = conn.execute(query, {"table": table_name}).scalar()
                return result or -1
            else:
                return -1  # Não suporta estimativa para outros bancos
    except Exception:
        return -1

# -----------------------------
# Funções Principais
# -----------------------------

def get_table_rowcount(
    db: Session,
    engine: Engine,
    table_name: str,
    connection_id: int,
    db_type: str,
    structure_id: Optional[int] = None,
    schema: Optional[str] = None
) -> dict | None:
    """
    Retorna a contagem de registros (real ou estimada) de uma tabela.
    Usa estatísticas específicas por SGBD quando disponível.

    Retorno:
        { "name": <nome_tabela>, "rowcount": <int> }
    """
    count = -1

    try:
        # Pega coluna principal para contagem otimizada, se existir
        column_obj = get_fields_by_structure_pk(db, structure_id) if structure_id else None
        column_name = column_obj.name if column_obj else None
        
        # Gera query otimizada
        query_text = _get_table_count_query(db_type, table_name, schema, column_name)
        
        with engine.connect() as conn:
            # Tenta contagem exata primeiro
            count = conn.execute(text(query_text)).scalar() or 0
            
            # Se contagem for muito alta (> 1M), considera usar estimativa no futuro
            if count > 1000000:
                log_message(f"📊 Tabela '{table_name}' tem {count} registros - considere usar estimativas", level="info")

    except Exception as e:
        log_message(f"❌ Erro ao contar registros da tabela '{table_name}': {e}", "error")
        
        # Fallback para estimativa se contagem exata falhar
        count = _estimate_table_count(engine, table_name, schema, db_type)
        if count == -1:
            log_message(f"⚠️ Não foi possível obter contagem para '{table_name}'", level="warning")

    # Atualiza cache
        update_or_create_cache(db, connection_id, table_name, count)

        return { "name": table_name, "rowcount": count }


def get_table_count_streams(db: Session, id_user: int) -> StreamingResponse:

    def sse_data(payload: dict) -> str:
        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

    def sse_event(event_name: str, payload: dict) -> str:
        return f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    try:
        if id_user <= 0:
            raise ValueError("id_user inválido")

        # 🔥 pega conexão UMA VEZ só
        engine, connection = ConnectionManager.ensure_connection(db, id_user)

        if not engine or not connection:
            raise ValueError("Não foi possível estabelecer conexão com o banco")

        connection_id = connection.id
        connection_type = (connection.type or "").lower()

        if not connection_type:
            raise ValueError("Tipo de conexão inválido")

        table_info = get_cached_table_info(db, connection_id)

        # =========================================================
        # 🔵 CACHE STREAM
        # =========================================================
        if table_info:

            async def cached_event_generator():
                db_local = SessionLocal()
                processed_tables = set()
                loop = asyncio.get_event_loop()

                try:
                    for item in table_info:
                        table = None

                        try:
                            if not item or len(item) < 2:
                                continue

                            table, count = item

                            if not table or table in processed_tables:
                                continue

                            processed_tables.add(table)

                            if count in (-1, "-1", None):
                                structure = get_db_structure_cached(
                                    db_local, connection_id, table
                                )

                                if not structure:
                                    raise ValueError(f"Sem estrutura: {table}")

                                count = await loop.run_in_executor(
                                    None,
                                    lambda: get_table_rowcount_cached(
                                        db=db_local,
                                        engine=engine,
                                        table_name=table,
                                        connection_id=connection_id,
                                        db_type=connection_type,
                                        structure_id=structure.id,
                                        schema=structure.schema_name,
                                    ),
                                )

                            yield sse_data({
                                "table": table,
                                "count": count,
                                "cached": True,
                                "ts": time.time(),
                            })

                            if connection_type != "sqlite":
                                await asyncio.sleep(0.03)

                        except Exception as err:
                            log_message(f"[CACHE] {table} -> {err}", "error")

                            yield sse_data({
                                "table": table,
                                "count": -1,
                                "error": str(err),
                                "cached": True,
                            })

                    yield sse_event("end", {"source": "cache"})

                finally:
                    db_local.close()

            return StreamingResponse(
                cached_event_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        # =========================================================
        # 🔴 LIVE STREAM
        # =========================================================

        async def live_event_generator():
            db_local = SessionLocal()
            processed_tables = set()
            loop = asyncio.get_event_loop()

            try:
                inspector = inspect(engine)
                default_schema = inspector.default_schema_name

                tables = await loop.run_in_executor(
                    None,
                    lambda: inspector.get_table_names(schema=default_schema),
                )

                total = len(tables)
                done = 0

                yield sse_data({"type": "progress", "total": total, "done": 0})

                for table in tables:
                    if not table or table in processed_tables:
                        continue

                    processed_tables.add(table)

                    try:
                        structure = verificar_ou_atualizar_estrutura(
                            db_local, connection_id, table, default_schema
                        )

                        if not structure:
                            continue

                        count = await loop.run_in_executor(
                            None,
                            lambda: get_table_rowcount_cached(
                                db=db_local,
                                engine=engine,
                                table_name=table,
                                connection_id=connection_id,
                                db_type=connection_type,
                                structure_id=structure.id,
                                schema=structure.schema_name,
                            ),
                        )

                        done += 1

                        yield sse_data({
                            "table": table,
                            "count": count,
                            "cached": False,
                            "ts": time.time(),
                        })

                        if done % 5 == 0:
                            yield sse_data({
                                "type": "progress",
                                "total": total,
                                "done": done,
                            })

                        if connection_type != "sqlite":
                            await asyncio.sleep(0.1)

                    except Exception as err:
                        log_message(f"[LIVE] {table} -> {err}", "error")

                        yield sse_data({
                            "table": table,
                            "count": -1,
                            "error": str(err),
                            "cached": False,
                        })

                yield sse_event("end", {"source": "live"})

            finally:
                db_local.close()

        return StreamingResponse(
            live_event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    except Exception as e:
        error_msg = str(e)
        log_message(f"[INIT ERROR] {str(e)}", "error")

        async def error_generator():
            yield sse_event("error", {"error": error_msg})

        return StreamingResponse(
            error_generator(),
            media_type="text/event-stream",
        )