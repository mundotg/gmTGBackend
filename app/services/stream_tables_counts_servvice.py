import asyncio
import json
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session
from fastapi.responses import StreamingResponse
from typing import List, Tuple, Optional
from app.config.cache_manager import cache_result
from app.cruds.dbstatistics_crud import get_cached_row_count_all_tupla, update_or_create_cache
from app.cruds.dbstructure_crud import get_db_structures_by_conn_id_and_table, get_fields_by_structure_pk
from app.services.editar_linha import quote_identifier
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message
from sqlalchemy.engine import Engine

# -----------------------------
# Funções com Cache
# -----------------------------

@cache_result(ttl=180, user_id="user_{user_id}")  # 3 minutos de cache para contagens
def get_table_rowcount_cached(
    db: Session,
    engine: Engine,
    table_name: str,
    connection_id: int,
    db_type: str,
    structure_id: Optional[int] = None,
    schema: Optional[str] = None
) -> dict:
    """
    Retorna a contagem de registros (real ou estimada) de uma tabela com cache.
    """
    return get_table_rowcount(db, engine, table_name, connection_id, db_type, structure_id, schema)

@cache_result(ttl=300, user_id="user_{user_id}")  # 5 minutos de cache para lista de tabelas
def get_cached_table_info(
    db: Session, 
    connection_id: int
) -> List[Tuple[str, int]]:
    """
    Obtém informações de tabelas em cache.
    """
    return get_cached_row_count_all_tupla(db, connection_id)

@cache_result(ttl=600, user_id="user_{user_id}")  # 10 minutos de cache para estrutura
def get_db_structure_cached(
    db: Session, 
    connection_id: int, 
    table_name: str
):
    """
    Obtém estrutura da tabela com cache.
    """
    return get_db_structures_by_conn_id_and_table(db, connection_id, table_name)

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
) -> dict:
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
    """
    Stream em tempo real das contagens de tabelas com cache inteligente.
    """
    try:
        engine, connection = ConnectionManager.ensure_connection(db, id_user)
        if not engine or not connection:
            raise ValueError("Não foi possível estabelecer conexão com o banco")

        # Obtém informações em cache
        table_info = get_cached_table_info(db, connection.id)
        
        if table_info:
            # Se tem cache, usa dados cacheados com validação
            async def cached_event_generator():
                processed_tables = set()
                try:
                    # Primeiro envia dados do cache
                    for table, count in table_info:
                        if table in processed_tables:
                            continue
                            
                        processed_tables.add(table)
                        
                        # Se count é inválido, busca atualização
                        if count in (-1, '-1', None):
                            structure = get_db_structure_cached(db, connection.id, table)
                            count = get_table_rowcount_cached(
                                db, engine, table, connection.id, 
                                connection.type.lower(), structure.id, structure.schema_name
                            )
                        
                        data = json.dumps({
                            "table": table, 
                            "count": count,
                            "cached": True,
                            "timestamp": asyncio.get_event_loop().time()
                        })
                        yield f"data: {data}\n\n"
                        await asyncio.sleep(0.03)  # Delay menor para cache
                    
                    yield "event: end\ndata: {\"status\": \"completed\", \"source\": \"cache\"}\n\n"
                    
                except Exception as e:
                    error_data = json.dumps({"error": str(e), "source": "cache"})
                    yield f"event: error\ndata: {error_data}\n\n"

            return StreamingResponse(cached_event_generator(), media_type="text/event-stream")

        # Sem cache, faz busca completa
        async def live_event_generator():
            processed_tables = set()
            try:
                inspector = inspect(engine)
                default_schema = inspector.default_schema_name
                tables = inspector.get_table_names(schema=default_schema)
                
                total_tables = len(tables)
                completed_tables = 0
                
                # Envia progresso inicial
                progress_data = json.dumps({
                    "type": "progress",
                    "total": total_tables,
                    "completed": 0,
                    "message": f"Iniciando contagem de {total_tables} tabelas"
                })
                yield f"data: {progress_data}\n\n"
                
                for table in tables:
                    if table in processed_tables:
                        continue
                        
                    processed_tables.add(table)
                    
                    try:
                        structure = get_db_structure_cached(db, connection.id, table)
                        count = get_table_rowcount_cached(
                            db, engine, table, connection.id, 
                            connection.type.lower(), structure.id, structure.schema_name
                        )
                        
                        completed_tables += 1
                        
                        # Envia dados da tabela
                        table_data = json.dumps({
                            "table": table, 
                            "count": count,
                            "cached": False,
                            "timestamp": asyncio.get_event_loop().time()
                        })
                        yield f"data: {table_data}\n\n"
                        
                        # Envia progresso a cada 5 tabelas
                        if completed_tables % 5 == 0:
                            progress_data = json.dumps({
                                "type": "progress", 
                                "total": total_tables,
                                "completed": completed_tables,
                                "message": f"Processadas {completed_tables}/{total_tables} tabelas"
                            })
                            yield f"data: {progress_data}\n\n"
                        
                        await asyncio.sleep(0.1)  # Delay para não sobrecarregar
                        
                    except Exception as table_error:
                        log_message(f"❌ Erro na tabela '{table}': {table_error}", "error")
                        error_data = json.dumps({
                            "table": table,
                            "error": str(table_error),
                            "count": -1
                        })
                        yield f"data: {error_data}\n\n"
                
                # Finalização
                completion_data = json.dumps({
                    "type": "completion",
                    "total_tables": total_tables,
                    "processed_tables": completed_tables,
                    "message": "Contagem concluída"
                })
                yield f"data: {completion_data}\n\n"
                yield "event: end\ndata: {\"status\": \"completed\", \"source\": \"live\"}\n\n"

            except Exception as e:
                log_message(f"❌ Erro no stream: {e}", "error")
                error_data = json.dumps({"error": str(e), "source": "live"})
                yield f"event: error\ndata: {error_data}\n\n"

        return StreamingResponse(live_event_generator(), media_type="text/event-stream")

    except Exception as e:
        log_message(f"❌ Erro crítico no get_table_count_streams: {e}", "error")
        
        async def error_generator():
            error_data = json.dumps({"error": str(e), "source": "initialization"})
            yield f"event: error\ndata: {error_data}\n\n"
        
        return StreamingResponse(error_generator(), media_type="text/event-stream")

# -----------------------------
# Endpoints Adicionais para Cache
# -----------------------------

def clear_table_cache_endpoint(db: Session, id_user: int) -> dict:
    """
    Limpa o cache de contagens de tabelas.
    """
    try:
        connection = ConnectionManager.get_connection(db, id_user)
        connection_id = connection.id if connection else None
        
        # _clear_table_cache(id_user, connection_id)
        
        return {
            "success": True,
            "message": "Cache de tabelas limpo com sucesso",
            "user_id": id_user,
            "connection_id": connection_id
        }
    except Exception as e:
        log_message(f"❌ Erro ao limpar cache de tabelas: {e}", "error")
        return {
            "success": False,
            "error": str(e),
            "user_id": id_user
        }

def get_cache_stats(db: Session, id_user: int) -> dict:
    """
    Retorna estatísticas do cache de tabelas.
    """
    try:
        connection = ConnectionManager.get_connection(db, id_user)
        if not connection:
            return {"error": "Nenhuma conexão ativa"}
            
        table_info = get_cached_table_info(db, connection.id)
        cached_tables = len(table_info) if table_info else 0
        
        inspector = inspect(ConnectionManager.get_engine(id_user))
        total_tables = len(inspector.get_table_names())
        
        return {
            "user_id": id_user,
            "connection_id": connection.id,
            "cached_tables": cached_tables,
            "total_tables": total_tables,
            "cache_coverage": f"{(cached_tables / total_tables * 100) if total_tables > 0 else 0:.1f}%",
            "cache_enabled": True
        }
    except Exception as e:
        log_message(f"❌ Erro ao obter estatísticas do cache: {e}", "error")
        return {"error": str(e)}