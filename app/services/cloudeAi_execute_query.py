"""
Serviço para execução de queries com streaming de resultados.
Versão melhorada com melhor estrutura, performance e tratamento de erros.
"""

from datetime import datetime
import json
import asyncio
import re
from time import time
import traceback
from typing import Any, AsyncGenerator, Dict, List, Tuple, Optional
from dataclasses import dataclass
from fastapi.responses import StreamingResponse
from sqlalchemy import Result, text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy.exc import SQLAlchemyError

from app.cruds.queryhistory_crud import create_query_history_async, get_query_history_by_user_and_query_async
from app.models.connection_models import DBConnection
from app.schemas.query_select_upAndInsert_schema import CondicaoFiltro, QueryPayload
from app.schemas.queryhistory_schemas import QueryHistoryCreateAsync,  QueryType
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.build_query import get_count_query, get_filter_condition_with_operation, get_query_string_advance
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message


@dataclass
class QueryExecutionResult:
    """Resultado da execução de uma query."""
    success: bool
    query: str
    duration_ms: int
    cached: bool = False
    error_message: Optional[str] = None
    # Para queries SELECT
    columns: Optional[List[str]] = None
    preview: Optional[List[Dict]] = None
    params: Optional[Dict[str, Any]] = None
    count: Optional[int] = None


class QuerySecurityValidator:
    """Validador de segurança para queries SQL."""
    
    # Palavras reservadas perigosas
    FORBIDDEN_KEYWORDS = {
        "drop", "delete", "update", "insert", "alter", "truncate", 
        "create", "grant", "revoke", "execute", "exec", "xp_"
    }
    
    # Padrão para identificadores válidos
    IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    @staticmethod
    def is_safe_value(value: Any, column_type: str) -> bool:
        """
        Valida se o valor fornecido é seguro para uso em SQL, baseado no tipo da coluna.
        Suporta valores como "1,3" para listas de inteiros.
        """
        # Permite None
        if value is None:
            return True
        # Tipos básicos
        if column_type in ("int", "integer", "bigint", "smallint"):
            # Suporta listas de inteiros em string separada por vírgula
            if isinstance(value, str) and "," in value:
                try:
                    return all(int(v.strip()) or v.strip() == "0" for v in value.split(","))
                except Exception:
                    return False
            try:
                int(value)
                return True
            except Exception:
                return False
        if column_type in ("float", "double", "real", "numeric", "decimal"):
            # Suporta listas de floats em string separada por vírgula
            if isinstance(value, str) and "," in value:
                try:
                    return all(float(v.strip()) for v in value.split(","))
                except Exception:
                    return False
            try:
                float(value)
                return True
            except Exception:
                return False
        if column_type in ("bool", "boolean"):
            return isinstance(value, bool) or value in (0, 1, "0", "1", "true", "false", "True", "False")
        # Para strings, limita tamanho e caracteres perigosos
        if column_type in ("str", "string", "varchar", "text", "char"):
            if not isinstance(value, str):
                return False
            if len(value) > 1000:
                return False
            # Não permite ; ou comentários SQL
            if ";" in value or "--" in value or "/*" in value or "*/" in value:
                return False
            return True
        # Para outros tipos, aceita se não for objeto complexo
        if isinstance(value, (dict, list, set, tuple)):
            return False
        return True

    @classmethod
    def is_safe_identifier(cls, identifier: str) -> bool:
        """Valida se um identificador SQL é seguro."""
        if not identifier or not isinstance(identifier, str):
            return False
            
        # Verifica padrão de identificador válido
        if not cls.IDENTIFIER_PATTERN.match(identifier):
            return False
            
        # Verifica palavras reservadas perigosas
        return identifier.lower() not in cls.FORBIDDEN_KEYWORDS
    
    @classmethod
    def ensure_base_table_in_query(cls, payload: QueryPayload) -> QueryPayload:
        """Verifica se a tabela base está no SELECT ou WHERE, caso não esteja, substitui pela primeira tabela disponível."""
        
        if not payload.baseTable:
            return payload

        # 1. Extrai o nome puro da tabela base (remove o schema para comparações seguras)
        # Ex: "public.db_fields" vira "db_fields"
        raw_base_table = payload.baseTable.split('.')[-1]
        
        # Verifica se a tabela base está presente no SELECT
        base_table_in_select = False
        if payload.aliaisTables:
            for alias in payload.aliaisTables.keys():
                parts = alias.split('.')
                # Se for schema.tabela.coluna ou tabela.coluna, o penúltimo é SEMPRE a tabela
                if len(parts) >= 2:
                    alias_table = parts[-2]
                    if alias_table == raw_base_table:
                        base_table_in_select = True
                        break
                # Se por acaso for só a coluna (len == 1), não dá pra inferir a tabela
        
        # Verifica se a tabela base está presente no WHERE
        base_table_in_where = False
        if payload.where:
            for condition in payload.where:
                # Compara ignorando schema (ex: "db_fields" == "db_fields")
                if condition.table_name_fil.split('.')[-1] == raw_base_table:
                    base_table_in_where = True
                    break
        
        # Se a tabela base não está em nenhum lugar, faz a substituição
        if not base_table_in_select and not base_table_in_where:
            available_tables = []
            
            # Procura na table_list primeiro (É a melhor fonte, pois o frontend já manda com schema: "public.users")
            if payload.table_list:
                for table in payload.table_list:
                    if table.split('.')[-1] != raw_base_table and table not in available_tables:
                        available_tables.append(table)
                        
            # Procura tabelas nos joins
            if payload.joins:
                for join_table in payload.joins.keys():
                    if join_table.split('.')[-1] != raw_base_table and join_table not in available_tables:
                        available_tables.append(join_table)

            # Procura tabelas no SELECT (aliaisTables) reconstruindo o nome com schema
            if payload.aliaisTables:
                for alias in payload.aliaisTables.keys():
                    parts = alias.split('.')
                    if len(parts) >= 2:
                        # Ex: "public.db_structures.id" -> junta "public.db_structures"
                        table_with_schema = ".".join(parts[:-1])
                        if parts[-2] != raw_base_table and table_with_schema not in available_tables:
                            available_tables.append(table_with_schema)
            
            # Procura tabelas no WHERE
            if payload.where:
                for condition in payload.where:
                    if condition.table_name_fil.split('.')[-1] != raw_base_table and condition.table_name_fil not in available_tables:
                        available_tables.append(condition.table_name_fil)
            
            # Substitui pela primeira tabela disponível
            if available_tables:
                new_base_table = available_tables[0]
                # print(f"⚠️  Tabela base '{payload.baseTable}' não encontrada no SELECT ou WHERE. Substituindo por '{new_base_table}'")
                payload.baseTable = new_base_table
        
        return payload
    
    @classmethod
    def validate_query_payload(cls, payload: QueryPayload) -> None:
        """Valida apenas os campos críticos do payload para segurança."""
        try:
            # 1. Validação ESSENCIAL: Tabelas
            if not cls.is_safe_identifier(payload.baseTable):
                raise ValueError(f"Nome da tabela base inválido: {payload.baseTable}")
            
            if payload.table_list:
                for table in payload.table_list:
                    if not cls.is_safe_identifier(table):
                        raise ValueError(f"Nome da tabela na lista inválido: {table}")
            
            # 2. Validação CRÍTICA: Joins (apenas estrutura básica)
            if payload.joins:
                for table_name, join_option in payload.joins.items():
                    if not cls.is_safe_identifier(table_name):
                        raise ValueError(f"Nome da tabela de join inválido: {table_name}")
                    
                    # Valida apenas condições principais dos joins
                    for condition in join_option.conditions:
                        # Valida apenas leftColumn (crítico para SQL injection)
                        if condition.leftColumn:
                            parts = condition.leftColumn.split('.')
                            if len(parts) == 2:
                                if not cls.is_safe_identifier(parts[0]) or not cls.is_safe_identifier(parts[1]):
                                    raise ValueError(f"Coluna inválida em leftColumn: {condition.leftColumn}")
                        
                        # Valida apenas rightColumn quando não usa value
                        if not condition.useValue and condition.rightColumn:
                            parts = condition.rightColumn.split('.')
                            if len(parts) == 2:
                                if not cls.is_safe_identifier(parts[0]) or not cls.is_safe_identifier(parts[1]):
                                    raise ValueError(f"Coluna inválida em rightColumn: {condition.rightColumn}")
            
            # 3. Validação ESSENCIAL: Condições WHERE
            if payload.where:
                for condition in payload.where:
                    if not cls.is_safe_identifier(condition.table_name_fil):
                        raise ValueError(f"Nome da tabela no filtro inválido: {condition.table_name_fil}")
                    if not cls.is_safe_identifier(condition.column):
                        raise ValueError(f"Nome da coluna no filtro inválido: {condition.column}")
                    
                    # Valida apenas valores em operadores de risco
                    if condition.operator in ['IN', 'NOT IN'] and condition.value:
                        if isinstance(condition.value, list):
                            for val in condition.value:
                                if not cls.is_safe_value(val,condition.column_type):
                                    raise ValueError(f"Valor inválido na condição IN: {val}")
                        elif not cls.is_safe_value(condition.value,condition.column_type):
                            raise ValueError(f"Valor inválido: {condition.value}")
            
            # 4. Validação RÁPIDA: Aliases (apenas formato básico)
            if payload.aliaisTables:
                for alias, original in payload.aliaisTables.items():
                    if '.' in alias:
                        parts = alias.split('.')
                        if len(parts) == 2:
                            if not cls.is_safe_identifier(parts[0]) or not cls.is_safe_identifier(parts[1]):
                                raise ValueError(f"Alias inválido: {alias}")
                    elif not cls.is_safe_identifier(alias):
                        raise ValueError(f"Alias inválido: {alias}")
                    
        except ValueError as e:
            raise
        except Exception as e:
            raise ValueError(f"Erro na validação do payload: {e}")


class QueryFilterBuilder:
    """Construtor de filtros SQL com parâmetros seguros."""
    
    @staticmethod
    async def build_where_clause(
        conditions: List[CondicaoFiltro],
        db_type: str = "postgres"
    ) -> Tuple[str, Dict[str, Any]]:
        """Constrói cláusula WHERE com parâmetros seguros."""
        if not conditions:
            return "", {}

        where_clauses = []
        params: Dict[str, Any] = {}

        for i, condition in enumerate(conditions):
            # Validação de segurança já feita pelo SecurityValidator
            field = f"{condition.table_name_fil}.{condition.column}"
            # print(f"🔍 DEBUG: Processando condição {condition}")
            # Gera nome único do parâmetro
            param_prefix = f"param_{i}"
            
            sql_part = get_filter_condition_with_operation(
                col_name=field,
                col_type=condition.column_type,
                value=condition.value,
                params=params,
                db_type=db_type,
                operation=condition.operator,
                param_name=param_prefix,
                enum_values={},
                value_otheir_between=str(condition.value2),
            )

            logic = condition.logicalOperator or "AND"
            where_clauses.append((logic, sql_part))

        # Monta WHERE final
        if not where_clauses:
            return "", {}
            
        where_sql = where_clauses[0][1]
        for logic, clause in where_clauses[1:]:
            where_sql += f" {logic} {clause}"

        return f"WHERE {where_sql}", params


class QueryCacheManager:
    """Gerenciador de cache de queries."""
    
    @staticmethod
    async def get_cached_result(
        db: AsyncSession, 
        user_id: int, 
        connection_id: int, 
        query_string: str,
        is_count_query: bool
    ) -> Optional[QueryExecutionResult]:
        """Tenta recuperar resultado do cache."""
        try:
            cached = await get_query_history_by_user_and_query_async(
                db, user_id, connection_id, query_string
            )
            
            if not cached:
                return None
                
            # log_message("📋 Resultado recuperado do cache", "info")
            
            # Se houve erro no cache, propaga o erro
            if cached.error_message: # type: ignore
                return QueryExecutionResult(
                    success=False,
                    query=str(cached.query),
                    duration_ms=cached.duration_ms, # type: ignore
                    cached=True,
                    error_message=cached.error_message # type: ignore
                )
            
            # Resultado de COUNT
            if is_count_query:
                return QueryExecutionResult(
                    success=True,
                    query=cached.query,
                    duration_ms=cached.duration_ms,
                    cached=True,
                    count=int(cached.result_preview) if cached.result_preview else 0
                )
            
            # Resultado de SELECT
            preview_data = json.loads(cached.result_preview) if cached.result_preview else []
            
            columns = list(preview_data[0].keys()) if preview_data else []
            
            return QueryExecutionResult(
                success=True,
                query=cached.query,
                duration_ms=cached.duration_ms,
                cached=True,
                columns=columns,
                preview=preview_data
            )
            
        except Exception as e:
            raise Exception(f"Erro ao acessar cache: {str(e)}{traceback.format_exc()}")


class QueryExecutor:
    """Executor de queries SQL."""
    
    def __init__(self, engine: AsyncEngine, db_type: str):
        self.engine = engine
        self.db_type = db_type
        # Limite de linhas a manter no preview (evita OOM)
        self.MAX_PREVIEW_ROWS = 500
        # Tamanho do lote para fetchmany
        self.FETCH_BATCH_SIZE = 100
    
    async def execute_query(
            self,
            query_string: str,
            params: Optional[Dict[str, Any]] = None,
            is_count_query: bool = False,
            select_tables: List[str] = []
        ):
            """Executa a query SQL e retorna resultado e colunas."""
            colunas: List[str] = []
            try:
                async with self.engine.connect() as conn:
                    result: Result = await conn.execute(text(query_string), params or {})

                    if is_count_query:
                        count_result = result.scalar_one_or_none()
                        return count_result or 0, ["count"]

                    else:
                        preview_rows: List[Dict[str, Any]] = []
                        keys = list(result.keys())
                        colunas = keys

                        fetched = 0
                        while True:
                            batch = result.fetchmany(self.FETCH_BATCH_SIZE)
                            if not batch:
                                break

                            for row in batch:
                                if select_tables:
                                    # se select informado, usa ordem do select
                                    preview_rows.append(dict(zip(select_tables, row)))
                                else:
                                    preview_rows.append(dict(zip(keys, row)))

                                fetched += 1
                                if fetched >= self.MAX_PREVIEW_ROWS:
                                    break

                            if fetched >= self.MAX_PREVIEW_ROWS:
                                break

                        return preview_rows, colunas
            except Exception as e:
                raise Exception(f"Erro ao executar query: {e}{traceback.format_exc()}")


class QueryService:
    """Serviço principal para execução de queries."""
    
    def __init__(self):
        self.security_validator = QuerySecurityValidator()
        self.filter_builder = QueryFilterBuilder()
        self.cache_manager = QueryCacheManager()
    
    async def execute_query_with_cache(
        self,
        db: AsyncSession,
        user_id: int,
        connection: DBConnection,
        engine: AsyncEngine,
        query_payload: QueryPayload,
        use_cache: bool = True
    ) -> QueryExecutionResult:
        """Executa query com suporte a cache."""
        start_time = time()
        
        try:
            # 1. Validação de segurança
            self.security_validator.validate_query_payload(query_payload)
            query_payload = self.security_validator.ensure_base_table_in_query(query_payload)
         
            # 2. Construção dos filtros
            filters, params = await self.filter_builder.build_where_clause(
                query_payload.where or [], connection.type
            )
            
            # 3. Construção da query
            query_string = await self._build_query_string(
                query_payload, filters, connection.type
            )
            
            if use_cache:
                cached_result = await self.cache_manager.get_cached_result(
                    db, user_id, connection.id, query_string, query_payload.isCountQuery
                )
                if cached_result:
                    return cached_result  
            
            # 5. Execução da query
            executor = QueryExecutor(engine, connection.type)
            result_data, columns = await executor.execute_query(
                query_string, params, query_payload.isCountQuery, list(query_payload.aliaisTables.values())
            )
            
            duration_ms = int((time() - start_time) * 1000)
            
            # CORREÇÃO: Para COUNT, usar count em vez de preview
            if query_payload.isCountQuery:
                execution_result = QueryExecutionResult(
                    success=True,
                    query=query_string,
                    duration_ms=duration_ms,
                    count=result_data,  # ✅ Agora usa count
                    params=params
                )
            else:
                # print(list(query_payload.aliaisTables.keys()) if query_payload.aliaisTables else columns)
                execution_result = QueryExecutionResult(
                    success=True,
                    query=query_string,
                    duration_ms=duration_ms,
                    columns=list(query_payload.aliaisTables.keys()) if query_payload.aliaisTables else columns,
                    preview=result_data, 
                    params=params
                )
            
            result_preview = None
            if not query_payload.isCountQuery and result_data:
                result_preview = json.dumps(result_data[:10], default=str)  # salva apenas 10 linhas

            # 7. Salvar no histórico
            await self._save_query_history(
                db=db, user_id=user_id, connection_id=connection.id,
                query=query_string,
                duration_ms=duration_ms,
                result_preview=result_preview,
                is_count_query=query_payload.isCountQuery,
                error_message=None,
                app_source="API",
                executed_by="system",
                meta_info={},
                modified_by=None,
                query_payload=query_payload,
                row_count=len(result_data) if isinstance(result_data, list) else result_data
            )

            
            return execution_result
            
        except Exception as e:
            duration_ms = int((time() - start_time) * 1000)
            error_message = _lidar_com_erro_sql(e)
            
            # Salvar erro no histórico
            await self._save_query_history(
                db=db, user_id=user_id, connection_id=connection.id, query=query_string if 'query_string' in locals() else "",
                duration_ms=duration_ms, result_preview=None, is_count_query=query_payload.isCountQuery, error_message=error_message,
                app_source="API", executed_by="system", meta_info={"error": error_message}, modified_by=None, query_payload=query_payload, row_count=None
            )
            
            return QueryExecutionResult(
                success=False,
                query=query_string if 'query_string' in locals() else "",
                duration_ms=duration_ms,
                error_message=error_message
            )
    
    async def _build_query_string(
        self, 
        payload: QueryPayload, 
        filters: str, 
        db_type: str
    ) -> str:
        """Constrói a string SQL da query."""
        if payload.isCountQuery:
            return get_count_query(
                base_table=payload.baseTable,
                joins=payload.joins,
                filters=filters,
                distinct=payload.distinct,
                db_type=db_type,
            )
        else:
            return get_query_string_advance(
                base_table=payload.baseTable,
                select=payload.select,
                joins=payload.joins,
                aliases=payload.aliaisTables,
                filters=filters,
                table_list=payload.table_list,
                order_by=payload.orderBy,
                max_rows=payload.limit,
                offset=payload.offset,
                db_type=db_type,
                distinct=payload.distinct,
            )

    async def _save_query_history(
        self,
        db: AsyncSession,
        user_id: int,
        connection_id: int,
        query: str,
        duration_ms: int,
        result_preview: Optional[str],
        is_count_query: bool,
        error_message: Optional[str] = None,
        app_source: Optional[str] = None,
        client_ip: Optional[str] = None,
        executed_by: Optional[str] = None,
        meta_info: Optional[dict] = None,
        modified_by: Optional[str] = None,
        query_payload: Optional[QueryPayload] = None,
        row_count: Optional[int] = None,
    ) -> None:
        """
        Salva o histórico da query com detalhes adicionais, incluindo:
        - Tempo de execução e data/hora UTC
        - Tabelas envolvidas, filtros e parâmetros
        - Origem da requisição e IP do cliente
        - Resultado resumido (nº de linhas, colunas)
        - Tipo de query e status (sucesso/erro)
        """
        try:
            start_ts = datetime.utcnow()

            # Montagem do contexto detalhado
            meta_context = {
                "executed_at_utc": start_ts.isoformat(),
                "execution_time_ms": duration_ms,
                "status": "error" if error_message else "success",
                "query_length": len(query or ""),
                "row_count": row_count,
                "base_table": getattr(query_payload, "baseTable", None),
                "tables_involved": getattr(query_payload, "table_list", None),
                "joins": list(getattr(query_payload, "joins", {}).keys()) if getattr(query_payload, "joins", None) else [],
                "filters": [f"{w.table_name_fil}.{w.column} {w.operator} {w.value}" for w in getattr(query_payload, "where", [])] if getattr(query_payload, "where", None) else [],
                "order_by": getattr(query_payload, "orderBy", None),
                "limit": getattr(query_payload, "limit", None),
                "offset": getattr(query_payload, "offset", None),
                "distinct": getattr(query_payload, "distinct", None),
                "params_used": meta_info.get("params") if meta_info else None,
                "app_source": app_source or "API",
                "client_ip": client_ip,
                "executed_by": executed_by or "system",
                "modified_by": modified_by,
            }

            # Criação do registro de histórico
            history_data = QueryHistoryCreateAsync(
                user_id=user_id,
                db_connection_id=connection_id,
                query=query.strip(),
                query_type=QueryType.COUNT if is_count_query else QueryType.SELECT,
                duration_ms=duration_ms,
                result_preview=result_preview if result_preview and len(result_preview) < 15000 else None,  # evita salvar preview muito grande
                error_message=error_message,
                is_favorite=False,
                tags="count" if is_count_query else "select_preview",
                app_source=app_source or "API",
                client_ip=client_ip,
                executed_by=executed_by or "system",
                meta_info=meta_context,
                modified_by=modified_by,
            )

            created_query = await create_query_history_async(db=db, data=history_data)

            log_message(
                f"✅ Histórico detalhado salvo: "
                f"User={user_id}, Conn={connection_id}, Duração={duration_ms}ms, "
                f"Tabelas={meta_context.get('tables_involved')}, Linhas={row_count}, Status={meta_context['status']}",
                "success",
            )

        except SQLAlchemyError as e:
            await db.rollback()
            log_message(f"💥 Erro SQLAlchemy ao salvar histórico: {e}\n{traceback.format_exc()}", "error")
        except Exception as e:
            await db.rollback()
            log_message(f"❌ Erro inesperado ao salvar histórico detalhado: {e}\n{traceback.format_exc()}", "error")



# Instância global do serviço
query_service = QueryService()

async def executar_query_e_salvar_stream(
    db: AsyncSession,
    user_id: int,
    body: QueryPayload,
) -> StreamingResponse:
    """
    Executa query e retorna resultados via Server-Sent Events (SSE).
    Primeiro faz a consulta SELECT (com chunking se necessário), depois o COUNT.
    """

    async def event_stream() -> AsyncGenerator[str, None]:
        engine = connection = None
        try:
            # print("🔍 DEBUG: Iniciando event_stream")
            yield f"event: status\ndata: {json.dumps({'status': 'started'})}\n\n"

            # print("🔍 DEBUG: Obtendo engine e connection")
            engine, connection = await ConnectionManager.get_engine_async(db, user_id)
            # print(f"🔍 DEBUG: Engine obtido: {engine is not None}, Connection: {connection is not None}")

            # Verifica se precisa repartir a consulta
            needs_chunking = body.limit and body.limit > 150
            chunk_size = 150
            total_limit = body.limit if body.limit else 0

            # print(f"🔍 DEBUG: needs_chunking={needs_chunking}, limit={body.limit}")

            if needs_chunking:
                yield f"event: info\ndata: {json.dumps({'info': f'Consulta repartida em chunks de {chunk_size} registros'})}\n\n"

            # ---------------------------
            # PRIMEIRO: SELECT
            # ---------------------------
            select_body = body.copy()
            # print("🔍 DEBUG: SELECT body copiado")

            if not needs_chunking:
                # Execução normal
                # print("🔍 DEBUG: Iniciando SELECT normal")
                select_result = await query_service.execute_query_with_cache(
                    db=db,
                    user_id=user_id,
                    connection=connection,
                    engine=engine,
                    query_payload=select_body,
                    use_cache=False,
                )
                # print(f"🔍 DEBUG: SELECT resultado - success: {select_result.success}")

                if not select_result.success:
                    yield f"event: error\ndata: {json.dumps({'error': select_result.error_message})}\n\n"
                    return

                # ✅ CORREÇÃO: Garantir que os dados são serializáveis
                # print("🔍 DEBUG: Preparando dados SELECT para serialização")
                select_data = {
                    "success": select_result.success,
                    "query": select_result.query,
                    "duration_ms": select_result.duration_ms,
                    "columns": select_result.columns,
                    "preview": select_result.preview,
                    "cached": select_result.cached,
                    "is_complete": True,
                    "chunk_info": None,
                }
                # print("🔍 DEBUG: Serializando dados SELECT")
                yield f"event: data\ndata: {json.dumps(select_data, default=str)}\n\n"
                # print("🔍 DEBUG: Dados SELECT enviados")

            else:
                # Execução em chunks
                # print("🔍 DEBUG: Iniciando SELECT com chunking")
                all_preview = []
                total_chunks = (total_limit + chunk_size - 1) // chunk_size
                total_duration = 0
                last_columns = []

                for chunk_index in range(total_chunks):
                    current_offset = chunk_index * chunk_size
                    current_limit = min(chunk_size, total_limit - current_offset)
                    select_body.offset = current_offset
                    select_body.limit = current_limit
                    
                    # print(f"🔍 DEBUG: Chunk {chunk_index + 1}/{total_chunks} - offset: {current_offset}, limit: {current_limit}")

                    yield f"event: info\ndata: {json.dumps({'info': f'Executando chunk {chunk_index + 1}/{total_chunks}'})}\n\n"

                    chunk_result = await query_service.execute_query_with_cache(
                        db=db,
                        user_id=user_id,
                        connection=connection,
                        engine=engine,
                        query_payload=select_body,
                        use_cache=False,
                    )

                    # print(f"🔍 DEBUG: Chunk {chunk_index + 1} resultado - success: {chunk_result.success}")

                    if not chunk_result.success:
                        yield f"event: error\ndata: {json.dumps({'error': chunk_result.error_message})}\n\n"
                        return

                    all_preview.extend(chunk_result.preview)
                    total_duration += chunk_result.duration_ms
                    last_columns = chunk_result.columns

                    # ✅ CORREÇÃO: Garantir que os dados são serializáveis
                    # print(f"🔍 DEBUG: Preparando chunk {chunk_index + 1} para serialização")
                    chunk_data = {
                        "success": chunk_result.success,
                        "query": chunk_result.query,
                        "duration_ms": chunk_result.duration_ms,
                        "columns": chunk_result.columns,
                        "preview": chunk_result.preview,
                        "cached": chunk_result.cached,
                        "is_complete": False,
                        "chunk_info": {
                            "current_chunk": chunk_index + 1,
                            "total_chunks": total_chunks,
                            "chunk_size": len(chunk_result.preview),
                            "total_so_far": len(all_preview),
                        },
                    }
                    # print("🔍 DEBUG: Serializando chunk data")
                    yield f"event: data\ndata: {json.dumps(chunk_data, default=str)}\n\n"
                    # print(f"🔍 DEBUG: Chunk {chunk_index + 1} enviado")

                    await asyncio.sleep(0.05)

                # Dados consolidados
                # print("🔍 DEBUG: Preparando dados consolidados")
                complete_data = {
                    "success": True,
                    "query": "Consulta consolidada de múltiplos chunks",
                    "duration_ms": total_duration,
                    "columns": last_columns,
                    "preview": all_preview,
                    "cached": False,
                    "is_complete": True,
                    "chunk_info": {
                        "total_chunks": total_chunks,
                        "total_records": len(all_preview),
                    },
                }
                # print("🔍 DEBUG: Serializando dados consolidados")
                yield f"event: data\ndata: {json.dumps(complete_data, default=str)}\n\n"
                # print("🔍 DEBUG: Dados consolidados enviados")

            await asyncio.sleep(0.1)

            # ---------------------------
            # SEGUNDO: COUNT
            # ---------------------------
            # print("🔍 DEBUG: Iniciando COUNT")
            yield f"event: status\ndata: {json.dumps({'status': 'counting'})}\n\n"

            count_body = body.copy()
            count_body.isCountQuery = True
            # print("🔍 DEBUG: COUNT body preparado")

            count_result = await query_service.execute_query_with_cache(
                db=db,
                user_id=user_id,
                connection=connection,
                engine=engine,
                query_payload=count_body,
                use_cache=True,
            )

            # print(f"🔍 DEBUG: COUNT resultado - success: {count_result.success}, count: {count_result}")

            if not count_result.success:
                yield f"event: error\ndata: {json.dumps({'error': count_result.error_message})}\n\n"
                return

            # ✅ CORREÇÃO: Garantir que count é serializável
            # print("🔍 DEBUG: Preparando COUNT para serialização")
            count_data = {
                "success": count_result.success,
                "count": count_result.count,
                "query": count_result.query,
                "duration_ms": count_result.duration_ms,
                "cached": count_result.cached,
            }
            # print("🔍 DEBUG: Serializando COUNT data")
            yield f"event: count\ndata: {json.dumps(count_data)}\n\n"
            # print("🔍 DEBUG: COUNT enviado")

            await asyncio.sleep(0.01)
            yield f"event: status\ndata: {json.dumps({'status': 'completed'})}\n\n"
            # print("🔍 DEBUG: Stream completado com sucesso")

        except Exception as e:
            error_msg = str(e)
            # print(f"🔍 DEBUG: ERRO CAPTURADO: {error_msg}")
            # print(f"🔍 DEBUG: TRACEBACK: {traceback.format_exc()}")
            log_message(f"❌ Erro no stream SSE: {error_msg}\n{traceback.format_exc()}", "error")
            yield f"event: error\ndata: {json.dumps({'error': error_msg})}\n\n"

        finally:
            if db:
                try:
                    await db.close()
                except Exception as e:
                    log_message(f"Erro ao fechar connection: {e}\n{traceback.format_exc()}", "warning")

            # print("🔍 DEBUG: Entrando no finally")
            if engine:
                try:
                    # print("🔍 DEBUG: Fechando engine")
                    await engine.dispose()
                    # print("🔍 DEBUG: Engine fechado")
                except Exception as e:
                    # print(f"🔍 DEBUG: Erro ao fechar engine: {e}")
                    log_message(f"Erro ao fechar engine: {e}{traceback.format_exc()}", "warning")

    # ✅ CORREÇÃO: Retornar o StreamingResponse diretamente
    # print("🔍 DEBUG: Criando StreamingResponse")
    return StreamingResponse(
        event_stream(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        }
    )