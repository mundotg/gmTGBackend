from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from time import time
from typing import Any, Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.cruds.queryhistory_crud import create_query_history_async
from app.models.connection_models import DBConnection
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.schemas.queryhistory_schemas import (
    QueryExecutionResult,
    QueryHistoryCreateAsync,
    QueryType,
)

from app.services.query_cache_manager import QueryCacheManager
from app.services.query_executor_sse import QueryExecutor, QueryFilterBuilder
from app.services.query_security_validator import QuerySecurityValidator
from app.ultils.build_query import get_count_query, get_query_string_advance
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message


class QueryService:
    """Serviço principal para validação, construção e execução de queries."""

    def __init__(self) -> None:
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
        use_cache: bool = True,
    ) -> QueryExecutionResult:
        """
        Executa uma query com suporte a cache, histórico e tratamento padronizado de erros.
        """
        start_time = time()
        query_string = ""
        # print(query_payload.dict)

        try:
            sanitized_payload = self._validate_and_prepare_payload(query_payload)

            filters, params = await self._build_filters(
                payload=sanitized_payload,
                db_type=str(connection.type),
            )

            query_string = self._build_query_string(
                payload=sanitized_payload,
                filters=filters,
                db_type=str(connection.type),
            )

            # print("Query construída:", "\n\ncom parâmetros:", params)
            if use_cache:
                cached_result = await self.cache_manager.get_cached_result(
                    db=db,
                    user_id=user_id,
                    connection_id=connection.id,  # type: ignore
                    query_string=query_string,
                    is_count_query=query_payload.isCountQuery,
                )
                if cached_result:
                    return cached_result

            result_data, columns = await self._execute_query(
                engine=engine,
                connection_type=connection.type,
                query_string=query_string,
                params=params,
                payload=sanitized_payload,
            )

            duration_ms = self._get_duration_ms(start_time)

            execution_result = self._build_execution_result(
                payload=sanitized_payload,
                query_string=query_string,
                duration_ms=duration_ms,
                result_data=result_data,
                columns=columns,
                params=params,
            )

            await self._save_success_history(
                db=db,
                user_id=user_id,
                connection_id=connection.id,  # type: ignore
                query=query_string,
                duration_ms=duration_ms,
                query_payload=sanitized_payload,
                result_data=result_data,
                params=params,
            )

            return execution_result

        except Exception as exc:
            duration_ms = self._get_duration_ms(start_time)
            error_message = _lidar_com_erro_sql(exc)

            await self._save_error_history(
                db=db,
                user_id=user_id,
                connection_id=connection.id,  # type: ignore
                query=query_string,
                duration_ms=duration_ms,
                query_payload=query_payload,
                error_message=error_message,
            )

            return QueryExecutionResult(
                success=False,
                query=query_string,
                duration_ms=duration_ms,
                error_message=error_message,
            )

    def _validate_and_prepare_payload(self, payload: QueryPayload) -> QueryPayload:
        """
        Valida o payload e garante a tabela base na query.
        """
        self.security_validator.validate_query_payload(payload)
        return self.security_validator.ensure_base_table_in_query(payload)

    async def _build_filters(
        self,
        payload: QueryPayload,
        db_type: str,
    ) -> tuple[str, dict[str, Any]]:
        """
        Constrói a cláusula WHERE e os parâmetros associados.
        """

        filters, params = await self.filter_builder.build_where_clause(
            payload.where or [],
            db_type,
        )
        return filters, params

    async def _execute_query(
        self,
        engine: AsyncEngine,
        connection_type: Any,
        query_string: str,
        params: dict[str, Any],
        payload: QueryPayload,
    ) -> tuple[Any, list[str]]:
        """
        Executa a query no banco.
        """
        executor = QueryExecutor(engine, connection_type)

        return await executor.execute_query(
            query_string=query_string,
            params=params,
            is_count_query=payload.isCountQuery,
            # select_tables=payload.table_list or []
        )

    def _build_query_string(
        self,
        payload: QueryPayload,
        filters: str,
        db_type: str,
    ) -> str:
        """
        Constrói a SQL final da query.
        """
        if payload.isCountQuery:
            return get_count_query(
                base_table=payload.baseTable,
                joins=payload.joins,
                filters=filters,
                distinct=payload.distinct,
                db_type=db_type,
            )

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

    def _build_execution_result(
        self,
        payload: QueryPayload,
        query_string: str,
        duration_ms: int,
        result_data: Any,
        columns: list[str],
        params: dict[str, Any],
    ) -> QueryExecutionResult:
        """
        Monta o resultado padronizado de execução.
        """
        if payload.isCountQuery:
            return QueryExecutionResult(
                success=True,
                query=query_string,
                duration_ms=duration_ms,
                count=result_data,
                params=params,
            )

        resolved_columns = (
            list(payload.aliaisTables.keys()) if payload.aliaisTables else columns
        )

        return QueryExecutionResult(
            success=True,
            query=query_string,
            duration_ms=duration_ms,
            columns=resolved_columns,
            preview=result_data,
            params=params,
        )

    async def _save_success_history(
        self,
        db: AsyncSession,
        user_id: int,
        connection_id: int,
        query: str,
        duration_ms: int,
        query_payload: QueryPayload,
        result_data: Any,
        params: dict[str, Any],
    ) -> None:
        """
        Salva histórico de execução com sucesso.
        """
        result_preview = self._build_result_preview(
            is_count_query=query_payload.isCountQuery,
            result_data=result_data,
        )

        row_count = self._extract_row_count(result_data)

        await self._save_query_history(
            db=db,
            user_id=user_id,
            connection_id=connection_id,
            query=query,
            duration_ms=duration_ms,
            result_preview=result_preview,
            is_count_query=query_payload.isCountQuery,
            error_message=None,
            app_source="API",
            executed_by="system",
            meta_info={"params": params},
            modified_by=None,
            query_payload=query_payload,
            row_count=row_count,
        )

    async def _save_error_history(
        self,
        db: AsyncSession,
        user_id: int,
        connection_id: int,
        query: str,
        duration_ms: int,
        query_payload: QueryPayload,
        error_message: str,
    ) -> None:
        """
        Salva histórico de execução com erro.
        """
        await self._save_query_history(
            db=db,
            user_id=user_id,
            connection_id=connection_id,
            query=query,
            duration_ms=duration_ms,
            result_preview=None,
            is_count_query=query_payload.isCountQuery,
            error_message=error_message,
            app_source="API",
            executed_by="system",
            meta_info={"error": error_message},
            modified_by=None,
            query_payload=query_payload,
            row_count=None,
        )

    def _build_result_preview(
        self,
        is_count_query: bool,
        result_data: Any,
    ) -> Optional[str]:
        """
        Cria um preview reduzido do resultado para armazenar no histórico.
        """
        if is_count_query or not result_data:
            return None

        if isinstance(result_data, list):
            return json.dumps(result_data[:10], default=str, ensure_ascii=False)

        return json.dumps(result_data, default=str, ensure_ascii=False)

    def _extract_row_count(self, result_data: Any) -> Optional[int]:
        """
        Extrai a quantidade de linhas retornadas.
        """
        if isinstance(result_data, list):
            return len(result_data)

        if isinstance(result_data, int):
            return result_data

        return None

    def _get_duration_ms(self, start_time: float) -> int:
        """
        Calcula a duração em milissegundos.
        """
        return int((time() - start_time) * 1000)

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
        meta_info: Optional[dict[str, Any]] = None,
        modified_by: Optional[str] = None,
        query_payload: Optional[QueryPayload] = None,
        row_count: Optional[int] = None,
    ) -> None:
        """
        Salva o histórico detalhado da query.
        """
        try:
            executed_at = datetime.now(timezone.utc)

            meta_context = self._build_history_meta_context(
                query=query,
                duration_ms=duration_ms,
                row_count=row_count,
                error_message=error_message,
                app_source=app_source,
                client_ip=client_ip,
                executed_by=executed_by,
                modified_by=modified_by,
                query_payload=query_payload,
                meta_info=meta_info,
                executed_at=executed_at,
            )

            history_data = QueryHistoryCreateAsync(
                user_id=user_id,
                db_connection_id=connection_id,
                query=(query or "").strip(),
                query_type=QueryType.COUNT if is_count_query else QueryType.SELECT,
                duration_ms=duration_ms,
                result_preview=self._truncate_preview(result_preview),
                error_message=error_message,
                is_favorite=False,
                tags="count" if is_count_query else "select_preview",
                app_source=app_source or "API",
                client_ip=client_ip,
                executed_by=executed_by or "system",
                meta_info=meta_context,
                modified_by=modified_by,
            )

            await create_query_history_async(db=db, data=history_data)

            log_message(
                f"✅ Histórico salvo: "
                f"User={user_id}, Conn={connection_id}, "
                f"Duração={duration_ms}ms, "
                f"Tabelas={meta_context.get('tables_involved')}, "
                f"Linhas={row_count}, "
                f"Status={meta_context['status']}",
                "success",
            )

        except SQLAlchemyError as exc:
            await db.rollback()
            log_message(
                f"💥 Erro SQLAlchemy ao salvar histórico: {exc}\n{traceback.format_exc()}",
                "error",
            )
        except Exception as exc:
            await db.rollback()
            log_message(
                f"❌ Erro inesperado ao salvar histórico: {exc}\n{traceback.format_exc()}",
                "error",
            )

    def _build_history_meta_context(
        self,
        query: str,
        duration_ms: int,
        row_count: Optional[int],
        error_message: Optional[str],
        app_source: Optional[str],
        client_ip: Optional[str],
        executed_by: Optional[str],
        modified_by: Optional[str],
        query_payload: Optional[QueryPayload],
        meta_info: Optional[dict[str, Any]],
        executed_at: datetime,
    ) -> dict[str, Any]:
        """
        Monta o contexto detalhado do histórico.
        """
        query_payload = query_payload or QueryPayload()  # type: ignore

        where_items = getattr(query_payload, "where", None) or []
        joins = getattr(query_payload, "joins", None) or {}
        table_list = getattr(query_payload, "table_list", None)
        base_table = getattr(query_payload, "baseTable", None)

        filters = [
            f"{item.table_name_fil}.{item.column} {item.operator} {item.value}"
            for item in where_items
        ]

        return {
            "executed_at_utc": executed_at.isoformat(),
            "execution_time_ms": duration_ms,
            "status": "error" if error_message else "success",
            "query_length": len(query or ""),
            "row_count": row_count,
            "base_table": base_table,
            "tables_involved": table_list,
            "joins": list(joins.keys()) if joins else [],
            "filters": filters,
            "order_by": getattr(query_payload, "orderBy", None),
            "limit": getattr(query_payload, "limit", None),
            "offset": getattr(query_payload, "offset", None),
            "distinct": getattr(query_payload, "distinct", None),
            "params_used": (meta_info or {}).get("params"),
            "app_source": app_source or "API",
            "client_ip": client_ip,
            "executed_by": executed_by or "system",
            "modified_by": modified_by,
        }

    def _truncate_preview(
        self, result_preview: Optional[str], max_size: int = 15000
    ) -> Optional[str]:
        """
        Evita salvar preview muito grande no histórico.
        """
        if not result_preview:
            return None

        if len(result_preview) > max_size:
            return None

        return result_preview
