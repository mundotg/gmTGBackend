"""
Gerenciador de cache de queries.
Responsável por recuperar resultados já executados a partir do histórico.
"""

from __future__ import annotations

import json
import traceback
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.cruds.queryhistory_crud import get_query_history_by_user_and_query_async
from app.schemas.queryhistory_schemas import QueryExecutionResult
from app.ultils.logger import log_message


class QueryCacheManager:
    """Gerencia leitura de resultados em cache a partir do histórico de queries."""

    @staticmethod
    async def get_cached_result(
        db: AsyncSession,
        user_id: int,
        connection_id: int,
        query_string: str,
        is_count_query: bool,
    ) -> Optional[QueryExecutionResult]:
        """
        Tenta recuperar um resultado do cache.

        Retorna:
        - QueryExecutionResult, quando houver cache válido
        - None, quando não houver cache utilizável
        """
        try:
            cached = await get_query_history_by_user_and_query_async(
                db=db,
                user_id=user_id,
                connection_id=connection_id,
                query_string=query_string,
            )

            if not cached:
                return None

            if cached.error_message: # type: ignore
                return QueryCacheManager._build_error_result(cached)

            if is_count_query:
                return QueryCacheManager._build_count_result(cached)

            return QueryCacheManager._build_select_result(cached)

        except Exception as exc:
            log_message(
                f"❌ Erro ao acessar cache da query: {exc}\n{traceback.format_exc()}",
                "error",
            )
            return None

    @staticmethod
    def _build_error_result(cached: Any) -> QueryExecutionResult:
        """
        Constrói um resultado de erro baseado no histórico em cache.
        """
        return QueryExecutionResult(
            success=False,
            query=getattr(cached, "query", "") or "",
            duration_ms=getattr(cached, "duration_ms", 0) or 0,
            cached=True,
            error_message=getattr(cached, "error_message", "Erro desconhecido no cache"),
        )

    @staticmethod
    def _build_count_result(cached: Any) -> Optional[QueryExecutionResult]:
        """
        Constrói um resultado COUNT a partir do cache.
        """
        meta_info = getattr(cached, "meta_info", None) or {}
        row_count = meta_info.get("row_count")

        if row_count is None:
            return None

        return QueryExecutionResult(
            success=True,
            query=getattr(cached, "query", "") or "",
            duration_ms=getattr(cached, "duration_ms", 0) or 0,
            cached=True,
            count=row_count,
        )

    @staticmethod
    def _build_select_result(cached: Any) -> Optional[QueryExecutionResult]:
        """
        Constrói um resultado SELECT a partir do cache.
        """
        preview_data = QueryCacheManager._safe_load_preview(
            getattr(cached, "result_preview", None)
        )

        if preview_data is None:
            return None

        columns = QueryCacheManager._extract_columns(
            preview_data=preview_data,
            meta_info=getattr(cached, "meta_info", None),
        )

        return QueryExecutionResult(
            success=True,
            query=getattr(cached, "query", "") or "",
            duration_ms=getattr(cached, "duration_ms", 0) or 0,
            cached=True,
            columns=columns,
            preview=preview_data,
        )

    @staticmethod
    def _safe_load_preview(result_preview: Optional[str]) -> Optional[list[dict[str, Any]]]:
        """
        Faz parse seguro do preview salvo em JSON.
        """
        if not result_preview:
            return []

        try:
            data = json.loads(result_preview)

            if isinstance(data, list):
                return data

            return []
        except (json.JSONDecodeError, TypeError):
            log_message(
                "⚠️ Cache encontrado, mas result_preview está inválido e não pôde ser desserializado.",
                "warning",
            )
            return None

    @staticmethod
    def _extract_columns(
        preview_data: list[dict[str, Any]],
        meta_info: Optional[dict[str, Any]] = None,
    ) -> list[str]:
        """
        Extrai colunas do cache.
        Prioriza meta_info, e usa preview como fallback.
        """
        meta_info = meta_info or {}

        cached_columns = meta_info.get("columns")
        if isinstance(cached_columns, list) and cached_columns:
            return cached_columns

        if preview_data and isinstance(preview_data[0], dict):
            return list(preview_data[0].keys())

        return []