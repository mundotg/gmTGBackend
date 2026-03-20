"""
Serviço para execução de queries com streaming de resultados via SSE.
Versão reorganizada com melhor estrutura, legibilidade e tratamento de erros.
"""

from __future__ import annotations

import asyncio
import json
import traceback
from typing import Any, AsyncGenerator, Optional

from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.models.connection_models import DBConnection
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.services.query_service import QueryService
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message


query_service = QueryService()
CHUNK_SIZE = 150


def _clone_payload(payload: QueryPayload) -> QueryPayload:
    if hasattr(payload, "model_copy"):
        return payload.model_copy(deep=True)
    return payload.copy(deep=True)


def _json(data: Any) -> str:
    return json.dumps(data, default=str, ensure_ascii=False)


def _sse_event(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {_json(data)}\n\n"



async def _run_select_without_chunking(
    db: AsyncSession,
    user_id: int,
    connection: DBConnection,
    engine: AsyncEngine,
    body: QueryPayload,
) -> AsyncGenerator[str, None]:
    select_body = _clone_payload(body)

    result = await query_service.execute_query_with_cache(
        db=db,
        user_id=user_id,
        connection=connection,
        engine=engine,
        query_payload=select_body,
    )

    if not result.success:
        yield _sse_event("error", {"error": result.error_message})
        return

    yield _sse_event(
        "data",
        {
            "success": result.success,
            "query": result.query,
            "duration_ms": result.duration_ms,
            "columns": result.columns,
            "preview": result.preview,
            "cached": result.cached,
            "is_complete": True,
            "chunk_info": None,
        },
    )


async def _run_select_with_chunking(
    db: AsyncSession,
    user_id: int,
    connection: DBConnection,
    engine: AsyncEngine,
    body: QueryPayload,
) -> AsyncGenerator[str, None]:
    total_limit = body.limit or 0
    total_chunks = (total_limit + CHUNK_SIZE - 1) // CHUNK_SIZE

    all_preview: list[Any] = []
    total_duration = 0
    last_columns: list[str] = []

    yield _sse_event(
        "info",
        {"info": f"Consulta repartida em chunks de {CHUNK_SIZE} registros"},
    )

    for chunk_index in range(total_chunks):
        current_offset = chunk_index * CHUNK_SIZE
        current_limit = min(CHUNK_SIZE, total_limit - current_offset)

        chunk_body = _clone_payload(body)
        chunk_body.offset = current_offset
        chunk_body.limit = current_limit

        yield _sse_event(
            "info",
            {"info": f"Executando chunk {chunk_index + 1}/{total_chunks}"},
        )

        result = await query_service.execute_query_with_cache(
        db=db,
        user_id=user_id,
        connection=connection,
        engine=engine,
        query_payload=chunk_body,
        use_cache=False
    )

        if not result.success:
            yield _sse_event("error", {"error": result.error_message})
            return

        preview = result.preview or []
        all_preview.extend(preview)
        total_duration += result.duration_ms
        last_columns = result.columns or []

        yield _sse_event(
            "data",
            {
                "success": result.success,
                "query": result.query,
                "duration_ms": result.duration_ms,
                "columns": result.columns,
                "preview": preview,
                "cached": result.cached,
                "is_complete": False,
                "chunk_info": {
                    "current_chunk": chunk_index + 1,
                    "total_chunks": total_chunks,
                    "chunk_size": len(preview),
                    "total_so_far": len(all_preview),
                },
            },
        )

        await asyncio.sleep(0.05)

    yield _sse_event(
        "data",
        {
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
        },
    )


async def _run_count(
    db: AsyncSession,
    user_id: int,
    connection: DBConnection,
    engine: AsyncEngine,
    body: QueryPayload,
) -> AsyncGenerator[str, None]:
    yield _sse_event("status", {"status": "counting"})

    count_body = _clone_payload(body)
    count_body.isCountQuery = True

    result = await query_service.execute_query_with_cache(
        db=db,
        user_id=user_id,
        connection=connection,
        engine=engine,
        query_payload=count_body,
        use_cache=True
    )

    if not result.success:
        yield _sse_event("error", {"error": result.error_message})
        return

    yield _sse_event(
        "count",
        {
            "success": result.success,
            "count": result.count,
            "query": result.query,
            "duration_ms": result.duration_ms,
            "cached": result.cached,
        },
    )


async def executar_query_e_salvar_stream(
    db: AsyncSession,
    user_id: int,
    body: QueryPayload,
) -> StreamingResponse:
    async def event_stream() -> AsyncGenerator[str, None]:
        engine: Optional[AsyncEngine] = None
        connection: Optional[DBConnection] = None

        try:
            yield _sse_event("status", {"status": "started"})

            engine, connection = await ConnectionManager.get_engine_async(db, user_id)

            needs_chunking = bool(body.limit and body.limit > CHUNK_SIZE)

            if needs_chunking:
                async for event in _run_select_with_chunking(
                    db=db,
                    user_id=user_id,
                    connection=connection,
                    engine=engine,
                    body=body,
                ):
                    yield event
            else:
                async for event in _run_select_without_chunking(
                    db=db,
                    user_id=user_id,
                    connection=connection,
                    engine=engine,
                    body=body,
                ):
                    yield event

            await asyncio.sleep(0.1)

            async for event in _run_count(
                db=db,
                user_id=user_id,
                connection=connection,
                engine=engine,
                body=body,
            ):
                yield event

            await asyncio.sleep(0.01)
            yield _sse_event("status", {"status": "completed"})

        except Exception as exc:
            log_message(
                f"❌ Erro no stream SSE: {exc}\n{traceback.format_exc()}",
                "error",
            )
            yield _sse_event("error", {"error": str(exc)})
        except asyncio.CancelledError:
            log_message("SSE stream cancelado pelo cliente", "warning")

        finally:
            pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        },
    )