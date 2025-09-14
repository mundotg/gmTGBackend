# app/services/executar_query_e_salvar_stream.py
from datetime import datetime, timezone
import json
import asyncio
from time import time
from typing import Any, Dict, List, Tuple
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from app.config.dependencies import EngineManager
from app.cruds.queryhistory_crud import create_query_history, get_query_history_by_user_and_query
from app.models.connection_models import DBConnection
from app.schemas.queryhistory_schemas import CondicaoFiltro, QueryHistoryCreate, QueryPayload
from app.ultils.ativar_session_bd import get_connection_current
from app.ultils.build_query import get_count_query, get_filter_condition_with_operation, get_query_string
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message


def executar_query_e_salvar_stream(
    db: AsyncSession,
    user_id: int,
    body: QueryPayload,
):
    """
    Executa query e retorna resultados via SSE (Server-Sent Events).
    """
    async def event_stream():
        try:
            yield f"data: {json.dumps({'status': 'started'})}\n\n"

            # 🔄 Executa a query
            result = await executar_query_e_salvar_async(
                db=db,
                user_id=user_id,
                connection=get_connection_current(db, user_id),
                engine=EngineManager.get(user_id),
                queryrequest=body,
                cache_consulta=True,
            )

            if body.isCountQuery:
                yield f"data: {json.dumps({'count': result['count'], 'duration_ms': result['duration_ms']})}\n\n"
            else:
                # Envia colunas primeiro
                yield f"data: {json.dumps({'columns': result['columns']})}\n\n"

                # Envia cada linha separadamente
                for row in result["preview"]:
                    yield f"data: {json.dumps({'row': row})}\n\n"
                    await asyncio.sleep(0)  # cede controle ao loop

            yield f"data: {json.dumps({'status': 'finished'})}\n\n"

        except Exception as e:
            log_message(f"Erro no SSE: {str(e)}", "error")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")



# 🔒 Segurança contra SQL Injection
def is_safe_identifier(identifier: str) -> bool:
    return re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", identifier) is not None


async def montar_filter_com_parametros(
    conditions: List[CondicaoFiltro],
    db_type: str = "postgres"
) -> Tuple[str, Dict[str, Any]]:
    """Monta o SQL do WHERE e os parâmetros com base nas condições fornecidas."""
    if not conditions:
        return "", {}

    where_clauses = []
    params: Dict[str, Any] = {}

    for cond in conditions:
        if not is_safe_identifier(cond.table_name_fil) or not is_safe_identifier(cond.column):
            raise ValueError(f"Identificador inválido: {cond.table_name_fil}.{cond.column}")

        field = f"{cond.table_name_fil}.{cond.column}"

        sql_part = get_filter_condition_with_operation(
            col_name=field,
            col_type=cond.column_type,
            value=cond.value,
            params=params,
            db_type=db_type,
            operation=cond.operator,
            param_name="",
            enum_values="",
            value_otheir_between=cond.value2,
        )

        logic = cond.logicalOperator or "AND"
        where_clauses.append((logic, sql_part))

    where_sql = where_clauses[0][1]
    for logic, clause in where_clauses[1:]:
        where_sql += f" {logic} {clause}"

    return f"WHERE {where_sql}", params


async def executar_query_e_salvar_async( 
    db: AsyncSession,
    user_id: int,
    connection: DBConnection,
    engine: AsyncEngine,
    queryrequest: QueryPayload,
    cache_consulta: bool = False
) -> dict:
    """Executa uma query dinâmica de forma assíncrona, salva no histórico e retorna o resultado."""
    log_message("🔎 Iniciando execução da query (async)...", "info")
    start = time.time()

    # 🔍 Validação
    if not is_safe_identifier(queryrequest.baseTable):
        raise ValueError("Nome da tabela base inválido")

    for join in queryrequest.joins:
        if not is_safe_identifier(join.table):
            raise ValueError(f"Tabela de junção inválida: {join.table}")

    # WHERE
    filters, params = "", {}
    if queryrequest.where:
        filters, params = await montar_filter_com_parametros(queryrequest.where, connection.type)

    # Query final
    if queryrequest.isCountQuery:
        query_string = get_count_query(
            base_table=queryrequest.baseTable,
            joins=queryrequest.joins,
            filters=filters,
            distinct=queryrequest.distinct,
            db_type=connection.type,
        )
    else:
        query_string = get_query_string(
            base_table=queryrequest.baseTable,
            joins=queryrequest.joins,
            aliases=queryrequest.aliaisTables,
            filters=filters,
            table_list=queryrequest.table_list,
            order_by=queryrequest.orderBy,
            max_rows=queryrequest.limit,
            offset=queryrequest.offset,
            db_type=connection.type,
            distinct=queryrequest.distinct,
        )

    log_message(f"📘 Query montada:\n{query_string}", "debug")
    log_message(f"📦 Parâmetros:\n{json.dumps(params, indent=2)}", "debug")

    # 🔄 Cache
    if cache_consulta:
        consulta_existente = await get_query_history_by_user_and_query(db, user_id, connection.id, query_string)
        if consulta_existente:
            log_message("⚠️ Consulta já registrada no histórico (cache).", "warning")

            if consulta_existente.error_message:
                raise ValueError(consulta_existente.error_message)

            if queryrequest.isCountQuery:
                return {
                    "success": True,
                    "count": int(consulta_existente.result_preview),
                    "query": consulta_existente.query,
                    "duration_ms": consulta_existente.duration_ms,
                    "cached": True,
                }

            preview_data = json.loads(consulta_existente.result_preview) if consulta_existente.result_preview else []
            columns = list(preview_data[0].keys()) if preview_data else []
            return {
                "success": True,
                "query": consulta_existente.query,
                "params": params,
                "duration_ms": consulta_existente.duration_ms,
                "columns": columns,
                "preview": preview_data,
                "cached": True,
            }

    # ⚡ Execução da query
    result_preview, error_message, colunas = None, None, []
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(query_string), parameters=params)

            if queryrequest.isCountQuery:
                result_preview = result.scalar_one_or_none()
                colunas = ["count"]
            else:
                linhas = result.fetchall()
                colunas = result.keys()
                preview = [
                    dict(zip(queryrequest.select or colunas, linha))
                    for linha in linhas
                ] if linhas else []
                result_preview = json.dumps(preview, default=str)

        log_message("✅ Query executada com sucesso (async).", "success")

    except Exception as e:
        await db.rollback()
        error_message = _lidar_com_erro_sql(e)

    duration_ms = int((time.time() - start) * 1000)

    # 📝 Histórico
    historico = QueryHistoryCreate(
        user_id=user_id,
        db_connection_id=connection.id,
        query=query_string,
        query_type="SELECT",
        executed_at=datetime.now(timezone.utc),
        duration_ms=duration_ms,
        result_preview=str(result_preview) if queryrequest.isCountQuery else result_preview,
        error_message=error_message,
        is_favorite=False,
        tags="count" if queryrequest.isCountQuery else "select",
    )
    await create_query_history(db=db, data=historico)

    # 🚨 Caso erro
    if error_message:
        log_message(f"📝 {error_message}", "error")
        raise Exception(f"Erro na execução da query:\n{error_message}")

    # 📤 Resposta final
    if queryrequest.isCountQuery:
        return {
            "success": True,
            "count": int(result_preview),
            "query": query_string,
            "duration_ms": duration_ms,
            "cached": False,
        }

    return {
        "success": True,
        "query": query_string,
        "params": params,
        "duration_ms": duration_ms,
        "columns": list(colunas),
        "preview": json.loads(result_preview) if result_preview else [],
        "cached": False,
    }
    