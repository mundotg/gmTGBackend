import json
import time
import re
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine, Result
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError

from app.cruds.queryhistory_crud import create_query_history, get_query_history_by_user_and_query
from app.models.connection_models import DBConnection
from app.schemas.query_select_upAndInsert_schema import CondicaoFiltro, QueryPayload
from app.schemas.queryhistory_schemas import QueryHistoryCreate
from app.services.cloudeAi_execute_query import QueryFilterBuilder, QuerySecurityValidator
from app.ultils.build_query import get_count_query, get_filter_condition_with_operation, get_query_string_advance
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message

# Limite de linhas a manter no preview (evita OOM)
MAX_PREVIEW_ROWS = 500
# Tamanho do lote para fetchmany
FETCH_BATCH_SIZE = 100


def is_safe_identifier(identifier: str) -> bool:
    """
    Valida se um identificador é seguro.
    Aceita formatos: table, schema.table - sem caracteres especiais.
    """
    if not identifier:
        return False
    # permite opcionalmente "schema.table"
    return re.fullmatch(r"(?:[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:[A-Za-z_][A-Za-z0-9_]*))?", identifier) is not None


def montar_filter_com_parametros(
    conditions: Optional[List[CondicaoFiltro]],
    db_type: str = "postgres"
) -> Tuple[str, Dict[str, Any]]:
    """
    Monta a cláusula WHERE e o dicionário de parâmetros.
    Retorna ("", {}) se conditions for None ou vazio.
    Gera nomes de parâmetro únicos por condição para evitar colisões.
    """
    if not conditions:
        return "", {}

    where_clauses: List[str] = []
    params: Dict[str, Any] = {}

    for idx, cond in enumerate(conditions):
        if not is_safe_identifier(cond.table_name_fil) or not is_safe_identifier(cond.column):
            raise ValueError(f"Identificador inválido: {cond.table_name_fil}.{cond.column}")

        field = f"{cond.table_name_fil}.{cond.column}"
        # gera um prefixo único para o param name
        param_prefix = f"p_{idx}_"

        sql_part = get_filter_condition_with_operation(
            col_name=field,
            col_type=cond.column_type,
            value=cond.value,
            params=params,
            db_type=db_type,
            operation=cond.operator,
            param_name=param_prefix,
            enum_values={},
            value_otheir_between=cond.value2 or ""
        )

        logic = (cond.logicalOperator or "AND").strip().upper()
        if logic not in ("AND", "OR"):
            logic = "AND"
        where_clauses.append((logic, sql_part))

    if not where_clauses:
        return "", {}

    # Concatena respeitando operadores lógicos (assume ordem sequencial)
    where_sql = where_clauses[0][1]
    for logic, clause in where_clauses[1:]:
        where_sql = f"{where_sql} {logic} {clause}"

    return f"WHERE {where_sql}", params


async def executar_query_e_salvar(
    db: Session,
    user_id: int,
    connection: DBConnection,
    engine: Engine,
    queryrequest: QueryPayload,
    chache_consulta: bool = False
) -> Dict[str, Any]:
    """
    Executa a query de forma segura, salva histórico e retorna um dicionário com o resultado.
    Evita fetchall() para grandes consultas e trata erros de SQL separadamente.
    """
    log_message("🔎 Iniciando execução da query com filtros...", "info")
    start = time.time()
    security_validator = QuerySecurityValidator()
    filter_builder = QueryFilterBuilder()
    # Validações básicas
    if not queryrequest:
        raise ValueError("QueryPayload é obrigatório")

    security_validator.ensure_base_table_in_query(queryrequest)
    # Filtros WHERE 
    filters, params = await filter_builder.build_where_clause(
                queryrequest.where or [], connection.type
            )
    # filters, params = montar_filter_com_parametros(queryrequest.where, connection.type)
    # Monta query (count ou select)
    if queryrequest.isCountQuery:
        query_string = get_count_query(
            base_table=queryrequest.baseTable,
            joins=queryrequest.joins or [],
            filters=filters,
            distinct=queryrequest.distinct,
            db_type=connection.type
        )
    else:
        query_string = get_query_string_advance(
                base_table=queryrequest.baseTable,
                select=queryrequest.select,
                joins=queryrequest.joins,
                aliases=queryrequest.aliaisTables,
                filters=filters,
                table_list=queryrequest.table_list,
                order_by=queryrequest.orderBy,
                max_rows=queryrequest.limit or 1,
                offset=queryrequest.offset,
                db_type=connection.type,
                distinct=queryrequest.distinct,
            )
        """ get_query_string(
            base_table=queryrequest.baseTable,
            joins=queryrequest.joins or [],
            select=queryrequest.select,
            filters=filters,
            table_list=queryrequest.table_list,
            order_by=queryrequest.orderBy,
            max_rows=queryrequest.limit,
            offset=queryrequest.offset,
            db_type=connection.type,
            distinct=queryrequest.distinct
        ) """

    log_message(f"📘 Query montada:\n{query_string}", "debug")
    log_message(f"📦 Parâmetros: {json.dumps(params, indent=2, default=str)}", "debug")

    # Check cache
    if chache_consulta:
        try:
            consulta_existente = get_query_history_by_user_and_query(db, user_id, connection.id, query_string)
            if consulta_existente:
                log_message("⚠️ Consulta já registrada no histórico. Retornando resultado salvo.", "warning")
                # se existiu erro operacional, repassa
                if consulta_existente.error_message:
                    # se for erro operacional genérico, levanta para re-executar
                    if "Erro operacional ao executar a consulta" in (consulta_existente.error_message or ""):
                        pass
                    else:
                        raise ValueError(consulta_existente.error_message)

                if queryrequest.isCountQuery:
                    count_value = int(consulta_existente.result_preview or 0)
                    return {
                        "success": True,
                        "count": count_value,
                        "query": consulta_existente.query,
                        "duration_ms": consulta_existente.duration_ms,
                        "cached": True
                    }
                else:
                    preview_data = json.loads(consulta_existente.result_preview) if consulta_existente.result_preview else []
                    columns = list(preview_data[0].keys()) if isinstance(preview_data, list) and preview_data else []
                    return {
                        "success": True,
                        "query": consulta_existente.query,
                        "params": params,
                        "duration_ms": consulta_existente.duration_ms,
                        "columns": columns,
                        "preview": preview_data,
                        "cached": True
                    }
        except Exception as e:
            # erro ao consultar cache: loga e segue para executar ao invés de falhar
            log_message(f"⚠️ Erro ao acessar cache: {e}", "warning")

    # Variáveis de retorno
    result_preview: Optional[str] = None
    error_message: Optional[str] = None
    colunas: List[str] = []

    try:
        with engine.connect() as conn:
            result: Result = conn.execute(text(query_string), parameters=params)

            if queryrequest.isCountQuery:
                count_value = result.scalar()
                result_preview = str(count_value or 0)
                colunas = ["count"]
                log_message(f"📊 Count obtido: {result_preview}", "debug")
            else:
                # Evita trazer tudo para memória: monta preview em batches até MAX_PREVIEW_ROWS
                preview_rows: List[Dict[str, Any]] = []
                keys = list(result.keys())
                colunas = keys

                fetched = 0
                while True:
                    batch = result.fetchmany(FETCH_BATCH_SIZE)
                    if not batch:
                        break

                    for row in batch:
                        if queryrequest.select:
                            # se select informado, usa ordem do select
                            preview_rows.append(dict(zip(queryrequest.select, row)))
                        else:
                            preview_rows.append(dict(zip(keys, row)))

                        fetched += 1
                        if fetched >= MAX_PREVIEW_ROWS:
                            break

                    if fetched >= MAX_PREVIEW_ROWS:
                        break

                result_preview = json.dumps(preview_rows, default=str)

                log_message(f"✅ Preview gerado com {fetched} linhas (cap {MAX_PREVIEW_ROWS}).", "debug")

        log_message("✅ Query executada com sucesso.", "success")

    except SQLAlchemyError as sa_err:
        # trata erros SQL de forma específica
        error_message = _lidar_com_erro_sql(sa_err)
        log_message(f"⚠️ Erro SQL: {error_message}", "error")
    except Exception as e:
        # erro genérico
        error_message = str(e)
        log_message(f"❌ Erro inesperado: {error_message}", "error")

    duration_ms = int((time.time() - start) * 1000)

    # Salva histórico sempre — inclui error_message para auditoria
    try:
        historico = QueryHistoryCreate(
            user_id=user_id,
            db_connection_id=connection.id,
            query=query_string,
            query_type="SELECT",
            executed_at=datetime.now(timezone.utc),
            duration_ms=duration_ms,
            result_preview=(
                result_preview if not queryrequest.isCountQuery else str(result_preview)
            ),
            error_message=error_message,
            is_favorite=False,
            tags="count" if queryrequest.isCountQuery else "select",
            app_source="API",  # ou "Console", "UI", dependendo da origem
            client_ip=queryrequest.client_ip if hasattr(queryrequest, "client_ip") else None,
            executed_by=queryrequest.executed_by if hasattr(queryrequest, "executed_by") else f"user_{user_id}",
            modified_by=None,  # só será usado em updates posteriores
            meta_info={
                "base_table": queryrequest.baseTable,
                "limit": queryrequest.limit,
                "offset": queryrequest.offset,
                "joins": len(queryrequest.joins or []),
                "filters_count": len(queryrequest.where or []),
                "timestamp": datetime.utcnow().isoformat(),
                "cached_used": chache_consulta,
                "connection_type": connection.type,
            },
        )
        create_query_history(db=db,user_id=user_id, data=historico)
    except Exception as hist_err:
        log_message(f"⚠️ Falha ao salvar histórico: {hist_err}", "warning")

    # Se houve erro, lança para o controller lidar (ou retorna estrutura apropriada)
    if error_message:
        # aqui escolhemos lançar Exception com mensagem amigável
        raise Exception(f"Erro na execução da query:\n{error_message}")

    # Monta retorno
    if queryrequest.isCountQuery:
        return {
            "success": True,
            "count": int(result_preview) if result_preview is not None and str(result_preview).isdigit() else 0,
            "query": query_string,
            "duration_ms": duration_ms
        }

    return {
        "success": True,
        "query": query_string,
        "params": params,
        "duration_ms": duration_ms,
        "columns": colunas,
        "preview": json.loads(result_preview) if result_preview else [],
        "cached": False
    }
