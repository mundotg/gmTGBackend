import json
import time
import re
import traceback
from datetime import datetime, timezone
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

from app.cruds.queryhistory_crud import create_query_history
from app.models.connection_models import DBConnection
from app.schemas.queryhistory_schemas import CondicaoFiltro, QueryHistoryCreate, QueryPayload
from app.ultils.build_query import get_filter_condition_with_operation, get_query_string
from app.ultils.logger import log_message


def is_safe_identifier(identifier: str) -> bool:
    """Valida se um identificador (tabela/coluna) é seguro contra SQL Injection."""
    return re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", identifier) is not None


def montar_filter_com_parametros(
    conditions: List[CondicaoFiltro],
    db_type: str = "postgres"
) -> tuple[str, dict]:
    """Monta o SQL do WHERE e os parâmetros com base nas condições."""
    where_clauses = []
    params = {}

    for cond in conditions:
        if not is_safe_identifier(cond.table_name_fil) or not is_safe_identifier(cond.column):
            raise ValueError("Identificador inválido: tabela ou coluna com nome inseguro.")

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
            value_otheir_between=""
        )

        logic = cond.logicalOperator or "AND"
        where_clauses.append((logic, sql_part))

    # Concatenação lógica das cláusulas
    where_sql = where_clauses[0][1]
    for logic, clause in where_clauses[1:]:
        where_sql += f" {logic} {clause}"

    return f"WHERE {where_sql}", params


def executar_query_e_salvar(
    db: Session,
    user_id: int,
    connection: DBConnection,
    engine: Engine,
    queryrequest: QueryPayload
) -> dict:
    log_message("🔎 Iniciando execução da query com filtros...", "info")
    start = time.time()

    # Validação do nome da tabela base
    if not is_safe_identifier(queryrequest.baseTable):
        raise ValueError("Nome da tabela base inválido")

    # Validação das tabelas de junção
    for join in queryrequest.joins:
        if not is_safe_identifier(join.table):
            raise ValueError(f"Tabela de junção inválida: {join.table}")

    # Filtros WHERE
    filters = ""
    params = {}

    if queryrequest.where:
        filters, params = montar_filter_com_parametros(queryrequest.where, connection.type)

    # Montagem da query final
    query_string = get_query_string(
        base_table=queryrequest.baseTable,
        joins=queryrequest.joins,
        select=queryrequest.select,
        filters=filters,
        table_list=queryrequest.table_list,
        order_by=queryrequest.orderBy,
        max_rows=queryrequest.limit,
        offset=queryrequest.offset,
        db_type=connection.type,
        distinct=queryrequest.distinct  # Adicionado DISTINCT se estiver presente no schema
    )

    log_message(f"📘 Query montada:\n{query_string}", "debug")
    log_message(f"📦 Parâmetros:\n{json.dumps(params, indent=2)}", "debug")

    result_preview = None
    error_message = None
    colunas = []

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query_string), parameters=params)
            linhas = result.fetchall()
            colunas = result.keys()
            preview = [dict(zip(queryrequest.select, linha)) for linha in linhas] if linhas else []
            result_preview = json.dumps(preview, default=str)

        log_message("✅ Query executada com sucesso.", "success")

    except Exception as e:
        error_message = f"{str(e)}\n{traceback.format_exc()}"
        db.rollback()
        log_message(f"❌ Erro na execução da query:\n{error_message}", "error")

    duration_ms = int((time.time() - start) * 1000)

    historico = QueryHistoryCreate(
        user_id=user_id,
        db_connection_id=connection.id,
        query=query_string,
        query_type="SELECT",
        executed_at=datetime.now(timezone.utc),
        duration_ms=duration_ms,
        result_preview=result_preview,
        error_message=error_message,
        is_favorite=False,
        tags=""
    )

    create_query_history(db=db, data=historico)
    if error_message:
        log_message(f"📝  {error_message}", "error")
        raise Exception(f"Erro na execução da query:\n{error_message}")

    return {
        "success": True,
        "query": query_string,
        "params": params,
        "duration_ms": duration_ms,
        "columns": list(colunas),
        "preview": json.loads(result_preview) if result_preview else []
    }
