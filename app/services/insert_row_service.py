import time
import traceback
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.cruds.queryhistory_crud import create_query_history
from app.schemas.query_select_upAndInsert_schema import InsertRequest
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryType
from app.services.editar_linha import _convert_column_type_for_string_one, quote_identifier
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message

def build_insert_query(table_name, db_type, insert_values):
    """Constrói a query de inserção dinâmica."""
    if not insert_values:
        return None

    columns = []
    values = []

    for col, info in insert_values.items():
        value = info.get("value")
        col_type = info.get("type_column", "text")  # default string
        columns.append(quote_identifier(db_type, col))
        # IDEAL: Aqui deveríamos usar parâmetros nomeados (:p1, :p2) em vez de inserir o valor direto na string.
        # Vamos manter sua função original, mas fica o aviso para o futuro!
        values.append(_convert_column_type_for_string_one(value, col_type))

    query = text(f"""
        INSERT INTO {quote_identifier(db_type, table_name)}
        ({', '.join(columns)})
        VALUES ({', '.join(values)});
    """)
    return query

def insert_row_service(
    data: InsertRequest,
    engine: Engine,
    user_id: int,
    db_type: str,
    connection_id: str,
    db: Session,
    client_ip: Optional[str] = None,
    app_source: str = "API",
    executed_by: str = "sistema",
    modified_by: Optional[str] = None,
) -> dict:
    """
    Insere novos registros em uma ou mais tabelas com base em `data.createdRow`.
    Garante transação ACID: Se uma tabela falhar, todas as inserções são revertidas.
    """
    resposta_query = ""
    query_string = ""
    start_time = time.time()
    
    total_inseridos = 0
    total_tabelas = 0
    sucesso = False
    error_msg = None
    traceback_str = None

    try:
        # 🚀 Inicia a Transação
        with engine.begin() as conn:
            for table_name, raw_values in data.createdRow.items():
                
                # 1. Estruturação dos dados
                insert_values = {
                    col: {
                        "value": field["value"] if isinstance(field, dict) else getattr(field, "value", None),
                        "type_column": field.get("type_column", "text") if isinstance(field, dict) else getattr(field, "type_column", "text")
                    }
                    for col, field in raw_values.items()
                }

                # Se não houver dados, pula para a próxima tabela sem quebrar a transação
                if not insert_values:
                    log_message(f"Aviso: Tabela '{table_name}' ignorada (nenhuma coluna informada).", "warning")
                    continue

                total_tabelas += 1

                # 2. Monta a Query
                query = build_insert_query(
                    table_name=table_name,
                    db_type=db_type,
                    insert_values=insert_values
                )

                if query is None:
                    continue

                query_string += f"-- INSERT em {table_name}\n{query}\n"
                
                # 3. Execução
                rs = conn.execute(query)
                linhas_afetadas = rs.rowcount or 0
                
                total_inseridos += linhas_afetadas
                resposta_query += f"{table_name}: {linhas_afetadas} linha(s) inserida(s).\n"

        sucesso = True
        log_message(f"✅ Registro(s) inserido(s) com sucesso:\n{resposta_query}", "success")

    except SQLAlchemyError as sa_err:
        error_msg = _lidar_com_erro_sql(sa_err)
        traceback_str = traceback.format_exc()
        log_message(f"❌ Erro de Banco de Dados no INSERT: {error_msg}", "error")
    except Exception as e:
        error_msg = str(e)
        traceback_str = traceback.format_exc()
        log_message(f"❌ Erro inesperado no INSERT: {error_msg}", "error")

    # ==========================================
    # 🧾 SALVAMENTO DO HISTÓRICO (Roda sempre)
    # ==========================================
    duration_ms = int((time.time() - start_time) * 1000)

    historico = QueryHistoryCreate(
        user_id=user_id,
        db_connection_id=connection_id, # type: ignore
        query=query_string.strip() or "INSERT não gerou query.",
        query_type=QueryType.INSERT,
        executed_at=datetime.now(timezone.utc),
        duration_ms=duration_ms,
        result_preview=resposta_query.strip() if sucesso else "Sem resultado devido a falha.",
        error_message=error_msg,
        is_favorite=False,
        tags="insert" if sucesso else "insert_error",
        app_source=app_source,
        client_ip=getattr(data, "client_ip", client_ip),
        executed_by=getattr(data, "executed_by", executed_by) or f"user_{user_id}",
        modified_by=modified_by,
        meta_info={
            "tabelas_afetadas": list(data.createdRow.keys()) if hasattr(data, "createdRow") else [],
            "total_inseridos": total_inseridos,
            "total_tabelas": total_tabelas,
            "db_type": db_type,
            "status": "success" if sucesso else "failed",
            "traceback": traceback_str if not sucesso else None,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )

    try:
        create_query_history(db=db, user_id=user_id, data=historico)
    except Exception as hist_err:
        log_message(f"⚠️ Falha ao salvar histórico de INSERT: {hist_err}", "warning")

    # ==========================================
    # 🚀 RETORNO / EXCEÇÃO
    # ==========================================
    if not sucesso:
        raise ValueError(error_msg)

    return {
        "status": "sucesso",
        "inserted": data.createdRow,
        "response": resposta_query.strip(),
        "tempo_ms": duration_ms,
        "linhas_inseridas": total_inseridos
    }