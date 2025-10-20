from datetime import datetime, timezone
import time
import traceback

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session
from app.cruds.queryhistory_crud import create_query_history
from app.schemas.query_select_upAndInsert_schema import InsertRequest
from app.schemas.queryhistory_schemas import QueryHistoryCreate
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
    db: Session
):
    """
    Insere novos registros em uma ou mais tabelas com base em `data.createdRow`.
    Agora com registro de histórico completo e metadados.
    """
    resposta_query = ""
    query_string = ""
    duration_ms = 0
    total_inseridos = 0
    total_tabelas = 0

    try:
        start = time.time()
        with engine.begin() as conn:
            for table_name, raw_values in data.createdRow.items():
                total_tabelas += 1
                insert_values = {
                    col: {
                        "value": field["value"] if isinstance(field, dict) else getattr(field, "value", None),
                        "type_column": field["type_column"] if isinstance(field, dict) else getattr(field, "type_column", "text")
                    }
                    for col, field in raw_values.items()
                }

                if not insert_values:
                    raise ValueError(f"Tabela {table_name}: Nenhuma coluna para inserir")
                # print("insert_values",insert_values)
                query = build_insert_query(
                    table_name=table_name,
                    db_type=db_type,
                    insert_values=insert_values
                )

                query_string += f"{table_name}: {query}\n"
                rs = conn.execute(query)
                total_inseridos += rs.rowcount or 0
                resposta_query += f"\n{table_name}: {rs.rowcount} linha(s) inserida(s)"

        duration_ms = int((time.time() - start) * 1000)

        # ✅ Monta o histórico completo com metadados adicionais
        historico = QueryHistoryCreate(
            user_id=user_id,
            db_connection_id=connection_id,
            query=query_string.strip(),
            query_type="INSERT",
            executed_at=datetime.now(timezone.utc),
            duration_ms=duration_ms,
            result_preview=resposta_query or "Nenhuma linha inserida",
            error_message=None,
            is_favorite=False,
            tags="insert",
            app_source="API",
            client_ip=getattr(data, "client_ip", None),
            executed_by=getattr(data, "executed_by", f"user_{user_id}"),
            modified_by=None,
            meta_info={
                "tabelas_afetadas": list(data.createdRow.keys()),
                "total_inseridos": total_inseridos,
                "total_tabelas": total_tabelas,
                "tempo_execucao_ms": duration_ms,
                "db_type": db_type,
                "connection_id": connection_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )

        create_query_history(db=db, data=historico)
        log_message(f"✅ Registro(s) inserido(s) com sucesso: {resposta_query}", "success")

        return {
            "status": "sucesso",
            "inserted": data.createdRow,
            "response": resposta_query,
            "tempo_ms": duration_ms,
            "linhas_inseridas": total_inseridos
        }

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000) if duration_ms == 0 else duration_ms
        error_msg = _lidar_com_erro_sql(e)
        log_message(f"❌ Erro ao inserir registros: {error_msg}", "error")

        # 🔥 Grava histórico mesmo em caso de erro
        historico = QueryHistoryCreate(
            user_id=user_id,
            db_connection_id=connection_id,
            query=query_string.strip() or "INSERT falhou antes de montar a query",
            query_type="INSERT",
            executed_at=datetime.now(timezone.utc),
            duration_ms=duration_ms,
            result_preview=resposta_query or "Sem resultado devido ao erro",
            error_message=error_msg,
            is_favorite=False,
            tags="insert_error",
            app_source="API",
            client_ip=getattr(data, "client_ip", None),
            executed_by=getattr(data, "executed_by", f"user_{user_id}"),
            modified_by=None,
            meta_info={
                "tabelas_afetadas": list(data.createdRow.keys()) if hasattr(data, "createdRow") else [],
                "tempo_execucao_ms": duration_ms,
                "db_type": db_type,
                "connection_id": connection_id,
                "exception_type": type(e).__name__,
                "traceback": traceback.format_exc()
            }
        )

        try:
            create_query_history(db=db, data=historico)
        except Exception as hist_err:
            log_message(f"⚠️ Falha ao salvar histórico de erro: {hist_err}", "warning")

        raise ValueError(error_msg)
