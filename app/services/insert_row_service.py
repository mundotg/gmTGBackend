from datetime import datetime, timezone
import time

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session
from app.cruds.queryhistory_crud import create_query_history
from app.schemas.queryhistory_schemas import InsertRequest, QueryHistoryCreate
from app.services.editar_linha import _convert_column_type_for_string_one
from app.ultils.build_query import quote_identifier
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
    Insere novos registros em uma ou mais tabelas com base em `data.updatedRow`.
    """
    resposta_query = ""
    query_string = ""
    duration_ms = 0

    try:
        start = time.time()
        with engine.begin() as conn:
            for table_name, raw_values in data.createdRow.items():
                insert_values = {
                    col: {
                        "value": field["value"] if isinstance(field, dict) else getattr(field, "value", None),
                        "type_column": field["type_column"] if isinstance(field, dict) else getattr(field, "type_column", "text")
                    }
                    for col, field in raw_values.items()
                }

                if not insert_values:
                    raise ValueError(f"Tabela {table_name}: Nenhuma coluna para inserir")

                query = build_insert_query(
                    table_name=table_name,
                    db_type=db_type,
                    insert_values=insert_values
                )

                query_string += f"{table_name}: {query}\n"
                rs = conn.execute(query)
                resposta_query += f"\n{table_name}: {rs.rowcount} linha(s) inserida(s)"

        duration_ms = int((time.time() - start) * 1000)

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
            tags="insert"
        )
        create_query_history(db=db, data=historico)
        log_message(f"registro inserido com sucesso {resposta_query}")

        return {
            "status": resposta_query or "Nenhuma linha inserida",
            "inserted": data.createdRow,
            "response": resposta_query
        }

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000) if duration_ms == 0 else duration_ms
        error_msg = _lidar_com_erro_sql(e)
        log_message(error_msg , "error")

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
            tags="error"
        )
        create_query_history(db=db, data=historico)

        raise ValueError(error_msg)
