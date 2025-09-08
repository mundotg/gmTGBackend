from datetime import datetime
import json
import uuid
from sqlalchemy import Engine, text
from app.cruds.queryhistory_crud import create_query_history
from app.schemas.queryhistory_schemas import QueryHistoryCreate, UpdateRequest
from app.ultils.build_query import quote_identifier
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message
from sqlalchemy.orm import Session


def _map_column_type(col_type: str):
    """Mapeia o tipo da coluna para a função de conversão correspondente."""
    col_type = col_type.lower()

    if any(t in col_type for t in [
        "int", "integer", "smallint", "bigint", "tinyint", "serial", "bigserial", "number"
    ]):
        return int

    if any(t in col_type for t in [
        "float", "real", "double", "double precision", "decimal", "numeric"
    ]):
        return float

    elif any(t in col_type for t in ["bool", "bit", "boolean"]):
        return lambda x: str(x).lower() in ("true", "1", "yes", "t", "on") if x is not None else None

    elif "timestamp" in col_type:
        return lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S") if x else None

    elif "uuid" in col_type:
        return lambda x: uuid.UUID(x) if x else None

    elif any(t in col_type for t in ["json", "jsonb"]):
        return lambda x: json.loads(x) if isinstance(x, str) and x.strip() else None

    elif any(t in col_type for t in ["blob", "binary"]):
        return lambda x: bytes(x, "utf-8") if x is not None else None

    else:
        return str


def _convert_column_type_for_string_one(value, col_type):
    """Converte o valor baseado no tipo da coluna e retorna string SQL."""
    if value is None or value == "":
        return "NULL"

    converter = _map_column_type(col_type)
    try:
        if converter in [int, float]:
            return str(converter(value))
        elif converter.__name__ == "<lambda>":  # bool, datetime, uuid, json, bytes
            converted = converter(value)
            if isinstance(converted, bool):
                return str(converted).upper()  # PostgreSQL espera TRUE/FALSE
            elif isinstance(converted, (datetime, uuid.UUID)):
                return f"'{converted}'"
            elif isinstance(converted, (dict, list)):
                return f"'{json.dumps(converted)}'"
            elif isinstance(converted, bytes):
                return f"'{converted.decode('utf-8')}'"
            else:
                return f"'{converted}'"
        else:
            return f"'{converter(value)}'"
    except Exception as e:
        log_message(f"Erro ao converter valor '{value}' para {col_type}: {e}", "error")
        return "NULL"


def build_update_query(table_name, db_type, updated_values, primary_key, primary_value):
    """Constrói a query de atualização dinâmica."""
    set_clauses = []
    for col, info in updated_values.items():
        value = info.get("value")
        col_type = info.get("type_column", "text")  # default string
        set_clauses.append(f"{quote_identifier(db_type, col)} = {_convert_column_type_for_string_one(value, col_type)}")

    if not set_clauses:
        return None

    primary_value_formatted = f"'{primary_value}'" if isinstance(primary_value, str) else str(primary_value)
    query = text(f"""
        UPDATE {quote_identifier(db_type, table_name)}
        SET {', '.join(set_clauses)}
        WHERE {quote_identifier(db_type, primary_key)} = {primary_value_formatted};
    """)
    return query
def update_row_service(
    data: UpdateRequest,
    engine: Engine,
    user_id: int,
    db_type: str,
    connection_id: str,
    db: Session
):
    import time
    from datetime import datetime, timezone

    resposta_query = ""
    query_string = ""
    duration_ms = 0

    try:
        start = time.time()
        with engine.begin() as conn:
            for table_name, primary_key_data in data.tables_primary_keys_values.items():
                if "primaryKey" not in primary_key_data or "valor" not in primary_key_data:
                    raise ValueError(f"Tabela {table_name} não contém chave primária válida")

                primary_key = primary_key_data["primaryKey"]
                primary_value = primary_key_data["valor"]

                raw_values = data.updatedRow.get(table_name, {})

                updated_values = {
                    col: {
                        "value": field["value"] if isinstance(field, dict) else getattr(field, "value", None),
                        "type_column": field["type_column"] if isinstance(field, dict) else getattr(field, "type_column", "text")
                    }
                    for col, field in raw_values.items()
                }

                if not updated_values:
                    raise ValueError(f"Tabela {table_name}: Nenhuma coluna para atualizar")

                query = build_update_query(
                    table_name=table_name,
                    db_type=db_type,
                    updated_values=updated_values,
                    primary_key=primary_key,
                    primary_value=primary_value
                )

                query_string += f"{table_name}: {query}\n"
                rs = conn.execute(query)
                resposta_query += f"\n{table_name}: {rs.rowcount} linha(s) atualizada(s)"

        duration_ms = int((time.time() - start) * 1000)

        historico = QueryHistoryCreate(
            user_id=user_id,
            db_connection_id=connection_id,
            query=query_string.strip(),
            query_type="UPDATE",
            executed_at=datetime.now(timezone.utc),
            duration_ms=duration_ms,
            result_preview=resposta_query or "Nenhuma linha atualizada",
            error_message=None,
            is_favorite=False,
            tags="update"
        )
        create_query_history(db=db, data=historico)

        return {
            "status": resposta_query or "Nenhuma linha atualizada",
            "updated": data.updatedRow,
            "response": resposta_query
        }

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000) if duration_ms == 0 else duration_ms
        error_msg = _lidar_com_erro_sql(e)
        log_message(error_msg, "error")
        historico = QueryHistoryCreate(
            user_id=user_id,
            db_connection_id=connection_id,
            query=query_string.strip() or "UPDATE falhou antes de montar a query",
            query_type="UPDATE",
            executed_at=datetime.now(timezone.utc),
            duration_ms=duration_ms,
            result_preview=resposta_query or "Sem resultado devido ao erro",
            error_message=error_msg,
            is_favorite=False,
            tags="error"
        )
        create_query_history(db=db, data=historico)

        raise ValueError(error_msg)
