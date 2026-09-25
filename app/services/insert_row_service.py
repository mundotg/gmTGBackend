import json
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.cruds.queryhistory_crud import create_query_history
from app.schemas.query_select_upAndInsert_schema import InsertRequest
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryType
from app.services.editar_linha import _convert_column_type_for_string_one, quote_identifier
from app.ultils.conect_database import assert_sql_engine, is_mongo, get_mongo_database
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message

def build_insert_query(table_name, db_type, insert_values):
    """Constrói a query de inserção dinâmica."""
    if not insert_values:
        return None

    # print(f"DEBUG: db_type in build_insert_query = '{db_type}'")  # DEBUG
    
    columns = []
    values = []

    for col, info in insert_values.items():
        value = info.get("value")
        col_type = info.get("type_column", "text")  # default string
        
        # Quote the column name
        quoted_col = quote_identifier(db_type, col)
        # print(f"DEBUG: Column '{col}' -> quoted as '{quoted_col}'")  # DEBUG
        
        columns.append(quoted_col)
        values.append(_convert_column_type_for_string_one(value, col_type))

    # Quote the table name
    quoted_table = quote_identifier(db_type, table_name)
    # print(f"DEBUG: Table '{table_name}' -> quoted as '{quoted_table}'")  # DEBUG

    query = text(f"""
        INSERT INTO {quoted_table}
        ({', '.join(columns)})
        VALUES ({', '.join(values)});
    """)
    
    # print(f"DEBUG: Final query: {query}")  # DEBUG
    return query

# ============================================================
#  MongoDB (NoSQL) — inserção de documentos
# ============================================================
def _coerce_mongo_value(col: str, value: Any, type_column: Optional[str]) -> Any:
    """Converte o valor (que chega como string) para o tipo adequado no Mongo."""
    if value is None:
        return None
    s = str(value)
    t = (type_column or "").lower()

    # _id em formato ObjectId (24 hex) → ObjectId; caso contrário fica como está.
    if col == "_id":
        try:
            from bson import ObjectId

            if isinstance(value, str) and len(value) == 24:
                return ObjectId(value)
        except Exception:  # noqa: BLE001 - valor pode não ser ObjectId
            return value
        return value

    if any(k in t for k in ("int", "serial")) and "point" not in t:
        try:
            f = float(s)
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return value
    if any(k in t for k in ("float", "double", "decimal", "numeric", "real", "money", "number")):
        try:
            return float(s)
        except (TypeError, ValueError):
            return value
    if "bool" in t or t == "bit":
        return s.strip().lower() in ("true", "1", "yes", "sim", "t")
    if any(k in t for k in ("json", "jsonb", "object", "array")):
        try:
            return json.loads(s)
        except Exception:  # noqa: BLE001
            return value
    return value  # texto / data / uuid ficam como estão


def _insert_mongo_service(
    data: InsertRequest,
    engine: Any,
    user_id: int,
    connection_id: str,
    db: Session,
    *,
    client_ip: Optional[str],
    app_source: str,
    executed_by: str,
    modified_by: Optional[str],
) -> dict:
    """
    Equivalente NoSQL do INSERT: cada "tabela" de `createdRow` é uma COLEÇÃO e
    cada conjunto de campos vira um DOCUMENTO (`insert_one`). Devolve o mesmo
    formato do caminho SQL. Campos vazios são omitidos (o Mongo aplica default
    / gera o `_id`).
    """
    start_time = time.time()
    total_inseridos = 0
    total_tabelas = 0
    resposta = ""
    inserted_ids: dict = {}
    error_msg = None
    sucesso = False

    try:
        for table_name, raw_values in data.createdRow.items():
            doc: dict = {}
            for col, field in raw_values.items():
                value = field["value"] if isinstance(field, dict) else getattr(field, "value", None)
                tcol = (
                    field.get("type_column", "text")
                    if isinstance(field, dict)
                    else getattr(field, "type_column", "text")
                )
                if value is None or str(value).strip() == "":
                    continue  # omite vazios → default/_id automático
                doc[col] = _coerce_mongo_value(col, value, tcol)

            if not doc:
                log_message(f"Aviso: coleção '{table_name}' ignorada (sem campos).", "warning")
                continue

            # Resolve base + coleção.
            if "." in table_name:
                db_name, coll_name = table_name.split(".", 1)
                database = engine[db_name]
            else:
                database = get_mongo_database(engine)
                coll_name = table_name
            if database is None:
                raise ValueError("Não foi possível determinar a base MongoDB.")

            res = database[coll_name].insert_one(doc)
            total_tabelas += 1
            total_inseridos += 1
            inserted_ids[table_name] = str(res.inserted_id)
            resposta += f"{coll_name}: 1 documento inserido (_id={res.inserted_id}).\n"

        sucesso = True
        log_message(f"✅ Documento(s) inserido(s) no MongoDB:\n{resposta}", "success")

    except Exception as e:  # noqa: BLE001
        error_msg = str(e)
        log_message(f"❌ Erro no INSERT MongoDB: {error_msg}\n{traceback.format_exc()}", "error")

    duration_ms = int((time.time() - start_time) * 1000)

    # Histórico (best-effort).
    try:
        create_query_history(
            db=db,
            user_id=user_id,
            data=QueryHistoryCreate(
                user_id=user_id,
                db_connection_id=connection_id,  # type: ignore
                query=f"db.insertOne(...) → {', '.join(inserted_ids.keys()) or 'n/a'}",
                query_type=QueryType.INSERT,
                executed_at=datetime.now(timezone.utc),
                duration_ms=duration_ms,
                result_preview=resposta.strip() if sucesso else "Falha na inserção.",
                error_message=error_msg,
                is_favorite=False,
                tags="insert" if sucesso else "insert_error",
                app_source=app_source,
                client_ip=client_ip,
                executed_by=executed_by or f"user_{user_id}",
                modified_by=modified_by,
                meta_info={
                    "engine": "mongodb",
                    "colecoes_afetadas": list(data.createdRow.keys()),
                    "total_inseridos": total_inseridos,
                    "inserted_ids": inserted_ids,
                    "status": "success" if sucesso else "failed",
                },
            ),
        )
    except Exception as hist_err:  # noqa: BLE001
        log_message(f"⚠️ Falha ao salvar histórico (mongo insert): {hist_err}", "warning")

    if not sucesso:
        raise ValueError(error_msg)

    return {
        "status": "sucesso",
        "inserted": data.createdRow,
        "inserted_ids": inserted_ids,
        "response": resposta.strip(),
        "tempo_ms": duration_ms,
        "linhas_inseridas": total_inseridos,
    }


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
    # NoSQL: MongoDB não usa SQL/engine.begin() → insere documentos.
    if is_mongo(engine):
        return _insert_mongo_service(
            data,
            engine,
            user_id,
            connection_id,
            db,
            client_ip=client_ip,
            app_source=app_source,
            executed_by=executed_by,
            modified_by=modified_by,
        )

    assert_sql_engine(engine, "Inserção de registos")

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