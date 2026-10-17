import time
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List, Union

from sqlalchemy.orm import Session
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.cruds.queryhistory_crud import create_query_history
from app.schemas.query_select_upAndInsert_schema import UpdateRequest, UpdateAllRequest, UpdateSelectedRequest
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryType
from app.services.editar_linha import (
    quote_identifier,
    _convert_column_type_for_string_one,
    _convert_column_type_for_string_one_V1,
)
from app.services.query_executor import montar_filter_com_parametros
from app.ultils.conect_database import is_mongo
from app.ultils.build_query import build_update_query
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message

def update_row_service(
    data: UpdateRequest,
    engine: Any,
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
    Atualiza registros existentes em uma ou mais tabelas e salva no histórico completo.
    Garante transação ACID: Se uma tabela falhar, todas as alterações são revertidas.
    """
    resposta_query = ""
    query_string = ""
    start_time = time.time()
    
    sucesso = False
    error_msg = None

    try:
        # 1. Suporte NoSQL MongoDB
        if is_mongo(engine):
            from app.services.mongo_query_executor import _resolve_db_and_collection
            from app.ultils.conect_database import get_mongo_database

            for table_name, primary_key_data in data.tables_primary_keys_values.items():
                primary_key = primary_key_data.get("primaryKey")
                primary_value = primary_key_data.get("valor")

                if not primary_key or primary_value is None:
                    continue

                raw_values = data.updatedRow.get(table_name, {})
                if not raw_values:
                    continue

                db_name, collection_name = _resolve_db_and_collection(None, table_name)
                database = engine[db_name] if db_name else get_mongo_database(engine)
                if database is None:
                    continue

                collection = database[collection_name]
                pk_clean = primary_key.split(".")[-1]

                def _to_mongo_pk(val):
                    try:
                        from bson import ObjectId
                        if isinstance(val, str) and ObjectId.is_valid(val):
                            return ObjectId(val)
                    except Exception:
                        pass
                    return val

                if pk_clean in ("_id", "id"):
                    id_val = _to_mongo_pk(primary_value)
                    filtro = {"$or": [{"_id": id_val}, {"id": primary_value}]} if pk_clean == "id" else {"_id": id_val}
                else:
                    filtro = {pk_clean: primary_value}

                set_dict = {
                    col.split(".")[-1]: field["value"] if isinstance(field, dict) else getattr(field, "value", None)
                    for col, field in raw_values.items()
                }

                if set_dict:
                    res = collection.update_one(filtro, {"$set": set_dict})
                    resposta_query += f"{collection_name}: {res.modified_count} documento(s) atualizado(s).\n"

            sucesso = True
        else:
            # 2. Bancos Relacionais (PostgreSQL, MySQL, SQLite, Oracle, SQL Server, etc.)
            with engine.begin() as conn:
                for table_name, primary_key_data in data.tables_primary_keys_values.items():
                    primary_key = primary_key_data.get("primaryKey")
                    primary_value = primary_key_data.get("valor")

                    if not primary_key or primary_value is None:
                        raise ValueError(f"A tabela '{table_name}' não contém uma chave primária válida para o UPDATE.")

                    raw_values = data.updatedRow.get(table_name, {})
                    updated_values = {
                        col.split(".")[-1]: {
                            "value": field["value"] if isinstance(field, dict) else getattr(field, "value", None),
                            "type_column": field.get("type_column", "text") if isinstance(field, dict) else getattr(field, "type_column", "text")
                        }
                        for col, field in raw_values.items()
                    }

                    if not updated_values:
                        log_message(f"Aviso: Tabela '{table_name}' ignorada pois não possui colunas alteradas.", "warning")
                        continue

                    query = build_update_query(
                        table_name=table_name,
                        db_type=db_type,
                        updated_values=updated_values,
                        primary_key=primary_key,
                        primary_value=primary_value
                    )
                    if query is None:
                        continue

                    query_string += f"-- UPDATE em {table_name}\n{str(query)}\n"
                    rs = conn.execute(query)
                    linhas = rs.rowcount if rs.rowcount is not None and rs.rowcount >= 0 else 1
                    resposta_query += f"{table_name}: {linhas} linha(s) atualizada(s).\n"

            sucesso = True
        log_message(f"✅ Atualização concluída:\n{resposta_query}", "success")

    except SQLAlchemyError as sa_err:
        error_msg = _lidar_com_erro_sql(sa_err)
        log_message(f"❌ Erro de Banco de Dados ao atualizar registro: {error_msg}", "error")
    except Exception as e:
        error_msg = str(e)
        log_message(f"❌ Erro inesperado ao atualizar registro: {error_msg}", "error")

    # ==========================================
    # 🧾 SALVAMENTO DO HISTÓRICO (Roda sempre)
    # ==========================================
    duration_ms = int((time.time() - start_time) * 1000)

    historico = QueryHistoryCreate(
        user_id=user_id,
        db_connection_id=connection_id, # type: ignore
        query=query_string.strip() or "UPDATE não gerou query.",
        query_type=QueryType.UPDATE,
        executed_at=datetime.now(timezone.utc),
        duration_ms=duration_ms,
        result_preview=resposta_query.strip() if sucesso else "Sem resultado devido a falha.",
        error_message=error_msg, # Fica None se sucesso for True
        is_favorite=False,
        tags="update" if sucesso else "update_error",
        app_source=app_source,
        client_ip=client_ip,
        executed_by=executed_by,
        modified_by=modified_by or executed_by,
        meta_info={
            "db_type": db_type,
            "tables_attempted": list(data.updatedRow.keys()),
            "primary_keys": data.tables_primary_keys_values,
            "status": "success" if sucesso else "failed",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )

    # Salva o histórico (mesmo se o update original deu Rollback, essa sessão DB salva o log)
    create_query_history(db=db, user_id=user_id, data=historico)

    # ==========================================
    # 🚀 RETORNO / EXCEÇÃO
    # ==========================================
    if not sucesso:
        # Repassa o erro formatado para o FastAPI devolver um 400/500 amigável pro frontend
        raise ValueError(error_msg)

    return {
        "status": "success",
        "updated": data.updatedRow,
        "response": resposta_query.strip() or "Nenhuma alteração foi realizada."
    }


def update_all_by_query_service(
    data: UpdateAllRequest,
    engine: Any,
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
    Atualiza todos os registros correspondentes a uma query (baseTable + where).
    Suporta bancos relacionais (via SQL) e NoSQL (MongoDB).
    """
    resposta_query = ""
    query_string = ""
    start_time = time.time()
    total_afetados = 0
    sucesso = False
    error_msg = None

    try:
        # 1. Suporte NoSQL MongoDB
        if is_mongo(engine):
            from app.services.mongo_query_executor import (
                _resolve_db_and_collection,
                _build_mongo_filter,
            )
            from app.ultils.conect_database import get_mongo_database

            mongo_filter = _build_mongo_filter(data.query.where)
            for table_name, raw_values in data.updatedRow.items():
                target_table = table_name or data.query.baseTable
                db_name, collection_name = _resolve_db_and_collection(None, target_table)
                database = engine[db_name] if db_name else get_mongo_database(engine)
                if database is None:
                    continue
                collection = database[collection_name]
                set_dict = {
                    col: field["value"] if isinstance(field, dict) else getattr(field, "value", None)
                    for col, field in raw_values.items()
                }
                if set_dict:
                    res = collection.update_many(mongo_filter, {"$set": set_dict})
                    total_afetados += res.modified_count
                    resposta_query += f"{collection_name}: {res.modified_count} documento(s) atualizado(s).\n"
            sucesso = True
        else:
            # 2. Bancos Relacionais (PostgreSQL, MySQL, SQLite, Oracle, SQL Server, etc.)
            where_sql, where_params = montar_filter_com_parametros(data.query.where, db_type=db_type)
            clean_w = (where_sql or "").strip()
            if clean_w:
                where_clause = clean_w if clean_w.upper().startswith("WHERE") else f"WHERE {clean_w}"
            else:
                where_clause = ""

            with engine.begin() as conn:
                for table_name, raw_values in data.updatedRow.items():
                    set_clauses = []
                    table_params = {}
                    for idx, (col, field) in enumerate(raw_values.items()):
                        val = field["value"] if isinstance(field, dict) else getattr(field, "value", None)
                        col_type = (
                            field.get("type_column", "text")
                            if isinstance(field, dict)
                            else getattr(field, "type_column", "text")
                        )
                        clean_col = col.split(".")[-1]
                        quoted_col = quote_identifier(db_type, clean_col)
                        param_name = f"bulk_val_{idx}"
                        set_clauses.append(f"{quoted_col} = :{param_name}")
                        table_params[param_name] = _convert_column_type_for_string_one_V1(val, col_type)

                    if not set_clauses:
                        continue

                    full_params = {**(where_params or {}), **table_params}
                    target_table = table_name or data.query.baseTable
                    quoted_table = quote_identifier(db_type, target_table)
                    sql = f"UPDATE {quoted_table} SET {', '.join(set_clauses)} {where_clause}".strip()
                    query_string += f"-- UPDATE em {target_table}\n{sql}\n"

                    rs = conn.execute(text(sql), full_params)
                    linhas = rs.rowcount if rs.rowcount is not None and rs.rowcount >= 0 else 0
                    total_afetados += linhas
                    resposta_query += f"{target_table}: {linhas} linha(s) atualizada(s).\n"

            sucesso = True
            log_message(f"✅ Atualização em massa concluída: {total_afetados} linha(s) afetada(s)", "success")

    except SQLAlchemyError as sa_err:
        error_msg = _lidar_com_erro_sql(sa_err)
        log_message(f"❌ Erro SQL ao atualizar em massa: {error_msg}", "error")
    except Exception as e:
        error_msg = str(e)
        log_message(f"❌ Erro inesperado ao atualizar em massa: {error_msg}", "error")

    duration_ms = int((time.time() - start_time) * 1000)

    historico = QueryHistoryCreate(
        user_id=user_id,
        db_connection_id=connection_id,
        query=query_string.strip() or f"UPDATE ALL {data.query.baseTable}",
        query_type=QueryType.UPDATE,
        executed_at=datetime.now(timezone.utc),
        duration_ms=duration_ms,
        result_preview=resposta_query.strip() if sucesso else "Sem resultado devido a falha.",
        error_message=error_msg,
        is_favorite=False,
        tags="update_all" if sucesso else "update_all_error",
        app_source=app_source,
        client_ip=client_ip,
        executed_by=executed_by,
        modified_by=modified_by or executed_by,
        meta_info={
            "db_type": db_type,
            "bulk_edit": True,
            "base_table": data.query.baseTable,
            "affected_rows": total_afetados,
            "status": "success" if sucesso else "failed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )

    create_query_history(db=db, user_id=user_id, data=historico)

    if not sucesso:
        raise ValueError(error_msg)

    return {
        "status": "success",
        "affected_rows": total_afetados,
        "updated": data.updatedRow,
        "response": resposta_query.strip() or f"{total_afetados} linha(s) atualizada(s).",
    }


def update_selected_service(
    data: UpdateSelectedRequest,
    engine: Any,
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
    Atualiza múltiplos registros selecionados específicos (identificados pelas suas chaves primárias).
    Garante transação ACID: se qualquer registro falhar, tudo é revertido.
    Suporta bancos relacionais (PostgreSQL, MySQL, SQLite, Oracle, SQL Server, etc.) e NoSQL (MongoDB).
    """
    resposta_query = ""
    query_string = ""
    start_time = time.time()
    total_afetados = 0
    sucesso = False
    error_msg = None

    try:
        if is_mongo(engine):
            from app.services.mongo_query_executor import _resolve_db_and_collection
            from app.ultils.conect_database import get_mongo_database
            from bson import ObjectId

            def _to_mongo_pk(val):
                try:
                    if isinstance(val, str) and ObjectId.is_valid(val):
                        return ObjectId(val)
                except Exception:
                    pass
                return val

            for row_dict in data.rows_primary_keys:
                for table_name, pk_data in row_dict.items():
                    primary_key = pk_data.get("primaryKey")
                    primary_value = pk_data.get("valor")
                    if not primary_key or primary_value is None:
                        continue

                    raw_values = data.updatedRow.get(table_name, {})
                    if not raw_values:
                        continue

                    db_name, collection_name = _resolve_db_and_collection(None, table_name)
                    database = engine[db_name] if db_name else get_mongo_database(engine)
                    if database is None:
                        continue

                    collection = database[collection_name]
                    pk_clean = primary_key.split(".")[-1]

                    if pk_clean in ("_id", "id"):
                        id_val = _to_mongo_pk(primary_value)
                        filtro = {"$or": [{"_id": id_val}, {"id": primary_value}]} if pk_clean == "id" else {"_id": id_val}
                    else:
                        filtro = {pk_clean: primary_value}

                    set_dict = {
                        col.split(".")[-1]: field["value"] if isinstance(field, dict) else getattr(field, "value", None)
                        for col, field in raw_values.items()
                    }

                    if set_dict:
                        res = collection.update_one(filtro, {"$set": set_dict})
                        total_afetados += res.modified_count
                        resposta_query += f"{collection_name} ({primary_value}): {res.modified_count} doc.\n"

            sucesso = True
        else:
            with engine.begin() as conn:
                for row_dict in data.rows_primary_keys:
                    for table_name, pk_data in row_dict.items():
                        primary_key = pk_data.get("primaryKey")
                        primary_value = pk_data.get("valor")

                        if not primary_key or primary_value is None:
                            continue

                        raw_values = data.updatedRow.get(table_name, {})
                        if not raw_values:
                            continue

                        updated_values = {
                            col.split(".")[-1]: {
                                "value": field["value"] if isinstance(field, dict) else getattr(field, "value", None),
                                "type_column": field.get("type_column", "text") if isinstance(field, dict) else getattr(field, "type_column", "text")
                            }
                            for col, field in raw_values.items()
                        }

                        if not updated_values:
                            continue

                        query = build_update_query(
                            table_name=table_name,
                            db_type=db_type,
                            updated_values=updated_values,
                            primary_key=primary_key,
                            primary_value=primary_value
                        )
                        if query is None:
                            continue

                        query_string += f"-- UPDATE em {table_name} (PK={primary_value})\n{str(query)}\n"
                        rs = conn.execute(query)
                        linhas = rs.rowcount if rs.rowcount is not None and rs.rowcount >= 0 else 1
                        total_afetados += linhas
                        resposta_query += f"{table_name} (PK={primary_value}): {linhas} linha(s).\n"

            sucesso = True
        log_message(f"✅ Atualização de registros selecionados concluída: {total_afetados} linha(s) afetada(s)", "success")

    except SQLAlchemyError as sa_err:
        error_msg = _lidar_com_erro_sql(sa_err)
        log_message(f"❌ Erro SQL ao atualizar registros selecionados: {error_msg}", "error")
    except Exception as e:
        error_msg = str(e)
        log_message(f"❌ Erro inesperado ao atualizar registros selecionados: {error_msg}", "error")

    duration_ms = int((time.time() - start_time) * 1000)

    historico = QueryHistoryCreate(
        user_id=user_id,
        db_connection_id=connection_id,
        query=query_string.strip() or f"UPDATE SELECTED ({len(data.rows_primary_keys)} rows)",
        query_type=QueryType.UPDATE,
        executed_at=datetime.now(timezone.utc),
        duration_ms=duration_ms,
        result_preview=resposta_query.strip() if sucesso else "Sem resultado devido a falha.",
        error_message=error_msg,
        is_favorite=False,
        tags="update_selected" if sucesso else "update_selected_error",
        app_source=app_source,
        client_ip=client_ip,
        executed_by=executed_by,
        modified_by=modified_by or executed_by,
        meta_info={
            "db_type": db_type,
            "selected_edit": True,
            "total_selected": len(data.rows_primary_keys),
            "affected_rows": total_afetados,
            "status": "success" if sucesso else "failed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )

    create_query_history(db=db, user_id=user_id, data=historico)

    if not sucesso:
        raise ValueError(error_msg)

    return {
        "status": "success",
        "affected_rows": total_afetados,
        "updated": data.updatedRow,
        "response": resposta_query.strip() or f"{total_afetados} linha(s) atualizada(s).",
    }