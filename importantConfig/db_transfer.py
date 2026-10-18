# app/importantConfig/db_transfer.py
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import json
import traceback
from typing import Dict, AsyncGenerator, List, Any

from bson import ObjectId
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine

from app.schemas.db_transfer_schema import TableMapping, ColumnMapping
from app.services.editar_linha import (
    _convert_column_type_for_string_one_V1,
    quote_identifier,
)
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.ativar_session_bd import get_connection_id_async
from app.ultils.conect_database import is_mongo, get_mongo_database, close_engine
from app.ultils.connection_access import assert_user_connection_level_async
from app.schemas.connetion_schema import ConnectionAccessLevel
from app.ultils.logger import log_message
from app.ultils.data_serializer import (
    serialize_for_mongo,
    serialize_mongo_document,
    serialize_for_sql,
    serialize_sql_params,
)


def convert_value_for_mongo(val: Any, dst_type: str, col_name: str) -> Any:
    """Função compatível que delega para serialize_for_mongo."""
    return serialize_for_mongo(val, dst_type, col_name)


@asynccontextmanager
async def _optional_sql_conn(engine: Any, is_mongo_flag: bool):
    """Abre conexão SQL assíncrona apenas se o engine não for MongoDB."""
    if is_mongo_flag or engine is None:
        yield None
    else:
        async with engine.connect() as conn:
            yield conn


def _build_sql_fetch_query(
    src_type: str,
    origem: str,
    select_cols: str,
    custom_query: Optional[str] = None,
    where_clause: Optional[str] = None,
) -> text:
    quoted_table_src = quote_identifier(src_type, origem)
    if custom_query and str(custom_query).strip():
        cq = str(custom_query).strip().rstrip(";")
        upper_cq = cq.upper()
        if upper_cq.startswith("SELECT") or upper_cq.startswith("WITH"):
            return text(f"SELECT {select_cols} FROM ({cq}) AS _src_sub")
        else:
            return text(f"SELECT {select_cols} FROM {quoted_table_src} WHERE {cq}")
    elif where_clause and str(where_clause).strip():
        wc = str(where_clause).strip()
        if wc.upper().startswith("WHERE "):
            wc = wc[6:]
        return text(f"SELECT {select_cols} FROM {quoted_table_src} WHERE {wc}")
    else:
        return text(f"SELECT {select_cols} FROM {quoted_table_src}")


def _build_mongo_filter(custom_query: Optional[str] = None) -> dict:
    if not custom_query or not str(custom_query).strip():
        return {}
    cq = str(custom_query).strip()
    if cq.startswith("{") and cq.endswith("}"):
        try:
            return json.loads(cq)
        except Exception as e:
            print(f"⚠️ Erro ao decodificar filtro JSON Mongo: {e}")
    return {}


async def transfer_data(
    id_user: int,
    db: AsyncSession,
    id_connectio_origen: int,
    id_connectio_distino: int,
    tables_origen: Dict[str, TableMapping],
    batch_size: int = 5000,
) -> AsyncGenerator[str, None]:
    """
    Transfere dados entre duas conexões (SQL ou MongoDB) com feedback via WebSocket / SSE.
    """
    print("==========  INÍCIO transfer_data ==========")
    print(f"user={id_user} origem={id_connectio_origen} destino={id_connectio_distino}")
    print(f"tabelas recebidas={len(tables_origen)} batch_size={batch_size}")

    source_engine: Any = None
    target_engine: Any = None

    try:
        print("🔌 Obtendo modelo de conexão ORIGEM...")
        conn1 = await get_connection_id_async(db, id_user, id_connectio_origen)
        if not conn1:
            raise ValueError(f"Conexão de origem (ID {id_connectio_origen}) não encontrada.")

        print("🔌 Obtendo modelo de conexão DESTINO...")
        conn2 = await get_connection_id_async(db, id_user, id_connectio_distino)
        if not conn2:
            raise ValueError(f"Conexão de destino (ID {id_connectio_distino}) não encontrada.")

        await assert_user_connection_level_async(db, conn1, id_user, ConnectionAccessLevel.read)
        await assert_user_connection_level_async(db, conn2, id_user, ConnectionAccessLevel.write)

        src_type = str(conn1.type or "").lower()
        dst_type = str(conn2.type or "").lower()

        # Cria engines dedicadas para a transferência (suporta conexões distintas sem conflito de cache)
        if id_connectio_origen == id_connectio_distino:
            source_engine = await ConnectionManager._create_async_engine(conn1)
            target_engine = source_engine
        else:
            source_engine = await ConnectionManager._create_async_engine(conn1)
            target_engine = await ConnectionManager._create_async_engine(conn2)

        src_is_mongo = src_type == "mongodb" or is_mongo(source_engine)
        dst_is_mongo = dst_type == "mongodb" or is_mongo(target_engine)

        print(f" Tipos DB: origem={src_type} (is_mongo={src_is_mongo}) | destino={dst_type} (is_mongo={dst_is_mongo})")

        async with _optional_sql_conn(source_engine, src_is_mongo) as src_conn, \
                   _optional_sql_conn(target_engine, dst_is_mongo) as tgt_conn:

            print(" Conexões inicializadas com sucesso")

            for table_id, tbl_map in tables_origen.items():
                print("\n--------------------------------------------")
                print(f" Processando tabela/coleção id={table_id}")

                origem = tbl_map.tabela_name_origem
                destino = tbl_map.tabela_name_destino
                columns_map = tbl_map.colunas_relacionados_para_transacao or []

                print(f" origem={origem} destino={destino}")
                print(f" colunas mapeadas={len(columns_map)}")

                if not destino:
                    msg = f"⚠️ Destino não mapeado para '{origem}'. Pulando."
                    print(msg)
                    yield msg
                    continue

                yield f"🔄 Transferindo {origem} → {destino}"

                # Filtra colunas válidas e ativas
                columns_valid = [
                    c
                    for c in columns_map
                    if c.enabled and c.coluna_origen_name and c.coluna_distino_name
                ]

                print(f" colunas válidas={len(columns_valid)}")

                if not columns_valid:
                    msg = f"⚠️ Nenhuma coluna habilitada. Pulando {origem}."
                    print(msg)
                    yield msg
                    continue

                # Remove duplicação nas colunas de destino
                seen_dest = set()
                aligned_cols: List[ColumnMapping] = []
                for c in columns_valid:
                    key = c.coluna_distino_name.strip().lower()
                    if key in seen_dest:
                        print(f"⚠️ Coluna destino duplicada ignorada: {c.coluna_distino_name}")
                        continue
                    seen_dest.add(key)
                    aligned_cols.append(c)

                print(f" colunas alinhadas={len(aligned_cols)}")
                for c in aligned_cols:
                    print(f"   {c.coluna_origen_name} -> {c.coluna_distino_name} ({c.type_coluna_destino})")

                col_types_dst = {c.coluna_distino_name: c.type_coluna_destino for c in aligned_cols}
                sql_types_dst = {f"p{i}": c.type_coluna_destino for i, c in enumerate(aligned_cols)}

                # ============================================================
                # CENÁRIO 1: MONGO → MONGO
                # ============================================================
                if src_is_mongo and dst_is_mongo:
                    src_db_name = conn1.database_name or getattr(conn1, "database", None)
                    src_db = source_engine[src_db_name] if src_db_name else get_mongo_database(source_engine)
                    src_coll = src_db[origem]

                    tgt_db_name = conn2.database_name or getattr(conn2, "database", None)
                    tgt_db = target_engine[tgt_db_name] if tgt_db_name else get_mongo_database(target_engine)
                    tgt_coll = tgt_db[destino]

                    proj = {c.coluna_origen_name: 1 for c in aligned_cols}
                    if not any(c.coluna_origen_name == "_id" for c in aligned_cols):
                        proj["_id"] = 0

                    mongo_filter = _build_mongo_filter(getattr(tbl_map, "custom_query", None))
                    cursor = src_coll.find(mongo_filter, proj, batch_size=batch_size)
                    total = 0
                    batch_docs: List[dict] = []

                    for doc in cursor:
                        row_doc = {}
                        for c in aligned_cols:
                            raw_val = doc.get(c.coluna_origen_name)
                            row_doc[c.coluna_distino_name] = serialize_for_mongo(
                                raw_val, c.type_coluna_destino, c.coluna_distino_name
                            )
                        batch_docs.append(serialize_mongo_document(row_doc, col_types_dst))

                        if len(batch_docs) >= batch_size:
                            try:
                                res = tgt_coll.insert_many(batch_docs, ordered=False)
                                total += len(res.inserted_ids)
                                msg = f"📦 {total} registros inseridos em {destino}"
                                print(msg)
                                yield msg
                            except Exception as batch_err:
                                inserted_count = getattr(batch_err, "details", {}).get("nInserted", 0)
                                total += inserted_count
                                err = f"⚠️ Aviso no lote ({destino}): {batch_err}"
                                print(err)
                                yield err
                            finally:
                                batch_docs.clear()
                            await asyncio.sleep(0)

                    if batch_docs:
                        try:
                            res = tgt_coll.insert_many(batch_docs, ordered=False)
                            total += len(res.inserted_ids)
                        except Exception as batch_err:
                            inserted_count = getattr(batch_err, "details", {}).get("nInserted", 0)
                            total += inserted_count
                            err = f"⚠️ Aviso no lote final ({destino}): {batch_err}"
                            print(err)
                            yield err
                        finally:
                            batch_docs.clear()

                    msg = f"✅ Concluído {origem} → {destino}: {total} registros."
                    print(msg)
                    yield msg

                # ============================================================
                # CENÁRIO 2: MONGO → SQL
                # ============================================================
                elif src_is_mongo and not dst_is_mongo:
                    src_db_name = conn1.database_name or getattr(conn1, "database", None)
                    src_db = source_engine[src_db_name] if src_db_name else get_mongo_database(source_engine)
                    src_coll = src_db[origem]

                    proj = {c.coluna_origen_name: 1 for c in aligned_cols}
                    if not any(c.coluna_origen_name == "_id" for c in aligned_cols):
                        proj["_id"] = 0

                    mongo_filter = _build_mongo_filter(getattr(tbl_map, "custom_query", None))
                    cursor = src_coll.find(mongo_filter, proj, batch_size=batch_size)

                    quoted_table_dst = quote_identifier(dst_type, destino)
                    dst_cols_quoted = [quote_identifier(dst_type, c.coluna_distino_name) for c in aligned_cols]
                    placeholders = [f":p{i}" for i in range(len(dst_cols_quoted))]
                    insert_sql = (
                        f"INSERT INTO {quoted_table_dst} "
                        f"({', '.join(dst_cols_quoted)}) "
                        f"VALUES ({', '.join(placeholders)})"
                    )
                    insert_query = text(insert_sql)

                    total = 0
                    batch_params: List[dict] = []

                    for doc in cursor:
                        params = {}
                        for i, c in enumerate(aligned_cols):
                            raw_val = doc.get(c.coluna_origen_name)
                            params[f"p{i}"] = serialize_for_sql(raw_val, c.type_coluna_destino)
                        batch_params.append(serialize_sql_params(params, sql_types_dst))

                        if len(batch_params) >= batch_size:
                            try:
                                await tgt_conn.execute(insert_query, batch_params)
                                await tgt_conn.commit()
                                total += len(batch_params)
                                msg = f"📦 {total} registros inseridos em {destino}"
                                print(msg)
                                yield msg
                            except Exception as batch_err:
                                await tgt_conn.rollback()
                                err = f"⚠️ Falha no lote ({destino}): {batch_err}"
                                print(err)
                                yield err
                            finally:
                                batch_params.clear()
                            await asyncio.sleep(0)

                    if batch_params:
                        try:
                            await tgt_conn.execute(insert_query, batch_params)
                            await tgt_conn.commit()
                            total += len(batch_params)
                        except Exception as batch_err:
                            await tgt_conn.rollback()
                            err = f"⚠️ Falha lote final ({destino}): {batch_err}"
                            print(err)
                            yield err
                        finally:
                            batch_params.clear()

                    msg = f"✅ Concluído {origem} → {destino}: {total} registros."
                    print(msg)
                    yield msg

                # ============================================================
                # CENÁRIO 3: SQL → MONGO
                # ============================================================
                elif not src_is_mongo and dst_is_mongo:
                    src_cols_quoted = [quote_identifier(src_type, c.coluna_origen_name) for c in aligned_cols]
                    select_cols = ", ".join(src_cols_quoted)
                    fetch_query = _build_sql_fetch_query(
                        src_type,
                        origem,
                        select_cols,
                        getattr(tbl_map, "custom_query", None),
                        getattr(tbl_map, "where_clause", None),
                    )

                    tgt_db_name = conn2.database_name or getattr(conn2, "database", None)
                    tgt_db = target_engine[tgt_db_name] if tgt_db_name else get_mongo_database(target_engine)
                    tgt_coll = tgt_db[destino]

                    try:
                        result_stream = await src_conn.stream(fetch_query)
                    except Exception as stream_err:
                        if getattr(tbl_map, "custom_query", None):
                            cq = str(tbl_map.custom_query).strip().rstrip(";")
                            print(f"⚠️ Tentando query direta após falha em subquery: {cq}")
                            result_stream = await src_conn.stream(text(cq))
                        else:
                            raise stream_err
                    total = 0
                    batch_docs: List[dict] = []

                    async for row in result_stream:
                        row_map = row._mapping
                        row_doc = {}
                        for c in aligned_cols:
                            raw_val = row_map.get(c.coluna_origen_name)
                            row_doc[c.coluna_distino_name] = serialize_for_mongo(
                                raw_val, c.type_coluna_destino, c.coluna_distino_name
                            )
                        batch_docs.append(serialize_mongo_document(row_doc, col_types_dst))

                        if len(batch_docs) >= batch_size:
                            try:
                                res = tgt_coll.insert_many(batch_docs, ordered=False)
                                total += len(res.inserted_ids)
                                msg = f"📦 {total} registros inseridos em {destino}"
                                print(msg)
                                yield msg
                            except Exception as batch_err:
                                inserted_count = getattr(batch_err, "details", {}).get("nInserted", 0)
                                total += inserted_count
                                err = f"⚠️ Aviso no lote ({destino}): {batch_err}"
                                print(err)
                                yield err
                            finally:
                                batch_docs.clear()
                            await asyncio.sleep(0)

                    if batch_docs:
                        try:
                            res = tgt_coll.insert_many(batch_docs, ordered=False)
                            total += len(res.inserted_ids)
                        except Exception as batch_err:
                            inserted_count = getattr(batch_err, "details", {}).get("nInserted", 0)
                            total += inserted_count
                            err = f"⚠️ Aviso no lote final ({destino}): {batch_err}"
                            print(err)
                            yield err
                        finally:
                            batch_docs.clear()

                    msg = f"✅ Concluído {origem} → {destino}: {total} registros."
                    print(msg)
                    yield msg

                # ============================================================
                # CENÁRIO 4: SQL → SQL
                # ============================================================
                else:
                    quoted_table_dst = quote_identifier(dst_type, destino)
                    src_cols_quoted = [quote_identifier(src_type, c.coluna_origen_name) for c in aligned_cols]
                    dst_cols_quoted = [quote_identifier(dst_type, c.coluna_distino_name) for c in aligned_cols]

                    select_cols = ", ".join(src_cols_quoted)
                    fetch_query = _build_sql_fetch_query(
                        src_type,
                        origem,
                        select_cols,
                        getattr(tbl_map, "custom_query", None),
                        getattr(tbl_map, "where_clause", None),
                    )

                    placeholders = [f":p{i}" for i in range(len(dst_cols_quoted))]
                    insert_sql = (
                        f"INSERT INTO {quoted_table_dst} "
                        f"({', '.join(dst_cols_quoted)}) "
                        f"VALUES ({', '.join(placeholders)})"
                    )
                    insert_query = text(insert_sql)

                    try:
                        result_stream = await src_conn.stream(fetch_query)
                    except Exception as stream_err:
                        if getattr(tbl_map, "custom_query", None):
                            cq = str(tbl_map.custom_query).strip().rstrip(";")
                            print(f"⚠️ Tentando query direta após falha em subquery: {cq}")
                            result_stream = await src_conn.stream(text(cq))
                        else:
                            raise stream_err
                    total = 0
                    batch_params: List[dict] = []

                    async for row in result_stream:
                        row_map = row._mapping
                        params = {}
                        for i, c in enumerate(aligned_cols):
                            raw_value = row_map.get(c.coluna_origen_name)
                            params[f"p{i}"] = serialize_for_sql(raw_value, c.type_coluna_destino)
                        batch_params.append(serialize_sql_params(params, sql_types_dst))

                        if len(batch_params) >= batch_size:
                            try:
                                await tgt_conn.execute(insert_query, batch_params)
                                await tgt_conn.commit()
                                total += len(batch_params)
                                msg = f"📦 {total} registros inseridos em {destino}"
                                print(msg)
                                yield msg
                            except Exception as batch_err:
                                await tgt_conn.rollback()
                                err = f"⚠️ Falha no lote ({destino}): {batch_err}"
                                print(err)
                                yield err
                            finally:
                                batch_params.clear()
                            await asyncio.sleep(0)

                    if batch_params:
                        try:
                            await tgt_conn.execute(insert_query, batch_params)
                            await tgt_conn.commit()
                            total += len(batch_params)
                        except Exception as batch_err:
                            await tgt_conn.rollback()
                            err = f"⚠️ Falha lote final ({destino}): {batch_err}"
                            print(err)
                            yield err
                        finally:
                            batch_params.clear()

                    msg = f"✅ Concluído {origem} → {destino}: {total} registros."
                    print(msg)
                    yield msg

        print(" Transferência finalizada com sucesso")
        yield "🚀 Transferência finalizada com sucesso"

    except Exception as e:
        err = f"❌ Erro na transferência: {e}"
        print(err)
        print(traceback.format_exc())
        yield err
        log_message(f"[User {id_user}] {err}\n{traceback.format_exc()}", "error")

    finally:
        print(" Finalizando engines da transferência")
        if source_engine:
            if is_mongo(source_engine):
                close_engine(source_engine)
            else:
                try:
                    await source_engine.dispose()
                except Exception as e:
                    print(f"Erro ao liberar source_engine: {e}")
            print(" Engine origem liberada")

        if target_engine and target_engine is not source_engine:
            if is_mongo(target_engine):
                close_engine(target_engine)
            else:
                try:
                    await target_engine.dispose()
                except Exception as e:
                    print(f"Erro ao liberar target_engine: {e}")
            print(" Engine destino liberada")

        print("==========  FIM transfer_data ==========")
