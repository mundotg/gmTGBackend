# app/importantConfig/db_transfer.py
import json
from typing import Dict, AsyncGenerator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from app.schemas.db_transfer_schema import ColumnMapping, TableMapping
from app.services.editar_linha import (
    _convert_column_type_for_string_one,
    quote_identifier,
)
import traceback

from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message


def converter_tables_origen(tables_origen_str: str) -> Dict[str, TableMapping]:
    """Converte JSON do payload para Dict[id_tabela_origem, TableMapping]"""

    data = json.loads(tables_origen_str)
    result: Dict[str, TableMapping] = {}

    for table_id, table_data in data.items():
        if not table_data.get("tabela_name_destino"):
            continue  # ignorar tabelas sem destino definido

        colunas = [
            ColumnMapping(
                coluna_origen_name=c.get("coluna_origen_name"),
                coluna_distino_name=c.get("coluna_distino_name"),
                type_coluna_origem=c.get("type_coluna_origem"),
                type_coluna_destino=c.get("type_coluna_destino"),
                id_coluna_origem=(
                    str(c.get("id_coluna_origem"))
                    if c.get("id_coluna_origem")
                    else None
                ),
                id_coluna_destino=(
                    str(c.get("id_coluna_destino"))
                    if c.get("id_coluna_destino")
                    else None
                ),
                enabled=c.get("enabled", False),
            )
            for c in table_data.get("colunas_relacionados_para_transacao", [])
        ]

        result[table_id] = TableMapping(
            tabela_name_origem=table_data.get("tabela_name_origem"),
            tabela_name_destino=table_data.get("tabela_name_destino"),
            id_tabela_origen=table_data.get("id_tabela_origen"),
            id_tabela_destino=table_data.get("id_tabela_destino"),
            colunas_relacionados_para_transacao=colunas,
        )

    return result


async def transfer_data(
    id_user: int,
    db: AsyncSession,
    id_connectio_origen: int,
    id_connectio_distino: int,
    tables_origen: Dict[str, TableMapping],
    batch_size: int = 5000,
) -> AsyncGenerator[str, None]:
    """Transfere dados entre duas conexões usando AsyncEngine com SSE feedback."""

    source_engine: AsyncEngine | None = None
    target_engine: AsyncEngine | None = None

    try:
        source_engine, conn1 = await ConnectionManager.get_engine_idconn_async(
            db=db, user_id=id_user, id_connection=id_connectio_origen
        )
        target_engine, conn2 = await ConnectionManager.get_engine_idconn_async(
            db=db, user_id=id_user, id_connection=id_connectio_distino
        )
        # print(f"tables_origen: {tables_origen.values()}")

        async with source_engine.connect() as src_conn, target_engine.begin() as tgt_conn:
            for table_id, tbl_map in tables_origen.items():
                origem = tbl_map.tabela_name_origem
                destino = tbl_map.tabela_name_destino
                columns_map = tbl_map.colunas_relacionados_para_transacao or []

                if not destino:
                    yield f"⚠️ Tabela destino não mapeada para origem '{origem}'. Pulando."
                    continue

                quoted_table_src = quote_identifier(conn1.type, origem)
                quoted_table_dst = quote_identifier(conn2.type, destino)

                yield f"🔄 Transferindo {origem} → {destino}"

                columns_valid = [
                    c for c in columns_map
                    if c.enabled and c.coluna_origen_name and c.coluna_distino_name
                ]

                colunas_origem = [
                    (c.coluna_origen_name, quote_identifier(conn1.type, c.coluna_origen_name))
                    for c in columns_valid
                ]

                colunas_destino = []
                seen_dest = set()
                for c in columns_valid:
                    dest = quote_identifier(conn2.type, c.coluna_distino_name)
                    if dest not in seen_dest:
                        seen_dest.add(dest)
                        colunas_destino.append(dest)

                if not colunas_origem:
                    yield f"⚠️ Nenhuma coluna habilitada. Pulando {origem}."
                    continue

                # Busca dados
                select_cols = ", ".join([q for (_, q) in colunas_origem])
                fetch_query = text(f"SELECT {select_cols} FROM {quoted_table_src}")
                result_stream = await src_conn.stream(fetch_query)

                batch = []
                total = 0

                async for row in result_stream:
                    row_values = []

                    for idx, (orig_raw, _) in enumerate(colunas_origem):
                        dest_type = columns_valid[idx].type_coluna_destino
                        formatted_value = _convert_column_type_for_string_one(
                            row._mapping[orig_raw], dest_type
                        )
                        row_values.append(formatted_value)

                    batch.append(f"({', '.join(row_values)})")

                    # ✅ Só cria a query quando o batch enche
                    if len(batch) >= batch_size:
                        try:
                            insert_query = text(
                                f"INSERT INTO {quoted_table_dst} ({', '.join(colunas_destino)}) VALUES "
                                + ", ".join(batch)
                            )
                            await tgt_conn.execute(insert_query)
                            await tgt_conn.commit()  # ✅ Confirma o batch
                            total += len(batch)
                            yield f"📦 {total} registros inseridos em {destino}"
                        except Exception as batch_err:
                            await tgt_conn.rollback()  # ❌ Só desfaz esse batch
                            yield f"⚠️ Falha no batch, continuando... Erro: {batch_err}"
                            log_message(f"Erro na transferência posi batch {total}: {batch_err} {traceback.format_exc()}", "error")
                        batch.clear()

                # ✅ Insere o restante
                if batch:
                    try:
                        insert_query = text(
                            f"INSERT INTO {quoted_table_dst} ({', '.join(colunas_destino)}) VALUES "
                            + ", ".join(batch)
                        )
                        await tgt_conn.execute(insert_query)
                        await tgt_conn.commit()
                        total += len(batch)
                    except Exception as batch_err:
                        await tgt_conn.rollback()
                        yield f"⚠️ Falha ao inserir restante do batch: {batch_err}"
                        log_message(f"Erro na transferência posi batch {total}: {batch_err} {traceback.format_exc()}", "error")

                yield f"✅ Concluído {origem} → {destino}: {total} registros."

        yield "🚀 Transferência finalizada com sucesso"


    except Exception as e:
        err = f"❌ Erro na transferência: {e}"
        yield err
        log_message(f"Erro na transferência: {e} {traceback.format_exc()}", "error")

    finally:
        if db:
            await db.close()
        if source_engine:
            await source_engine.dispose()
        if target_engine:
            await target_engine.dispose()
