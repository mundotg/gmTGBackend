# app/importantConfig/db_transfer.py
from __future__ import annotations

from typing import Dict, AsyncGenerator, List
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine

from app.schemas.db_transfer_schema import TableMapping, ColumnMapping
from app.services.editar_linha import (
    _convert_column_type_for_string_one,
    quote_identifier,
)
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message
import traceback


async def transfer_data(
    id_user: int,
    db: AsyncSession,
    id_connectio_origen: int,
    id_connectio_distino: int,
    tables_origen: Dict[str, TableMapping],
    batch_size: int = 5000,
) -> AsyncGenerator[str, None]:
    """
    Transfere dados entre duas conexões usando AsyncEngine com feedback via SSE.
    Debug total: print em todas as etapas.
    """

    print("==========  INÍCIO transfer_data ==========")
    print(f"user={id_user} origem={id_connectio_origen} destino={id_connectio_distino}")
    print(f"tabelas recebidas={len(tables_origen)} batch_size={batch_size}")

    source_engine: AsyncEngine | None = None
    target_engine: AsyncEngine | None = None

    try:
        print("🔌 Obtendo engine ORIGEM...")
        source_engine, conn1 = await ConnectionManager.get_engine_idconn_async(
            db=db, user_id=id_user, id_connection=id_connectio_origen
        )

        print("🔌 Obtendo engine DESTINO...")
        target_engine, conn2 = await ConnectionManager.get_engine_idconn_async(
            db=db, user_id=id_user, id_connection=id_connectio_distino
        )
        print("Engine DESTINO obtida")

        src_type = str(conn1.type)
        dst_type = str(conn2.type)

        print(f" Tipos DB: origem={src_type} destino={dst_type}")

        async with source_engine.connect() as src_conn, target_engine.connect() as tgt_conn:
            print(" Conexões abertas com sucesso")

            for table_id, tbl_map in tables_origen.items():
                print("\n--------------------------------------------")
                print(f" Processando tabela id={table_id}")

                origem = tbl_map.tabela_name_origem
                destino = tbl_map.tabela_name_destino
                columns_map = tbl_map.colunas_relacionados_para_transacao or []

                print(f" origem={origem} destino={destino}")
                print(f" colunas mapeadas={len(columns_map)}")

                if not destino:
                    msg = f"⚠️ Tabela destino não mapeada para '{origem}'. Pulando."
                    print(msg)
                    yield msg
                    continue

                yield f"🔄 Transferindo {origem} → {destino}"

                # filtra colunas válidas
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

                # quote tables
                quoted_table_src = quote_identifier(src_type, origem)
                quoted_table_dst = quote_identifier(dst_type, destino)

                print(f" tabela origem quoted={quoted_table_src}")
                print(f" tabela destino quoted={quoted_table_dst}")

                # remove duplicação destino
                seen_dest = set()
                aligned_cols: List[ColumnMapping] = []

                for c in columns_valid:
                    key = c.coluna_distino_name.strip().lower()
                    if key in seen_dest:
                        print(
                            f"⚠️ coluna destino duplicada ignorada: {c.coluna_distino_name}"
                        )
                        continue
                    seen_dest.add(key)
                    aligned_cols.append(c)

                print(f" colunas alinhadas={len(aligned_cols)}")
                for c in aligned_cols:
                    print(
                        f"   {c.coluna_origen_name} -> {c.coluna_distino_name} ({c.type_coluna_destino})"
                    )

                # colunas origem/destino
                src_cols_raw = [c.coluna_origen_name for c in aligned_cols]
                src_cols_quoted = [
                    quote_identifier(src_type, c.coluna_origen_name)
                    for c in aligned_cols
                ]
                dst_cols_quoted = [
                    quote_identifier(dst_type, c.coluna_distino_name)
                    for c in aligned_cols
                ]

                # SELECT
                select_cols = ", ".join(src_cols_quoted)
                fetch_query = text(f"SELECT {select_cols} FROM {quoted_table_src}")

                print(f" SELECT SQL: {fetch_query}")

                result_stream = await src_conn.stream(fetch_query)
                # print(f"result_stream obtido, iniciando leitura... {result_stream}")
                # INSERT parametrizado
                placeholders = [f":p{i}" for i in range(len(dst_cols_quoted))]
                insert_sql = (
                    f"INSERT INTO {quoted_table_dst} "
                    f"({', '.join(dst_cols_quoted)}) "
                    f"VALUES ({', '.join(placeholders)})"
                )
                insert_query = text(insert_sql)

                print(f" INSERT SQL: {insert_sql}")

                batch_params: List[dict] = []
                total = 0

                async for row in result_stream:
                    print(f" Lendo linha {total + 1} de {row}")
                    row_map = row._mapping
                    params = {}

                    for i, c in enumerate(aligned_cols):
                        raw_value = row_map.get(c.coluna_origen_name)
                        converted = _convert_column_type_for_string_one(
                            raw_value, c.type_coluna_destino
                        )
                        params[f"p{i}"] = converted

                    batch_params.append(params)

                    if len(batch_params) >= batch_size:
                        print(f" Executando batch ({len(batch_params)}) em {destino}")
                        try:
                            await tgt_conn.execute(insert_query, batch_params)
                            await tgt_conn.commit()
                            total += len(batch_params)
                            msg = f" {total} registros inseridos em {destino}"
                            print(msg)
                            yield msg
                        except Exception as batch_err:
                            await tgt_conn.rollback()
                            err = f" Falha no batch ({destino}): {batch_err}"
                            print(err)
                            log_message(
                                f"[User {id_user}] {err}\n{traceback.format_exc()}",
                                "error",
                            )
                            yield err
                        finally:
                            batch_params.clear()

                print(f" Fim do stream SELECT ({origem}) restante={len(batch_params)}")

                # resto do batch
                if batch_params:
                    print(f" Inserindo resto do batch ({len(batch_params)})")
                    try:
                        await tgt_conn.execute(insert_query, batch_params)
                        await tgt_conn.commit()
                        total += len(batch_params)
                    except Exception as batch_err:
                        await tgt_conn.rollback()
                        err = f"⚠️ Falha resto batch ({destino}): {batch_err}"
                        print(err)
                        log_message(
                            f"[User {id_user}] {err}\n{traceback.format_exc()}",
                            "error",
                        )
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
        print(" Finalizando engines")
        if source_engine:
            await source_engine.dispose()
            print(" Engine origem liberada")
        if target_engine:
            await target_engine.dispose()
            print(" Engine destino liberada")

        print("==========  FIM transfer_data ==========")
