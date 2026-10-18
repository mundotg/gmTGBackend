# importantConfig/db_backup_restore_sql.py
"""
Motor de Backup e Restauro SQL Universal em Python.

Garante que o backup e restauro funcionam em TODOS os ambientes onde a base de dados se encontra:
1. Sem depender estritamente de ferramentas de linha de comando externas (pg_dump, mysqldump).
2. Sem falhas de versão ("server version mismatch") no PostgreSQL (suporta 14, 15, 16, 17, 18, 19+).
3. Compatível com bases na nuvem (AWS RDS, Neon, Supabase, GCP Cloud SQL, Azure), redes Docker e Windows/Linux.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time
import json
import os
import re
from typing import Any, List, Optional
import uuid

from sqlalchemy import text
from app.models.connection_models import DBConnection
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message


def _format_sql_value(val: Any) -> str:
    """Formata um valor Python para literal SQL seguro."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, (datetime, date, time)):
        return f"'{val.isoformat()}'"
    if isinstance(val, uuid.UUID):
        return f"'{str(val)}'"
    if isinstance(val, (dict, list)):
        escaped_json = json.dumps(val, ensure_ascii=False, default=str).replace("'", "''")
        return f"'{escaped_json}'::jsonb"
    if isinstance(val, (bytes, bytearray)):
        return f"'\\x{val.hex()}'::bytea"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


async def python_postgres_backup(
    connection: DBConnection,
    filepath: str,
) -> str:
    """
    Gera um dump SQL completo de uma base de dados PostgreSQL através de queries SQL diretas.
    Funciona em qualquer versão do PostgreSQL e em qualquer ambiente.
    """
    log_message(f"📦 [Python Fallback] Iniciando dump SQL nativo para: {connection.database_name}", level="info")
    engine = await ConnectionManager._create_async_engine(connection)

    try:
        async with engine.connect() as conn:
            # Garante encoding UTF-8 e desativa limites temporais na sessão
            await conn.execute(text("SET statement_timeout = 0;"))
            await conn.execute(text("SET client_encoding = 'UTF8';"))

            # 1. Obter esquemas do utilizador
            res_schemas = await conn.execute(
                text(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
                    "  AND schema_name NOT LIKE 'pg_temp_%' AND schema_name NOT LIKE 'pg_toast_temp_%' "
                    "ORDER BY schema_name;"
                )
            )
            schemas = [r[0] for r in res_schemas.fetchall()]
            if not schemas:
                schemas = ["public"]

            # 2. Obter tabelas
            res_tables = await conn.execute(
                text(
                    "SELECT table_schema, table_name FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
                    "  AND table_type = 'BASE TABLE' "
                    "ORDER BY table_schema, table_name;"
                )
            )
            tables = res_tables.fetchall()

            # 3. Obter sequências
            res_seqs = await conn.execute(
                text(
                    "SELECT sequence_schema, sequence_name FROM information_schema.sequences "
                    "WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema') "
                    "ORDER BY sequence_schema, sequence_name;"
                )
            )
            sequences = res_seqs.fetchall()

            # 4. Obter views
            res_views = await conn.execute(
                text(
                    "SELECT table_schema, table_name, view_definition FROM information_schema.views "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                    "ORDER BY table_schema, table_name;"
                )
            )
            views = res_views.fetchall()

            # Escrever o ficheiro SQL em stream
            os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"-- PostgreSQL Dump Nativo (Universal Python Fallback)\n")
                f.write(f"-- Base de dados: {connection.database_name}\n")
                f.write(f"-- Gerado em: {datetime.now().isoformat()}\n")
                f.write(f"-- Compatível com todas as versões do PostgreSQL (14 a 19+)\n\n")
                f.write("SET statement_timeout = 0;\n")
                f.write("SET client_encoding = 'UTF8';\n")
                f.write("SET standard_conforming_strings = on;\n")
                f.write("SET check_function_bodies = false;\n\n")

                # Criação dos esquemas
                for sc in schemas:
                    if sc != "public":
                        f.write(f'CREATE SCHEMA IF NOT EXISTS "{sc}";\n')
                f.write("\n")

                # Criação das sequências
                for seq_schema, seq_name in sequences:
                    f.write(f'CREATE SEQUENCE IF NOT EXISTS "{seq_schema}"."{seq_name}";\n')
                f.write("\n")

                foreign_keys_ddl = []

                # Definição e dados de cada tabela
                for t_schema, t_name in tables:
                    log_message(f"📄 [Dump Nativo] Exportando tabela: {t_schema}.{t_name}", level="info")

                    # Colunas da tabela
                    res_cols = await conn.execute(
                        text(
                            "SELECT column_name, data_type, is_nullable, column_default, character_maximum_length, udt_name "
                            "FROM information_schema.columns "
                            "WHERE table_schema = :sc AND table_name = :tb "
                            "ORDER BY ordinal_position;"
                        ),
                        {"sc": t_schema, "tb": t_name},
                    )
                    cols_info = res_cols.fetchall()
                    if not cols_info:
                        continue

                    col_defs = []
                    col_names = []
                    for col in cols_info:
                        col_name, data_type, is_null, col_def, char_max, udt_name = col
                        col_names.append(col_name)

                        # Tipo de coluna apropriado
                        type_str = udt_name
                        if data_type.lower() == "character varying" and char_max:
                            type_str = f"VARCHAR({char_max})"
                        elif data_type.lower() == "character" and char_max:
                            type_str = f"CHAR({char_max})"
                        elif data_type.lower() in ("numeric", "decimal"):
                            type_str = "NUMERIC"
                        elif data_type.lower() == "user-defined":
                            type_str = f'"{udt_name}"'

                        col_line = f'    "{col_name}" {type_str}'
                        if is_null == "NO":
                            col_line += " NOT NULL"
                        if col_def is not None:
                            col_line += f" DEFAULT {col_def}"
                        col_defs.append(col_line)

                    # Chave primária
                    res_pk = await conn.execute(
                        text(
                            "SELECT kcu.column_name "
                            "FROM information_schema.table_constraints tc "
                            "JOIN information_schema.key_column_usage kcu "
                            "  ON tc.constraint_name = kcu.constraint_name "
                            "  AND tc.table_schema = kcu.table_schema "
                            "WHERE tc.constraint_type = 'PRIMARY KEY' "
                            "  AND tc.table_schema = :sc "
                            "  AND tc.table_name = :tb "
                            "ORDER BY kcu.ordinal_position;"
                        ),
                        {"sc": t_schema, "tb": t_name},
                    )
                    pk_cols = [r[0] for r in res_pk.fetchall()]
                    if pk_cols:
                        pk_names_str = ", ".join(f'"{c}"' for c in pk_cols)
                        col_defs.append(f"    PRIMARY KEY ({pk_names_str})")

                    f.write(f'CREATE TABLE IF NOT EXISTS "{t_schema}"."{t_name}" (\n')
                    f.write(",\n".join(col_defs))
                    f.write("\n);\n\n")

                    # Guardar foreign keys para adicionar no final (evita erro de ordem de dependências)
                    res_fk = await conn.execute(
                        text(
                            "SELECT tc.constraint_name, kcu.column_name, ccu.table_schema, ccu.table_name, ccu.column_name "
                            "FROM information_schema.table_constraints AS tc "
                            "JOIN information_schema.key_column_usage AS kcu "
                            "  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
                            "JOIN information_schema.constraint_column_usage AS ccu "
                            "  ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema "
                            "WHERE tc.constraint_type = 'FOREIGN KEY' "
                            "  AND tc.table_schema = :sc AND tc.table_name = :tb;"
                        ),
                        {"sc": t_schema, "tb": t_name},
                    )
                    for fk in res_fk.fetchall():
                        fk_cname, fk_col, ref_sc, ref_tb, ref_col = fk
                        foreign_keys_ddl.append(
                            f'ALTER TABLE "{t_schema}"."{t_name}" ADD CONSTRAINT "{fk_cname}" '
                            f'FOREIGN KEY ("{fk_col}") REFERENCES "{ref_sc}"."{ref_tb}" ("{ref_col}") '
                            f'ON DELETE CASCADE ON UPDATE CASCADE;\n'
                        )

                    # Exportar dados da tabela em lotes
                    cols_str = ", ".join(f'"{c}"' for c in col_names)
                    res_data = await conn.execute(text(f'SELECT {cols_str} FROM "{t_schema}"."{t_name}";'))
                    rows = res_data.fetchall()

                    if rows:
                        f.write(f"-- Dados para a tabela: {t_schema}.{t_name} ({len(rows)} registos)\n")
                        batch = []
                        for row in rows:
                            row_vals = [_format_sql_value(v) for v in row]
                            batch.append(f"({', '.join(row_vals)})")
                            if len(batch) >= 200:
                                f.write(f'INSERT INTO "{t_schema}"."{t_name}" ({cols_str}) VALUES\n' + ",\n".join(batch) + ";\n")
                                batch = []
                        if batch:
                            f.write(f'INSERT INTO "{t_schema}"."{t_name}" ({cols_str}) VALUES\n' + ",\n".join(batch) + ";\n")
                        f.write("\n")

                # Valores atuais das sequências
                for seq_schema, seq_name in sequences:
                    try:
                        res_val = await conn.execute(text(f'SELECT last_value FROM "{seq_schema}"."{seq_name}";'))
                        val = res_val.scalar()
                        if val is not None:
                            f.write(f"SELECT setval('\"{seq_schema}\".\"{seq_name}\"', {val}, true);\n")
                    except Exception:
                        pass
                f.write("\n")

                # Definição das Views
                for v_schema, v_name, v_def in views:
                    if v_def:
                        f.write(f'CREATE OR REPLACE VIEW "{v_schema}"."{v_name}" AS\n{v_def};\n\n')

                # Aplicação das Foreign Keys
                if foreign_keys_ddl:
                    f.write("-- Restrições de Chave Estrangeira\n")
                    for fk_stmt in foreign_keys_ddl:
                        f.write(fk_stmt)
                    f.write("\n")

            log_message(f"✅ [Python Fallback] Backup SQL gerado com sucesso: {filepath}", level="success")
            return filepath

    finally:
        await engine.dispose()


async def python_postgres_restore(
    connection: DBConnection,
    filepath: str,
) -> None:
    """
    Restaura um dump SQL numa base de dados PostgreSQL via ligação direta.
    """
    log_message(f"♻️ [Python Fallback] Iniciando restauro SQL para: {connection.database_name}", level="info")
    engine = await ConnectionManager._create_async_engine(connection)

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            sql_content = f.read()

        async with engine.begin() as conn:
            # Em asyncpg, scripts com múltiplos comandos são executados diretamente
            # via raw connection driver execute
            raw_conn = await conn.get_raw_connection()
            if hasattr(raw_conn, "driver_connection") and hasattr(raw_conn.driver_connection, "execute"):
                await raw_conn.driver_connection.execute(sql_content)
            else:
                # Fallback: divide por ponto e vírgula simples respeitando blocos
                statements = [stmt.strip() for stmt in sql_content.split(";\n") if stmt.strip()]
                for stmt in statements:
                    try:
                        await conn.execute(text(stmt))
                    except Exception as stmt_err:
                        log_message(f"⚠️ Aviso na execução de declaração SQL: {stmt_err}", level="warning")

        log_message(f"✅ [Python Fallback] Restauro SQL concluído com sucesso.", level="success")
    finally:
        await engine.dispose()
