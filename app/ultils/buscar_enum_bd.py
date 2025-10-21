import traceback
from typing import Dict, List
from sqlalchemy.orm import Session
from sqlalchemy import Engine, text

from app.cruds.dbstructure_crud import create_enum_field
from app.models.dbstructure_models import DBEnumField, DBField, DBStructure
from app.ultils.logger import log_message

import re

def _extract_check_values(definition: str):
    # procura o trecho ARRAY['A', 'B', 'C']
    match = re.search(r"ARRAY\[(.*?)\]", definition)
    if not match:
        return None

    values_str = match.group(1)
    values = [
        v.split("::")[0].strip().strip("'")  # remove cast e apóstrofos
        for v in values_str.split(",")
    ]
    return values

def _get_values(col_type: str, db_type: str, col_name: str, table_name: str, engine: Engine) -> dict:
    """Obtém valores ENUM ou CHECK do banco de dados, se disponíveis."""
    try:
        enum_values = {}

        if db_type == "postgresql":
            # ENUM
            query_enum = text("""
                SELECT e.enumlabel 
                FROM pg_type t
                JOIN pg_enum e ON t.oid = e.enumtypid
                WHERE t.typname = :col_type
            """)
            with engine.connect() as conn:
                result = conn.execute(query_enum, {"col_type": col_type}).fetchall()
                if result:
                    enum_values[col_name] = [row[0] for row in result]
                    return enum_values

            # CHECK
            query_check = text("""
                SELECT pg_get_constraintdef(con.oid)
                FROM pg_constraint con
                INNER JOIN pg_attribute att ON att.attnum = ANY (con.conkey)
                INNER JOIN pg_class cla ON cla.oid = con.conrelid
                WHERE cla.relname = :table_name
                  AND att.attname = :col_name
                  AND con.contype = 'c'
            """)
            with engine.connect() as conn:
                result = conn.execute(query_check, {"table_name": table_name, "col_name": col_name}).fetchall()
                for row in result:
                    values = _extract_check_values(row[0])
                    if values:
                        enum_values[col_name] = values

        elif db_type in ("mysql", "mariadb"):
            # ENUM
            query_enum = text(f"SHOW COLUMNS FROM `{table_name}` LIKE :col_name")
            with engine.connect() as conn:
                result = conn.execute(query_enum, {"col_name": col_name}).fetchall()
                if result:
                    enum_text = result[0][1]
                    if isinstance(enum_text, str) and "enum(" in enum_text.lower():
                        enum_values[col_name] = enum_text.replace("enum(", "").replace(")", "").replace("'", "").split(",")
                        return enum_values

            # CHECK
            query_check = text("""
                SELECT cc.CHECK_CLAUSE
                FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
                  ON cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                WHERE tc.TABLE_NAME = :table_name
                  AND tc.CONSTRAINT_TYPE = 'CHECK'
                  AND cc.CHECK_CLAUSE LIKE :col_name_like
            """)
            with engine.connect() as conn:
                result = conn.execute(query_check, {"table_name": table_name, "col_name_like": f"%{col_name}%"}).fetchall()
                for row in result:
                    values = _extract_check_values(row[0])
                    if values:
                        enum_values[col_name] = values

        elif db_type in ("mssql", "sql server"):
            query = text("""
                SELECT con.definition 
                FROM sys.check_constraints con
                JOIN sys.columns col ON con.parent_object_id = col.object_id
                JOIN sys.tables tab ON col.object_id = tab.object_id
                WHERE tab.name = :table_name
                  AND col.name = :col_name
            """)
            with engine.connect() as conn:
                result = conn.execute(query, {"table_name": table_name, "col_name": col_name}).fetchall()
                for row in result:
                    values = _extract_check_values(row[0])
                    if values:
                        enum_values[col_name] = values

        elif db_type == "sqlite":
            query = text(f"PRAGMA table_info({table_name})")
            with engine.connect() as conn:
                result = conn.execute(query).fetchall()
                for row in result:
                    if row[1] == col_name and "CHECK" in (row[5] or ""):
                        values = _extract_check_values(row[5])
                        if values:
                            enum_values[col_name] = values

        elif db_type == "oracle":
            query = text("""
                SELECT con.search_condition 
                FROM user_constraints con
                JOIN user_cons_columns col ON con.constraint_name = col.constraint_name
                WHERE con.constraint_type = 'C'
                  AND col.table_name = :table_name
                  AND col.column_name = :col_name
            """)
            with engine.connect() as conn:
                result = conn.execute(query, {"table_name": table_name.upper(), "col_name": col_name.upper()}).fetchall()
                for row in result:
                    values = _extract_check_values(row[0])
                    if values:
                        enum_values[col_name] = values

        return enum_values

    except Exception as e:
        log_message(f"Erro ao obter valores de enum/check: {e} {traceback.format_exc()}", level="warning")
        return {}
    
def _fetch_enum_values(
    db: Session, columns: List[DBField],
    engine: Engine, structure: DBStructure, db_type: str
) -> Dict[str, List[str]]:
    """
    Obtém os valores ENUM de cada coluna ENUM da tabela.
    Se já existirem valores no banco local, retorna esses.
    Caso contrário, busca diretamente no banco de dados.
    """
    enum_values: Dict[str, List[str]] = {}

    try:
        for col in columns:
            # 1. Busca valores já salvos localmente
            # enums_local = list_enum_fields_by_field(db, col.id)
            # if enums_local:
            #     enum_values[col.name] = [e.valor for e in enums_local]
            #     continue

            # 2. Busca diretamente no banco de dados
            valores_enum = _get_values(col.type, db_type, col.name, structure.table_name, engine)
            enum_values[col.name] = []

            for valor in valores_enum.get(col.name, []):
                enum_model = DBEnumField(field_id=col.id, valor=valor)
                create_enum_field(db, enum_model)
                enum_values[col.name].append(valor)

        log_message(f"🟢 Valores ENUM sincronizados: {enum_values}", "info")
        return enum_values

    except Exception as e:
        log_message(f"❌ Erro ao buscar ENUMs: {e}\n{traceback.format_exc()}", "warning")
        return {}
