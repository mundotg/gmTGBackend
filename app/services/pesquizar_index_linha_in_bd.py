import traceback
import numpy as np
from typing import List, Optional, Union, Any, Dict
from sqlalchemy import inspect, text, Engine
from app.schemas.query_select_upAndInsert_schema import OrderByOption
from app.services.editar_linha import _convert_column_type_for_string_one
from app.ultils.logger import log_message
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional, Dict, Any
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
import traceback
def pesquisar_in_db(
    engine: Engine,
    db_type: str,
    orderby: Optional[List["OrderByOption"]],
    primary_key_value: Optional[str],
    campo_primary_key: str,
    table_name: str,
    col_type: str = "text",
    selected_row_index: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Busca uma linha completa a partir de:
      - chave primária (primary_key_value), OU
      - índice (OFFSET) com ordenação.
    Retorna os dados com chaves no formato tabela.coluna.
    """
    try:
        db_type = (db_type or "").strip().lower()
        valid_db_types = [
            "postgres", "postgresql", "sqlite", "mysql",
            "oracle", "mssql", "sql server", "sqlserver"
        ]

        if not any(db in db_type for db in valid_db_types):
            log_message(f"⚠ Banco de dados `{db_type}` não suportado.", level="error")
            return None

        # -------------------------
        # 🔹 Caso 1: Buscar pela chave primária
        # -------------------------
        if primary_key_value is not None:
            if "mysql" in db_type:
                query = f"SELECT * FROM `{table_name}` WHERE `{campo_primary_key}` = :pk LIMIT 1"
            elif "oracle" in db_type or "mssql" in db_type or "sql server" in db_type or "sqlserver" in db_type:
                query = f'SELECT * FROM "{table_name}" WHERE "{campo_primary_key}" = :pk'
            else:  # postgres/sqlite
                query = f'SELECT * FROM "{table_name}" WHERE "{campo_primary_key}" = :pk LIMIT 1'

            with engine.connect() as conn:
                result = conn.execute(text(query), {"pk": _convert_column_type_for_string_one(primary_key_value,col_type)}).fetchone()
                if result:
                    row_with_prefix = {f"{table_name}.{col}": val for col, val in dict(result._mapping).items()}
                    return row_with_prefix
                return None

        # -------------------------
        # 🔹 Caso 2: Buscar por indexação (OFFSET)
        # -------------------------
        if selected_row_index is None:
            log_message("⚠ Nem PK nem índice informado, impossível buscar.", level="warning")
            return None

        # Monta o ORDER BY
        if orderby and len(orderby) > 0:
            order_parts = []
            for opt in orderby:
                col = opt.column.strip()
                direction = "ASC" if str(opt.direction).lower() == "asc" else "DESC"
                if "mysql" in db_type:
                    order_parts.append(f"`{col}` {direction}")
                else:
                    order_parts.append(f'"{col}" {direction}')
            order_clause = ", ".join(order_parts)
        else:
            order_clause = f'"{campo_primary_key}"'

        # Queries específicas por banco
        if "postgres" in db_type or "sqlite" in db_type:
            query = f'SELECT * FROM "{table_name}" ORDER BY {order_clause} LIMIT 1 OFFSET :offset_value'
        elif "mysql" in db_type:
            query = f"SELECT * FROM `{table_name}` ORDER BY {order_clause} LIMIT :offset_value, 1"
        elif "oracle" in db_type:
            query = f"""
                SELECT * FROM (
                    SELECT t.*, ROWNUM AS rn FROM (
                        SELECT * FROM "{table_name}" ORDER BY {order_clause}
                    ) t
                ) WHERE rn = :offset_plus
            """
        else:  # mssql / sql server / sqlserver
            query = f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY {order_clause}) AS rn
                    FROM "{table_name}"
                ) AS subquery WHERE rn = :offset_plus
            """

        params = {"offset_value": selected_row_index}
        if "oracle" in db_type or "mssql" in db_type or "sql server" in db_type or "sqlserver" in db_type:
            params["offset_plus"] = selected_row_index + 1  # ROW_NUMBER/ROWNUM começa em 1

        with engine.connect() as conn:
            result = conn.execute(text(query), params).fetchone()

            if result:
                row_with_prefix = {f"{table_name}.{col}": val for col, val in dict(result._mapping).items()}
                return row_with_prefix
            return None

    except Exception as e:
        log_message(f"❌ Erro ao buscar linha no banco: {e}\n{traceback.format_exc()}", level="error")
        return None




def verificar_num_column(
    table_name: str,
    name_campo_primary_key: str,
    engine: Engine,
    db_type: str,
    record_id: Union[str, int]
) -> Optional[Dict[str, Any]]:
    """
    Retorna os dados da linha identificada por uma chave primária.
    """
    try:
        table = table_name.strip().replace("`", "").replace('"', "")
        pk = name_campo_primary_key.strip().replace("`", "").replace('"', "")
        record_value = convert_values(record_id)

        query = None
        params = None

        if db_type in ["mysql", "sqlite"]:
            query = f"SELECT * FROM `{table}` WHERE `{pk}` = :record_id"
            params = {"record_id": record_value}
        elif db_type in ["postgresql", "postgres", "oracle"]:
            query = f'SELECT * FROM "{table}" WHERE "{pk}" = :record_id'
            params = {"record_id": record_value}
        elif db_type in ["mssql", "sql server","sqlserver"]:
            query = f"SELECT * FROM [{table}] WHERE [{pk}] = :record_id"
            params = {"record_id": record_value}
        else:
            log_message(f"⚠ Banco de dados `{db_type}` não suportado.", level="error")
            return None

        with engine.connect() as conn:
            result = conn.execute(text(query), params).fetchone()

        if result:
            return dict(result._mapping)
        else:
            log_message(f"❌ Nenhuma linha encontrada para {pk} = {record_value}", level="warning")
            return None

    except Exception as e:
        log_message(f"❌ Erro ao consultar o banco de dados: {e}\n{traceback.format_exc()}", level="error")
        return None


def convert_values(value: Any) -> Any:
    """
    Converte valores para tipos compatíveis com bancos de dados.
    """
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value)
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (np.ndarray, list)):
        return str(value)
    return value


def _get_foreign_keys(engine: Engine, table_name: str) -> Dict[str, Dict[str, str]]:
    """
    Obtém as chaves estrangeiras de uma ou mais tabelas usando o SQLAlchemy Inspector.
    
    Args:
        engine (Engine): Conexão com o banco de dados.
        table_name (str): Nome da tabela (ou lista de tabelas no futuro).

    Returns:
        Dict[str, Dict[str, str]]: Ex: { "tabela": { "coluna": "tabela_referenciada" } }
    """
    try:
        inspector = inspect(engine)
        table_names = [table_name] if isinstance(table_name, str) else table_name

        all_foreign_keys: Dict[str, Dict[str, str]] = {}

        for table in table_names:
            foreign_keys: Dict[str, str] = {}

            try:
                # Obter colunas da tabela
                columns_info = inspector.get_columns(table)
                existing_columns = {col['name'] for col in columns_info}

                for fk in inspector.get_foreign_keys(table):
                    referred_table = fk.get('referred_table')
                    for column in fk.get('constrained_columns', []):
                        column_name = column.split('.')[-1]  # remove "tabela." se houver
                        if column_name in existing_columns and referred_table:
                            foreign_keys[column_name] = referred_table

                if foreign_keys:
                    all_foreign_keys[table] = foreign_keys

            except SQLAlchemyError as e:
                log_message(f"Erro ao obter chaves estrangeiras da tabela '{table}': {str(e)}", level="error")
                continue

        # if all_foreign_keys:
        #     save_columns_to_file(all_foreign_keys, "tables_columns_foreign_key_relationship.pkl", log_message=log_message)
        #     log_message("✅ Chaves estrangeiras salvas com sucesso.", level="info")

        return all_foreign_keys

    except Exception as e:
        log_message(f"Erro inesperado ao obter chaves estrangeiras: {str(e)}", level="error")
        return {}
