import traceback
import uuid
import numpy as np
from typing import Optional, Union, Any, Dict
from sqlalchemy import inspect, text, Engine
from app.ultils.logger import log_message
from sqlalchemy.exc import SQLAlchemyError


def pesquisar_in_db(
    engine: Engine,
    db_type: str,
    campo_primary_key: str,
    table_name: str,
    selected_row_index: int,
    orderby: Optional[str] = None,
    columnOrder: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Busca uma linha completa a partir do índice (OFFSET) na tabela especificada,
    retornando os dados com chaves no formato tabela.coluna.
    """
    try:
        db_type = (db_type or "").strip().lower()
        valid_db_types = ["postgres", "postgresql", "sqlite", "mysql", "oracle", "mssql", "sql server", "sqlserver"]

        if not any(db in db_type for db in valid_db_types):
            log_message(f"⚠ Banco de dados `{db_type}` não suportado.", level="error")
            return None

        order_field = campo_primary_key

        queries = {
            "postgres": f'SELECT * FROM "{table_name}" ORDER BY "{order_field}" LIMIT 1 OFFSET :offset_value',
            "postgresql": f'SELECT * FROM "{table_name}" ORDER BY "{order_field}" LIMIT 1 OFFSET :offset_value',
            "sqlite": f'SELECT * FROM "{table_name}" ORDER BY "{order_field}" LIMIT 1 OFFSET :offset_value',
            "mysql": f"SELECT * FROM `{table_name}` ORDER BY `{order_field}` LIMIT :offset_value, 1",
            "oracle": f'''
                SELECT * FROM (
                    SELECT t.*, ROWNUM AS rn FROM (
                        SELECT * FROM {table_name} ORDER BY {order_field}
                    ) t
                ) WHERE rn = :offset_value
            ''',
            "mssql": f'''
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY [{order_field}]) AS rn
                    FROM [{table_name}]
                ) AS subquery WHERE rn = :offset_value
            ''',
            "sql server": f'''
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY [{order_field}]) AS rn
                    FROM [{table_name}]
                ) AS subquery WHERE rn = :offset_value
            ''',
            "sqlserver": f'''
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY [{order_field}]) AS rn
                    FROM [{table_name}]
                ) AS subquery WHERE rn = :offset_value
            '''
        }

        query = queries.get(db_type)
        if not query:
            log_message(f"⚠ Consulta não disponível para o banco `{db_type}`.", level="error")
            return None

        with engine.connect() as conn:
            result = conn.execute(text(query), {"offset_value": selected_row_index + 1}).fetchone()

            if result:
                original_row = dict(result._mapping)
                # Prefixa com o nome da tabela: "tabela.coluna"
                row_with_prefix = {
                    f"{table_name}.{col}": val for col, val in original_row.items()
                }
                log_message(f"✅ Linha encontrada no índice {selected_row_index}: {row_with_prefix}")
                return row_with_prefix
            else:
                log_message(f"❌ Nenhuma linha encontrada no índice {selected_row_index}.", level="warning")
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
