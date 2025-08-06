from typing import List, Optional
from sqlalchemy import Boolean, Date, DateTime, Numeric, text

from app.schemas.queryhistory_schemas import DistinctList, JoinOption, OrderByOption


def _build_enum_query(col_name: str, table_name: str, db_type: str):
    if db_type == "postgresql":
        return text(f"""
            SELECT e.enumlabel 
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            WHERE t.typname = '{col_name}';
        """)
    elif db_type in ("mysql", "mariadb"):
        return text(f"SHOW COLUMNS FROM {table_name} LIKE '{col_name}'")
    elif db_type in ("mssql", "sql server", "sqlserver"):
        return text(f"""
            SELECT definition 
            FROM sys.check_constraints con
            JOIN sys.columns col ON con.parent_object_id = col.object_id
            JOIN sys.tables tab ON col.object_id = tab.object_id
            WHERE tab.name = '{table_name}' 
            AND col.name = '{col_name}' 
            AND con.definition LIKE 'IN (%)';
        """)
    elif db_type == "sqlite":
        return text(f"PRAGMA table_info({table_name})")
    elif db_type == "oracle":
        return text(f"""
            SELECT con.search_condition 
            FROM user_constraints con
            JOIN user_cons_columns col ON con.constraint_name = col.constraint_name
            WHERE con.constraint_type = 'C'
            AND col.table_name = '{table_name.upper()}'
            AND col.column_name = '{col_name.upper()}';
        """)
    return None


def _parse_enum_result(db_type: str, result, col_name: str) -> List[str]:
    if not result:
        return []

    if db_type == "postgresql":
        return [row[0] for row in result]

    elif db_type in ("mysql", "mariadb"):
        enum_text = result[0][1]
        if "enum(" in enum_text.lower():
            return enum_text.replace("enum(", "").replace(")", "").replace("'", "").split(",")

    elif db_type in ("mssql", "sql server", "oracle", "sqlserver"):
        check_clause = result[0][0]
        if "IN (" in check_clause.upper():
            valores_brutos = check_clause.split("IN (")[1].replace(")", "")
            return [v.strip().replace("'", "") for v in valores_brutos.split(",")]

    elif db_type == "sqlite":
        for row in result:
            if row[1] == col_name and "CHECK" in row[5].upper():
                valores_brutos = row[5].split("IN (")[1].replace(")", "").replace("'", "")
                return [v.strip() for v in valores_brutos.split(",")]

    return []


def get_filter_condition_with_operation(
    col_name: str,
    col_type: str,
    value: str,
    params: dict,
    db_type: str = "postgres",
    operation: str = "",
    value_otheir_between: str = "",
    param_name: Optional[str] = None,
    enum_values: Optional[dict] = None
) -> str:
    """
    Gera a cláusula SQL parametrizada com base no tipo da coluna e operação desejada.

    :param col_name: Nome da coluna (ex: "users.age")
    :param col_type: Tipo da coluna (ex: "integer", "varchar", etc.)
    :param value: Valor inicial
    :param params: Dicionário de parâmetros para query
    :param db_type: Tipo do banco de dados (postgres, mysql, sqlite...)
    :param operation: Operação desejada (=, !=, Contém, Entre, etc.)
    :param value_otheir_between: Segundo valor para operações como Entre
    :param param_name: Nome do parâmetro no dicionário (opcional)
    :param enum_values: Mapeamento opcional de enums {col_name: [valores]}
    """
    param_name = param_name or col_name.replace(".", "_")
    col_type_str = str(col_type).lower()
    db_type_lower = db_type.lower()
    value = value.strip()

    if value == "" and operation not in ["Entre"]:
        raise ValueError(f"Valor vazio para '{col_name}' com operação '{operation}'.")

    # Escapar nome da coluna
    col_escaped = quote_identifier(db_type_lower, col_name)

    # 🔹 Enum
    if enum_values and enum_values.get(col_name):
        params[param_name] = value
        return f"{col_escaped} = :{param_name}"

    # 🔹 UUID
    if "uuid" in col_type_str:
        params[param_name] = str(value)
        return f"{col_escaped} = :{param_name}"

    # 🔹 Booleanos
    if "boolean" in col_type_str or isinstance(col_type, Boolean):
        bool_map = {
            "true": True, "1": True, "yes": True, "sim": True,
            "false": False, "0": False, "no": False, "não": False
        }
        if value.lower() not in bool_map:
            raise ValueError(f"Valor booleano inválido para '{col_name}'.")
        params[param_name] = bool_map[value.lower()]
        return f"{col_escaped} = :{param_name}"

    # 🔹 Tipos
    is_date = any(t in col_type_str for t in ["date", "timestamp", "time"]) or isinstance(col_type, (Date, DateTime))
    is_number = any(t in col_type_str for t in ["int", "decimal", "float", "numeric"]) or isinstance(col_type, Numeric)
    is_text = any(t in col_type_str for t in ["text", "char", "varchar"])

    def basic_op(field_escaped: str) -> str:
        op_map = {
            "=": "=",
            "!=": "!=",
            "<": "<",
            "<=": "<=",
            ">": ">",
            ">=": ">="
        }

        if operation in op_map:
            params[param_name] = float(value) if is_number else value
            return f"{field_escaped} {op_map[operation]} :{param_name}"

        elif operation == "Entre":
            if not value_otheir_between.strip():
                raise ValueError(f"Segundo valor ausente para operação 'Entre' em '{col_name}'.")
            params[f"{param_name}_min"] = float(value) if is_number else value
            params[f"{param_name}_max"] = float(value_otheir_between) if is_number else value_otheir_between
            return f"{field_escaped} BETWEEN :{param_name}_min AND :{param_name}_max"

        elif operation in ["Contém", "Não Contém"] and (is_text or is_date or "json" in col_type_str):
            like_value = f"%{value}%"
            params[param_name] = like_value
            not_ = "NOT " if operation == "Não Contém" else ""
            return f"{field_escaped} {not_}LIKE :{param_name}"

        elif operation == "Antes de" and is_date:
            params[param_name] = value
            return f"{field_escaped} < :{param_name}"

        elif operation == "Depois de" and is_date:
            params[param_name] = value
            return f"{field_escaped} > :{param_name}"

        raise ValueError(f"Operação '{operation}' não suportada para tipo '{col_type}'.")

    # 🔹 JSON
    if "json" in col_type_str:
        json_expr = f"{col_escaped}::TEXT" if db_type_lower in ["postgres", "postgresql"] else f"CAST({col_escaped} AS TEXT)"
        return basic_op(json_expr)

    # 🔹 Datas (formatar conforme banco)
    if is_date:
        if db_type_lower == "mysql":
            date_expr = f"CONVERT({col_escaped}, CHAR)"
        elif db_type_lower in ["mssql", "sql server", "sqlserver"]:
            date_expr = f"CONVERT(CHAR, {col_escaped}, 23)"  # yyyy-MM-dd
        elif db_type_lower == "sqlite":
            date_expr = f"{col_escaped}"  # já são strings
        else:
            date_expr = f"CAST({col_escaped} AS TEXT)"
        return basic_op(date_expr)

    # 🔹 Número ou Texto
    return basic_op(col_escaped)

def get_query_string(
    base_table: str,
    joins: Optional[List[JoinOption]] = None,
    filters: Optional[str] = None,
    select: Optional[List[str]] = None,
    table_list: Optional[List[str]] = None,
    max_rows: int = 1000,
    db_type: str = "mysql",
    order_by: Optional[OrderByOption] = None,
    offset: Optional[int] = None,
    distinct: Optional[DistinctList] = None
) -> str:
    """
    Gera uma query SQL adaptada ao tipo de banco de dados, com DISTINCT, filtros, joins, ordenação e paginação.
    """
    db_type = db_type.lower()

    # Escape table
    quoted_base_table = quote_identifier(db_type, base_table)
    print(f"🔍 quoted_base_table: {quoted_base_table} joins {joins} table_list {table_list} ")
    # JOINs
    if joins:
        join_sql = " " + " ".join(
            f"{join.type.upper()} {quote_identifier(db_type, join.table)} ON {join.on}"
            for join in joins
        )
    elif table_list:
        join_tables = [
            quote_identifier(db_type, table)
            for table in table_list
            if table != base_table
        ]
        join_sql = ", " + ", ".join(join_tables) if join_tables else ""
    else:
        join_sql = ""


    # SELECT
    select_view = ", ".join(quote_identifier(db_type, col) for col in select) if select else "*"

    if distinct and distinct.useDistinct:
        distinct_cols = ", ".join(quote_identifier(db_type, col) for col in distinct.distinct_columns)
        if db_type in {"postgres", "postgresql"} and distinct_cols:
            query = f"SELECT DISTINCT ON ({distinct_cols}) {select_view} FROM {quoted_base_table}{join_sql}"
        else:
            query = f"SELECT DISTINCT {select_view} FROM {quoted_base_table}{join_sql}"
    else:
        query = f"SELECT {select_view} FROM {quoted_base_table}{join_sql}"

    # WHERE
    if filters:
        query += f" {filters}"

    # ORDER BY
    if order_by and order_by.column:
        query += f" ORDER BY {quote_identifier(db_type, order_by.column)} {order_by.direction.upper()}"

    # Paginação
    if db_type in {"mysql", "sqlite", "postgres", "postgresql"}:
        query += f" LIMIT {max_rows}"
        if offset is not None:
            query += f" OFFSET {offset}"
    
    elif db_type in {"mssql", "sql server","sqlserver"}:
        if not order_by or not order_by.column:
            query += f" ORDER BY (SELECT NULL)"  # fallback
        query += f" OFFSET {offset or 0} ROWS FETCH NEXT {max_rows} ROWS ONLY"

    elif db_type == "oracle":
        if offset is not None:
            query = (
                f"SELECT * FROM (SELECT a.*, ROWNUM rnum FROM ({query}) a "
                f"WHERE ROWNUM <= {offset + max_rows}) WHERE rnum > {offset}"
            )
        else:
            query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {max_rows}"
    # print(f"🔍 db type: {db_type}, query: {query}")
    return query


def quote_identifier(db_type: str, identifier: str) -> str:
    """
    Escapa um identificador SQL conforme o tipo de banco.
    Suporta identificadores compostos como 'tabela.coluna'.

    Ex:
    - PostgreSQL: "tabela"."coluna"
    - MySQL: `tabela`.`coluna`
    - MSSQL: [tabela].[coluna]
    """
    db_type = db_type.lower()
    parts = identifier.split(".")
    
    if db_type in ['postgresql', 'postgres', 'oracle']:
        return ".".join(f'"{part}"' for part in parts)
    elif db_type in ['mssql', 'sql server']:
        return ".".join(f'[{part}]' for part in parts)
    elif db_type in ['mysql']:
        return ".".join(f'`{part}`' for part in parts)
    else:
        return identifier

