from typing import List, Optional
import uuid
from sqlalchemy import Boolean, Date, DateTime, Engine, Numeric, text
from app.cruds.queryhistory_crud import create_query_history
from app.schemas.query_select_upAndInsert_schema import AdvancedJoinOption, DistinctList, JoinOption, OrderByOption, UpdateRequest
from app.schemas.queryhistory_schemas import  QueryHistoryCreate
from app.services.editar_linha import _convert_column_type_for_string_one, quote_identifier
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message
from app.ultils.logica_de_join_advance import build_join_clause

def is_valid_uuid(value: str) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except ValueError:
        return False
    
def build_contains_condition(
    field_escaped: str,
    operation: str,
    value: str,
    db_type: str,
    col_type: str,
    param_name: str,
    params: dict
) -> str:
    """
    Monta condição SQL para operações 'Contém' e 'Não Contém',
    suportando TEXT, DATE, JSON e conversão automática para INTEGER/NUMERIC.
    Compatível com PostgreSQL, MySQL, SQL Server e Oracle.
    """
    col_type_str = col_type.lower()
    like_value = f"%{value}%"
    params[param_name] = like_value
    not_ = "NOT " if operation == "Não Contém" else ""

    db_type = db_type.lower()

    # Strings e JSON → LIKE direto
    if any(t in col_type_str for t in ["text", "char", "json"]):
        return f"{field_escaped} {not_}LIKE :{param_name}"

    # Datas → precisa converter para texto dependendo do banco
    elif "date" in col_type_str or "time" in col_type_str:
        if db_type in ["postgres", "postgresql"]:
            return f"CAST({field_escaped} AS TEXT) {not_}LIKE :{param_name}"
        elif db_type == "mysql":
            return f"CAST({field_escaped} AS CHAR) {not_}LIKE :{param_name}"
        elif db_type in ["sqlserver", "mssql"]:
            return f"CONVERT(VARCHAR, {field_escaped}, 120) {not_}LIKE :{param_name}"
        elif db_type == "oracle":
            return f"TO_CHAR({field_escaped}, 'YYYY-MM-DD HH24:MI:SS') {not_}LIKE :{param_name}"
        else:
            raise ValueError(f"Banco de dados '{db_type}' não suportado para operação Contém em datas.")

    # Números → CAST para string
    elif any(t in col_type_str for t in ["int", "numeric", "decimal", "float", "double"]):
        if db_type in ["postgres", "postgresql"]:
            return f"CAST({field_escaped} AS TEXT) {not_}LIKE :{param_name}"
        elif db_type == "mysql":
            return f"CAST({field_escaped} AS CHAR) {not_}LIKE :{param_name}"
        elif db_type in ["sqlserver", "mssql"]:
            return f"CAST({field_escaped} AS NVARCHAR(MAX)) {not_}LIKE :{param_name}"
        elif db_type == "oracle":
            return f"TO_CHAR({field_escaped}) {not_}LIKE :{param_name}"
        else:
            raise ValueError(f"Banco de dados '{db_type}' não suportado para operação Contém em números.")

    # Caso não seja suportado
    raise ValueError(f"Operação '{operation}' não suportada para tipo '{col_type}'.")


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

    if value == "" and operation not in ["Entre"] and operation not in ["IS NULL", "IS NOT NULL"]:
        raise ValueError(f"Valor vazio para '{col_name}' com operação '{operation}'.")

    # Escapar nome da coluna
    col_escaped = quote_identifier(db_type_lower, col_name)

    # 🔹 Enum
    if enum_values and enum_values.get(col_name):
        params[param_name] = value
        return f"{col_escaped} = :{param_name}"

    # 🔹 UUID
    if "uuid" in col_type_str:
        if not is_valid_uuid(value):
            raise ValueError(f"Valor inválido para UUID: {value}")
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
    # is_text = any(t in col_type_str for t in ["text", "char", "varchar"])

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
        
        elif operation == "IS NULL":
            return f"{field_escaped} IS NULL"

        elif operation == "IS NOT NULL":
            return f"{field_escaped} IS NOT NULL"

        elif operation == "Entre":
            # Verificar se value_otheir_between tem valor válido
            if not value_otheir_between or not str(value_otheir_between).strip() or str(value_otheir_between).strip() == "-1":
                # Se value_otheir_between é inválido, verificar se value tem */-1
                if value and '*/-1' in str(value):
                    parts = str(value).split('*/-1')
                    if len(parts) == 2:
                        value_min = parts[0]
                        value_max = parts[1]
                    else:
                        raise ValueError(f"Separador '*/-1' encontrado mas não produziu dois valores em '{col_name}': {value}")
                else:
                    raise ValueError(f"Segundo valor ausente para operação 'Entre' em '{col_name}'")
            else:
                # Usar os valores separados normalmente
                value_min = value
                value_max = value_otheir_between
            
            # Validar valores
            value_min = str(value_min).strip()
            value_max = str(value_max).strip()
            
            if not value_min or not value_max:
                raise ValueError(f"Valores mínimo e máximo são obrigatórios para operador 'Entre' em '{col_name}'")
            
            # Converter para float se for número
            if is_number:
                params[f"{param_name}_min"] = float(value_min)
                params[f"{param_name}_max"] = float(value_max)
            else:
                params[f"{param_name}_min"] = value_min
                params[f"{param_name}_max"] = value_max
            
            return f"{field_escaped} BETWEEN :{param_name}_min AND :{param_name}_max"

        elif operation in ["Contém", "Não Contém"]:
            return build_contains_condition(
                field_escaped=field_escaped,
                operation=operation,
                value=value,
                col_type=col_type,
                db_type=db_type,
                param_name=param_name,
                params=params
            )
        elif operation in ["IN", "NOT IN"]:
            values_list = [v.strip() for v in value.split(",") if v.strip()]
            if not values_list:
                raise ValueError(f"Lista vazia para operação '{operation}' em '{col_name}'.")

            placeholders = []
            for i, v in enumerate(values_list):
                pname = f"{param_name}_{i}"
                params[pname] = float(v) if is_number else v
                placeholders.append(f":{pname}")

            not_ = "NOT " if operation == "NOT IN" else ""
            return f"{field_escaped} {not_}IN ({', '.join(placeholders)})"

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

def format_column(db_type: str, col: str, alias: Optional[str] = None) -> str:
    """
    Formata uma coluna com quote e alias (se existir).
    """
    if alias and alias != col:
        return f"{quote_identifier(db_type, col)} AS {quote_identifier(db_type, alias)}"
    return quote_identifier(db_type, col)

def build_select_view(db_type: str, select: Optional[list[str]], aliases: Optional[dict[str, str]]) -> str:
    """
    Monta o SELECT, considerando select explícito e aliases.
    - Se select foi enviado → usa select.
    - Se select é None/[] e aliases existe → usa aliases como fonte.
    - Caso contrário → usa '*'.
    """
    if select and len(select) > 0:
        return ", ".join(
            format_column(db_type, col, aliases.get(col) if aliases else None)
            for col in select
        )
    elif aliases:
        return ", ".join(
            format_column(db_type, col, alias) for col, alias in aliases.items()
        )
    else:
        return "*"
    
from typing import Optional, Union

def format_order_by(db_type: str, order_by: Optional[list[Union[dict, "OrderByOption"]]] = None) -> str:
    """
    Monta a cláusula ORDER BY a partir de uma lista de dicts ou objetos OrderByOption.
    """
    if not order_by:
        return ""

    order_parts = []
    for item in order_by:
        # Caso venha como objeto Pydantic
        if hasattr(item, "column") and hasattr(item, "direction"):
            col = item.column
            direction = item.direction or "ASC"
        else:  # Caso venha como dict
            col = item.get("column")
            direction = item.get("direction", "ASC")

        if not col:
            continue

        order_parts.append(f"{quote_identifier(db_type, col)} {direction.upper()}")

    return "ORDER BY " + ", ".join(order_parts) if order_parts else ""



def get_query_string(
    base_table: str,
    joins: Optional[List[JoinOption]] = None,
    filters: Optional[str] = None,
    select: Optional[List[str]] = None,
    table_list: Optional[List[str]] = None,
    max_rows: int = 1000,
    db_type: str = "mysql",
    order_by: Optional[list[OrderByOption]] = None,
    offset: Optional[int] = None,
    distinct: Optional[DistinctList] = None,
    aliases: Optional[dict[str, str]] = None
) -> str:
    """
    Gera uma query SQL adaptada ao tipo de banco de dados, com DISTINCT, filtros, joins, ordenação e paginação.
    """
    db_type = db_type.lower()

    # Escape table
    quoted_base_table = quote_identifier(db_type, base_table)
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
    select_view = build_select_view(db_type, select, aliases)

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
    if order_by and len(order_by) > 0:
        query += f" {format_order_by(db_type, order_by)}"

    # Paginação
    if db_type in {"mysql", "sqlite", "postgres", "postgresql"}:
        query += f" LIMIT {max_rows}"
        if offset is not None:
            query += f" OFFSET {offset}"
    
    elif db_type in {"mssql", "sql server","sqlserver"}:
        if not order_by or len(order_by) == 0:
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
    return query




def get_query_string_advance(
    base_table: str,
    joins: Optional[dict[str, AdvancedJoinOption]] = None,
    filters: Optional[str] = None,
    select: Optional[List[str]] = None,
    table_list: Optional[List[str]] = None,
    max_rows: int = 1000,
    db_type: str = "mysql",
    order_by: Optional[list[OrderByOption]] = None,
    offset: Optional[int] = None,
    distinct: Optional[DistinctList] = None,
    aliases: Optional[dict[str, str]] = None,
) -> str:
    """
    Gera uma query SQL adaptada ao tipo de banco de dados,
    com DISTINCT, filtros, joins, ordenação e paginação.
    """
    db_type = db_type.lower()
    quoted_base_table = quote_identifier(db_type, base_table)

    # --- JOINs ---
    join_sql = build_join_clause(db_type, base_table, joins, table_list)

    
    # SELECT
    select_view = build_select_view(db_type, select, aliases)

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
    if order_by and len(order_by) > 0:
        query += f" {format_order_by(db_type, order_by)}"

    # Paginação
    if db_type in {"mysql", "sqlite", "postgres", "postgresql"}:
        query += f" LIMIT {max_rows}"
        if offset is not None:
            query += f" OFFSET {offset}"
    
    elif db_type in {"mssql", "sql server","sqlserver"}:
        if not order_by or len(order_by) == 0:
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

    return query


def get_count_query(
    base_table: str,
    joins: Optional[List[JoinOption]] = None,
    filters: Optional[str] = None,
    distinct: Optional[DistinctList] = None,
    db_type: str = "mysql"
) -> str:
    """
    Gera uma query SQL para contar registros, considerando joins, filtros e DISTINCT.
    """
    db_type = db_type.lower()
    quoted_base_table = quote_identifier(db_type, base_table)

    # JOINs
    if joins:
         # --- JOINs ---
        join_sql = build_join_clause(db_type, base_table, joins, None)
    else:
        join_sql = ""

    # COUNT com DISTINCT
    if distinct and distinct.useDistinct and distinct.distinct_columns:
        distinct_cols = ", ".join(quote_identifier(db_type, col) for col in distinct.distinct_columns)
        query = f"SELECT COUNT(DISTINCT {distinct_cols}) FROM {quoted_base_table}{join_sql}"
    else:
        query = f"SELECT COUNT(*) FROM {quoted_base_table}{join_sql}"

    # WHERE
    if filters:
        query += f" {filters}"

    return query

def build_update_query(table_name, db_type, updated_values, primary_key, primary_value):
    """Constrói a query de atualização dinâmica."""
    set_clauses = []
    for col, info in updated_values.items():
        value = info.get("value")
        col_type = info.get("type_column", "text")  # default string
        set_clauses.append(f"{quote_identifier(db_type, col)} = {_convert_column_type_for_string_one(value, col_type)}")

    if not set_clauses:
        return None

    primary_value_formatted = f"'{primary_value}'" if isinstance(primary_value, str) else str(primary_value)
    query = text(f"""
        UPDATE {quote_identifier(db_type, table_name)}
        SET {', '.join(set_clauses)}
        WHERE {quote_identifier(db_type, primary_key)} = {primary_value_formatted};
    """)
    return query

from sqlalchemy.orm import Session
def update_row_service(
    data: UpdateRequest,
    engine: Engine,
    user_id: int,
    db_type: str,
    connection_id: str,
    db: Session,
    client_ip: str = None,
    app_source: str = "API",
    executed_by: str = "sistema",
    modified_by: str = None,
):
    """
    Atualiza registros existentes em uma ou mais tabelas e salva no histórico completo.

    Args:
        data (UpdateRequest): Dados contendo as tabelas, chaves primárias e colunas a atualizar.
        engine (Engine): Conexão SQLAlchemy para execução da query.
        user_id (int): ID do usuário executando a operação.
        db_type (str): Tipo de banco (ex: 'postgres', 'mysql', 'sqlite').
        connection_id (str): ID da conexão do banco de dados.
        db (Session): Sessão SQLAlchemy para salvar histórico.
        client_ip (str, optional): Endereço IP do cliente.
        app_source (str, optional): Origem da execução (ex: API, Console, UI).
        executed_by (str, optional): Nome de quem executou (usuário ou sistema).
        modified_by (str, optional): Usuário que modificou o registro.

    Returns:
        dict: Resumo da operação com status e linhas afetadas.
    """
    import time
    from datetime import datetime, timezone

    resposta_query = ""
    query_string = ""
    duration_ms = 0
    start = time.time()

    try:
        with engine.begin() as conn:
            for table_name, primary_key_data in data.tables_primary_keys_values.items():
                # ✅ Validação da chave primária
                primary_key = primary_key_data.get("primaryKey")
                primary_value = primary_key_data.get("valor")
                if not primary_key or primary_value is None:
                    raise ValueError(f"Tabela '{table_name}' não contém chave primária válida")

                # ✅ Obtenção e estruturação dos dados a atualizar
                raw_values = data.updatedRow.get(table_name, {})
                updated_values = {
                    col: {
                        "value": field["value"] if isinstance(field, dict) else getattr(field, "value", None),
                        "type_column": field.get("type_column", "text") if isinstance(field, dict) else getattr(field, "type_column", "text")
                    }
                    for col, field in raw_values.items()
                }

                if not updated_values:
                    raise ValueError(f"Tabela '{table_name}': Nenhuma coluna para atualizar")

                # ✅ Monta a query SQL final
                query = build_update_query(
                    table_name=table_name,
                    db_type=db_type,
                    updated_values=updated_values,
                    primary_key=primary_key,
                    primary_value=primary_value
                )

                # ✅ Execução da query
                query_string += f"{table_name}: {query}\n"
                rs = conn.execute(query)
                resposta_query += f"\n{table_name}: {rs.rowcount} linha(s) atualizada(s)"

        duration_ms = int((time.time() - start) * 1000)

        # ✅ Criação do histórico completo
        historico = QueryHistoryCreate(
            user_id=user_id,
            db_connection_id=connection_id,
            query=query_string.strip(),
            query_type="UPDATE",
            executed_at=datetime.now(timezone.utc),
            duration_ms=duration_ms,
            result_preview=resposta_query or "Nenhuma linha atualizada",
            error_message=None,
            is_favorite=False,
            tags="update",
            app_source=app_source,
            client_ip=client_ip,
            executed_by=executed_by,
            modified_by=modified_by or executed_by,
            meta_info={
                "db_type": db_type,
                "tables_updated": list(data.updatedRow.keys()),
                "primary_keys": data.tables_primary_keys_values,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )

        create_query_history(db=db, data=historico)
        log_message(f"Atualização concluída: {resposta_query}")

        return {
            "status": resposta_query or "Nenhuma linha atualizada",
            "updated": data.updatedRow,
            "response": resposta_query
        }

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        error_msg = _lidar_com_erro_sql(e)

        # ✅ Log e histórico de erro completo
        log_message(f"Erro ao atualizar registro: {error_msg}", "error")

        historico = QueryHistoryCreate(
            user_id=user_id,
            db_connection_id=connection_id,
            query=query_string.strip() or "UPDATE falhou antes de montar a query",
            query_type="UPDATE",
            executed_at=datetime.now(timezone.utc),
            duration_ms=duration_ms,
            result_preview=resposta_query or "Sem resultado devido ao erro",
            error_message=error_msg,
            is_favorite=False,
            tags="error",
            app_source=app_source,
            client_ip=client_ip,
            executed_by=executed_by,
            modified_by=modified_by or executed_by,
            meta_info={
                "db_type": db_type,
                "tables_attempted": list(data.updatedRow.keys()),
                "primary_keys": data.tables_primary_keys_values,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )

        create_query_history(db=db, data=historico)
        raise ValueError(error_msg)


