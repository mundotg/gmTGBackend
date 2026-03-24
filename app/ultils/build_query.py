from typing import Any, Callable, List, Optional, Union
import uuid
from sqlalchemy import Boolean, Date, DateTime, Numeric, TextClause, text
from app.schemas.query_select_upAndInsert_schema import (
    AdvancedJoinOption,
    DistinctList,
    JoinOption,
    OrderByOption,
    Pattern,
)
from app.services.editar_linha import (
    _convert_column_type_for_string_one,
    quote_identifier,
)
from app.ultils.logica_de_join_advance import build_join_clause


def is_valid_uuid(value: str) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except ValueError:
        return False


def _normalize_table_name(table_name: Optional[str]) -> str:
    if not table_name:
        return ""

    normalized = table_name.strip().replace('"', "").lower()

    # mantém schema+tabela se existir
    if "." in normalized:
        parts = [p.strip() for p in normalized.split(".") if p.strip()]
        return ".".join(parts)

    return normalized


def _normalize_table_variants(table_name: Optional[str]) -> set[str]:
    """
    Gera variantes para comparar:
    - public.query_history
    - query_history
    """
    normalized = _normalize_table_name(table_name)
    if not normalized:
        return set()

    variants = {normalized}

    if "." in normalized:
        variants.add(normalized.split(".")[-1])

    return variants


def _sanitize_table_list(
    base_table: str,
    table_list: Optional[List[str]],
) -> List[str]:
    """
    Remove duplicados e remove a base_table da table_list.
    """
    if not table_list:
        return []

    base_variants = _normalize_table_variants(base_table)
    seen: set[str] = set()
    result: list[str] = []

    for table in table_list:
        normalized = _normalize_table_name(table)
        if not normalized:
            continue

        short_name = normalized.split(".")[-1]
        candidates = {normalized, short_name}

        if candidates & base_variants:
            continue

        if normalized in seen:
            continue

        seen.add(normalized)
        result.append(table)

    return result


def get_filter_condition_with_operation(
    col_name: str,
    col_type: str,
    value: str,
    params: dict,
    db_type: str = "postgres",
    operation: str = "",
    value_otheir_between: str = "",
    param_name: Optional[str] = None,
    enum_values: Optional[dict] = None,
    pattern: Optional[Pattern] = None,
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

    if (
        value == ""
        and operation not in ["Entre"]
        and operation not in ["IS NULL", "IS NOT NULL"]
    ):
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
            "true": True,
            "1": True,
            "yes": True,
            "sim": True,
            "false": False,
            "0": False,
            "no": False,
            "não": False,
        }
        if value.lower() not in bool_map:
            raise ValueError(f"Valor booleano inválido para '{col_name}'.")
        params[param_name] = bool_map[value.lower()]
        return f"{col_escaped} = :{param_name}"

    # 🔹 Tipos
    is_date = any(
        t in col_type_str for t in ["date", "timestamp", "time"]
    ) or isinstance(col_type, (Date, DateTime))
    is_number = any(
        t in col_type_str for t in ["int", "decimal", "float", "numeric"]
    ) or isinstance(col_type, Numeric)
    # is_text = any(t in col_type_str for t in ["text", "char", "varchar"])

    def basic_op(field_escaped: str) -> str:
        op_map = {"=": "=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}

        if operation in op_map:
            params[param_name] = float(value) if is_number else value
            return f"{field_escaped} {op_map[operation]} :{param_name}"

        elif operation == "IS NULL":
            return f"{field_escaped} IS NULL"

        elif operation == "IS NOT NULL":
            return f"{field_escaped} IS NOT NULL"

        elif operation == "Entre":
            # Verificar se value_otheir_between tem valor válido
            if (
                not value_otheir_between
                or not str(value_otheir_between).strip()
                or str(value_otheir_between).strip() == "-1"
            ):
                # Se value_otheir_between é inválido, verificar se value tem */-1
                if value and "*/-1" in str(value):
                    parts = str(value).split("*/-1")
                    if len(parts) == 2:
                        value_min = parts[0]
                        value_max = parts[1]
                    else:
                        raise ValueError(
                            f"Separador '*/-1' encontrado mas não produziu dois valores em '{col_name}': {value}"
                        )
                else:
                    raise ValueError(
                        f"Segundo valor ausente para operação 'Entre' em '{col_name}'"
                    )
            else:
                # Usar os valores separados normalmente
                value_min = value
                value_max = value_otheir_between

            # Validar valores
            value_min = str(value_min).strip()
            value_max = str(value_max).strip()

            if not value_min or not value_max:
                raise ValueError(
                    f"Valores mínimo e máximo são obrigatórios para operador 'Entre' em '{col_name}'"
                )

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
                params=params,
                pattern=pattern,
            )
        elif operation in ["IN", "NOT IN"]:
            values_list = [v.strip() for v in value.split(",") if v.strip()]
            if not values_list:
                raise ValueError(
                    f"Lista vazia para operação '{operation}' em '{col_name}'."
                )

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

        raise ValueError(
            f"Operação '{operation}' não suportada para tipo '{col_type}'."
        )

    # 🔹 JSON
    if "json" in col_type_str:
        json_expr = (
            f"{col_escaped}::TEXT"
            if db_type_lower in ["postgres", "postgresql"]
            else f"CAST({col_escaped} AS TEXT)"
        )
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
    Formata uma coluna com quote e alias, escapando palavras reservadas.
    """
    # Lista de palavras reservadas comuns do SQL
    RESERVED_KEYWORDS = {
        "default",
        "select",
        "insert",
        "update",
        "delete",
        "from",
        "where",
        "order",
        "group",
        "by",
        "having",
        "join",
        "inner",
        "left",
        "right",
        "outer",
        "on",
        "as",
        "and",
        "or",
        "not",
        "null",
        "is",
        "true",
        "false",
        "primary",
        "key",
        "foreign",
        "references",
        "table",
        "column",
        "index",
        "create",
        "alter",
        "drop",
        "truncate",
        "grant",
        "revoke",
        "commit",
        "rollback",
        "savepoint",
        "begin",
        "transaction",
        "lock",
        "unlock",
        "user",
        "role",
        "database",
        "schema",
        "view",
        "function",
        "procedure",
        "trigger",
        "event",
        "type",
        "domain",
        "constraint",
        "check",
        "unique",
        "current",
        "time",
        "date",
        "timestamp",
        "interval",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "zone",
        "value",
        "values",
    }

    def needs_escape(identifier: str) -> bool:
        """Verifica se um identificador precisa ser escapado."""
        # Remove quotes existentes para verificar o nome puro
        clean = identifier.strip('"`[]')
        return clean.lower() in RESERVED_KEYWORDS or not clean.isidentifier()

    def quote_identifier_safe(db_type: str, identifier: str) -> str:
        """
        Versão melhorada do quote_identifier que também escapa palavras reservadas.
        """
        # Se já está quotado, retorna como está
        if (
            (identifier.startswith('"') and identifier.endswith('"'))
            or (identifier.startswith("`") and identifier.endswith("`"))
            or (identifier.startswith("[") and identifier.endswith("]"))
        ):
            return identifier

        # Divide por pontos para tratar cada parte
        parts = identifier.split(".")
        quoted_parts = []

        for part in parts:
            # Limpa a parte
            clean_part = part.strip('"`[]')

            # Verifica se precisa escapar
            if needs_escape(clean_part):
                if db_type.lower() == "postgresql":
                    quoted_parts.append(f'"{clean_part}"')
                elif db_type.lower() == "mssql" or db_type.lower() == "sqlserver":
                    quoted_parts.append(f"[{clean_part}]")
                else:  # mysql, mariadb, sqlite
                    quoted_parts.append(f"`{clean_part}`")
            else:
                # Usa o quote padrão do banco
                if db_type.lower() == "postgresql":
                    quoted_parts.append(f'"{clean_part}"')
                elif db_type.lower() == "mssql" or db_type.lower() == "sqlserver":
                    quoted_parts.append(f"[{clean_part}]")
                else:
                    quoted_parts.append(f"`{clean_part}`")

        return ".".join(quoted_parts)

    # Escapa a coluna se necessário
    col_quoted = quote_identifier_safe(db_type, col)

    if alias:
        # Trata o alias de forma especial
        # Se o alias já tem quotes, usa como está
        if (
            (alias.startswith('"') and alias.endswith('"'))
            or (alias.startswith("`") and alias.endswith("`"))
            or (alias.startswith("[") and alias.endswith("]"))
        ):
            return f"{col_quoted} AS {alias}"

        # Escapa o alias se necessário
        clean_alias = alias.strip('"`[]')
        if needs_escape(clean_alias):
            if db_type.lower() == "postgresql":
                return f'{col_quoted} AS "{clean_alias}"'
            elif db_type.lower() == "mssql" or db_type.lower() == "sqlserver":
                return f"{col_quoted} AS [{clean_alias}]"
            else:
                return f"{col_quoted} AS `{clean_alias}`"
        else:
            # Alias sem pontos, usa quote padrão
            quote_char = "`" if db_type.lower() == "mysql" else '"'
            return f"{col_quoted} AS {quote_char}{alias}{quote_char}"

    return col_quoted


def build_select_view(
    db_type: str,
    select: Optional[list[str]],
    aliases: Optional[dict[str, str]],
) -> str:
    """
    Monta o SELECT com escape de palavras reservadas.
    - Se select foi enviado → usa select
    - Aplica alias apenas quando existir alias real
    - Se select vazio e aliases existir → usa aliases como fonte
    - Caso contrário → usa '*'
    """
    try:
        if select and len(select) > 0:
            parts = []
            for col in select:
                alias = aliases.get(col) if aliases else None
                parts.append(format_column(db_type, col, alias))
            return ", ".join(parts)

        if aliases:
            return ", ".join(
                format_column(db_type, col, alias) for col, alias in aliases.items()
            )

        return "*"

    except Exception as e:
        # Log do erro e fallback
        from app.ultils.logger import log_message

        log_message(f"Erro ao construir SELECT: {str(e)}", level="error")
        # Fallback seguro: retorna *
        return "*"


def format_order_by(
    db_type: str,
    order_by: Optional[List[Union[dict, "OrderByOption"]]] = None,
) -> str:
    """
    Garante ORDER BY válido.
    """
    if not order_by:
        return "ORDER BY (SELECT NULL)"

    order_parts = []

    for item in order_by:
        if hasattr(item, "column"):
            col = item.column  # type: ignore
            direction = item.direction or "ASC"  # type: ignore
        else:
            col = item.get("column")  # type: ignore
            direction = item.get("direction", "ASC")  # type: ignore

        if not col or not str(col).strip():
            continue

        direction = str(direction).upper()
        if direction not in ("ASC", "DESC"):
            direction = "ASC"

        order_parts.append(f"{quote_identifier(db_type, col)} {direction}")

    if not order_parts:
        return "ORDER BY (SELECT NULL)"

    return "ORDER BY " + ", ".join(order_parts)


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
    aliases: Optional[dict[str, str]] = None,
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
        distinct_cols = ", ".join(
            quote_identifier(db_type, col) for col in distinct.distinct_columns
        )
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
        query += f" {format_order_by(db_type, order_by)}"  # type: ignore

    # Paginação
    if db_type in {"mysql", "sqlite", "postgres", "postgresql"}:
        query += f" LIMIT {max_rows}"
        if offset is not None:
            query += f" OFFSET {offset}"

    elif db_type in {"mssql", "sql server", "sqlserver"}:
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
    max_rows: Optional[int] = None,
    db_type: str = "mysql",
    order_by: Optional[list[OrderByOption]] = None,
    offset: Optional[int] = None,
    distinct: Optional[DistinctList] = None,
    aliases: Optional[dict[str, str]] = None,
) -> str:
    """
    Gera query SQL adaptada ao banco, com JOIN, filtros, ordenação, DISTINCT e paginação.
    """
    db_type = db_type.lower()
    quoted_base_table = quote_identifier(db_type, base_table)

    safe_table_list = _sanitize_table_list(base_table, table_list)

    join_sql = build_join_clause(
        db_type=db_type,
        base_table=base_table,
        joins=joins,
        table_list=safe_table_list,
    )

    print("JOIN SQL:", join_sql)

    select_view = build_select_view(db_type, select, aliases)

    # ==========================================
    # BLOCO MODIFICADO: Apenas a lógica do Distinct
    # ==========================================
    if distinct and distinct.useDistinct:
        distinct_cols_list = [
            quote_identifier(db_type, col) for col in distinct.distinct_columns
        ]
        distinct_cols = ", ".join(distinct_cols_list)

        if db_type in {"postgres", "postgresql"} and distinct_cols:
            query = (
                f"SELECT DISTINCT ON ({distinct_cols}) "
                f"{select_view} "
                f"FROM {quoted_base_table}{join_sql}"
            )
        elif distinct_cols:
            # 1. Pega a primeira coluna para a ordenação obrigatória da Window Function
            first_col = distinct_cols_list[0]

            # 2. Prepara o filtro para aplicar DENTRO da subquery
            filter_str = f" {filters}" if filters else ""

            if db_type in {"oracle", "oracledb"}:
                query = (
                    f"SELECT * FROM ("
                    f"  SELECT {select_view}, "
                    f"  ROW_NUMBER() OVER(PARTITION BY {distinct_cols} ORDER BY {first_col}) AS _rn "
                    f"  FROM {quoted_base_table}{join_sql}{filter_str}"
                    f") _sub WHERE _rn = 1"
                )
            else:
                query = (
                    f"SELECT * FROM ("
                    f"  SELECT {select_view}, "
                    f"  ROW_NUMBER() OVER(PARTITION BY {distinct_cols} ORDER BY {first_col}) AS _rn "
                    f"  FROM {quoted_base_table}{join_sql}{filter_str}"
                    f") AS _sub WHERE _rn = 1"
                )

            # 3. TRUQUE: Como já aplicamos o filtro DENTRO da subquery acima,
            # zeramos a variável aqui para que o 'if filters:' lá em baixo não duplique e quebre a query.
            filters = None

        else:
            query = f"SELECT DISTINCT {select_view} FROM {quoted_base_table}{join_sql}"
    else:
        query = f"SELECT {select_view} FROM {quoted_base_table}{join_sql}"
    # ==========================================

    # Todo o resto do seu código permanece INTACTO
    if filters:
        query += f" {filters}"

    if order_by and len(order_by) > 0:
        query += f" {format_order_by(db_type, order_by)}"  # type: ignore

    if db_type in {"mysql", "sqlite", "postgres", "postgresql"}:
        if max_rows is not None:
            query += f" LIMIT {max_rows}"
        if offset is not None:
            query += f" OFFSET {offset}"

    elif db_type in {"mssql", "sql server", "sqlserver"}:
        if not order_by or len(order_by) == 0:
            query += " ORDER BY (SELECT NULL)"
        query += f" OFFSET {offset or 0} ROWS"
        if max_rows is not None:
            query += f" FETCH NEXT {max_rows} ROWS ONLY"

    elif db_type == "oracle":
        if max_rows is None:
            max_rows = 1000

        if offset is not None:
            query = (
                f"SELECT * FROM (SELECT a.*, ROWNUM rnum FROM ({query}) a "
                f"WHERE ROWNUM <= {offset + max_rows}) WHERE rnum > {offset}"
            )
        else:
            query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {max_rows}"

    # print("query:",query)
    return query


def get_count_query(
    base_table: str,
    joins: Optional[Any] = None,  # Ajuste para dict[str, AdvancedJoinOption]
    filters: Optional[str] = None,
    distinct: Optional[Any] = None,  # Ajuste para DistinctList
    db_type: str = "mysql",
) -> str:
    """
    Gera uma query SQL otimizada para contar registros.

    Otimizações:
    - Usa COUNT(*) quando possível (mais rápido)
    - Para DISTINCT com colunas, usa COUNT(DISTINCT colunas) para 1 coluna
    - Usa Subquery para múltiplas colunas DISTINCT (Padrão universal e evita erro de sintaxe)
    - Adiciona hints de otimização para bancos específicos
    """
    db_type = db_type.lower()
    quoted_base_table = quote_identifier(db_type, base_table)

    # JOINs
    join_sql = build_join_clause(db_type, base_table, joins, None) if joins else ""

    # Prepara o filtro com espaço inicial seguro
    filter_sql = f" {filters}" if filters else ""

    # Verifica se há Distinct e Colunas especificadas
    use_distinct = getattr(distinct, "useDistinct", False) if distinct else False
    distinct_cols_raw = getattr(distinct, "distinct_columns", []) if distinct else []

    if use_distinct and distinct_cols_raw:
        distinct_cols_list = [
            quote_identifier(db_type, col) for col in distinct_cols_raw
        ]
        distinct_cols_str = ", ".join(distinct_cols_list)

        # SE TIVER MAIS DE 1 COLUNA -> Usa Subquery (Universal: SQL Server, MySQL, Postgres, etc.)
        if len(distinct_cols_list) > 1:
            # Oracle não aceita 'AS' antes do alias da subquery
            alias_prefix = "" if db_type in {"oracle", "oracledb"} else "AS "

            # ATENÇÃO: O filter_sql precisa estar DENTRO da subquery
            query = (
                f"SELECT COUNT(*) FROM ("
                f"  SELECT DISTINCT {distinct_cols_str} "
                f"  FROM {quoted_base_table}{join_sql}{filter_sql}"
                f") {alias_prefix}_count_sub"
            )
            # Como o filtro já foi aplicado dentro da subquery, limpamos a variável
            # para não ser concatenada novamente lá no final.
            filter_sql = ""

        # SE TIVER SÓ 1 COLUNA -> COUNT(DISTINCT) nativo funciona
        else:
            query = f"SELECT COUNT(DISTINCT {distinct_cols_str}) FROM {quoted_base_table}{join_sql}"

    # SEM DISTINCT -> COUNT(*) é o mais rápido
    else:
        query = f"SELECT COUNT(*) FROM {quoted_base_table}{join_sql}"

    # Aplica o WHERE (se já não tiver sido aplicado na subquery)
    if filter_sql:
        query += filter_sql

    # ==========================================
    # HINTS E OTIMIZAÇÕES ESPECÍFICAS DE BANCO
    # Só aplicamos se for uma contagem limpa (sem distinct, joins e filtros)
    # ==========================================
    if not joins and not filters and not use_distinct:
        if db_type in {"mysql", "mariadb"}:
            query = f"SELECT /*+ NO_ICP({quoted_base_table}) */ COUNT(*) FROM {quoted_base_table}"

        elif db_type in {"mssql", "sql server", "sqlserver"}:
            query = f"SELECT COUNT(*) FROM {quoted_base_table} WITH (NOLOCK)"

        elif db_type == "oracle":
            query = f"SELECT /*+ PARALLEL({quoted_base_table}, 2) */ COUNT(*) FROM {quoted_base_table}"

    return query


# Versão alternativa ainda mais otimizada para casos específicos
def get_count_query_optimized(
    base_table: str,
    joins: Optional[dict[str, AdvancedJoinOption]] = None,
    filters: Optional[str] = None,
    distinct: Optional[DistinctList] = None,
    db_type: str = "mysql",
    use_estimate: bool = False,
) -> str:
    """
    Versão ultra-otimizada que pode usar estatísticas do banco para COUNT aproximado.

    Args:
        use_estimate: Se True, tenta usar métodos aproximados (mais rápidos, menos precisos)
    """
    db_type = db_type.lower()
    quoted_base_table = quote_identifier(db_type, base_table)

    # Se queremos estimativa e não há filtros complexos
    if use_estimate and not filters and not distinct:
        if db_type in {"postgresql", "postgres"}:
            # PostgreSQL: usa estatísticas do planner
            return f"SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = '{base_table.split('.')[-1]}'"

        elif db_type in {"mysql", "mariadb"}:
            # MySQL: usa informações do INFORMATION_SCHEMA
            schema = base_table.split(".")[0] if "." in base_table else None
            table = base_table.split(".")[-1]
            if schema:
                return f"SELECT table_rows FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'"
            else:
                return f"SELECT table_rows FROM information_schema.tables WHERE table_name = '{table}'"

        elif db_type in {"mssql", "sql server", "sqlserver"}:
            # SQL Server: usa estatísticas
            return f"SELECT rows FROM sys.partitions WHERE object_id = OBJECT_ID('{base_table}') AND index_id IN (0,1)"

    # Se não, usa COUNT normal
    return get_count_query(base_table, joins, filters, distinct, db_type)


# Função auxiliar para decidir qual estratégia de COUNT usar
def should_use_estimate(
    total_rows: Optional[int] = None, table_size: str = "unknown"
) -> bool:
    """
    Decide se deve usar COUNT estimado baseado no tamanho da tabela.

    Args:
        total_rows: Número aproximado de linhas (se conhecido)
        table_size: 'small', 'medium', 'large', 'unknown'
    """
    if total_rows is not None:
        return total_rows > 1000000  # > 1 milhão de linhas

    # Baseado em heurística
    size_map = {"small": False, "medium": False, "large": True, "unknown": False}
    return size_map.get(table_size, False)


def build_update_query(
    table_name, db_type, updated_values, primary_key, primary_value
) -> TextClause | None:
    """Constrói a query de atualização dinâmica."""
    set_clauses = []
    for col, info in updated_values.items():
        value = info.get("value")
        col_type = info.get("type_column", "text")  # default string
        set_clauses.append(
            f"{quote_identifier(db_type, col)} = {_convert_column_type_for_string_one(value, col_type)}"
        )

    if not set_clauses:
        return None

    primary_value_formatted = (
        f"'{primary_value}'" if isinstance(primary_value, str) else str(primary_value)
    )
    query = text(
        f"""
        UPDATE {quote_identifier(db_type, table_name)}
        SET {', '.join(set_clauses)}
        WHERE {quote_identifier(db_type, primary_key)} = {primary_value_formatted};
    """
    )
    return query
