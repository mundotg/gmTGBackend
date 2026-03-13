from __future__ import annotations

from typing import Optional

from app.schemas.query_select_upAndInsert_schema import AdvancedJoinOption
from app.services.editar_linha import (
    _convert_column_type_for_string_one,
    _map_column_type,
    quote_identifier,
)


def _normalize_table_name(table_name: Optional[str]) -> str:
    """
    Normaliza nome de tabela para comparação lógica.
    Ex:
    - public.query_history
    - "public"."query_history"
    - "query_history"
    viram formatos comparáveis.
    """
    if not table_name:
        return ""

    normalized = table_name.strip().replace('"', "").lower()

    # Se vier com schema, mantemos só o nome final para comparação defensiva
    # Ex: public.query_history -> query_history
    if "." in normalized:
        normalized = normalized.split(".")[-1]

    return normalized


def _unique_extra_tables(
    base_table: str,
    table_list: Optional[list[str]],
) -> list[str]:
    """
    Remove da table_list a tabela base e duplicados lógicos.
    """
    if not table_list:
        return []

    base_normalized = _normalize_table_name(base_table)
    seen: set[str] = set()
    result: list[str] = []

    for table in table_list:
        table_normalized = _normalize_table_name(table)

        if not table_normalized:
            continue

        if table_normalized == base_normalized:
            continue

        if table_normalized in seen:
            continue

        seen.add(table_normalized)
        result.append(table)

    return result


def _build_condition_sql(db_type: str, cond) -> str:
    """
    Monta uma condição SQL individual.
    """
    left = quote_identifier(db_type, cond.leftColumn)

    if cond.useValue:
        value_column_type = cond.valueColumnType.lower()

        if cond.operator in ["IN", "NOT IN"]:
            values_list = [
                _convert_column_type_for_string_one(
                    _map_column_type(value_column_type)(value.strip()),
                    value_column_type,
                )
                for value in (cond.rightValue or "").split(",")
                if value.strip()
            ]

            if not values_list:
                raise ValueError(
                    f"Lista vazia para operação '{cond.operator}' em '{left}'."
                )

            right = f"({', '.join(values_list)})"
        else:
            right_value = _map_column_type(value_column_type)(cond.rightValue)
            right = _convert_column_type_for_string_one(
                right_value,
                value_column_type,
            )
    else:
        right = quote_identifier(db_type, cond.rightColumn)

    return f"{left} {cond.operator} {right}"


def build_join_clause(
    db_type: str,
    base_table: str,
    joins: Optional[dict[str, AdvancedJoinOption]] = None,
    table_list: Optional[list[str]] = None,
) -> str:
    """
    Monta a cláusula JOIN ou tabelas adicionais.
    """
    if joins:
        join_parts: list[str] = []

        for table_name, join in joins.items():
            conds: list[str] = []

            for idx, cond in enumerate(join.conditions):
                cond_sql = _build_condition_sql(db_type, cond)

                if join.groupStart:
                    for group in join.groupStart:
                        if group.initIndex == idx and group.is_:
                            cond_sql = f"({cond_sql}"

                if idx > 0 and cond.logicalOperator:
                    cond_sql = f"{cond.logicalOperator} {cond_sql}"

                if join.groupEnd:
                    for group in join.groupEnd:
                        if group.endIndex == idx and group.is_:
                            cond_sql = f"{cond_sql})"

                conds.append(cond_sql)

            on_clause = " ".join(conds)

            table_ref = quote_identifier(db_type, table_name)
            if join.alias:
                table_ref += f" AS {quote_identifier(db_type, join.alias)}"

            join_parts.append(f"{join.typeJoin} {table_ref} ON {on_clause}")

        return " " + " ".join(join_parts)

    extra_tables = _unique_extra_tables(base_table, table_list)
    if not extra_tables:
        return ""

    join_tables = [quote_identifier(db_type, table) for table in extra_tables]
    return ", " + ", ".join(join_tables)


def build_join_clause_for_delete(
    db_type: str,
    base_table: str,
    joins: Optional[dict[str, AdvancedJoinOption]] = None,
    table_list: Optional[list[str]] = None,
    is_delete: bool = False,
) -> str:
    """
    Monta a cláusula de JOIN/USING compatível com múltiplos bancos.
    """
    db_type = db_type.lower()

    # ORACLE: DELETE com EXISTS
    if is_delete and "oracle" in db_type and joins:
        where_parts: list[str] = []

        for table_name, join in joins.items():
            conds: list[str] = []

            for cond in join.conditions:
                cond_sql = _build_condition_sql(db_type, cond)
                conds.append(cond_sql)

            where_sql = " AND ".join(conds)
            alias = f" {quote_identifier(db_type, join.alias)}" if join.alias else ""

            where_parts.append(
                f"EXISTS (SELECT 1 FROM {quote_identifier(db_type, table_name)}{alias} WHERE {where_sql})"
            )

        return " WHERE " + " AND ".join(where_parts)

    # PostgreSQL: DELETE ... USING tabela1, tabela2
    if is_delete and db_type in {"postgres", "postgresql"}:
        using_tables: list[str] = []

        if joins:
            seen: set[str] = set()
            base_normalized = _normalize_table_name(base_table)

            for table_name in joins.keys():
                normalized = _normalize_table_name(table_name)

                if not normalized or normalized == base_normalized or normalized in seen:
                    continue

                seen.add(normalized)
                using_tables.append(quote_identifier(db_type, table_name))

        else:
            extra_tables = _unique_extra_tables(base_table, table_list)
            using_tables = [quote_identifier(db_type, t) for t in extra_tables]

        return f" USING {', '.join(using_tables)}" if using_tables else ""

    # MySQL, SQL Server, SQLite: JOIN padrão
    if joins:
        join_parts: list[str] = []

        for table_name, join in joins.items():
            conds = [_build_condition_sql(db_type, cond) for cond in join.conditions]
            on_clause = " AND ".join(conds)

            table_ref = quote_identifier(db_type, table_name)
            if join.alias:
                table_ref += f" AS {quote_identifier(db_type, join.alias)}"

            join_parts.append(f"{join.typeJoin} {table_ref} ON {on_clause}")

        return " " + " ".join(join_parts)

    extra_tables = _unique_extra_tables(base_table, table_list)
    if not extra_tables:
        return ""

    join_tables = [quote_identifier(db_type, table) for table in extra_tables]
    return ", " + ", ".join(join_tables)