from typing import Optional
from app.schemas.query_select_upAndInsert_schema import AdvancedJoinOption
from app.services.editar_linha import (
    _convert_column_type_for_string_one,
    _map_column_type,
    quote_identifier,
)


def build_join_clause(
    db_type: str,
    base_table: str,
    joins: Optional[dict[str, AdvancedJoinOption]] = None,
    table_list: Optional[list[str]] = None,
) -> str:
    """
    Monta a cláusula de JOIN ou lista de tabelas adicionais.
    """
    join_sql = ""

    if joins:
        join_parts = []
        for table_name, join in joins.items():
            conds = []
            for idx, cond in enumerate(join.conditions):
                left = quote_identifier(db_type, cond.leftColumn)

                if cond.useValue:
                    if cond.operator in ["IN", "NOT IN"]:
                        not_ = "NOT " if cond.operator == "NOT IN" else ""
                        values_list = [
                            _convert_column_type_for_string_one(
                                _map_column_type(cond.valueColumnType.lower())(
                                    v.strip()
                                ),
                                cond.valueColumnType.lower(),
                            )
                            for v in cond.rightValue.split(",")
                            if v.strip()
                        ]
                        if not values_list:
                            raise ValueError(
                                f"Lista vazia para operação '{cond.operator}' em '{left}'."
                            )
                        right = f"({', '.join(values_list)})"
                    else:
                        right = _convert_column_type_for_string_one(
                            _map_column_type(cond.valueColumnType.lower())(
                                cond.rightValue
                            ),
                            cond.valueColumnType.lower(),
                        )
                else:
                    right = quote_identifier(db_type, cond.rightColumn)

                cond_sql = f"{left} {cond.operator} {right}"

                # --- agrupamento ( ) ---
                if join.groupStart:
                    for g in join.groupStart:
                        if g.initIndex == idx and g.is_:
                            cond_sql = f"({cond_sql}"

                if idx > 0 and cond.logicalOperator:
                    cond_sql = f"{cond.logicalOperator} {cond_sql}"

                if join.groupEnd:
                    for g in join.groupEnd:
                        if g.endIndex == idx and g.is_:
                            cond_sql = f"{cond_sql})"

                conds.append(cond_sql)

            on_clause = " ".join(conds)

            table_ref = quote_identifier(db_type, table_name)
            if join.alias:
                table_ref += f" AS {quote_identifier(db_type, join.alias)}"

            join_parts.append(f"{join.typeJoin} {table_ref} ON {on_clause}")

        join_sql = " " + " ".join(join_parts)

    elif table_list:
        join_tables = [
            quote_identifier(db_type, table)
            for table in table_list
            if table != base_table
        ]
        join_sql = ", " + ", ".join(join_tables) if join_tables else ""

    return join_sql


def build_join_clause_for_delete(
    db_type: str,
    base_table: str,
    joins: Optional[dict[str, "AdvancedJoinOption"]] = None,
    table_list: Optional[list[str]] = None,
    is_delete: bool = False,
) -> str:
    """
    Monta a cláusula de JOIN/USING compatível com múltiplos bancos.

    Suporte:
      ✅ PostgreSQL - USING (para DELETE)
      ✅ MySQL/MariaDB - INNER JOIN (padrão)
      ✅ SQL Server - JOIN padrão
      ✅ SQLite - JOIN simples
      ✅ Oracle - converte JOIN em subquery EXISTS no WHERE

    Args:
        db_type: Tipo do banco ('postgresql', 'mysql', 'oracle', etc.)
        base_table: Nome da tabela principal
        joins: Dicionário de definições de join
        table_list: Lista de tabelas adicionais (opcional)
        is_delete: Se for True, usa sintaxe DELETE adaptada
    """
    join_sql = ""
    db_type = db_type.lower()
    print("saffsdf: ", db_type)
    # === 1️⃣ ORACLE: DELETE não aceita JOIN, usa EXISTS ===
    if is_delete and "oracle" in db_type and joins:
        where_parts = []
        for table_name, join in joins.items():
            conds = []
            for cond in join.conditions:
                left = quote_identifier(db_type, cond.leftColumn)

                # ✅ Suporte a colunas e valores fixos
                if cond.useValue:
                    right_value = _map_column_type(cond.valueColumnType.lower())(
                        cond.rightValue
                    )
                    right = _convert_column_type_for_string_one(
                        right_value, cond.valueColumnType.lower()
                    )
                else:
                    right = quote_identifier(db_type, cond.rightColumn)

                cond_sql = f"{left} {cond.operator} {right}"
                conds.append(cond_sql)

            where_sql = " AND ".join(conds)
            alias = f" {quote_identifier(db_type, join.alias)}" if join.alias else ""
            where_parts.append(
                f"EXISTS (SELECT 1 FROM {quote_identifier(db_type, table_name)}{alias} WHERE {where_sql})"
            )

        return " WHERE " + " AND ".join(where_parts)
    print("saffsdf: ", db_type)
    # === 2️⃣ PostgreSQL: usa USING ===
    if is_delete and db_type in ["postgres", "postgresql"]:
        join_parts = []
        if joins:
            for table_name in joins.keys():
                join_parts.append(f"USING {quote_identifier(db_type, table_name)}")
        elif table_list:
            join_parts = [
                f"USING {quote_identifier(db_type, t)}"
                for t in table_list
                if t != base_table
            ]
        return " " + " ".join(join_parts) if join_parts else ""

    # === 3️⃣ MySQL, SQL Server, SQLite: JOIN padrão ===
    if joins:
        join_parts = []
        for table_name, join in joins.items():
            conds = []
            for cond in join.conditions:
                left = quote_identifier(db_type, cond.leftColumn)

                if cond.useValue:
                    # ✅ Suporte seguro a valores literais
                    right_value = _map_column_type(cond.valueColumnType.lower())(
                        cond.rightValue
                    )
                    right = _convert_column_type_for_string_one(
                        right_value, cond.valueColumnType.lower()
                    )
                else:
                    right = quote_identifier(db_type, cond.rightColumn)

                conds.append(f"{left} {cond.operator} {right}")

            on_clause = " AND ".join(conds)
            table_ref = quote_identifier(db_type, table_name)
            if join.alias:
                table_ref += f" AS {quote_identifier(db_type, join.alias)}"

            join_parts.append(f"{join.typeJoin} {table_ref} ON {on_clause}")

        join_sql = " " + " ".join(join_parts)

    elif table_list:
        join_tables = [
            quote_identifier(db_type, t) for t in table_list if t != base_table
        ]
        join_sql = ", " + ", ".join(join_tables) if join_tables else ""

    return join_sql
