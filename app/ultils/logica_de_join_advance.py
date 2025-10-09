from typing import Optional
from app.schemas.query_select_upAndInsert_schema import AdvancedJoinOption
from app.services.editar_linha import _convert_column_type_for_string_one, _map_column_type, quote_identifier


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
                    right = _convert_column_type_for_string_one(
                        _map_column_type(cond.valueColumnType.lower())(cond.rightValue),
                        cond.valueColumnType.lower()
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
