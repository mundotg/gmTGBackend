
import traceback
from typing import Any,  Dict, List, Tuple, Optional
from sqlalchemy import Result, text
from sqlalchemy.ext.asyncio import  AsyncEngine
from app.schemas.query_select_upAndInsert_schema import CondicaoFiltro
from app.ultils.build_query import get_filter_condition_with_operation

# --- dentro de QueryFilterBuilder ---

class QueryFilterBuilder:
    """Construtor de filtros SQL com parâmetros seguros."""

    @staticmethod
    def _normalize_column_ref(table_ref: str, column_ref: str) -> str:
        """
        Normaliza referências:
        - se column_ref já vier como "t.col" ou "schema.t.col", devolve como está
        - se vier só "col", vira "{table_ref}.col"
        """
        if not column_ref:
            return column_ref

        # já vem qualificado: "users.email" ou "public.users.email"
        if "." in column_ref:
            return column_ref

        # vem só "email" => usa table_ref (que pode ser "users" ou "public.users")
        return f"{table_ref}.{column_ref}"

    @staticmethod
    async def build_where_clause(
        conditions: List[CondicaoFiltro],
        db_type: str = "postgres"
    ) -> Tuple[str, Dict[str, Any]]:
        if not conditions:
            return "", {}

        where_clauses = []
        params: Dict[str, Any] = {}

        for i, condition in enumerate(conditions):
            # ✅ aqui é o fix: não colar sempre table + "." + column
            field = QueryFilterBuilder._normalize_column_ref(
                condition.table_name_fil,
                condition.column
            )

            param_prefix = f"param_{i}"

            sql_part = get_filter_condition_with_operation(
                col_name=field,
                col_type=condition.column_type,
                value=condition.value,
                params=params,
                db_type=db_type,
                operation=condition.operator,
                param_name=param_prefix,
                enum_values={},
                value_otheir_between=str(condition.value2),
            )

            logic = condition.logicalOperator or "AND"
            where_clauses.append((logic, sql_part))

        if not where_clauses:
            return "", {}

        where_sql = where_clauses[0][1]
        for logic, clause in where_clauses[1:]:
            where_sql += f" {logic} {clause}"

        return f"WHERE {where_sql}", params


class QueryExecutor:
    """Executor de queries SQL."""
    
    def __init__(self, engine: AsyncEngine, db_type: str):
        self.engine = engine
        self.db_type = db_type
        # Limite de linhas a manter no preview (evita OOM)
        self.MAX_PREVIEW_ROWS = 500
        # Tamanho do lote para fetchmany
        self.FETCH_BATCH_SIZE = 100
    
    async def execute_query(
            self,
            query_string: str,
            params: Optional[Dict[str, Any]] = None,
            is_count_query: bool = False,
            select_tables: List[str] = []
        ):
            """Executa a query SQL e retorna resultado e colunas."""
            colunas: List[str] = []
            try:
                async with self.engine.connect() as conn:
                    result: Result = await conn.execute(text(query_string), params or {})

                    if is_count_query:
                        count_result = result.scalar_one_or_none()
                        return count_result or 0, ["count"]

                    else:
                        preview_rows: List[Dict[str, Any]] = []
                        keys = list(result.keys())
                        colunas = keys

                        fetched = 0
                        while True:
                            batch = result.fetchmany(self.FETCH_BATCH_SIZE)
                            if not batch:
                                break

                            for row in batch:
                                if select_tables:
                                    # se select informado, usa ordem do select
                                    preview_rows.append(dict(zip(select_tables, row)))
                                else:
                                    preview_rows.append(dict(zip(keys, row)))

                                fetched += 1
                                if fetched >= self.MAX_PREVIEW_ROWS:
                                    break

                            if fetched >= self.MAX_PREVIEW_ROWS:
                                break

                        return preview_rows, colunas
            except Exception as e:
                raise Exception(f"Erro ao executar query: {e}{traceback.format_exc()}")

