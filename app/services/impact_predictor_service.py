"""
Serviço de Previsão de Impacto e Dry-Run (Impact Predictor).

Inspirado no mecanismo de Dry-Run do Google BigQuery e adaptado para
bancos relacionais (PostgreSQL, MySQL, SQLite, Oracle, SQL Server) e NoSQL (MongoDB).

Avalia o custo, linhas estimadas, tipo de varredura (Index Scan vs Sequential/Collection Scan)
e identifica riscos de desempenho antes da execução real da query.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional
from sqlalchemy import text

from app.ultils.conect_database import is_mongo, get_mongo_database
from app.ultils.logger import log_message


def _clean_statement(query: str) -> str:
    """Remove comentários e espaços em branco excedentes."""
    q = re.sub(r"--.*$", "", query, flags=re.MULTILINE)
    q = re.sub(r"/\*.*?\*/", "", q, flags=re.DOTALL)
    return q.strip().rstrip(";")


def _detect_dml(query: str) -> Optional[str]:
    """Retorna INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE se for DML/DDL."""
    m = re.match(r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE)\b", query, re.IGNORECASE)
    return m.group(1).upper() if m else None


class ImpactPredictorService:
    @staticmethod
    async def predict_impact(
        engine: Any,
        connection: Any,
        query: str,
        table_hint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executa uma análise de Dry-Run contra o banco sem executar mutações.
        Retorna métricas de impacto, linhas estimadas, nível de risco e sugestões.
        """
        clean_q = _clean_statement(query)
        if not clean_q:
            return {
                "supported": False,
                "dialect": "unknown",
                "risk_level": "LOW",
                "risk_score": 0,
                "scan_type": "UNKNOWN",
                "table_name": None,
                "estimated_rows": 0,
                "estimated_cost": 0.0,
                "estimated_duration_ms": 0.0,
                "summary": "Consulta vazia.",
                "warning": None,
                "suggestions": [],
                "requires_confirmation": False,
                "raw_plan": None,
            }

        dml_type = _detect_dml(clean_q)
        if dml_type in ("DROP", "TRUNCATE"):
            return {
                "supported": True,
                "dialect": "sql",
                "risk_level": "CRITICAL",
                "risk_score": 100,
                "scan_type": "DESTRUCTIVE_DDL",
                "table_name": table_hint,
                "estimated_rows": 0,
                "estimated_cost": 9999.0,
                "estimated_duration_ms": None,
                "summary": f"Instrução {dml_type} é destrutiva e remove tabelas ou todos os registros permanentemente.",
                "warning": f"ATENÇÃO: {dml_type} causa perda irreversível de dados se executado.",
                "suggestions": ["Certifique-se de ter um backup recente antes de prosseguir."],
                "requires_confirmation": True,
                "raw_plan": None,
            }

        if dml_type in ("UPDATE", "DELETE"):
            has_where = bool(re.search(r"\bWHERE\b", clean_q, re.IGNORECASE))
            if not has_where:
                return {
                    "supported": True,
                    "dialect": "sql",
                    "risk_level": "CRITICAL",
                    "risk_score": 95,
                    "scan_type": "FULL_TABLE_MUTATION",
                    "table_name": table_hint,
                    "estimated_rows": 0,
                    "estimated_cost": 9000.0,
                    "estimated_duration_ms": None,
                    "summary": f"Comando {dml_type} sem cláusula WHERE afetará TODOS os registros da tabela!",
                    "warning": f"PERIGO: {dml_type} sem WHERE alterará/apagará toda a tabela indiscriminadamente.",
                    "suggestions": ["Adicione uma cláusula WHERE para restringir os registros afetados."],
                    "requires_confirmation": True,
                    "raw_plan": None,
                }

        # ── MongoDB ───────────────────────────────────────────────────────────
        if is_mongo(engine):
            return await ImpactPredictorService._predict_mongo(engine, connection, clean_q, table_hint)

        # ── Relacional SQL (PostgreSQL, MySQL, SQLite, etc.) ──────────────────
        return await ImpactPredictorService._predict_sql(engine, clean_q, table_hint)

    @staticmethod
    async def _predict_mongo(
        engine: Any,
        connection: Any,
        query: str,
        table_hint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Avalia impacto para MongoDB via explain('queryPlanner')."""
        try:
            # Detecta coleção e filtro
            coll_name = table_hint
            mongo_filter = {}

            # Suporta padrão db.collection.find(...)
            m = re.search(
                r"(?:db\.)?(?P<coll>[\w\.]+)\.(?P<op>find|aggregate|countDocuments|count)\s*\((?P<args>.*)\)\s*;?\s*$",
                query,
                re.IGNORECASE | re.DOTALL,
            )

            if m:
                coll_name = m.group("coll")
                if "." in coll_name:
                    coll_name = coll_name.split(".", 1)[1]
                args_str = m.group("args").strip()
                if args_str:
                    try:
                        # Tenta pegar o primeiro argumento (filtro)
                        if args_str.startswith("{"):
                            depth = 0
                            end_idx = len(args_str)
                            for idx, c in enumerate(args_str):
                                if c == "{":
                                    depth += 1
                                elif c == "}":
                                    depth -= 1
                                    if depth == 0:
                                        end_idx = idx + 1
                                        break
                            mongo_filter = json.loads(args_str[:end_idx])
                    except Exception:
                        mongo_filter = {}
            elif query.strip().startswith("{") and table_hint:
                try:
                    mongo_filter = json.loads(query.strip())
                except Exception:
                    mongo_filter = {}

            db_name = getattr(connection, "database_name", None)
            database = engine[db_name] if db_name else get_mongo_database(engine)
            if database is None or not coll_name:
                return {
                    "supported": False,
                    "dialect": "mongodb",
                    "risk_level": "LOW",
                    "risk_score": 10,
                    "scan_type": "UNKNOWN",
                    "table_name": coll_name,
                    "estimated_rows": 0,
                    "estimated_cost": 0.0,
                    "estimated_duration_ms": 1.0,
                    "summary": "Consulta MongoDB aceita.",
                    "warning": None,
                    "suggestions": [],
                    "requires_confirmation": False,
                    "raw_plan": None,
                }

            coll = database[coll_name]
            total_docs = coll.estimated_document_count()

            # Executa explain seguro
            plan = coll.find(mongo_filter).explain()
            winning_plan = plan.get("queryPlanner", {}).get("winningPlan", {})

            stages = []
            def _extract_stages(node):
                if not isinstance(node, dict):
                    return
                stage = node.get("stage")
                if stage:
                    stages.append(stage)
                for k in ("inputStage", "inputStages", "then", "else"):
                    child = node.get(k)
                    if isinstance(child, list):
                        for c in child:
                            _extract_stages(c)
                    elif isinstance(child, dict):
                        _extract_stages(child)

            _extract_stages(winning_plan)

            is_collscan = "COLLSCAN" in stages
            is_ixscan = "IXSCAN" in stages

            scan_type = "COLLECTION_SCAN" if is_collscan else ("INDEX_SCAN" if is_ixscan else "FILTERED_SCAN")

            # Avaliação de risco
            filter_keys = list(mongo_filter.keys())
            suggestions = []
            warning = None

            if is_collscan:
                if total_docs > 100_000:
                    risk_level = "CRITICAL"
                    risk_score = 90
                    warning = f"A coleção '{coll_name}' possui ~{total_docs:,} documentos e será varrida completamente (COLLSCAN)."
                elif total_docs > 20_000:
                    risk_level = "HIGH"
                    risk_score = 75
                    warning = f"Varredura completa (COLLSCAN) em ~{total_docs:,} documentos sem uso de índice."
                elif total_docs > 2_000:
                    risk_level = "MEDIUM"
                    risk_score = 50
                else:
                    risk_level = "LOW"
                    risk_score = 25

                if filter_keys:
                    idx_fields = ", ".join(f'"{k}": 1' for k in filter_keys)
                    suggestions.append(f"Crie um índice no MongoDB para acelerar: db.{coll_name}.createIndex({{{idx_fields}}})")
                else:
                    suggestions.append("A consulta não possui filtro WHERE/filtro e examinará toda a coleção.")
            else:
                risk_level = "LOW"
                risk_score = 15

            # Verifica LIMIT
            if not re.search(r"\.limit\(\s*\d+\s*\)", query) and total_docs > 1000:
                suggestions.append("Considere adicionar .limit(100) para limitar a quantidade de documentos retornados.")

            summary = (
                f"Consulta MongoDB em '{coll_name}': {scan_type} "
                f"({'com índice' if is_ixscan else 'SEM índice'}). Documentos estimados na coleção: ~{total_docs:,}."
            )

            return {
                "supported": True,
                "dialect": "mongodb",
                "risk_level": risk_level,
                "risk_score": risk_score,
                "scan_type": scan_type,
                "table_name": coll_name,
                "estimated_rows": total_docs if is_collscan else min(total_docs, 100),
                "estimated_cost": float(total_docs if is_collscan else 10),
                "estimated_duration_ms": round(total_docs * 0.005, 2) if is_collscan else 1.5,
                "summary": summary,
                "warning": warning,
                "suggestions": suggestions,
                "requires_confirmation": risk_level in ("HIGH", "CRITICAL"),
                "raw_plan": plan.get("queryPlanner"),
            }
        except Exception as e:
            log_message("WARNING", "impact_predictor", f"Falha no explain MongoDB: {e}")
            return {
                "supported": False,
                "dialect": "mongodb",
                "risk_level": "LOW",
                "risk_score": 10,
                "scan_type": "UNKNOWN",
                "table_name": table_hint,
                "estimated_rows": 0,
                "estimated_cost": 0.0,
                "estimated_duration_ms": 0.0,
                "summary": "MongoDB explain não pôde ser calculado.",
                "warning": None,
                "suggestions": [],
                "requires_confirmation": False,
                "raw_plan": None,
            }

    @staticmethod
    async def _predict_sql(
        engine: Any,
        query: str,
        table_hint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Avalia impacto para bancos SQL (PostgreSQL, MySQL, SQLite, etc.) via EXPLAIN."""
        dialect = getattr(getattr(engine, "dialect", None), "name", "sql").lower()

        # Extrai primeiro SELECT
        first_stmt = query
        for s in query.split(";"):
            s_clean = s.strip()
            if s_clean.upper().startswith("SELECT") or s_clean.upper().startswith("WITH"):
                first_stmt = s_clean
                break

        # Tabelas da query
        tables_found = re.findall(r"\b(?:FROM|JOIN)\s+([`\"\[]?[\w\.]+[`\"\]]?)", first_stmt, re.IGNORECASE)
        table_name = table_hint or (tables_found[0].strip('`"[]') if tables_found else "tabela")

        has_limit = bool(re.search(r"\bLIMIT\s+\d+\b", first_stmt, re.IGNORECASE))
        has_where = bool(re.search(r"\bWHERE\b", first_stmt, re.IGNORECASE))
        has_select_all = bool(re.search(r"\bSELECT\s+\*\s+FROM\b", first_stmt, re.IGNORECASE))

        # Tenta executar o EXPLAIN nativo do dialeto
        try:
            async with engine.connect() as conn:
                if dialect == "postgresql":
                    res = await conn.execute(text(f"EXPLAIN (FORMAT JSON) {first_stmt}"))
                    raw = res.scalar()
                    plan_data = raw[0] if isinstance(raw, list) and raw else (json.loads(raw)[0] if isinstance(raw, str) else raw)
                    return ImpactPredictorService._parse_postgres_plan(plan_data, table_name, has_limit, has_where, has_select_all)

                elif dialect in ("mysql", "mariadb"):
                    res = await conn.execute(text(f"EXPLAIN FORMAT=JSON {first_stmt}"))
                    raw = res.scalar()
                    plan_data = json.loads(raw) if isinstance(raw, str) else raw
                    return ImpactPredictorService._parse_mysql_plan(plan_data, table_name, has_limit, has_where, has_select_all)

                elif dialect == "sqlite":
                    res = await conn.execute(text(f"EXPLAIN QUERY PLAN {first_stmt}"))
                    rows = res.fetchall()
                    return ImpactPredictorService._parse_sqlite_plan(rows, table_name, has_limit, has_where, has_select_all)

                else:
                    # Fallback SQL genérico
                    res = await conn.execute(text(f"EXPLAIN {first_stmt}"))
                    rows = res.fetchall()
                    return ImpactPredictorService._parse_generic_plan(rows, dialect, table_name, has_limit, has_where)

        except Exception as e:
            log_message("INFO", "impact_predictor", f"EXPLAIN nativo ({dialect}) indisponível ou falhou: {e}")
            # Análise estática baseada em heurísticas se o EXPLAIN falhar
            return ImpactPredictorService._heuristic_analysis(first_stmt, dialect, table_name, has_limit, has_where, has_select_all)

    @staticmethod
    def _parse_postgres_plan(
        plan_dict: Dict[str, Any],
        table_name: str,
        has_limit: bool,
        has_where: bool,
        has_select_all: bool,
    ) -> Dict[str, Any]:
        root_plan = plan_dict.get("Plan", {}) if isinstance(plan_dict, dict) else {}

        node_types = []
        filter_cols = []
        max_rows = root_plan.get("Plan Rows", 0)
        total_cost = root_plan.get("Total Cost", 0.0)

        def _walk(node):
            nonlocal max_rows, total_cost
            if not isinstance(node, dict):
                return
            nt = node.get("Node Type", "")
            if nt:
                node_types.append(nt)
            rows = node.get("Plan Rows", 0)
            if rows > max_rows:
                max_rows = rows
            cost = node.get("Total Cost", 0.0)
            if cost > total_cost:
                total_cost = cost
            flt = node.get("Filter")
            if flt:
                filter_cols.append(str(flt))
            for child in node.get("Plans", []):
                _walk(child)

        _walk(root_plan)

        is_seq_scan = any("Seq Scan" in nt for nt in node_types)
        is_index_scan = any("Index" in nt for nt in node_types)

        scan_type = "SEQUENTIAL_SCAN" if is_seq_scan else ("INDEX_SCAN" if is_index_scan else "GENERIC_SCAN")

        suggestions = []
        warning = None

        if is_seq_scan:
            if max_rows > 100_000:
                risk_level = "CRITICAL"
                risk_score = 90
                warning = f"A consulta fará um Sequential Scan em ~{max_rows:,} linhas na tabela '{table_name}' com custo {total_cost:.1f}."
            elif max_rows > 20_000:
                risk_level = "HIGH"
                risk_score = 75
                warning = f"Sequential Scan detectado em ~{max_rows:,} linhas na tabela '{table_name}'."
            elif max_rows > 2_000:
                risk_level = "MEDIUM"
                risk_score = 50
            else:
                risk_level = "LOW"
                risk_score = 25

            if filter_cols:
                # Extrai colunas do filtro
                clean_cols = set(re.findall(r"\b([a-zA-Z_]\w*)\b", " ".join(filter_cols))) - {"and", "or", "not", "is", "null", "true", "false"}
                if clean_cols:
                    col_sample = list(clean_cols)[:2]
                    suggestions.append(f"Considere criar um índice: CREATE INDEX idx_{table_name}_{'_'.join(col_sample)} ON {table_name}({', '.join(col_sample)});")
            elif not has_where:
                suggestions.append("Falta cláusula WHERE: a tabela inteira será varrida do disco.")
        else:
            risk_level = "LOW"
            risk_score = 15

        if not has_limit and max_rows > 500:
            suggestions.append("Adicione LIMIT 1000 para limitar o tráfego de rede e a memória do cliente.")
        if has_select_all:
            suggestions.append("Evite 'SELECT *': especifique apenas as colunas que realmente precisa.")

        summary = (
            f"PostgreSQL Plan: {scan_type} em '{table_name}'. "
            f"Linhas estimadas: ~{max_rows:,} (Custo: {total_cost:.1f})."
        )

        return {
            "supported": True,
            "dialect": "postgresql",
            "risk_level": risk_level,
            "risk_score": risk_score,
            "scan_type": scan_type,
            "table_name": table_name,
            "estimated_rows": max_rows,
            "estimated_cost": total_cost,
            "estimated_duration_ms": round(max_rows * 0.003 + 2.0, 2),
            "summary": summary,
            "warning": warning,
            "suggestions": suggestions,
            "requires_confirmation": risk_level in ("HIGH", "CRITICAL"),
            "raw_plan": plan_dict,
        }

    @staticmethod
    def _parse_mysql_plan(
        plan_dict: Dict[str, Any],
        table_name: str,
        has_limit: bool,
        has_where: bool,
        has_select_all: bool,
    ) -> Dict[str, Any]:
        qb = plan_dict.get("query_block", {}) if isinstance(plan_dict, dict) else {}
        tbl = qb.get("table", {})

        access_type = tbl.get("access_type", "ALL")
        rows_examined = int(tbl.get("rows_examined_per_scan", 0) or 0)
        query_cost = float(qb.get("cost_info", {}).get("query_cost", 0.0) or 0.0)
        used_key = tbl.get("key")

        is_all_scan = access_type == "ALL"
        scan_type = "FULL_TABLE_SCAN" if is_all_scan else "INDEX_SCAN"

        suggestions = []
        warning = None

        if is_all_scan:
            if rows_examined > 100_000:
                risk_level = "CRITICAL"
                risk_score = 90
                warning = f"MySQL Full Table Scan (access_type=ALL) em ~{rows_examined:,} linhas examinadas."
            elif rows_examined > 20_000:
                risk_level = "HIGH"
                risk_score = 75
                warning = f"Full Table Scan em ~{rows_examined:,} linhas na tabela '{table_name}'."
            elif rows_examined > 2_000:
                risk_level = "MEDIUM"
                risk_score = 50
            else:
                risk_level = "LOW"
                risk_score = 25

            suggestions.append(f"A tabela '{table_name}' não usou índice (key=null). Crie um índice para evitar varredura ALL.")
        else:
            risk_level = "LOW"
            risk_score = 15

        if not has_limit and rows_examined > 500:
            suggestions.append("Adicione uma cláusula LIMIT para restringir os registros.")
        if has_select_all:
            suggestions.append("Evite 'SELECT *' para melhorar a performance de I/O.")

        summary = f"MySQL Plan: {scan_type} em '{table_name}'. Linhas examinadas: ~{rows_examined:,} (Custo: {query_cost:.1f})."

        return {
            "supported": True,
            "dialect": "mysql",
            "risk_level": risk_level,
            "risk_score": risk_score,
            "scan_type": scan_type,
            "table_name": table_name,
            "estimated_rows": rows_examined,
            "estimated_cost": query_cost,
            "estimated_duration_ms": round(rows_examined * 0.004 + 1.5, 2),
            "summary": summary,
            "warning": warning,
            "suggestions": suggestions,
            "requires_confirmation": risk_level in ("HIGH", "CRITICAL"),
            "raw_plan": plan_dict,
        }

    @staticmethod
    def _parse_sqlite_plan(
        rows: List[Any],
        table_name: str,
        has_limit: bool,
        has_where: bool,
        has_select_all: bool,
    ) -> Dict[str, Any]:
        details = [str(r._mapping.get("detail", "") if hasattr(r, "_mapping") else r[-1]) for r in rows]
        is_scan = any("SCAN TABLE" in d for d in details)
        is_index = any("USING INDEX" in d or "USING COVERING INDEX" in d or "USING INTEGER PRIMARY KEY" in d for d in details)

        scan_type = "FULL_TABLE_SCAN" if is_scan else ("INDEX_SCAN" if is_index else "GENERIC_SCAN")
        risk_level = "MEDIUM" if is_scan and not has_limit else "LOW"
        risk_score = 50 if is_scan else 15

        suggestions = []
        warning = None
        if is_scan:
            warning = f"SQLite realizará varredura completa (SCAN TABLE) em '{table_name}'."
            suggestions.append(f"Crie um índice na coluna filtrada da tabela '{table_name}'.")
        if not has_limit:
            suggestions.append("Adicione LIMIT 100 para limitar o retorno.")

        summary = f"SQLite Plan: {scan_type} em '{table_name}'."

        return {
            "supported": True,
            "dialect": "sqlite",
            "risk_level": risk_level,
            "risk_score": risk_score,
            "scan_type": scan_type,
            "table_name": table_name,
            "estimated_rows": 1000 if is_scan else 50,
            "estimated_cost": 100.0 if is_scan else 10.0,
            "estimated_duration_ms": 2.0,
            "summary": summary,
            "warning": warning,
            "suggestions": suggestions,
            "requires_confirmation": risk_level in ("HIGH", "CRITICAL"),
            "raw_plan": details,
        }

    @staticmethod
    def _parse_generic_plan(
        rows: List[Any],
        dialect: str,
        table_name: str,
        has_limit: bool,
        has_where: bool,
    ) -> Dict[str, Any]:
        text_plan = " ".join(str(r) for r in rows)
        is_scan = "SCAN" in text_plan.upper() and "INDEX" not in text_plan.upper()
        scan_type = "FULL_TABLE_SCAN" if is_scan else "INDEX_SCAN"
        risk_level = "MEDIUM" if is_scan else "LOW"

        return {
            "supported": True,
            "dialect": dialect,
            "risk_level": risk_level,
            "risk_score": 40 if is_scan else 15,
            "scan_type": scan_type,
            "table_name": table_name,
            "estimated_rows": 500,
            "estimated_cost": 50.0,
            "estimated_duration_ms": 2.0,
            "summary": f"{dialect.upper()} Plan: {scan_type} em '{table_name}'.",
            "warning": "Varredura de tabela detectada." if is_scan else None,
            "suggestions": ["Adicione cláusula WHERE e LIMIT para otimizar."] if is_scan else [],
            "requires_confirmation": False,
            "raw_plan": [str(r) for r in rows],
        }

    @staticmethod
    def _heuristic_analysis(
        query: str,
        dialect: str,
        table_name: str,
        has_limit: bool,
        has_where: bool,
        has_select_all: bool,
    ) -> Dict[str, Any]:
        """Análise estática inteligente quando o EXPLAIN não pode ser executado."""
        suggestions = []
        warning = None

        if not has_where:
            risk_level = "MEDIUM" if has_limit else "HIGH"
            risk_score = 65 if not has_limit else 35
            warning = f"A consulta em '{table_name}' não possui cláusula WHERE de filtro."
            suggestions.append("Adicione uma cláusula WHERE para filtrar os dados.")
        else:
            risk_level = "LOW"
            risk_score = 20

        if not has_limit:
            suggestions.append("Adicione uma cláusula LIMIT para restringir os resultados retornados.")
        if has_select_all:
            suggestions.append("Evite 'SELECT *': projete colunas específicas.")

        summary = f"Análise estática ({dialect}): {table_name} ({'sem filtro WHERE' if not has_where else 'com WHERE'})."

        return {
            "supported": True,
            "dialect": dialect,
            "risk_level": risk_level,
            "risk_score": risk_score,
            "scan_type": "UNKNOWN",
            "table_name": table_name,
            "estimated_rows": 1000 if not has_where else 100,
            "estimated_cost": 50.0,
            "estimated_duration_ms": 5.0,
            "summary": summary,
            "warning": warning,
            "suggestions": suggestions,
            "requires_confirmation": risk_level in ("HIGH", "CRITICAL"),
            "raw_plan": None,
        }
