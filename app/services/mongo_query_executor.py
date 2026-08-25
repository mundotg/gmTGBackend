"""
Executor de leitura para conexões MongoDB.

O pipeline de queries (`query_service` → `query_executor_sse`) é feito para
SQL: constrói uma string SQL e chama `engine.connect()`. Num MongoClient isso
rebenta com "'Database' object is not callable" (ver `assert_sql_engine`).

Este módulo dá o equivalente NoSQL: converte o mesmo `QueryPayload` (baseTable,
select, where, orderBy, limit, offset, isCountQuery) num `find`/`count_documents`
do pymongo, devolvendo o MESMO formato que o executor SQL — `(preview_rows,
colunas)` ou `(count, ["count"])` — para reutilizar cache, histórico e a
serialização SSE sem mudar mais nada.

Reutiliza `is_mongo` / `get_mongo_database` de `conect_database`.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from pymongo import ASCENDING, DESCENDING, MongoClient

try:  # bson vem com o pymongo
    from bson import ObjectId
    from bson.errors import InvalidId
except Exception:  # pragma: no cover - pymongo sempre traz bson
    ObjectId = None  # type: ignore
    InvalidId = Exception  # type: ignore

from app.models.connection_models import DBConnection
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.ultils.conect_database import get_mongo_database
from app.ultils.logger import log_message

DEFAULT_LIMIT = 100
MAX_PREVIEW_ROWS = 500


# ─────────────────────────── helpers ───────────────────────────
def _leaf(name: str) -> str:
    """Último segmento de uma referência (`db.coll.campo` → `campo`)."""
    return str(name).split(".")[-1] if name else name


def _resolve_db_and_collection(
    connection: DBConnection, base_table: str
) -> Tuple[Optional[str], str]:
    """
    Descobre (base_de_dados, coleção) a partir do `baseTable`.

    `notificacao_db.mensagensEnviadas` → ("notificacao_db", "mensagensEnviadas").
    Sem prefixo, a base vem da própria conexão.
    """
    if base_table and "." in base_table:
        db_name, collection = base_table.split(".", 1)
        return db_name, collection
    return getattr(connection, "database_name", None), base_table


def _coerce_value(value: Any, value_type: str, field: str) -> Any:
    """Converte o valor (que chega como string) para o tipo adequado no Mongo."""
    if value is None:
        return None

    # _id costuma ser ObjectId
    if field == "_id" and ObjectId is not None and isinstance(value, str):
        try:
            return ObjectId(value)
        except (InvalidId, Exception):  # noqa: BLE001 - valor pode não ser ObjectId
            return value

    if value_type == "number":
        try:
            f = float(value)
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return value

    if value_type == "boolean":
        return str(value).strip().lower() in ("true", "1", "yes", "t", "sim")

    return value


def _sql_like_to_regex(pattern: str) -> str:
    """Converte um padrão SQL LIKE (`%`, `_`) para regex."""
    import re

    escaped = re.escape(str(pattern))
    # re.escape transforma % e _ em literais; repõe a semântica do LIKE
    escaped = escaped.replace(r"\%", ".*").replace(r"\_", ".")
    return f"^{escaped}$"


def _condition_to_mongo(cond: Any) -> Optional[Dict[str, Any]]:
    """Traduz uma `CondicaoFiltro` para um filtro Mongo `{campo: expr}`."""
    field = _leaf(getattr(cond, "column", "") or "")
    if not field:
        return None

    op = (getattr(cond, "operator", "") or "=").strip().upper()
    vtype = getattr(cond, "value_type", "string") or "string"
    raw = getattr(cond, "value", None)
    val = _coerce_value(raw, vtype, field)

    if op in ("=", "==", "EQ", "IGUAL", "IS"):
        return {field: val}
    if op in ("!=", "<>", "NEQ", "DIFERENTE"):
        return {field: {"$ne": val}}
    if op in (">", "GT"):
        return {field: {"$gt": val}}
    if op in (">=", "GTE"):
        return {field: {"$gte": val}}
    if op in ("<", "LT"):
        return {field: {"$lt": val}}
    if op in ("<=", "LTE"):
        return {field: {"$lte": val}}
    if op in ("LIKE", "ILIKE", "CONTAINS", "CONTÉM"):
        return {field: {"$regex": _sql_like_to_regex(raw), "$options": "i"}}
    if op in ("STARTSWITH", "COMECA", "COMEÇA"):
        import re

        return {field: {"$regex": f"^{re.escape(str(raw))}", "$options": "i"}}
    if op in ("ENDSWITH", "TERMINA"):
        import re

        return {field: {"$regex": f"{re.escape(str(raw))}$", "$options": "i"}}
    if op in ("IN", "EM"):
        items = [
            _coerce_value(v.strip(), vtype, field)
            for v in str(raw).split(",")
            if v.strip() != ""
        ]
        return {field: {"$in": items}}
    if op in ("NOT IN", "NOT_IN", "NIN"):
        items = [
            _coerce_value(v.strip(), vtype, field)
            for v in str(raw).split(",")
            if v.strip() != ""
        ]
        return {field: {"$nin": items}}
    if op in ("IS NULL", "ISNULL", "NULO"):
        return {field: None}
    if op in ("IS NOT NULL", "NOTNULL", "NAO NULO", "NÃO NULO"):
        return {field: {"$ne": None}}
    if op in ("BETWEEN", "ENTRE"):
        v2 = _coerce_value(getattr(cond, "value2", None), vtype, field)
        return {field: {"$gte": val, "$lte": v2}}

    # Operador desconhecido → igualdade simples (comportamento seguro).
    return {field: val}


def _build_mongo_filter(where: Optional[List[Any]]) -> Dict[str, Any]:
    """
    Constrói o filtro Mongo a partir das condições.

    Suporta AND (junção de cláusulas) e OR (`$or`). Combinações mistas AND/OR
    são simplificadas: se existir algum OR, todas as cláusulas entram num
    `$or`; caso contrário juntam-se com AND. Chega para o caso comum de browse
    (0–2 filtros); filtros complexos podem ser refinados depois.
    """
    if not where:
        return {}

    clauses: List[Dict[str, Any]] = []
    has_or = False
    for i, cond in enumerate(where):
        mongo = _condition_to_mongo(cond)
        if mongo is None:
            continue
        if i > 0 and (getattr(cond, "logicalOperator", "AND") or "AND").upper() == "OR":
            has_or = True
        clauses.append(mongo)

    if not clauses:
        return {}
    if len(clauses) == 1:
        return clauses[0]
    if has_or:
        log_message(
            "ℹ️ Filtro Mongo com OR: cláusulas combinadas em $or (lógica mista simplificada).",
            "info",
        )
        return {"$or": clauses}
    return {"$and": clauses}


def _build_projection(payload: QueryPayload) -> Optional[Dict[str, int]]:
    """Projeção a partir do `select` (nomes de campo = último segmento)."""
    select = payload.select or []
    if not select:
        return None
    proj: Dict[str, int] = {}
    for col in select:
        proj[_leaf(col)] = 1
    return proj or None


def _build_sort(payload: QueryPayload) -> Optional[List[Tuple[str, int]]]:
    if not payload.orderBy:
        return None
    sort: List[Tuple[str, int]] = []
    for o in payload.orderBy:
        field = _leaf(getattr(o, "column", "") or "")
        if not field:
            continue
        direction = (getattr(o, "direction", "asc") or "asc").lower()
        sort.append((field, DESCENDING if direction.startswith("desc") else ASCENDING))
    return sort or None


def _jsonable(value: Any) -> Any:
    """Torna valores do Mongo serializáveis (ObjectId, datas, aninhados)."""
    if ObjectId is not None and isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            return str(value)
    return value


def _output_columns(payload: QueryPayload, docs_sample: List[Dict[str, Any]]) -> List[str]:
    """
    Colunas de saída — alinhadas com o que o frontend espera.

    Prioridade: chaves de `aliaisTables` (mesmo formato do caminho SQL) →
    `select` → chaves reais dos documentos.
    """
    if payload.aliaisTables:
        return list(payload.aliaisTables.keys())
    if payload.select:
        return list(payload.select)
    keys: List[str] = []
    for d in docs_sample:
        for k in d.keys():
            if k not in keys:
                keys.append(k)
    return keys


def describe_mongo_query(connection: DBConnection, payload: QueryPayload) -> str:
    """
    Representação textual estável do `find` — serve de chave de cache e de
    registo no histórico (equivalente à string SQL do caminho relacional).
    """
    db_name, collection = _resolve_db_and_collection(connection, payload.baseTable)
    descriptor = {
        "engine": "mongodb",
        "db": db_name,
        "collection": collection,
        "filter": _build_mongo_filter(payload.where),
        "projection": _build_projection(payload),
        "sort": _build_sort(payload),
        "limit": payload.limit,
        "offset": payload.offset,
        "count": payload.isCountQuery,
    }
    return json.dumps(descriptor, default=str, ensure_ascii=False, sort_keys=True)


async def run_mongo_query(
    engine: MongoClient,
    connection: DBConnection,
    payload: QueryPayload,
) -> Tuple[Any, List[str]]:
    """
    Executa o `QueryPayload` como um `find`/`count_documents` no MongoDB.

    Devolve `(count, ["count"])` para contagens ou `(preview_rows, colunas)`
    para leituras — o mesmo contrato do executor SQL.
    """
    db_name, collection_name = _resolve_db_and_collection(connection, payload.baseTable)

    database = engine[db_name] if db_name else get_mongo_database(engine)
    if database is None:
        raise RuntimeError("Não foi possível determinar a base de dados MongoDB.")

    collection = database[collection_name]
    mongo_filter = _build_mongo_filter(payload.where)

    # Contagem
    if payload.isCountQuery:
        total = collection.count_documents(mongo_filter)
        return int(total), ["count"]

    projection = _build_projection(payload)
    sort = _build_sort(payload)
    limit = payload.limit if payload.limit and payload.limit > 0 else DEFAULT_LIMIT
    limit = min(limit, MAX_PREVIEW_ROWS)
    offset = payload.offset or 0

    cursor = collection.find(mongo_filter, projection)
    if sort:
        cursor = cursor.sort(sort)
    if offset:
        cursor = cursor.skip(offset)
    cursor = cursor.limit(limit)

    docs = [dict(d) for d in cursor]

    columns = _output_columns(payload, docs)

    # Alinha cada linha com as colunas de saída. Se as colunas forem
    # qualificadas (db.coll.campo, do aliaisTables), mapeia pelo último
    # segmento para o valor real do documento.
    preview_rows: List[Dict[str, Any]] = []
    for doc in docs:
        jdoc = _jsonable(doc)
        row: Dict[str, Any] = {}
        for col in columns:
            leaf = _leaf(col)
            row[col] = jdoc.get(leaf, jdoc.get(col))
        preview_rows.append(row)

    log_message(
        f"✅ Mongo find: {collection_name} → {len(preview_rows)} doc(s) "
        f"(filtro={'sim' if mongo_filter else 'não'})",
        "info",
    )
    return preview_rows, columns
