"""
Executor de leitura para conexões MongoDB.

O pipeline de queries (`query_service` → `query_executor_sse`) é feito para
SQL: constrói uma string SQL e chama `engine.connect()`. Num MongoClient isso
rebenta com "'Database' object is not callable" (ver `assert_sql_engine`).

Este módulo dá o equivalente NoSQL: converte o mesmo `QueryPayload` (baseTable,
select, joins, where, orderBy, limit, offset, isCountQuery) num `find`/`count_documents`
ou `aggregate` ($lookup) do pymongo, devolvendo o MESMO formato que o executor SQL —
`(preview_rows, colunas)` ou `(count, ["count"])` — para reutilizar cache, histórico
e a serialização SSE sem mudar mais nada.

Reutiliza `is_mongo` / `get_mongo_database` de `conect_database`.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from pymongo import ASCENDING, DESCENDING, MongoClient

try:  # bson vem com o pymongo
    from bson import ObjectId
    from bson.errors import InvalidId
    from bson.dbref import DBRef
except Exception:  # pragma: no cover - pymongo sempre traz bson
    ObjectId = None  # type: ignore
    InvalidId = Exception  # type: ignore
    DBRef = None  # type: ignore

from app.models.connection_models import DBConnection
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.ultils.conect_database import get_mongo_database
from app.ultils.logger import log_message

DEFAULT_LIMIT = 50000
MAX_PREVIEW_ROWS = 50000


# ─────────────────────────── helpers ───────────────────────────
def _leaf(name: str) -> str:
    """Último segmento de uma referência (`db.coll.campo` → `campo`)."""
    return str(name).strip('"`[]').split(".")[-1] if name else name


def _bare_table(name: str) -> str:
    """Nome da tabela/coleção sem prefixo de base (`db.coll` → `coll`)."""
    return str(name).strip('"`[]').split(".")[-1] if name else name


def _parse_col_ref(col_str: str, default_table: str = "") -> Tuple[str, str]:
    """
    Divide 'mensagens.salaId' em ('mensagens', 'salaId').
    'chat_db.mensagens.salaId' -> ('mensagens', 'salaId').
    'salaId' -> (default_table, 'salaId').
    """
    if not col_str:
        return default_table, ""
    parts = col_str.strip('"`[]').split(".")
    if len(parts) >= 2:
        return _bare_table(parts[-2]), parts[-1]
    return default_table, parts[0]


def _resolve_db_and_collection(
    connection: DBConnection, base_table: str
) -> Tuple[Optional[str], str]:
    """
    Descobre (base_de_dados, coleção) a partir do `baseTable`.

    `notificacao_db.mensagensEnviadas` → ("notificacao_db", "mensagensEnviadas").
    Sem prefixo, a base vem da própria conexão.
    """
    clean = base_table.strip('"`[]') if base_table else ""
    if clean and "." in clean:
        db_name, collection = clean.split(".", 1)
        return db_name, collection
    return getattr(connection, "database_name", None), clean


def _coerce_value(value: Any, value_type: str, field: str) -> Any:
    """Converte o valor (que chega como string) para o tipo adequado no Mongo."""
    if value is None:
        return None

    # _id costuma ser ObjectId se for hex de 24 caracteres
    leaf_f = _leaf(field)
    if leaf_f == "_id" and ObjectId is not None and isinstance(value, str):
        try:
            return ObjectId(value)
        except (InvalidId, Exception):  # noqa: BLE001 - valor pode ser UUID ou string
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
    escaped = re.escape(str(pattern))
    escaped = escaped.replace(r"\%", ".*").replace(r"\_", ".")
    return f"^{escaped}$"


def _condition_to_mongo(
    cond: Any, is_joined: bool = False, base_table: str = ""
) -> Optional[Dict[str, Any]]:
    """
    Traduz uma `CondicaoFiltro` para um filtro Mongo `{campo: expr}`.
    Se `is_joined` for True, qualifica o campo com o nome da tabela (`tabela.campo`).
    """
    raw_col = getattr(cond, "column", "") or ""
    col_leaf = _leaf(raw_col)
    if not col_leaf:
        return None

    if is_joined:
        tbl_fil = _bare_table(getattr(cond, "table_name_fil", "") or "")
        if not tbl_fil:
            tbl_fil, _ = _parse_col_ref(raw_col, base_table)
        field = f"{tbl_fil}.{col_leaf}" if tbl_fil else col_leaf
    else:
        field = col_leaf

    op = (getattr(cond, "operator", "") or "=").strip().upper()
    vtype = getattr(cond, "value_type", "string") or "string"
    raw = getattr(cond, "value", None)
    val = _coerce_value(raw, vtype, field)

    # Suporte flexível para _id: se for 24-hex ObjectId, aceita tanto ObjectId como String literal
    if col_leaf == "_id" and ObjectId is not None and isinstance(raw, str) and len(raw) == 24:
        try:
            oid = ObjectId(raw)
            if op in ("=", "==", "EQ", "IGUAL", "IS"):
                return {field: {"$in": [oid, raw]}}
            if op in ("!=", "<>", "NEQ", "DIFERENTE"):
                return {field: {"$nin": [oid, raw]}}
        except Exception:
            pass

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
        return {field: {"$regex": f"^{re.escape(str(raw))}", "$options": "i"}}
    if op in ("ENDSWITH", "TERMINA"):
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

    return {field: val}


def _build_mongo_filter(
    where: Optional[List[Any]], is_joined: bool = False, base_table: str = ""
) -> Dict[str, Any]:
    """Constrói o filtro Mongo a partir das condições."""
    if not where:
        return {}

    clauses: List[Dict[str, Any]] = []
    has_or = False
    for i, cond in enumerate(where):
        mongo = _condition_to_mongo(cond, is_joined=is_joined, base_table=base_table)
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
    """Torna valores do Mongo serializáveis (ObjectId, DBRef, datas, aninhados)."""
    if ObjectId is not None and isinstance(value, ObjectId):
        return str(value)
    if DBRef is not None and isinstance(value, DBRef):
        return str(value.id)
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


def _has_joins(payload: QueryPayload) -> bool:
    return bool(payload.joins and len(payload.joins) > 0)


def build_mongo_pipeline(
    payload: QueryPayload, base_collection: str
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Constrói o pipeline de agregação para queries com JOINs no MongoDB.
    Retorna (colecao_principal, pipeline).
    """
    joins = payload.joins or {}
    base_clean = _bare_table(base_collection)

    join_items = list(joins.items())
    first_tbl_raw, first_join = join_items[0]
    first_tbl = _bare_table(first_tbl_raw)
    first_type = (getattr(first_join, "typeJoin", "") or "").upper()

    pipeline: List[Dict[str, Any]] = []

    if len(join_items) == 1 and "RIGHT" in first_type:
        # A RIGHT JOIN B: a coleção de condução é B (first_tbl), preservando todos os seus registros
        driving_collection = first_tbl
        other_collection = base_clean

        pipeline.append({"$project": {driving_collection: "$$ROOT"}})

        conds = getattr(first_join, "conditions", []) or []
        if conds:
            cond = conds[0]
            t1, f1 = _parse_col_ref(getattr(cond, "leftColumn", ""), other_collection)
            t2, f2 = _parse_col_ref(getattr(cond, "rightColumn", ""), driving_collection)

            if t1 == driving_collection:
                local_f = f"{driving_collection}.{f1}"
                foreign_f = f2
            else:
                local_f = f"{driving_collection}.{f2}"
                foreign_f = f1

            pipeline.append({
                "$lookup": {
                    "from": other_collection,
                    "localField": local_f,
                    "foreignField": foreign_f,
                    "as": other_collection,
                }
            })
            pipeline.append({
                "$unwind": {
                    "path": f"${other_collection}",
                    "preserveNullAndEmptyArrays": True,
                }
            })
    else:
        # LEFT JOIN ou INNER JOIN: a coleção de condução é a base_collection
        driving_collection = base_clean
        pipeline.append({"$project": {driving_collection: "$$ROOT"}})

        for tbl_name_raw, join_opt in join_items:
            join_tbl = _bare_table(tbl_name_raw)
            jtype = (getattr(join_opt, "typeJoin", "") or "").upper()
            is_preserve = "INNER" not in jtype

            conds = getattr(join_opt, "conditions", []) or []
            if conds:
                cond = conds[0]
                t1, f1 = _parse_col_ref(getattr(cond, "leftColumn", ""), driving_collection)
                t2, f2 = _parse_col_ref(getattr(cond, "rightColumn", ""), join_tbl)

                if t2 == join_tbl:
                    local_f = f"{t1}.{f1}" if t1 else f"{driving_collection}.{f1}"
                    foreign_f = f2
                else:
                    local_f = f"{t2}.{f2}" if t2 else f"{driving_collection}.{f2}"
                    foreign_f = f1

                pipeline.append({
                    "$lookup": {
                        "from": join_tbl,
                        "localField": local_f,
                        "foreignField": foreign_f,
                        "as": join_tbl,
                    }
                })
                pipeline.append({
                    "$unwind": {
                        "path": f"${join_tbl}",
                        "preserveNullAndEmptyArrays": is_preserve,
                    }
                })

    # Filtro WHERE (aplicado sobre os campos com namespace tabela.coluna)
    mongo_filter = _build_mongo_filter(payload.where, is_joined=True, base_table=driving_collection)
    if mongo_filter:
        pipeline.append({"$match": mongo_filter})

    # Contagem
    if payload.isCountQuery:
        pipeline.append({"$count": "count"})
        return driving_collection, pipeline

    # Ordenação
    if payload.orderBy:
        sort_spec: Dict[str, int] = {}
        for o in payload.orderBy:
            col = getattr(o, "column", "") or ""
            t, c = _parse_col_ref(col, driving_collection)
            sort_field = f"{t}.{c}" if t else c
            direction = (
                DESCENDING
                if (getattr(o, "direction", "asc") or "asc").lower().startswith("desc")
                else ASCENDING
            )
            sort_spec[sort_field] = direction
        if sort_spec:
            pipeline.append({"$sort": sort_spec})

    # Paginação
    offset = payload.offset or 0
    if offset > 0:
        pipeline.append({"$skip": offset})

    limit = payload.limit if payload.limit and payload.limit > 0 else DEFAULT_LIMIT
    limit = min(limit, MAX_PREVIEW_ROWS)
    pipeline.append({"$limit": limit})

    return driving_collection, pipeline


def _extract_row_value(doc: Dict[str, Any], col: str, is_joined: bool) -> Any:
    """Extrai o valor de um campo para a coluna de saída, respeitando namespaces."""
    if is_joined:
        t, f = _parse_col_ref(col)
        if t and t in doc and isinstance(doc[t], dict):
            return doc[t].get(f)
        if col in doc:
            return doc[col]
        leaf = _leaf(col)
        if leaf in doc:
            return doc[leaf]
        return None

    leaf = _leaf(col)
    return doc.get(leaf, doc.get(col))


def _output_columns(
    payload: QueryPayload, docs_sample: List[Dict[str, Any]], is_joined: bool = False
) -> List[str]:
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
        for k, v in d.items():
            if k == "_id" and is_joined:
                continue
            if is_joined and isinstance(v, dict):
                for subk in v.keys():
                    full_col = f"{k}.{subk}"
                    if full_col not in keys:
                        keys.append(full_col)
            else:
                if k not in keys:
                    keys.append(k)
    return keys


def describe_mongo_query(connection: DBConnection, payload: QueryPayload) -> str:
    """
    Representação textual estável da consulta Mongo — serve de chave de cache e
    de registo no histórico (equivalente à string SQL do caminho relacional).
    """
    db_name, collection_name = _resolve_db_and_collection(connection, payload.baseTable)
    if _has_joins(payload):
        driving_coll, pipeline = build_mongo_pipeline(payload, collection_name)
        descriptor = {
            "engine": "mongodb",
            "db": db_name,
            "collection": driving_coll,
            "pipeline": pipeline,
        }
    else:
        descriptor = {
            "engine": "mongodb",
            "db": db_name,
            "collection": collection_name,
            "filter": _build_mongo_filter(payload.where, is_joined=False, base_table=collection_name),
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
    Executa o `QueryPayload` no MongoDB (via find ou aggregate com $lookup).

    Devolve `(count, ["count"])` para contagens ou `(preview_rows, colunas)`
    para leituras — o mesmo contrato do executor SQL.
    """
    db_name, collection_name = _resolve_db_and_collection(connection, payload.baseTable)

    database = engine[db_name] if db_name else get_mongo_database(engine)
    if database is None:
        raise RuntimeError("Não foi possível determinar a base de dados MongoDB.")

    is_joined = _has_joins(payload)

    if is_joined:
        driving_coll_name, pipeline = build_mongo_pipeline(payload, collection_name)
        collection = database[driving_coll_name]

        if payload.isCountQuery:
            res = list(collection.aggregate(pipeline))
            total = res[0]["count"] if res and "count" in res[0] else 0
            return int(total), ["count"]

        raw_docs = list(collection.aggregate(pipeline))
        columns = _output_columns(payload, raw_docs, is_joined=True)

        preview_rows: List[Dict[str, Any]] = []
        for doc in raw_docs:
            row: Dict[str, Any] = {}
            for col in columns:
                val = _extract_row_value(doc, col, is_joined=True)
                row[col] = _jsonable(val)
            preview_rows.append(row)

        log_message(
            f"✅ Mongo aggregate (join): {driving_coll_name} → {len(preview_rows)} doc(s)",
            "info",
        )
        return preview_rows, columns

    # Caminho simples (sem joins)
    collection = database[collection_name]
    mongo_filter = _build_mongo_filter(payload.where, is_joined=False, base_table=collection_name)

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
    columns = _output_columns(payload, docs, is_joined=False)

    preview_rows: List[Dict[str, Any]] = []
    for doc in docs:
        row: Dict[str, Any] = {}
        for col in columns:
            val = _extract_row_value(doc, col, is_joined=False)
            row[col] = _jsonable(val)
        preview_rows.append(row)

    log_message(
        f"✅ Mongo find: {collection_name} → {len(preview_rows)} doc(s) "
        f"(filtro={'sim' if mongo_filter else 'não'})",
        "info",
    )
    return preview_rows, columns
