"""
Backend do Editor SQL (`/sql-editor/...`).

O frontend em `app/home/editorsql` já existia mas apontava para rotas que NÃO
existiam (todas davam 404). Este módulo implementa-as, reutilizando a infra do
projeto:

- Execução: contra a conexão ATIVA do utilizador (`ConnectionManager`), com
  streaming SSE no mesmo formato que o editor consome (start / statement_start
  / columns / row / statement_complete / complete / limit / error).
- NoSQL: se a conexão ativa for MongoDB, aceita `db.coleção.find(...)`,
  `.aggregate([...])` e `.countDocuments(...)` (ver `_run_mongo_shell`).
- Persistência (guardar / histórico / métricas) em **Redis** (`app.config.redis`),
  por utilizador — com o mesmo padrão de fallback usado nos jobs de backup.
- Deteção de erros + sugestões (`/validate`) e **tempo real por WebSocket**
  (`/ws`): valida a query à medida que se escreve, com correção/sugestões.
- Autocomplete: o schema (tabelas/colunas) vem do cliente no pedido — assim não
  há introspeção cara por tecla; o cliente já tem esse metadata.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import decode_token
from app.config.redis import read_cache, write_cache
from app.database import get_db_async
from app.routes.connection_routes import get_current_user_id
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.services.mongo_query_executor import run_mongo_query
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.conect_database import is_mongo, get_mongo_database
from app.ultils.logger import log_message

router = APIRouter(prefix="/sql-editor", tags=["SQL Editor"])

MAX_ROWS = 2000
FETCH_BATCH = 200
HISTORY_MAX = 100
SAVED_MAX = 200

# Palavras-chave para lint/sugestões (espelho do que o editor conhece).
KEYWORDS = [
    "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "JOIN", "INNER",
    "LEFT", "RIGHT", "OUTER", "FULL", "CROSS", "ON", "AND", "OR", "NOT", "IN",
    "IS", "NULL", "GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "HAVING",
    "DISTINCT", "AS", "CREATE", "ALTER", "DROP", "TRUNCATE", "TABLE", "VIEW",
    "INDEX", "UNION", "ALL", "CASE", "WHEN", "THEN", "ELSE", "END", "BETWEEN",
    "LIKE", "ILIKE", "EXISTS", "COUNT", "SUM", "AVG", "MIN", "MAX", "COALESCE",
    "CAST", "RETURNING", "VALUES", "SET", "INTO", "ASC", "DESC", "USING",
]
_KEYWORDS_SET = {k for kw in KEYWORDS for k in kw.split()}


# ══════════════════════════ modelos ══════════════════════════
class ExecuteBody(BaseModel):
    query: str
    limit: Optional[int] = 1000
    offset: Optional[int] = 0
    stream: Optional[bool] = True


class SchemaTable(BaseModel):
    name: str
    columns: List[str] = []


class AnalyzeBody(BaseModel):
    query: str


class ValidateBody(BaseModel):
    query: str
    dialect: Optional[str] = None
    tables: Optional[List[SchemaTable]] = None


class SaveBody(BaseModel):
    name: str
    query: str


# ══════════════════════════ SSE helpers ══════════════════════════
def _sse(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str, ensure_ascii=False)}\n\n"


# ══════════════════════════ SQL parsing ══════════════════════════
def split_statements(sql: str) -> List[str]:
    """Divide em statements por `;`, respeitando strings, aspas e comentários."""
    stmts: List[str] = []
    buf: List[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        # comentário de linha
        if c == "-" and i + 1 < n and sql[i + 1] == "-":
            j = sql.find("\n", i)
            j = n if j == -1 else j
            buf.append(sql[i:j])
            i = j
            continue
        # comentário de bloco
        if c == "/" and i + 1 < n and sql[i + 1] == "*":
            j = sql.find("*/", i + 2)
            j = n if j == -1 else j + 2
            buf.append(sql[i:j])
            i = j
            continue
        # strings / identificadores citados
        if c in ("'", '"', "`"):
            j = i + 1
            while j < n:
                if sql[j] == "\\":
                    j += 2
                    continue
                if sql[j] == c:
                    break
                j += 1
            buf.append(sql[i : j + 1])
            i = j + 1
            continue
        if c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def statement_type(stmt: str) -> str:
    m = re.match(r"\s*(\w+)", stmt)
    return m.group(1).upper() if m else "UNKNOWN"


def extract_tables(stmt: str) -> List[str]:
    tables: List[str] = []
    for m in re.finditer(
        r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([`\"\[]?[\w\.]+[`\"\]]?)",
        stmt,
        re.IGNORECASE,
    ):
        t = m.group(1).strip('`"[]')
        if t and t.upper() not in _KEYWORDS_SET:
            tables.append(t)
    # preserva ordem, sem duplicados
    seen: set = set()
    return [t for t in tables if not (t in seen or seen.add(t))]


def analyze_statement(stmt: str) -> Dict[str, Any]:
    qtype = statement_type(stmt)
    upper = stmt.upper()
    risks: List[str] = []

    if qtype in ("DELETE", "UPDATE") and " WHERE " not in f" {upper} ":
        risks.append(f"{qtype} sem WHERE afeta TODAS as linhas.")
    if qtype in ("DROP", "TRUNCATE"):
        risks.append(f"{qtype} é destrutivo e irreversível.")
    if "SELECT *" in upper:
        risks.append("SELECT * pode trazer colunas a mais; prefira listar as necessárias.")

    complexity = (
        upper.count("JOIN")
        + upper.count("SELECT") - 1  # subqueries
        + upper.count("UNION")
        + (1 if "GROUP BY" in upper else 0)
        + (1 if "HAVING" in upper else 0)
    )

    return {
        "query": stmt,
        "queryType": qtype,
        "tables": extract_tables(stmt),
        "risk": {"safe": not risks, "risks": risks},
        "complexity": max(0, complexity),
    }


# ══════════════════════════ lint / sugestões ══════════════════════════
def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def validate_query(
    query: str, tables: Optional[List[SchemaTable]] = None
) -> Dict[str, Any]:
    """
    Lint leve + sugestões. NÃO executa nada — é seguro e barato (serve para o
    tempo real). Deteta: parênteses/aspas desequilibrados, palavras-chave com
    erros de escrita, tabelas/colunas fora do schema, e riscos (DELETE/UPDATE
    sem WHERE, DROP/TRUNCATE).
    """
    errors: List[Dict[str, Any]] = []
    suggestions: List[Dict[str, Any]] = []
    q = query or ""

    # parênteses
    depth = 0
    for ch in q:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                errors.append({"severity": "error", "message": "Parêntese ')' a mais."})
                depth = 0
    if depth > 0:
        errors.append({"severity": "error", "message": f"{depth} parêntese(s) '(' por fechar."})

    # aspas simples
    if q.count("'") % 2 != 0:
        errors.append({"severity": "error", "message": "Aspa simple (') por fechar na string."})

    known_tables = {t.name.lower(): t for t in (tables or [])}
    # também aceita o nome sem schema
    for t in tables or []:
        known_tables.setdefault(t.name.split(".")[-1].lower(), t)

    for stmt in split_statements(q):
        info = analyze_statement(stmt)
        for r in info["risk"]["risks"]:
            errors.append({"severity": "warning", "message": r})

        if info["queryType"] == "SELECT" and " FROM " not in f" {stmt.upper()} ":
            # SELECT expr sem FROM é válido nalguns SGBD; apenas aviso
            suggestions.append({"type": "hint", "message": "SELECT sem FROM — confirme se é intencional."})

        # tabelas fora do schema (se o cliente enviou schema)
        if tables:
            for tbl in info["tables"]:
                key = tbl.split(".")[-1].lower()
                if key not in known_tables:
                    near = _closest(key, list(known_tables.keys()))
                    msg = f"Tabela '{tbl}' não existe no schema."
                    if near:
                        msg += f" Quis dizer '{near}'?"
                    errors.append({"severity": "warning", "message": msg, "table": tbl, "suggestion": near})

    # palavras que PARECEM keywords mal escritas
    for word in set(re.findall(r"[A-Za-z_]{3,}", q)):
        wu = word.upper()
        if wu in _KEYWORDS_SET:
            continue
        near = _closest(wu, list(_KEYWORDS_SET), max_dist=2)
        if near and near != wu and _levenshtein(wu, near) == 1:
            suggestions.append(
                {"type": "keyword", "message": f"'{word}' — quis dizer '{near}'?", "replace": word, "with": near}
            )

    return {"valid": not any(e["severity"] == "error" for e in errors), "errors": errors, "suggestions": suggestions}


def _closest(word: str, options: List[str], max_dist: int = 3) -> Optional[str]:
    best, best_d = None, max_dist + 1
    for opt in options:
        d = _levenshtein(word, opt)
        if d < best_d:
            best, best_d = opt, d
    return best if best_d <= max_dist else None


def autocomplete(
    query: str, cursor: int, tables: Optional[List[SchemaTable]] = None
) -> List[Dict[str, str]]:
    """Sugestões no ponto do cursor: palavra-chave, tabela ou coluna."""
    prefix_match = re.search(r"([\w\.]*)$", (query or "")[: cursor if cursor is not None else len(query or "")])
    token = prefix_match.group(1) if prefix_match else ""
    items: List[Dict[str, str]] = []
    low = token.lower()

    # coluna qualificada: tabela.<prefixo>
    if "." in token:
        tname, _, col = token.rpartition(".")
        for t in tables or []:
            if t.name.lower() == tname.lower() or t.name.split(".")[-1].lower() == tname.lower():
                for c in t.columns:
                    if c.lower().startswith(col.lower()):
                        items.append({"label": c, "type": "column", "detail": t.name, "insert": c})
        return items[:50]

    for kw in KEYWORDS:
        if kw.lower().startswith(low):
            items.append({"label": kw, "type": "keyword", "detail": "keyword", "insert": kw})
    for t in tables or []:
        short = t.name.split(".")[-1]
        if short.lower().startswith(low) or t.name.lower().startswith(low):
            items.append({"label": t.name, "type": "table", "detail": "table", "insert": t.name})
        for c in t.columns:
            if c.lower().startswith(low) and low:
                items.append({"label": c, "type": "column", "detail": t.name, "insert": c})
    # keywords primeiro, depois tabelas, depois colunas
    order = {"keyword": 0, "table": 1, "column": 2}
    items.sort(key=lambda x: (order.get(x["type"], 9), x["label"].lower()))
    return items[:50]


# ══════════════════════════ Redis (por utilizador) ══════════════════════════
def _k_hist(uid: int) -> str:
    return f"sqleditor:hist:{uid}"


def _k_saved(uid: int) -> str:
    return f"sqleditor:saved:{uid}"


def _k_metrics(uid: int) -> str:
    return f"sqleditor:metrics:{uid}"


def _push_history(uid: int, query: str) -> None:
    hist = read_cache(_k_hist(uid)) or []
    hist.insert(0, {"query": query, "created_at": _now()})
    write_cache(_k_hist(uid), hist[:HISTORY_MAX])


def _bump_metric(uid: int, field: str, delta: int = 1) -> Dict[str, int]:
    m = read_cache(_k_metrics(uid)) or {"queriesExecuted": 0, "activeQueries": 0, "savedQueries": 0}
    m[field] = int(m.get(field, 0)) + delta
    write_cache(_k_metrics(uid), m)
    return m


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


# ══════════════════════════ execução Mongo (modo shell) ══════════════════════════
_MONGO_RE = re.compile(
    r"(?:db\.)?(?P<coll>[\w\.]+)\.(?P<op>find|aggregate|countDocuments|count)\s*\((?P<args>.*)\)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


async def _run_mongo_shell(engine, connection, query: str) -> AsyncGenerator[str, None]:
    m = _MONGO_RE.search(query.strip())
    if not m:
        yield _sse(
            "error",
            {"message": "Mongo: use `db.coleção.find({...})`, `.aggregate([...])` ou `.countDocuments({...})`."},
        )
        return

    coll_ref = m.group("coll")
    op = m.group("op").lower()
    args_raw = m.group("args").strip()

    # base de dados + coleção
    if "." in coll_ref:
        db_name, collection = coll_ref.split(".", 1)
    else:
        db_name, collection = getattr(connection, "database_name", None), coll_ref
    database = engine[db_name] if db_name else get_mongo_database(engine)
    if database is None:
        yield _sse("error", {"message": "Não foi possível determinar a base MongoDB."})
        return
    coll = database[collection]

    def _parse(a: str, default):
        a = a.strip()
        if not a:
            return default
        return json.loads(a)

    try:
        yield _sse("statement_start", {"type": op})
        if op in ("countdocuments", "count"):
            flt = _parse(args_raw, {})
            total = coll.count_documents(flt)
            yield _sse("columns", {"columns": ["count"]})
            yield _sse("row", {"row": {"count": total}})
            yield _sse("statement_complete", {"rows": 1})
        elif op == "aggregate":
            pipeline = _parse(args_raw, [])
            docs = list(coll.aggregate(pipeline))
            # emite colunas + linhas + statement_complete num único bloco SSE
            yield _run_docs(docs)
        else:  # find
            # find(<filter>, <projection>)
            parts = _split_top_level_args(args_raw)
            flt = _parse(parts[0], {}) if parts else {}
            proj = _parse(parts[1], None) if len(parts) > 1 else None
            docs = list(coll.find(flt, proj).limit(MAX_ROWS))
            cols = _collect_columns(docs)
            yield _sse("columns", {"columns": cols})
            for d in docs:
                yield _sse("row", {"row": _jsonable(d)})
            yield _sse("statement_complete", {"rows": len(docs)})
    except json.JSONDecodeError as e:
        yield _sse("error", {"message": f"Argumento Mongo inválido (JSON): {e}"})
        return
    except Exception as e:  # noqa: BLE001
        yield _sse("error", {"message": f"Erro Mongo: {e}"})
        return

    yield _sse("complete", {"ok": True})


def _split_top_level_args(s: str) -> List[str]:
    """Divide argumentos por vírgula no nível 0 (fora de {}, [], strings)."""
    out, buf, depth, i, n = [], [], 0, 0, len(s)
    quote = ""
    while i < n:
        c = s[i]
        if quote:
            buf.append(c)
            if c == quote:
                quote = ""
            i += 1
            continue
        if c in ("'", '"'):
            quote = c
            buf.append(c)
        elif c in "{[(":
            depth += 1
            buf.append(c)
        elif c in "}])":
            depth -= 1
            buf.append(c)
        elif c == "," and depth == 0:
            out.append("".join(buf))
            buf = []
        else:
            buf.append(c)
        i += 1
    if buf:
        out.append("".join(buf))
    return [a.strip() for a in out]


def _collect_columns(docs: List[dict]) -> List[str]:
    cols: List[str] = []
    for d in docs[:50]:
        for k in d.keys():
            if k not in cols:
                cols.append(k)
    return cols


def _jsonable(v: Any) -> Any:
    from datetime import date, datetime

    try:
        from bson import ObjectId  # type: ignore
    except Exception:  # pragma: no cover
        ObjectId = None  # type: ignore
    if ObjectId is not None and isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return v


def _run_docs(docs: List[dict]) -> str:
    # emite todos os docs de um aggregate como um único bloco de eventos
    cols = _collect_columns(docs)
    out = [_sse("columns", {"columns": cols})]
    for d in docs:
        out.append(_sse("row", {"row": _jsonable(d)}))
    out.append(_sse("statement_complete", {"rows": len(docs)}))
    return "".join(out)


# ══════════════════════════ execução SQL (raw, SSE) ══════════════════════════
async def _run_sql(engine, query: str, limit: int) -> AsyncGenerator[str, None]:
    statements = split_statements(query)
    total = len(statements)
    cap = min(limit or MAX_ROWS, MAX_ROWS)

    for idx, stmt in enumerate(statements, 1):
        qtype = statement_type(stmt)
        yield _sse("progress", {"current": idx, "total": total})
        yield _sse("statement_start", {"type": qtype})

        async with engine.connect() as conn:
            try:
                result = await conn.execute(text(stmt))

                if getattr(result, "returns_rows", False):
                    cols = list(result.keys())
                    yield _sse("columns", {"columns": cols})
                    fetched = 0
                    hit_limit = False
                    while True:
                        batch = result.fetchmany(FETCH_BATCH)
                        if not batch:
                            break
                        for row in batch:
                            yield _sse("row", {"row": dict(row._mapping)})
                            fetched += 1
                            if fetched >= cap:
                                hit_limit = True
                                break
                        if hit_limit:
                            break
                    yield _sse("statement_complete", {"rows": fetched, "type": qtype})
                    if hit_limit:
                        yield _sse("limit", {"cap": cap})
                else:
                    # DML/DDL — confirma a transação
                    await conn.commit()
                    yield _sse(
                        "statement_complete",
                        {"rows": getattr(result, "rowcount", 0) or 0, "type": qtype},
                    )
            except Exception as e:  # noqa: BLE001
                try:
                    await conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                yield _sse("error", {"message": str(e), "statement": stmt})
                return

    yield _sse("complete", {"ok": True})


# ══════════════════════════ rotas ══════════════════════════
@router.post("/execute")
async def execute(
    body: ExecuteBody,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    query = (body.query or "").strip()
    query_id = str(uuid.uuid4())

    async def gen() -> AsyncGenerator[str, None]:
        yield _sse("start", {"queryId": query_id})
        if not query:
            yield _sse("error", {"message": "Query vazia."})
            return
        try:
            engine, connection = await ConnectionManager.get_engine_async(db, user_id)
        except Exception as e:  # noqa: BLE001
            yield _sse("error", {"message": f"Sem conexão ativa: {e}"})
            return

        _push_history(user_id, query)
        _bump_metric(user_id, "queriesExecuted", 1)

        try:
            if is_mongo(engine):
                async for ev in _run_mongo_shell(engine, connection, query):
                    yield ev
            else:
                async for ev in _run_sql(engine, query, body.limit or 1000):
                    yield ev
        except Exception as e:  # noqa: BLE001
            log_message(f"[sql-editor] erro execução: {e}", "error")
            yield _sse("error", {"message": str(e)})

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"X-Query-Id": query_id, "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _ok(data: Any, message: str = "OK") -> JSONResponse:
    return JSONResponse({"success": True, "message": message, "data": data})


@router.post("/analyze")
async def analyze(body: AnalyzeBody, user_id: int = Depends(get_current_user_id)):
    stmts = split_statements(body.query or "")
    queries = [analyze_statement(s) for s in stmts]
    data = {
        "formatted": body.query,
        "queries": queries,
        "totalStatements": len(queries),
    }
    return _ok(data, "Análise concluída")


@router.post("/explain")
async def explain(
    body: AnalyzeBody,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    engine, connection = await ConnectionManager.get_engine_async(db, user_id)
    if is_mongo(engine):
        return _ok({"plan": [], "note": "EXPLAIN não suportado nesta forma para MongoDB."})
    stmt = split_statements(body.query or "")
    if not stmt:
        return _ok({"plan": []})
    try:
        async with engine.connect() as conn:
            res = await conn.execute(text(f"EXPLAIN {stmt[0]}"))
            plan = [dict(r._mapping) for r in res.fetchall()]
        return _ok({"plan": plan})
    except Exception as e:  # noqa: BLE001
        return JSONResponse(
            status_code=400, content={"success": False, "message": str(e), "data": None}
        )


@router.post("/validate")
async def validate(body: ValidateBody, user_id: int = Depends(get_current_user_id)):
    return _ok(validate_query(body.query, body.tables), "Validado")


@router.post("/save")
async def save(body: SaveBody, user_id: int = Depends(get_current_user_id)):
    saved = read_cache(_k_saved(user_id)) or []
    item = {"id": str(uuid.uuid4()), "name": body.name, "query": body.query, "created_at": _now()}
    saved.insert(0, item)
    write_cache(_k_saved(user_id), saved[:SAVED_MAX])
    _bump_metric(user_id, "savedQueries", 0)  # recalculado abaixo
    m = read_cache(_k_metrics(user_id)) or {}
    m["savedQueries"] = len(saved[:SAVED_MAX])
    write_cache(_k_metrics(user_id), m)
    return _ok(item, "Query guardada")


@router.get("/saved")
async def list_saved(user_id: int = Depends(get_current_user_id)):
    return _ok(read_cache(_k_saved(user_id)) or [])


@router.get("/history")
async def list_history(user_id: int = Depends(get_current_user_id)):
    return _ok(read_cache(_k_hist(user_id)) or [])


@router.get("/metrics")
async def metrics(user_id: int = Depends(get_current_user_id)):
    m = read_cache(_k_metrics(user_id)) or {"queriesExecuted": 0, "activeQueries": 0, "savedQueries": 0}
    m.setdefault("activeQueries", 0)
    return _ok(m)


# ══════════════════════════ WebSocket (tempo real) ══════════════════════════
def _ws_user_id(websocket: WebSocket) -> Optional[int]:
    token = websocket.cookies.get("access_token") or websocket.query_params.get("token")
    if not token:
        return None
    try:
        payload = decode_token(token)
        sub = payload.get("sub") if isinstance(payload, dict) else None
        return int(sub) if sub is not None else None
    except Exception:  # noqa: BLE001
        return None


@router.websocket("/ws")
async def realtime_ws(websocket: WebSocket):
    """
    Tempo real: o cliente envia `{type, query, cursor, tables}` e recebe
    validação/autocomplete sem recarregar. O último rascunho é guardado em
    Redis (`sqleditor:draft:{uid}`), permitindo retomar noutra aba/reconexão.
    """
    await websocket.accept()
    user_id = _ws_user_id(websocket)
    if user_id is None:
        await websocket.send_json({"type": "error", "message": "Não autenticado."})
        await websocket.close(code=4401)
        return

    draft_key = f"sqleditor:draft:{user_id}"
    draft = read_cache(draft_key)
    if draft:
        await websocket.send_json({"type": "draft", "query": draft})

    try:
        while True:
            msg = await websocket.receive_json()
            mtype = msg.get("type", "validate")
            query = msg.get("query", "")
            tables_raw = msg.get("tables") or []
            tables = [SchemaTable(**t) for t in tables_raw] if tables_raw else None

            # guarda rascunho (tempo real / persistência)
            write_cache(draft_key, query, ttl=60 * 60 * 24)

            if mtype == "autocomplete":
                cursor = int(msg.get("cursor", len(query)))
                await websocket.send_json(
                    {"type": "autocomplete", "items": autocomplete(query, cursor, tables)}
                )
            else:  # validate (default)
                result = validate_query(query, tables)
                await websocket.send_json({"type": "validate", **result})
    except WebSocketDisconnect:
        return
    except Exception as e:  # noqa: BLE001
        log_message(f"[sql-editor ws] {e}", "warning")
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001
            pass
