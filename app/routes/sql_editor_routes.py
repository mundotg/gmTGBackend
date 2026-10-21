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

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect

from fastapi.responses import JSONResponse, StreamingResponse

from pydantic import BaseModel, Field

from sqlalchemy import text

from sqlalchemy.ext.asyncio import AsyncSession

from app.config.redis import read_cache, write_cache

from app.database import get_db_async

from app.routes.connection_routes import get_current_user_id

from app.models.user_model import User

from app.schemas.connetion_schema import ConnectionAccessLevel

from app.services.impact_predictor_service import ImpactPredictorService

from app.ultils.ativar_engine import ConnectionManager

from app.ultils.conect_database import is_mongo, get_mongo_database

from app.ultils.connection_access import (

    assert_user_connection_level_async,

    get_effective_connection_rules_async,

    validate_connection_query_rules,

)

from app.ultils.logger import log_message

from app.ultils.permissions import get_current_user, require_permission, user_has_permission, ws_has_permission

from app.ultils.get_id_by_token import ws_user_id

# Baseline: quem pode executar consultas. Alterar dados exige ainda

# `data:write`, verificado por instrucao dentro de /execute e /explain,

# porque aqui o mesmo endpoint tanto le como escreve.

router = APIRouter(

    prefix="/sql-editor",

    tags=["SQL Editor"],

    dependencies=[Depends(require_permission("query:execute"))],

)

# As WebSockets ficam num router próprio, SEM a dependência de permissão acima:

# as dependências de router também correm nas rotas WS e ali rebentam

# (APIKeyCookie exige um Request HTTP). A permissão é verificada dentro da WS

# com `ws_has_permission`, depois de autenticar.

ws_router = APIRouter(prefix="/sql-editor", tags=["SQL Editor"])

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
    query: str = Field(..., min_length=1, max_length=100_000)
    limit: int = Field(default=1000, ge=1, le=MAX_ROWS)
    offset: int = Field(default=0, ge=0, le=1_000_000)
    stream: bool = True

class SchemaTable(BaseModel):
    name: str = Field(..., min_length=1, max_length=256)
    columns: List[str] = Field(default_factory=list)

class AnalyzeBody(BaseModel):
    query: str = Field(..., max_length=100_000)

class ValidateBody(BaseModel):
    query: str = Field(..., max_length=100_000)
    dialect: Optional[str] = None
    tables: Optional[List[SchemaTable]] = None

class SaveBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    query: str = Field(..., min_length=1, max_length=100_000)

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

                if sql[j] == "\\\\":

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

# ── nível de acesso exigido pelo lote de SQL ──────────────────────────────

# Verbos iniciais que não alteram nada. Tudo o resto conta como escrita.

_TIPOS_LEITURA = {"SELECT", "WITH", "SHOW", "EXPLAIN", "DESCRIBE", "DESC", "PRAGMA"}

# Palavras que denunciam escrita mesmo num statement que começa por verbo de

# leitura: CTEs que escrevem (`WITH x AS (DELETE ... RETURNING ...)`) e

# `SELECT ... INTO nova_tabela`.

_RE_ESCRITA = re.compile(

    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|MERGE"

    r"|GRANT|REVOKE|INTO|CALL|EXEC|EXECUTE)\b",

    re.IGNORECASE,

)

# Strings e comentários são removidos antes da análise para que um dado

# (`WHERE estado = 'DELETED'`) não seja confundido com um comando.

_RE_LITERAIS = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"\\]|\\.)*\"|--[^\n]*|/\*.*?\*/", re.DOTALL)

def exigir_permissao_de_escrita(actor: User) -> None:

    """

    403 se o utilizador nao pode alterar dados em lado nenhum.

    Complementa a verificacao de nivel na conexao: esta diz o que a PESSOA

    pode fazer (RBAC), a outra diz o que ela pode fazer NAQUELA conexao.

    """

    if not user_has_permission(actor.permissions, ("data:write",)):

        raise HTTPException(

            status_code=403,

            detail="Permissao insuficiente para alterar dados. E necessaria: data:write.",

        )

def requires_write(sql: str) -> bool:

    """

    True se alguma instrução do lote altera dados ou estrutura.

    Serve para pedir `write` na conexão só quando é mesmo preciso — um

    utilizador com nível `read` tem de continuar a poder correr SELECTs, que é

    exatamente o que a partilha de leitura lhe promete.

    Na dúvida devolve True: recusar uma leitura por excesso de zelo é um

    incómodo, deixar passar uma escrita é a falha que isto vem fechar.

    """

    for stmt in split_statements(sql):

        limpo = _RE_LITERAIS.sub(" ", stmt)

        if statement_type(limpo) not in _TIPOS_LEITURA:

            return True

        if _RE_ESCRITA.search(limpo):

            return True

    return False

def extract_tables(stmt: str) -> List[str]:

    tables: List[str] = []

    for m in re.finditer(

        r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([`\"\[]?[\w.]+[`\"\]]?)",

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

    prefix_match = re.search(r"([\w.]*)$", (query or "")[: cursor if cursor is not None else len(query or "")])

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
    """Mantém apenas o histórico recente e evita duplicados consecutivos."""
    history = read_cache(_k_hist(uid)) or []
    if history and history[0].get("query") == query:
        history[0]["created_at"] = _now()
    else:
        history.insert(0, {"query": query, "created_at": _now()})
    write_cache(_k_hist(uid), history[:HISTORY_MAX], ttl=60 * 60 * 24 * 30)

def _bump_metric(uid: int, field: str, delta: int = 1) -> Dict[str, int]:

    m = read_cache(_k_metrics(uid)) or {"queriesExecuted": 0, "activeQueries": 0, "savedQueries": 0}

    m[field] = int(m.get(field, 0)) + delta

    write_cache(_k_metrics(uid), m, ttl=60 * 60 * 24 * 90)

    return m

def _now() -> str:

    return time.strftime("%Y-%m-%dT%H:%M:%S")

# ══════════════════════════ execução Mongo (modo shell) ══════════════════════════

_MONGO_RE = re.compile(

    r"(?:db\.)?(?P<coll>[\w.]+)\.(?P<op>find|aggregate|countDocuments|count)\s*\((?P<args>.*?)\)\s*;?\s*$",

    re.IGNORECASE | re.DOTALL,

)

async def _run_mongo_shell(
    engine, connection, query: str, limit: int = MAX_ROWS, offset: int = 0
) -> AsyncGenerator[str, None]:
    """Executa o subconjunto Mongo suportado, sem carregar coleções inteiras em RAM."""
    match = _MONGO_RE.search(query.strip())
    if not match:
        yield _sse(
            "error",
            {"message": "Mongo: use `db.coleção.find({...})`, `.aggregate([...])` ou `.countDocuments({...})`."},
        )
        return

    collection_ref = match.group("coll")
    operation = match.group("op").lower()
    raw_args = match.group("args").strip()

    if "." in collection_ref:
        database_name, collection_name = collection_ref.split(".", 1)
    else:
        database_name = getattr(connection, "database_name", None)
        collection_name = collection_ref

    database = engine[database_name] if database_name else get_mongo_database(engine)
    if database is None:
        yield _sse("error", {"message": "Não foi possível determinar a base MongoDB."})
        return

    collection = database[collection_name]
    row_cap = min(max(1, limit), MAX_ROWS)
    row_offset = max(0, offset)

    def parse_argument(value: str, default):
        return json.loads(value) if value else default

    cursor = None
    try:
        yield _sse("statement_start", {"type": operation})

        if operation in ("countdocuments", "count"):
            filter_doc = parse_argument(raw_args, {})
            total = await asyncio.to_thread(collection.count_documents, filter_doc)
            yield _sse("columns", {"columns": ["count"]})
            yield _sse("row", {"row": {"count": total}})
            yield _sse("statement_complete", {"rows": 1})
        else:
            if operation == "aggregate":
                pipeline = parse_argument(raw_args, [])
                if not isinstance(pipeline, list):
                    raise ValueError("O argumento de aggregate deve ser uma lista JSON.")
                bounded_pipeline = list(pipeline)
                if row_offset:
                    bounded_pipeline.append({"$skip": row_offset})
                bounded_pipeline.append({"$limit": row_cap + 1})
                cursor = collection.aggregate(bounded_pipeline, batchSize=FETCH_BATCH)
            else:
                parts = _split_top_level_args(raw_args)
                filter_doc = parse_argument(parts[0], {}) if parts else {}
                projection = parse_argument(parts[1], None) if len(parts) > 1 else None
                cursor = (
                    collection.find(filter_doc, projection)
                    .skip(row_offset)
                    .limit(row_cap + 1)
                    .batch_size(FETCH_BATCH)
                )

            rows_sent = 0
            hit_limit = False
            batch = await asyncio.to_thread(_next_cursor_batch, cursor, min(FETCH_BATCH, row_cap + 1))
            yield _sse("columns", {"columns": _collect_columns(batch)})

            while batch:
                for document in batch:
                    if rows_sent >= row_cap:
                        hit_limit = True
                        break
                    yield _sse("row", {"row": _jsonable(document)})
                    rows_sent += 1
                if hit_limit or rows_sent >= row_cap:
                    if not hit_limit:
                        extra = await asyncio.to_thread(_next_cursor_batch, cursor, 1)
                        hit_limit = bool(extra)
                    break
                batch = await asyncio.to_thread(
                    _next_cursor_batch, cursor, min(FETCH_BATCH, row_cap - rows_sent + 1)
                )

            yield _sse("statement_complete", {"rows": rows_sent})
            if hit_limit:
                yield _sse("limit", {"cap": row_cap, "offset": row_offset})
    except json.JSONDecodeError as exc:
        yield _sse("error", {"message": f"Argumento Mongo inválido (JSON): {exc.msg}"})
        return
    except (TypeError, ValueError) as exc:
        yield _sse("error", {"message": f"Consulta Mongo inválida: {exc}"})
        return
    except Exception as exc:  # noqa: BLE001
        log_message(f"[sql-editor] erro Mongo: {exc}", "error")
        yield _sse("error", {"message": "Não foi possível executar a consulta MongoDB."})
        return
    finally:
        if cursor is not None:
            try:
                await asyncio.to_thread(cursor.close)
            except Exception:  # noqa: BLE001
                pass

    yield _sse("complete", {"ok": True})

def _next_cursor_batch(cursor, size: int) -> List[dict]:
    """Lê um lote limitado do cursor síncrono Mongo, sem acumular o resultado completo."""
    return list(islice(cursor, max(1, size)))

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
    """Obtém colunas dos primeiros documentos, preservando ordem e evitando buscas O(n²)."""
    columns: List[str] = []
    seen = set()
    for document in docs[:50]:
        for key in document:
            if key not in seen:
                seen.add(key)
                columns.append(key)
    return columns

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

# ══════════════════════════ execução SQL (raw, SSE) ══════════════════════════

async def _run_sql(engine, query: str, limit: int, offset: int = 0) -> AsyncGenerator[str, None]:
    statements = split_statements(query)
    total_statements = len(statements)
    row_cap = min(max(1, limit), MAX_ROWS)
    row_offset = max(0, offset)

    for index, statement in enumerate(statements, 1):
        query_type = statement_type(statement)
        yield _sse("progress", {"current": index, "total": total_statements})
        yield _sse("statement_start", {"type": query_type})

        async with engine.connect() as connection:
            result = None
            try:
                # Async streaming evita que drivers compatíveis materializem todo o resultado.
                result = await connection.stream(text(statement))
                # AsyncResult não expõe `returns_rows` em todas as versões do
                # SQLAlchemy. `keys()` é a API pública e permite distinguir
                # resultados tabulares (SELECT/RETURNING) de comandos sem linhas.
                columns = list(result.keys())
                if columns:
                    yield _sse("columns", {"columns": columns})
                    fetched = 0
                    skipped = 0
                    hit_limit = False

                    while True:
                        # Lê no máximo o espaço restante + uma linha de prova do limite.
                        batch_size = min(FETCH_BATCH, max(1, row_cap - fetched + 1))
                        batch = await result.fetchmany(batch_size)
                        if not batch:
                            break
                        for row in batch:
                            if skipped < row_offset:
                                skipped += 1
                                continue
                            if fetched >= row_cap:
                                hit_limit = True
                                break
                            yield _sse("row", {"row": dict(row._mapping)})
                            fetched += 1
                        if hit_limit:
                            break

                    yield _sse("statement_complete", {"rows": fetched, "type": query_type})
                    if hit_limit:
                        yield _sse("limit", {"cap": row_cap, "offset": row_offset})
                else:
                    await connection.commit()
                    yield _sse(
                        "statement_complete",
                        {"rows": getattr(result, "rowcount", 0) or 0, "type": query_type},
                    )
            except Exception as exc:  # noqa: BLE001
                try:
                    await connection.rollback()
                except Exception:  # noqa: BLE001
                    pass
                log_message(
                    f"[sql-editor] falha ao executar statement ({type(exc).__name__}): {exc}",
                    "error",
                )
                yield _sse(
                    "error",
                    {
                        "code": _db_error_code(exc),
                        "message": _db_error_message(exc),
                        "statement": statement,
                    },
                )
                return
            finally:
                if result is not None:
                    try:
                        await result.close()
                    except Exception:  # noqa: BLE001
                        pass

    yield _sse("complete", {"ok": True})

# ══════════════════════════ rotas ══════════════════════════

@router.post("/execute")

async def execute(

    body: ExecuteBody,

    db: AsyncSession = Depends(get_db_async),

    user_id: int = Depends(get_current_user_id),

    actor: User = Depends(get_current_user),

):

    query = (body.query or "").strip()

    query_id = str(uuid.uuid4())

    async def gen() -> AsyncGenerator[str, None]:

        yield _sse("start", {"queryId": query_id})

        if not query:

            yield _sse("error", {"message": "Query vazia."})

            return

        try:

            # Obtém-se com `read` (o mínimo para executar seja o que for) e só

            # depois se exige `write`, se o SQL escrito o justificar. Fazer o

            # contrário obrigaria a adivinhar o dialecto antes de ter a engine.

            engine, connection = await ConnectionManager.get_engine_async(db, user_id)

            # O shell Mongo aqui só aceita find/aggregate/count (`_MONGO_RE`),

            # portanto é leitura por construção e dispensa a análise de SQL.

            if not is_mongo(engine) and requires_write(query):

                exigir_permissao_de_escrita(actor)

                await assert_user_connection_level_async(

                    db, connection, user_id, ConnectionAccessLevel.write

                )

            # Validação granular de regras de conexão (tabelas, colunas, tipo de consulta, limites)

            rules = await get_effective_connection_rules_async(db, connection, actor)

            effective_limit = min(max(1, body.limit), MAX_ROWS)

            if rules.max_rows is not None and rules.max_rows > 0:

                effective_limit = min(effective_limit, rules.max_rows)

            if is_mongo(engine):

                m = _MONGO_RE.search(query.strip())

                if m:

                    coll_ref = m.group("coll")

                    coll_name = coll_ref.split(".", 1)[1] if "." in coll_ref else coll_ref

                    validate_connection_query_rules(rules, query_type="SELECT", tables=[coll_name])

            else:

                for stmt in split_statements(query):

                    qtype = statement_type(stmt)

                    tbls = extract_tables(stmt)

                    validate_connection_query_rules(rules, query_type=qtype, tables=tbls)

        except HTTPException as e:

            # 403 de nível insuficiente (e 400 de conexão em falta) têm de

            # chegar ao editor como erro legível, não como "Sem conexão ativa".

            yield _sse("error", {"message": e.detail})

            return

        except Exception as e:  # noqa: BLE001

            yield _sse("error", {"message": f"Sem conexão ativa: {e}"})

            return

        _push_history(user_id, query)

        _bump_metric(user_id, "queriesExecuted", 1)

        try:

            if is_mongo(engine):

                async for ev in _run_mongo_shell(engine, connection, query, effective_limit, body.offset):

                    yield ev

            else:

                async for ev in _run_sql(engine, query, effective_limit, body.offset):

                    yield ev

        except Exception as e:  # noqa: BLE001

            log_message(f"[sql-editor] erro execução: {e}", "error")

            yield _sse("error", {"message": _db_error_message(e)})

    return StreamingResponse(

        gen(),

        media_type="text/event-stream",

        headers={"X-Query-Id": query_id, "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},

    )

# Mensagens da base de dados reconhecidas → texto legível que nomeia o objeto.

# PostgreSQL (psycopg2/asyncpg), MySQL, SQL Server e SQLite.

_DB_ERROR_PATTERNS = [

    (re.compile(r'relation "?([\w.]+)"? does not exist', re.I), 'A tabela "{0}" não existe nesta base de dados.'),

    (re.compile(r"Table '([^']+)' doesn't exist", re.I), 'A tabela "{0}" não existe nesta base de dados.'),

    (re.compile(r"Invalid object name '([^']+)'", re.I), 'A tabela "{0}" não existe nesta base de dados.'),

    (re.compile(r"no such table: ([\w.]+)", re.I), 'A tabela "{0}" não existe nesta base de dados.'),

    (re.compile(r'column "?([\w.]+)"? does not exist', re.I), 'A coluna "{0}" não existe.'),

    (re.compile(r"Unknown column '([^']+)'", re.I), 'A coluna "{0}" não existe.'),

    (re.compile(r"Invalid column name '([^']+)'", re.I), 'A coluna "{0}" não existe.'),

    (re.compile(r"no such column: ([\w.]+)", re.I), 'A coluna "{0}" não existe.'),

]

def _db_error_code(exc: Exception) -> str:
    """Devolve um código estável para o frontend, sem expor detalhes internos."""
    message = str(exc).lower()
    if "asyncresult" in message and "returns_rows" in message:
        return "SQL_RESULT_PROCESSING_ERROR"
    if "unknown column" in message or "invalid column name" in message or "column" in message and "does not exist" in message:
        return "SQL_COLUMN_NOT_FOUND"
    if "does not exist" in message or "no such table" in message or "invalid object name" in message:
        return "SQL_TABLE_NOT_FOUND"
    if "syntax error" in message or "syntaxerror" in message:
        return "SQL_SYNTAX_ERROR"
    if "permission denied" in message or "not authorized" in message or "access denied" in message:
        return "SQL_PERMISSION_DENIED"
    return "SQL_EXECUTION_ERROR"


def _db_error_message(e: Exception) -> str:

    """

    Texto de erro para mostrar no editor.

    `str(e)` de uma exceção do SQLAlchemy traz a classe do driver, o SQL e um

    link de documentação ("(sqlalchemy.dialects.postgresql.asyncpg.

    ProgrammingError) <class '...UndefinedTableError'>: relation "us" does not

    exist [SQL: ...] (Background on this error at: ...)"). Fica só a mensagem

    da base de dados — traduzida quando é um caso comum.

    """

    orig = getattr(e, "orig", None)

    raw = str(orig if orig is not None else e).strip()

    raw = re.sub(r"^<class '[^']+'>:\s*", "", raw)

    first = next((ln.strip() for ln in raw.splitlines() if ln.strip()), "") or str(e)

    for pattern, template in _DB_ERROR_PATTERNS:

        m = pattern.search(first)

        if m:

            return template.format(m.group(1))

    return first

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

@router.post("/impact")

async def explain(

    body: AnalyzeBody,

    db: AsyncSession = Depends(get_db_async),

    user_id: int = Depends(get_current_user_id),

    actor: User = Depends(get_current_user),

):

    try:

        engine, connection = await ConnectionManager.get_engine_async(db, user_id)

        impact = await ImpactPredictorService.predict_impact(engine, connection, body.query or "")

        # Mantém compatibilidade retroativa com código que consome `plan`

        raw_plan = impact.get("raw_plan")

        if isinstance(raw_plan, list):

            impact["plan"] = raw_plan

        elif isinstance(raw_plan, dict):

            impact["plan"] = [raw_plan]

        else:

            impact["plan"] = []

        return _ok(impact, "Previsão de impacto concluída")

    except Exception as e:  # noqa: BLE001

        return JSONResponse(

            status_code=400,

            content={

                "success": False,

                "message": _db_error_message(e),

                "data": {

                    "supported": False,

                    "risk_level": "LOW",

                    "summary": "Não foi possível calcular o plano de impacto para esta consulta.",

                    "suggestions": [],

                    "plan": [],

                },

            },

        )

@router.post("/validate")

async def validate(body: ValidateBody, user_id: int = Depends(get_current_user_id)):

    return _ok(validate_query(body.query, body.tables), "Validado")

@router.post("/save")

async def save(body: SaveBody, user_id: int = Depends(get_current_user_id)):
    name = body.name.strip()
    query = body.query.strip()
    if not name or not query:
        raise HTTPException(status_code=422, detail="O nome e a consulta são obrigatórios.")

    saved = read_cache(_k_saved(user_id)) or []
    item = {"id": str(uuid.uuid4()), "name": name, "query": query, "created_at": _now()}

    saved.insert(0, item)

    write_cache(_k_saved(user_id), saved[:SAVED_MAX])

    metrics = read_cache(_k_metrics(user_id)) or {
        "queriesExecuted": 0,
        "activeQueries": 0,
        "savedQueries": 0,
    }
    metrics["savedQueries"] = min(len(saved), SAVED_MAX)
    write_cache(_k_metrics(user_id), metrics)

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

@ws_router.websocket("/ws")

async def realtime_ws(websocket: WebSocket):

    """

    Tempo real: o cliente envia `{type, query, cursor, tables}` e recebe

    validação/autocomplete sem recarregar. O último rascunho é guardado em

    Redis (`sqleditor:draft:{uid}`), permitindo retomar noutra aba/reconexão.

    """

    await websocket.accept()

    user_id = ws_user_id(websocket)

    if user_id is None:

        await websocket.send_json({"type": "error", "message": "Não autenticado."})

        await websocket.close(code=4401)

        return

    if not await ws_has_permission(user_id, "query:execute"):

        await websocket.send_json({"type": "error", "message": "Permissão insuficiente."})

        await websocket.close(code=4403)

        return

    draft_key = f"sqleditor:draft:{user_id}"

    try:

        # Dentro do try: se o browser já fechou a ligação (reload, re-render),

        # este envio levanta WebSocketDisconnect — fora do try escapava e o

        # uvicorn registava "Exception in ASGI application".

        draft = read_cache(draft_key)

        if draft:

            await websocket.send_json({"type": "draft", "query": draft})

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
