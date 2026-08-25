"""
Análise de dados / ciência de dados sobre o resultado de uma consulta.

- `WS /datascience/ws`: envia-se uma `QueryPayload` (ou linhas já carregadas) e
  recebem-se as estatísticas em ETAPAS (tempo real): overview → cada coluna →
  correlações → insights. Funciona para QUALQUER base (SQL e MongoDB), porque
  reutiliza o motor de query.
- `POST /datascience/analyze`: mesma análise, mas de uma vez (para partilhar/GET).
- **Redis** cacheia o resultado completo por hash → sob carga, repetições não
  voltam a computar.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.auth import decode_token
from app.config.redis import read_cache, write_cache
from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.services.data_science_service import analisar, analisar_stream
from app.ultils.QueryExecutionService import QueryExecutionService
from app.ultils.logger import log_message

router = APIRouter(prefix="/datascience", tags=["Data Science"])

MAX_ANALYZE_ROWS = 5000  # cap de linhas a analisar (memória/tempo)


class AnalyzeRequest(BaseModel):
    payload: Optional[QueryPayload] = None
    rows: Optional[List[Dict[str, Any]]] = None
    limit: int = MAX_ANALYZE_ROWS


def _hash_key(user_id: int, req: "AnalyzeRequest") -> str:
    try:
        if req.payload is not None:
            src = req.payload.model_dump(exclude_none=True)
            src["_limit"] = min(req.limit, MAX_ANALYZE_ROWS)
        else:
            src = {"rows_len": len(req.rows or []), "sample": (req.rows or [])[:2]}
        h = hashlib.sha1(json.dumps(src, sort_keys=True, default=str).encode()).hexdigest()[:20]
    except Exception:  # noqa: BLE001
        h = "nohash"
    return f"ds:{user_id}:{h}"


async def _obter_linhas(req: "AnalyzeRequest", db: Session, user_id: int) -> List[Dict[str, Any]]:
    """Linhas para analisar: das enviadas, ou buscadas pelo motor de query."""
    if req.rows:
        return req.rows[:MAX_ANALYZE_ROWS]
    if req.payload is None:
        return []
    payload = req.payload
    payload.limit = min(req.limit or MAX_ANALYZE_ROWS, MAX_ANALYZE_ROWS)
    payload.offset = None
    payload.isCountQuery = False
    result = await QueryExecutionService().execute_query(payload, db, user_id)
    return result.get("preview", []) if isinstance(result, dict) else []


# ══════════════════════════ REST ══════════════════════════
@router.post("/analyze")
async def analyze_endpoint(
    req: AnalyzeRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    cache_key = _hash_key(user_id, req)
    cached = read_cache(cache_key)
    if cached:
        return {"success": True, "cached": True, **cached}

    rows = await _obter_linhas(req, db, user_id)
    result = analisar(rows)
    try:
        write_cache(cache_key, result, ttl=300)
    except Exception:  # noqa: BLE001
        pass
    return {"success": True, "cached": False, **result}


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
async def datascience_ws(websocket: WebSocket):
    await websocket.accept()
    user_id = _ws_user_id(websocket)
    if user_id is None:
        await websocket.send_json({"stage": "error", "message": "Não autenticado."})
        await websocket.close(code=4401)
        return

    try:
        while True:
            msg = await websocket.receive_json()
            try:
                req = AnalyzeRequest(**msg)
            except Exception as e:  # noqa: BLE001
                await websocket.send_json({"stage": "error", "message": f"Pedido inválido: {e}"})
                continue

            cache_key = _hash_key(user_id, req)
            cached = read_cache(cache_key)
            if cached:
                # Envia o resultado completo já em cache, mas ainda "por etapas"
                # para o cliente pintar do mesmo modo.
                await websocket.send_json({"stage": "overview", "data": cached.get("overview", {}), "cached": True})
                for col in cached.get("columns", []):
                    await websocket.send_json({"stage": "column", "data": col})
                await websocket.send_json({"stage": "correlations", "data": cached.get("correlations", {})})
                await websocket.send_json({"stage": "quality", "data": cached.get("quality", {})})
                for st in ("trend", "anomalies", "segments"):
                    if cached.get(st):
                        await websocket.send_json({"stage": st, "data": cached[st]})
                await websocket.send_json({"stage": "insights", "data": cached.get("insights", [])})
                await websocket.send_json({"stage": "done", "cached": True})
                continue

            await websocket.send_json({"stage": "fetching"})
            db = next(get_db())
            try:
                rows = await _obter_linhas(req, db, user_id)
            except HTTPException as he:
                await websocket.send_json({"stage": "error", "message": str(he.detail)})
                db.close()
                continue
            except Exception as e:  # noqa: BLE001
                await websocket.send_json({"stage": "error", "message": f"Erro ao obter dados: {e}"})
                db.close()
                continue
            finally:
                try:
                    db.close()
                except Exception:  # noqa: BLE001
                    pass

            await websocket.send_json({"stage": "computing", "n_rows": len(rows)})

            # Streaming das etapas + acumula para o cache.
            full: Dict[str, Any] = {
                "overview": {}, "columns": [], "correlations": {}, "quality": {},
                "trend": None, "anomalies": None, "segments": None, "insights": [],
            }
            for stage, data in analisar_stream(rows):
                if stage == "column":
                    full["columns"].append(data)
                elif stage in full:
                    full[stage] = data
                await websocket.send_json({"stage": stage, "data": data})

            try:
                write_cache(cache_key, full, ttl=300)
            except Exception:  # noqa: BLE001
                pass

    except WebSocketDisconnect:
        return
    except Exception as e:  # noqa: BLE001
        log_message(f"[datascience ws] {e}", "warning")
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001
            pass
