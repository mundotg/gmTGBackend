"""
Análise de dados / ciência de dados sobre o resultado de uma consulta.

- `POST /datascience/store-query`: guarda a consulta encriptada no backend e devolve
  uma chave segura e encriptada (AES-256-GCM) para navegar para `/datascience?key=...`.
- `WS /datascience/ws`: recebe a chave encriptada `key` (ou `QueryPayload`), recupera
  e valida a consulta, executa com `payload_obj.limit = None` através de `QueryExecutionService`,
  efetua o estudo completo em ETAPAS (tempo real) e persiste o resultado integral na tabela
  `data_science_analysis`.
- `POST /datascience/analyze`: mesma análise completa persistida (REST).
- `GET /datascience/history`: lista as análises completas salvas do utilizador.
- `GET /datascience/history/{id}`: carrega uma análise salva completa.
- `DELETE /datascience/history/{id}`: remove uma análise do histórico.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config.redis import read_cache, write_cache
from app.database import SessionLocal, get_db
from app.models.datascience_models import DataScienceAnalysis
from app.models.user_model import User
from app.routes.connection_routes import get_current_user_id
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.services.crypto_utils import secret_decrypt, secret_encrypt
from app.services.data_science_service import analisar, analisar_stream
from app.ultils.QueryExecutionService import QueryExecutionService
from app.ultils.get_id_by_token import ws_user_id
from app.ultils.logger import log_message
from app.ultils.permissions import require_permission, ws_has_permission

router = APIRouter(
    prefix="/datascience",
    tags=["Data Science"],
    dependencies=[Depends(require_permission("table:read", "query:execute"))],
)

# As WebSockets ficam num router próprio, SEM a dependência de permissão acima.
ws_router = APIRouter(prefix="/datascience", tags=["Data Science"])

# Cache em memória como fallback resiliente caso o Redis esteja indisponível
_local_query_store: Dict[str, Tuple[float, str]] = {}


class StoreQueryRequest(BaseModel):
    query: Optional[QueryPayload] = None
    payload: Optional[QueryPayload] = None
    QueryPayload: Optional[QueryPayload] = None

    model_config = {"extra": "allow"}


class AnalyzeRequest(BaseModel):
    key: Optional[str] = None
    query: Optional[QueryPayload] = None
    payload: Optional[QueryPayload] = None
    QueryPayload: Optional[QueryPayload] = None
    rows: Optional[List[Dict[str, Any]]] = None

    model_config = {"extra": "allow"}


def _extrair_payload_obj(data: Any) -> Optional[QueryPayload]:
    """
    Extrai QueryPayload a partir dos vários formatos possíveis de entrada:
    - Já é uma instância de QueryPayload
    - AnalyzeRequest / StoreQueryRequest com 'query', 'payload' ou 'QueryPayload'
    - Dicionário com chaves aninhadas ('query', 'payload', 'QueryPayload', 'queryPayload')
    - Dicionário que é ele próprio a QueryPayload (possui 'baseTable')
    """
    if isinstance(data, QueryPayload):
        return data
    if isinstance(data, (AnalyzeRequest, StoreQueryRequest)):
        return data.query or data.payload or data.QueryPayload
    if isinstance(data, dict):
        for k in ("query", "payload", "QueryPayload", "queryPayload"):
            val = data.get(k)
            if isinstance(val, QueryPayload):
                return val
            if isinstance(val, dict) and "baseTable" in val:
                try:
                    return QueryPayload(**val)
                except Exception:
                    pass
        if "baseTable" in data:
            try:
                return QueryPayload(**data)
            except Exception:
                pass
    return None


def _guardar_query_encriptada(payload_obj: QueryPayload, user_id: int) -> str:
    """
    Guarda a query encriptada e gera uma chave de sessão encriptada em AES-256-GCM.
    Retorna o token encriptado que pode viajar na URL com total segurança.
    """
    session_id = secrets.token_urlsafe(24)
    json_str = payload_obj.model_dump_json(exclude_none=True)
    encrypted_payload = secret_encrypt(json_str)

    cache_key = f"ds_stored:{user_id}:{session_id}"
    ttl_seconds = 7200  # 2 horas
    write_cache(cache_key, encrypted_payload, ttl=ttl_seconds)
    _local_query_store[cache_key] = (time.time() + ttl_seconds, encrypted_payload)

    raw_token = f"ds:{user_id}:{session_id}"
    return secret_encrypt(raw_token)


def _desencriptar_e_limpar_chave(encrypted_key: str) -> str:
    from urllib.parse import unquote

    clean = unquote(encrypted_key.strip())
    if "%" in clean:
        clean = unquote(clean)
    return clean


def _recuperar_query_por_chave(
    encrypted_key: str, user_id: Optional[int] = None
) -> Optional[Tuple[QueryPayload, str, int]]:
    """
    Desencripta a chave recebida, valida a pertença do utilizador (se user_id for passado)
    ou infere o user_id da própria chave encriptada (AES-256-GCM com secret do servidor).
    Retorna (payload_obj, session_id, token_user_id).
    """
    try:
        clean_key = _desencriptar_e_limpar_chave(encrypted_key)
        decrypted_token = secret_decrypt(clean_key)
        parts = decrypted_token.split(":")
        if len(parts) != 3 or parts[0] != "ds":
            return None

        token_user_id = int(parts[1])
        session_id = parts[2]

        if user_id is not None and token_user_id != user_id:
            log_message(
                f"[datascience] Acesso negado: utilizador {user_id} tentou usar chave do user {token_user_id}",
                "warning",
            )
            return None

        actual_user_id = token_user_id
        cache_key = f"ds_stored:{actual_user_id}:{session_id}"
        encrypted_payload = read_cache(cache_key)

        if not encrypted_payload and cache_key in _local_query_store:
            exp, data = _local_query_store[cache_key]
            if time.time() <= exp:
                encrypted_payload = data

        if not encrypted_payload:
            log_message(
                f"[datascience] Consulta associada à chave não encontrada no cache ({cache_key}).",
                "warning",
            )
            return None

        json_str = secret_decrypt(encrypted_payload)
        data_dict = json.loads(json_str)
        return QueryPayload(**data_dict), session_id, actual_user_id
    except Exception as e:
        log_message(f"[datascience] Erro ao recuperar query encriptada por chave: {e}", "warning")
        return None


def _salvar_analise_na_base(
    user_id: int,
    payload_obj: Optional[QueryPayload],
    full_result: Dict[str, Any],
    query_result: Dict[str, Any],
    session_id: Optional[str] = None,
) -> Optional[int]:
    """
    Persiste o resultado completo da análise na tabela data_science_analysis com
    toda a informação disponível.
    """
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        conn_id = None
        if user and hasattr(user, "info_extra") and isinstance(user.info_extra, dict):
            conn_id = user.info_extra.get("id_connection")
        elif user and hasattr(user, "settings") and user.settings:
            conn_id = getattr(user.settings, "active_connection_id", None)

        base_table = payload_obj.baseTable if payload_obj else None
        overview = full_result.get("overview") or {}
        quality = full_result.get("quality") or {}

        analysis_record = DataScienceAnalysis(
            user_id=user_id,
            db_connection_id=conn_id,
            session_id=session_id,
            title=f"Análise: {base_table or 'Consulta Completa'}",
            base_table=base_table,
            query_payload=payload_obj.model_dump(exclude_none=True) if payload_obj else None,
            total_rows=int(overview.get("n_rows") or query_result.get("total", 0)),
            total_columns=int(overview.get("n_cols") or len(full_result.get("columns", []))),
            duration_ms=float(query_result.get("duration_ms", 0.0)),
            quality_score=float(quality.get("score")) if quality.get("score") is not None else None,
            quality_details=quality if quality else None,
            overview=overview,
            columns_stats=full_result.get("columns"),
            correlations=full_result.get("correlations"),
            trend=full_result.get("trend"),
            anomalies=full_result.get("anomalies"),
            segments=full_result.get("segments"),
            insights=full_result.get("insights"),
            full_result=full_result,
        )
        db.add(analysis_record)
        db.commit()
        db.refresh(analysis_record)
        log_message(f"✅ Análise de Data Science persistida com id={analysis_record.id} para user {user_id}", "info")
        return analysis_record.id
    except Exception as e:
        db.rollback()
        log_message(f"[datascience] Erro ao persistir análise na tabela: {e}", "warning")
        return None
    finally:
        db.close()


def _hash_key(
    user_id: int,
    payload_obj: Optional[QueryPayload],
    raw_rows: Optional[List[Dict[str, Any]]] = None,
) -> str:
    try:
        if payload_obj is not None:
            src = payload_obj.model_dump(exclude_none=True)
            src["_full"] = True
        else:
            src = {"rows_len": len(raw_rows or []), "sample": (raw_rows or [])[:2]}
        h = hashlib.sha1(json.dumps(src, sort_keys=True, default=str).encode()).hexdigest()[:20]
    except Exception:  # noqa: BLE001
        h = "nohash"
    return f"ds:{user_id}:{h}"


async def _obter_dados_consulta(
    payload_obj: Optional[QueryPayload],
    raw_rows: Optional[List[Dict[str, Any]]],
    db: Session,
    user_id: int,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Executa a query com QueryExecutionService sem limite (payload_obj.limit = None)
    garantindo o estudo completo de Data Science.
    """
    query_result: Dict[str, Any] = {}

    if payload_obj is not None:
        payload_obj.limit = None
        payload_obj.offset = None
        payload_obj.isCountQuery = False

        rs_dict = await QueryExecutionService().execute_query(payload_obj, db, user_id)

        preview = rs_dict.get("preview", []) if isinstance(rs_dict, dict) else []
        query_result["preview"] = preview
        query_result["duration_ms"] = (
            rs_dict.get("duration_ms", 0) if isinstance(rs_dict, dict) else 0
        )
        query_result["columns"] = (
            rs_dict.get("columns", [])
            if isinstance(rs_dict, dict)
            else (list(preview[0].keys()) if preview else [])
        )
        query_result["totalResults"] = (
            rs_dict.get("totalResults", len(preview))
            if isinstance(rs_dict, dict)
            else len(preview)
        )
        return preview, query_result

    if raw_rows is not None:
        preview = raw_rows
        query_result["preview"] = preview
        query_result["totalResults"] = len(preview)
        query_result["columns"] = list(preview[0].keys()) if preview else []
        return preview, query_result

    return [], {"preview": [], "totalResults": 0}


# ══════════════════════════ Armazenar Query Encriptada ══════════════════════════
@router.post("/store-query")
async def store_query_endpoint(
    req: Union[StoreQueryRequest, QueryPayload, Dict[str, Any]],
    user_id: int = Depends(get_current_user_id),
):
    """
    Guarda a query no backend de forma encriptada e devolve uma chave encriptada (AES-256-GCM).
    Essa chave é enviada como parâmetro para `/datascience?key=...`.
    """
    payload_obj = _extrair_payload_obj(req)
    if payload_obj is None:
        raise HTTPException(
            status_code=400,
            detail="Nenhuma consulta válida ('query' ou 'payload') fornecida para armazenar.",
        )

    encrypted_key = _guardar_query_encriptada(payload_obj, user_id)
    return {
        "success": True,
        "key": encrypted_key,
    }


# ══════════════════════════ REST de Análise ══════════════════════════
@router.post("/analyze")
async def analyze_endpoint(
    req: Union[AnalyzeRequest, QueryPayload, Dict[str, Any]],
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    payload_obj: Optional[QueryPayload] = None
    raw_rows: Optional[List[Dict[str, Any]]] = None
    session_id: Optional[str] = None

    if isinstance(req, dict):
        key = req.get("key")
        if key:
            recovered = _recuperar_query_por_chave(str(key), user_id)
            if recovered:
                payload_obj, session_id, _ = recovered
        if payload_obj is None:
            payload_obj = _extrair_payload_obj(req)
        raw_rows = req.get("rows")
    else:
        key = getattr(req, "key", None)
        if key:
            recovered = _recuperar_query_por_chave(str(key), user_id)
            if recovered:
                payload_obj, session_id, _ = recovered
        if payload_obj is None:
            payload_obj = _extrair_payload_obj(req)
        raw_rows = getattr(req, "rows", None)

    if payload_obj is None and not raw_rows:
        raise HTTPException(
            status_code=400,
            detail="Nenhuma consulta ('query' / 'key') válida fornecida para análise.",
        )

    cache_key = _hash_key(user_id, payload_obj, raw_rows)
    cached = read_cache(cache_key)
    if cached:
        return {"success": True, "cached": True, **cached}

    preview, query_result = await _obter_dados_consulta(payload_obj, raw_rows, db, user_id)
    result = analisar(preview)
    try:
        write_cache(cache_key, result, ttl=300)
    except Exception:  # noqa: BLE001
        pass

    # Persiste na tabela data_science_analysis
    analysis_id = _salvar_analise_na_base(
        user_id=user_id,
        payload_obj=payload_obj,
        full_result=result,
        query_result=query_result,
        session_id=session_id,
    )

    return {
        "success": True,
        "cached": False,
        "analysis_id": analysis_id,
        "query_result": {
            "total": len(preview),
            "duration_ms": query_result.get("duration_ms", 0),
            "columns": query_result.get("columns", []),
        },
        **result,
    }


# ══════════════════════════ Histórico de Análises Salvas ══════════════════════════
@router.get("/history")
async def list_analysis_history(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Lista as análises salvas do utilizador atual com métricas resumidas."""
    records = (
        db.query(DataScienceAnalysis)
        .filter(DataScienceAnalysis.user_id == user_id)
        .order_by(DataScienceAnalysis.created_at.desc())
        .limit(100)
        .all()
    )
    return {
        "success": True,
        "data": [
            {
                "id": r.id,
                "title": r.title,
                "base_table": r.base_table,
                "total_rows": r.total_rows,
                "total_columns": r.total_columns,
                "quality_score": r.quality_score,
                "duration_ms": r.duration_ms,
                "is_favorite": r.is_favorite,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in records
        ],
    }


@router.get("/history/{analysis_id}")
async def get_analysis_detail(
    analysis_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Carrega todo o estudo de ciência de dados salvo na íntegra."""
    r = (
        db.query(DataScienceAnalysis)
        .filter(DataScienceAnalysis.id == analysis_id, DataScienceAnalysis.user_id == user_id)
        .first()
    )
    if not r:
        raise HTTPException(status_code=404, detail="Análise não encontrada.")
    return {
        "success": True,
        "data": {
            "id": r.id,
            "title": r.title,
            "base_table": r.base_table,
            "query_payload": r.query_payload,
            "total_rows": r.total_rows,
            "total_columns": r.total_columns,
            "duration_ms": r.duration_ms,
            "quality_score": r.quality_score,
            "quality_details": r.quality_details,
            "overview": r.overview,
            "columns": r.columns_stats,
            "correlations": r.correlations,
            "trend": r.trend,
            "anomalies": r.anomalies,
            "segments": r.segments,
            "insights": r.insights,
            "full_result": r.full_result,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        },
    }


@router.delete("/history/{analysis_id}")
async def delete_analysis_record(
    analysis_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Remove uma análise salva do histórico."""
    r = (
        db.query(DataScienceAnalysis)
        .filter(DataScienceAnalysis.id == analysis_id, DataScienceAnalysis.user_id == user_id)
        .first()
    )
    if not r:
        raise HTTPException(status_code=404, detail="Análise não encontrada.")
    db.delete(r)
    db.commit()
    return {"success": True, "message": "Análise removida com sucesso."}


# ══════════════════════════ WebSocket (tempo real) ══════════════════════════
@ws_router.websocket("/ws")
async def datascience_ws(websocket: WebSocket):
    await websocket.accept()
    try:
        initial_user_id = ws_user_id(websocket)

        while True:
            msg = await websocket.receive_json()
            try:
                payload_obj: Optional[QueryPayload] = None
                raw_rows: Optional[List[Dict[str, Any]]] = None
                session_id: Optional[str] = None
                user_id = initial_user_id

                # Se recebeu a chave encriptada devolvida pelo backend
                if isinstance(msg, dict) and msg.get("key"):
                    recovered = _recuperar_query_por_chave(str(msg["key"]), user_id)
                    if recovered:
                        payload_obj, session_id, key_user_id = recovered
                        if user_id is None:
                            user_id = key_user_id
                    else:
                        await websocket.send_json({
                            "stage": "error",
                            "message": "Chave de consulta encriptada inválida ou expirada.",
                        })
                        continue
                else:
                    payload_obj = _extrair_payload_obj(msg)
                    if isinstance(msg, dict):
                        raw_rows = msg.get("rows")

                if user_id is None:
                    await websocket.send_json({"stage": "error", "message": "Não autenticado."})
                    await websocket.close(code=4401)
                    return

                if not await ws_has_permission(user_id, "table:read", "query:execute"):
                    await websocket.send_json({"stage": "error", "message": "Permissão insuficiente."})
                    await websocket.close(code=4403)
                    return

                if payload_obj is None and not raw_rows:
                    await websocket.send_json({
                        "stage": "error",
                        "message": "Nenhuma consulta ('key' ou 'query') fornecida para análise.",
                    })
                    continue
            except Exception as e:  # noqa: BLE001
                await websocket.send_json({"stage": "error", "message": f"Pedido inválido: {e}"})
                continue

            cache_key = _hash_key(user_id, payload_obj, raw_rows)
            cached = read_cache(cache_key)
            if cached:
                await websocket.send_json({
                    "stage": "overview",
                    "data": cached.get("overview", {}),
                    "cached": True,
                })
                for col in cached.get("columns", []):
                    await websocket.send_json({"stage": "column", "data": col})
                await websocket.send_json({
                    "stage": "correlations",
                    "data": cached.get("correlations", {}),
                })
                await websocket.send_json({
                    "stage": "quality",
                    "data": cached.get("quality", {}),
                })
                for st in ("trend", "anomalies", "segments"):
                    if cached.get(st):
                        await websocket.send_json({"stage": st, "data": cached[st]})
                await websocket.send_json({"stage": "insights", "data": cached.get("insights", [])})
                await websocket.send_json({"stage": "done", "cached": True})
                continue

            await websocket.send_json({"stage": "fetching"})
            db = next(get_db())
            try:
                preview, query_result = await _obter_dados_consulta(
                    payload_obj, raw_rows, db, user_id
                )
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

            await websocket.send_json({"stage": "computing", "n_rows": len(preview)})

            full: Dict[str, Any] = {
                "overview": {},
                "columns": [],
                "correlations": {},
                "quality": {},
                "trend": None,
                "anomalies": None,
                "segments": None,
                "insights": [],
            }
            for stage, data in analisar_stream(preview):
                if stage == "column":
                    full["columns"].append(data)
                elif stage in full:
                    full[stage] = data
                await websocket.send_json({"stage": stage, "data": data})

            try:
                write_cache(cache_key, full, ttl=300)
            except Exception:  # noqa: BLE001
                pass

            # Persiste o resultado completo na tabela data_science_analysis
            analysis_id = _salvar_analise_na_base(
                user_id=user_id,
                payload_obj=payload_obj,
                full_result=full,
                query_result=query_result,
                session_id=session_id,
            )

            await websocket.send_json({
                "stage": "done",
                "cached": False,
                "analysis_id": analysis_id,
                "query_result": {
                    "total": len(preview),
                    "duration_ms": query_result.get("duration_ms", 0),
                },
            })

    except WebSocketDisconnect:
        return
    except Exception as e:  # noqa: BLE001
        log_message(f"[datascience ws] {e}", "warning")
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001
            pass
