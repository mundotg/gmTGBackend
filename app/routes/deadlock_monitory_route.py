from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio, json, traceback
from datetime import datetime

from app.database import get_db_async
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message
from importantConfig.DeadlockManager import DeadlockManager


router = APIRouter(prefix="/database", tags=["Database Operations"])


def response_success(data: dict):
    return {
        "status": "success",
        "timestamp": datetime.utcnow().isoformat(),
        **data
    }

def response_error(msg: str, status_code: int = 500):
    log_message(f"❌ {msg}", level="error")
    return HTTPException(status_code, {"status": "error", "message": msg})


async def _get_deadlock_manager(db, user_id: int) -> DeadlockManager:
    try:
        engine, connInfo = await ConnectionManager.get_engine_async(db, user_id)
        if not engine:
            raise ValueError("Nenhuma conexão ativa configurada para este usuário.")
        return DeadlockManager(engine)
    except Exception as e:
        traceback.print_exc()
        raise response_error(str(e))


# ============================================================
# 🔍 Deadlock Consultas
# ============================================================

@router.get("/deadlocks")
async def get_deadlocks(db: AsyncSession = Depends(get_db_async),
                        user_id: int = Depends(get_current_user_id)):
    dm = await _get_deadlock_manager(db, user_id)
    processos = await dm.listar_processos_em_deadlock()
    return response_success({"total": len(processos), "processos": processos})


@router.get("/deadlocks/stats")
async def get_deadlock_stats(db: AsyncSession = Depends(get_db_async),
                             user_id: int = Depends(get_current_user_id)):
    dm = await _get_deadlock_manager(db, user_id)
    stats = await dm.obter_estatisticas_gerais()
    return response_success({"estatisticas": stats})


@router.get("/deadlocks/history")
async def get_deadlock_history(db: AsyncSession = Depends(get_db_async),
                               user_id: int = Depends(get_current_user_id)):
    dm = await _get_deadlock_manager(db, user_id)
    history = dm.obter_historico_deadlocks()
    return response_success({
        "total_registros": len(history),
        "historico": history
    })


# ============================================================
# ❌ Kill Process
# ============================================================

@router.get("/deadlocks/kill/{pid}")
async def kill_process(pid: int,
                       db: AsyncSession = Depends(get_db_async),
                       user_id: int = Depends(get_current_user_id)):
    dm = await _get_deadlock_manager(db, user_id)
    result = await dm.matar_processo(pid)

    if result.get("status") == "erro":
        raise response_error(result.get("mensagem"))

    return response_success({"resultado": result})


@router.get("/deadlocks/kill-all")
async def kill_all_blockers(db: AsyncSession = Depends(get_db_async),
                            user_id: int = Depends(get_current_user_id)):
    dm = await _get_deadlock_manager(db, user_id)
    result = await dm.matar_todos_processos_bloqueadores()
    return response_success({"resultado": result})


# ============================================================
# 📡 Monitoramento SSE (com timeout e logs)
# ============================================================

@router.get("/deadlocks/monitor/stream")
async def monitor_deadlocks_stream(db: AsyncSession = Depends(get_db_async),
                                   user_id: int = Depends(get_current_user_id)):
    dm = await _get_deadlock_manager(db, user_id)

    async def event_stream():
        last_count = -1
        idle_ticks = 0
        max_idle = 300  # ~20 min

        while True:
            try:
                processos = await dm.listar_processos_em_deadlock()
                count = len(processos)

                if count != last_count:
                    payload = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "count": count,
                        "items": processos
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_count = count
                    idle_ticks = 0
                else:
                    idle_ticks += 1

                if idle_ticks > max_idle:
                    yield "event: end\ndata: stream timeout\n\n"
                    break

                await asyncio.sleep(4)

            except asyncio.CancelledError:
                break
            except Exception as e:
                traceback.print_exc()
                log_message(f"❌ SSE error: {e}", level="error")
                yield f"data: {json.dumps({'erro': str(e)})}\n\n"
                await asyncio.sleep(10)

    return StreamingResponse(event_stream(), media_type="text/event-stream")
