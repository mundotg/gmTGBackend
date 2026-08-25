import asyncio
import json
import traceback
from datetime import datetime
from typing import AsyncGenerator, Optional

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import decode_token
from app.database import AsyncSessionLocal, get_db_async
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message
from importantConfig.convert_string_to_dict import PayloadError, converter_tables_origen

router = APIRouter(prefix="/transfer", tags=["Database Operations"])


def sse(event: str, data: str) -> str:
    lines = str(data).splitlines() or [""]
    return f"event: {event}\n" + "\n".join([f"data: {ln}" for ln in lines]) + "\n\n"


def _ws_user_id(websocket: WebSocket) -> Optional[int]:
    """Autentica a WS pelo cookie `access_token` (mesmo padrão do backup)."""
    token = websocket.cookies.get("access_token") or websocket.query_params.get("token")
    if not token:
        return None
    try:
        payload = decode_token(token)
        sub = payload.get("sub") if isinstance(payload, dict) else None
        return int(sub) if sub is not None else None
    except Exception:
        return None


@router.get("/stream")
async def transfer_stream(
    id_connectio_origen: int,
    id_connectio_distino: int,
    tables_origen: str,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id),
):
    log_message(f"[User {user_id}] Transfer stream iniciado | origem={id_connectio_origen} | destino={id_connectio_distino}", "info")

    async def event_stream() -> AsyncGenerator[str, None]:
        start_time = datetime.now()
        keepalive_task: asyncio.Task | None = None

        log_message(f"[User {user_id}] Iniciando stream de transferência", "info")

        async def keep_alive_sender(queue: asyncio.Queue[str]) -> None:
            log_message(f"[User {user_id}] Keep-alive do SSE iniciado", "info")
            try:
                while True:
                    await asyncio.sleep(15)
                    await queue.put(sse("ping", "keep-alive"))
            except asyncio.CancelledError:
                log_message(f"[User {user_id}] Keep-alive do SSE cancelado", "warning")
                return

        q: asyncio.Queue[str] = asyncio.Queue(maxsize=200)

        async def put(event: str, data: str) -> None:
            msg = sse(event, data)
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                log_message(f"[User {user_id}] SSE queue cheia; descartando evento antigo", "warning")
                try:
                    _ = q.get_nowait()
                except Exception:
                    pass
                try:
                    q.put_nowait(msg)
                except Exception:
                    log_message(f"[User {user_id}] Falha ao reenfileirar mensagem SSE", "error")

        try:
            keepalive_task = asyncio.create_task(keep_alive_sender(q))

            await put("status", " Iniciando transferência...")
            log_message(f"[User {user_id}] Iniciando transferência", "info")

            await put("status", " Validando conexões origem/destino...")
            log_message(f"[User {user_id}] Validando conexões origem/destino", "info")

            # ===============================
            # PARSE / VALIDA PAYLOAD
            # ===============================
            try:
                log_message(f"[User {user_id}] Convertendo payload tables_origen", "info")
                tables_dict, warnings = converter_tables_origen(
                    tables_origen, strict=False
                )

                log_message(f"[User {user_id}] Tabelas válidas detectadas: {len(tables_dict)}", "info")

                for w in warnings:
                    log_message(f"[User {user_id}] WARN payload: {w}", "warning")
                    await put("warning", f"⚠️ {w}")

                if not tables_dict:
                    log_message(f"[User {user_id}] Nenhuma tabela válida encontrada para transferir", "warning")
                    raise PayloadError("Nenhuma tabela válida para transferir.")

            except PayloadError as e:
                msg = f" Configuração inválida: {str(e)}"
                log_message(f"[User {user_id}] {msg}\n{traceback.format_exc()}", "error")
                await put("error", msg)
                return

            except Exception as e:
                msg = f" Erro ao processar configuração: {str(e)}"
                log_message(f"[User {user_id}] {msg}\n{traceback.format_exc()}", "error")
                await put("error", msg)
                return

            # ===============================
            # EXECUÇÃO DA TRANSFERÊNCIA
            # ===============================
            from importantConfig.db_transfer import transfer_data

            await put(
                "status",
                f" Executando... (origem={id_connectio_origen}, destino={id_connectio_distino}, tabelas={len(tables_dict)})",
            )

            log_message(f"[User {user_id}] Iniciando loop de transferência", "info")

            async for progress_msg in transfer_data(
                id_user=user_id,
                db=db,
                id_connectio_origen=id_connectio_origen,
                id_connectio_distino=id_connectio_distino,
                tables_origen=tables_dict,
            ):
                await put("log", progress_msg)

            await put("done", "✅ Transferência concluída!")
            log_message(f"[User {user_id}] Transferência concluída com sucesso", "success")

        except asyncio.CancelledError as ae:
            log_message(f"[User {user_id}] SSE cancelado pelo cliente.\n{traceback.format_exc()}", "warning")
            raise

        except HTTPException as e:
            msg = f" Erro HTTP: {e.detail}"
            log_message(f"[User {user_id}] {msg}\n{traceback.format_exc()}", "error")
            await put("error", msg)

        except Exception as e:
            msg = f" Erro: {str(e)}"
            log_message(f"[User {user_id}] {msg}\n{traceback.format_exc()}", "error")
            await put("error", msg)

        finally:
            if keepalive_task:
                keepalive_task.cancel()

            duration = (datetime.now() - start_time).total_seconds()
            log_message(f"[User {user_id}] Transferência finalizada em {duration:.2f}s", "info")
            await put("final", f"⏱️ Finalizado em {duration:.2f}s")

            while not q.empty():
                yield await q.get()

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }

    return StreamingResponse(
        event_stream(), media_type="text/event-stream", headers=headers
    )


# ============================================================
# 🔌 WebSocket de transferência
#
# Vantagens sobre o SSE (/stream):
#   - O payload (tables_origen) vai NA MENSAGEM, não no URL — deixa de haver
#     limite de tamanho (mapeamentos grandes rebentavam o query string).
#   - Bidirecional: o cliente pode enviar {"action":"cancel"} para parar.
#
# Protocolo:
#   cliente → servidor (1ª msg): {id_connectio_origen, id_connectio_distino, tables_origen}
#   servidor → cliente: {"event": "status|log|warning|error|done|final", "data": "..."}
#   cliente → servidor (opcional): {"action": "cancel"}
# ============================================================
@router.websocket("/ws")
async def transfer_ws(websocket: WebSocket):
    await websocket.accept()

    user_id = _ws_user_id(websocket)
    if user_id is None:
        await websocket.send_json({"event": "error", "data": "Não autenticado."})
        await websocket.close(code=4401)
        return

    async def send(event: str, data: str) -> None:
        try:
            await websocket.send_json({"event": event, "data": data})
        except Exception:
            pass

    start_time = datetime.now()

    try:
        # 1) Recebe a configuração da transferência.
        raw = await websocket.receive_text()
        try:
            cfg = json.loads(raw)
        except json.JSONDecodeError:
            await send("error", "Payload inicial inválido (não é JSON).")
            await websocket.close(code=4400)
            return

        id_origen = int(cfg.get("id_connectio_origen") or 0)
        id_distino = int(cfg.get("id_connectio_distino") or 0)
        tables_origen = cfg.get("tables_origen") or ""

        if not id_origen or not id_distino:
            await send("error", "Conexões de origem/destino são obrigatórias.")
            await websocket.close(code=4400)
            return

        log_message(
            f"[User {user_id}] Transfer WS iniciado | origem={id_origen} | destino={id_distino}",
            "info",
        )

        await send("status", "Iniciando transferência...")
        await send("status", "Validando conexões origem/destino...")

        # 2) Parse do payload.
        try:
            tables_dict, warnings = converter_tables_origen(tables_origen, strict=False)
            for w in warnings:
                await send("warning", f"⚠️ {w}")
            if not tables_dict:
                raise PayloadError("Nenhuma tabela válida para transferir.")
        except PayloadError as e:
            await send("error", f"Configuração inválida: {e}")
            return
        except Exception as e:  # noqa: BLE001
            log_message(f"[User {user_id}] Erro no payload: {e}\n{traceback.format_exc()}", "error")
            await send("error", f"Erro ao processar configuração: {e}")
            return

        await send(
            "status",
            f"Executando... (origem={id_origen}, destino={id_distino}, tabelas={len(tables_dict)})",
        )

        # 3) Corre a transferência numa task para poder cancelar.
        from importantConfig.db_transfer import transfer_data

        async def run_transfer() -> None:
            # Sessão própria: a WS vive para além do escopo do pedido.
            async with AsyncSessionLocal() as db:
                async for progress_msg in transfer_data(
                    id_user=user_id,
                    db=db,
                    id_connectio_origen=id_origen,
                    id_connectio_distino=id_distino,
                    tables_origen=tables_dict,
                ):
                    await send("log", progress_msg)

        transfer_task = asyncio.create_task(run_transfer())

        # 4) Em paralelo, ouve o cliente para um pedido de cancelamento.
        async def listen_cancel() -> None:
            try:
                while True:
                    msg = await websocket.receive_text()
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue
                    if data.get("action") == "cancel":
                        transfer_task.cancel()
                        return
            except WebSocketDisconnect:
                transfer_task.cancel()

        cancel_task = asyncio.create_task(listen_cancel())

        try:
            await transfer_task
            await send("done", "✅ Transferência concluída!")
            log_message(f"[User {user_id}] Transferência concluída com sucesso", "success")
        except asyncio.CancelledError:
            await send("warning", "⏹️ Transferência cancelada pelo utilizador.")
            log_message(f"[User {user_id}] Transferência cancelada", "warning")
        finally:
            cancel_task.cancel()

    except WebSocketDisconnect:
        log_message(f"[User {user_id}] Transfer WS desligado pelo cliente.", "warning")
        return
    except Exception as e:  # noqa: BLE001
        log_message(f"[User {user_id}] Erro no transfer WS: {e}\n{traceback.format_exc()}", "error")
        await send("error", f"Erro: {e}")
    finally:
        duration = (datetime.now() - start_time).total_seconds()
        await send("final", f"⏱️ Finalizado em {duration:.2f}s")
        try:
            await websocket.close()
        except Exception:
            pass
