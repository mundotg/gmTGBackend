import asyncio
import traceback
from datetime import datetime
from typing import AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db_async
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message
from importantConfig.convert_string_to_dict import PayloadError, converter_tables_origen

router = APIRouter(prefix="/transfer", tags=["Database Operations"])


def sse(event: str, data: str) -> str:
    lines = str(data).splitlines() or [""]
    return f"event: {event}\n" + "\n".join([f"data: {ln}" for ln in lines]) + "\n\n"


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
