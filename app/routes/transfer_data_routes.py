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
    print(" [STEP 0] Endpoint /transfer/stream chamado")
    print(f" user_id={user_id}")
    print(f" origem={id_connectio_origen}, destino={id_connectio_distino}")

    async def event_stream() -> AsyncGenerator[str, None]:
        start_time = datetime.now()
        keepalive_task: asyncio.Task | None = None

        print(" [STEP 1] Iniciando event_stream()")

        async def keep_alive_sender(queue: asyncio.Queue[str]) -> None:
            print(" [KEEPALIVE] Tarefa keep-alive iniciada")
            try:
                while True:
                    await asyncio.sleep(15)
                    print(" [KEEPALIVE] Enviando ping")
                    await queue.put(sse("ping", "keep-alive"))
            except asyncio.CancelledError:
                print(" [KEEPALIVE] Tarefa cancelada")
                return

        q: asyncio.Queue[str] = asyncio.Queue(maxsize=200)
        print(" [STEP 2] Queue criada (maxsize=200)")

        async def put(event: str, data: str) -> None:
            print(f" [PUT] event={event} | data={data[:80]}")
            msg = sse(event, data)
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                print(" [PUT] Queue cheia, descartando mensagem antiga")
                try:
                    _ = q.get_nowait()
                except Exception:
                    pass
                try:
                    q.put_nowait(msg)
                except Exception:
                    print(" [PUT] Falha ao reenfileirar mensagem")

        try:
            keepalive_task = asyncio.create_task(keep_alive_sender(q))
            print(" [STEP 3] keepalive_task criada")

            await put("status", " Iniciando transferência...")
            log_message(f"[User {user_id}] Iniciando transferência")
            print(" [STEP 4] Transferência iniciada")

            await put("status", " Validando conexões origem/destino...")
            print(" [STEP 5] Validando conexões")

            # ===============================
            # PARSE / VALIDA PAYLOAD
            # ===============================
            try:
                print(" [STEP 6] Convertendo payload tables_origen")
                tables_dict, warnings = converter_tables_origen(
                    tables_origen, strict=False
                )

                print(f"[STEP 6.1] Tabelas válidas: {len(tables_dict)}")

                for w in warnings:
                    print(f" [PAYLOAD WARNING] {w}")
                    log_message(f"[User {user_id}] WARN payload: {w}")
                    await put("warning", f"⚠️ {w}")

                if not tables_dict:
                    print(" [STEP 6.2] Nenhuma tabela válida encontrada")
                    raise PayloadError("Nenhuma tabela válida para transferir.")

            except PayloadError as e:
                msg = f" Configuração inválida: {str(e)}"
                print(f" [STEP 6.ERROR] {msg}")
                print(traceback.format_exc())
                log_message(f"[User {user_id}] {msg}")
                await put("error", msg)
                return

            except Exception as e:
                msg = f" Erro ao processar configuração: {str(e)}"
                print(f" [STEP 6.EXCEPTION] {msg}")
                print(traceback.format_exc())
                log_message(f"[User {user_id}] {msg}")
                await put("error", msg)
                return

            # ===============================
            # EXECUÇÃO DA TRANSFERÊNCIA
            # ===============================
            print(" [STEP 7] Importando transfer_data")
            from importantConfig.db_transfer import transfer_data

            await put(
                "status",
                f" Executando... (origem={id_connectio_origen}, destino={id_connectio_distino}, tabelas={len(tables_dict)})",
            )

            print(" [STEP 8] Iniciando loop de transferência")

            async for progress_msg in transfer_data(
                id_user=user_id,
                db=db,
                id_connectio_origen=id_connectio_origen,
                id_connectio_distino=id_connectio_distino,
                tables_origen=tables_dict,
            ):
                print(f" [TRANSFER] {progress_msg}")
                await put("log", progress_msg)

            await put("done", "✅ Transferência concluída!")
            print(" [STEP 9] Transferência concluída com sucesso")
            log_message(f"[User {user_id}] Transferência concluída com sucesso")

        except asyncio.CancelledError as ae:
            print(" [CANCEL] Cliente fechou a conexão SSE")
            print(str(ae))
            print(traceback.format_exc())
            log_message(f"[User {user_id}] SSE cancelado pelo cliente.")
            raise

        except HTTPException as e:
            msg = f" Erro HTTP: {e.detail}"
            print(f" [HTTP ERROR] {msg}")
            print(traceback.format_exc())
            log_message(f"[User {user_id}] {msg}")
            await put("error", msg)

        except Exception as e:
            msg = f" Erro: {str(e)}"
            print(f" [UNEXPECTED ERROR] {msg}")
            print(traceback.format_exc())
            log_message(f"[User {user_id}] {msg}")
            await put("error", msg)

        finally:
            if keepalive_task:
                keepalive_task.cancel()
                print(" [STEP 10] keepalive_task cancelada")

            duration = (datetime.now() - start_time).total_seconds()
            print(f" [STEP 11] Finalizado em {duration:.2f}s")
            await put("final", f"⏱️ Finalizado em {duration:.2f}s")

            print(" [STEP 12] Drenando fila SSE")
            while not q.empty():
                yield await q.get()

        print(" [STEP 13] Encerrando event_stream")
        while not q.empty():
            yield await q.get()

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }

    print(" [STEP FINAL] Retornando StreamingResponse")
    return StreamingResponse(
        event_stream(), media_type="text/event-stream", headers=headers
    )
