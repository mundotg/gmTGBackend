# app/api/logs/logs_routes.py

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.log_models import Log
from app.routes.connection_routes import get_current_user_id
from app.schemas.logs_schema import LogOut
from app.schemas.responsehttp_schema import ResponseWrapper
from app.ultils.log_file_reader import ler_do_fim, ler_por_blocos, normalizar_nivel
from app.ultils.logger import get_log_file_path, log_message


router = APIRouter(tags=["AuditLog"])


@router.get("/logs", response_model=ResponseWrapper[list[LogOut]])
async def get_logs(
    level: Optional[str] = Query(
        None, description="Filtrar por nível (info, error, warning, success)"
    ),
    limit: int = Query(100, le=500, description="Quantidade máxima de logs"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    📋 Retorna logs do sistema com opção de filtro por nível.
    """
    try:
        query = db.query(Log)

        if level:
            query = query.filter(Log.level == level)

        logs = query.order_by(Log.created_at.desc()).limit(limit).all()

        return ResponseWrapper(success=True, data=logs)

    except Exception as e:
        log_message(
            db,
            f"❌ Erro em get_logs: {e}",
            level="error",
            source="get_logs",
            user=user_id,
        )
        raise HTTPException(status_code=500, detail="Erro ao buscar logs.")


from sqlalchemy import func


@router.get("/logs/stats", response_model=ResponseWrapper[list])
async def get_logs_stats(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    📊 Retorna estatísticas de logs agrupadas por nível.
    """
    try:
        stats = db.query(Log.level, func.count(Log.id)).group_by(Log.level).all()

        return ResponseWrapper(
            success=True,
            data=[{"level": level, "count": count} for level, count in stats],
        )

    except Exception as e:
        log_message(
            db,
            f"❌ Erro em get_logs_stats: {e}",
            level="error",
            source="get_logs_stats",
            user=user_id,
        )
        raise HTTPException(
            status_code=500, detail="Erro ao gerar estatísticas de logs."
        )


# ------------------------------------------------------------
# Ficheiro de log (database_connector.log)
# ------------------------------------------------------------
# Os endpoints acima leem a tabela `logs`. Este lê o ficheiro em disco, que é
# onde ficam os erros anteriores à ligação à base de dados — precisamente os
# que a tabela nunca chega a registar.
#
# O caminho vem de `get_log_file_path()`, que pergunta ao handler activo do
# `logging` em vez de assumir a working directory. Ver `app/ultils/logger.py`.


def _ficheiro_ou_404() -> Path:
    caminho = get_log_file_path()
    if caminho is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Ficheiro de log não encontrado. Ainda pode não ter sido "
                "criado, ou está fora dos caminhos conhecidos — define a "
                "variável de ambiente DATABASE_CONNECTOR_LOG_FILE com o "
                "caminho completo."
            ),
        )
    return caminho


@router.get("/logs/file", response_model=ResponseWrapper[dict])
async def get_log_file(
    limit: int = Query(200, ge=1, le=5000, description="Últimas N linhas"),
    level: Optional[str] = Query(
        None, description="Filtrar por nível (debug, info, warning, error, critical)"
    ),
    search: Optional[str] = Query(
        None, min_length=1, description="Texto a procurar na linha"
    ),
    user_id: int = Depends(get_current_user_id),
):
    """
    📄 Lê o ficheiro `database_connector.log`, esteja ele onde estiver.
    """
    try:
        nivel = normalizar_nivel(level)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    caminho = _ficheiro_ou_404()

    try:
        # Leitura de disco é bloqueante: fora do event loop, senão trava os
        # restantes pedidos enquanto percorre o ficheiro.
        resultado = await run_in_threadpool(ler_do_fim, caminho, limit, nivel, search)
        modificado_em = datetime.fromtimestamp(
            caminho.stat().st_mtime, tz=timezone.utc
        ).isoformat()
    except OSError as e:
        log_message(
            f"❌ Erro ao ler ficheiro de log: {e}",
            level="error",
            source="get_log_file",
            user=user_id,
        )
        raise HTTPException(status_code=500, detail="Erro ao ler o ficheiro de log.")

    return ResponseWrapper(
        success=True,
        data={
            "caminho": str(caminho),
            "modificado_em": modificado_em,
            "filtros": {"limit": limit, "level": level, "search": search},
            **resultado,
        },
    )


@router.get("/logs/file/download")
async def download_log_file(user_id: int = Depends(get_current_user_id)):
    """
    ⬇️ Descarrega o ficheiro `database_connector.log` completo.
    """
    caminho = _ficheiro_ou_404()

    try:
        tamanho = caminho.stat().st_size
    except OSError:
        raise HTTPException(status_code=500, detail="Erro ao ler o ficheiro de log.")

    # Não se usa FileResponse aqui. O FileResponse anuncia Content-Length a
    # partir do stat() e depois lê até EOF — e este ficheiro cresce entre as
    # duas coisas: o RequestContextMiddleware regista uma linha por cada
    # pedido, incluindo este. O corpo saía maior do que o anunciado e o
    # uvicorn rebentava com "Response content longer than Content-Length",
    # sem falhar nenhum outro endpoint (é o único cujo corpo é o ficheiro que
    # o middleware escreve).
    #
    # Streaming sem Content-Length (chunked), limitado ao tamanho lido no
    # início: o cliente recebe o ficheiro tal como estava no momento do
    # pedido. O tamanho vai no header para quem quiser mostrar progresso.
    return StreamingResponse(
        ler_por_blocos(caminho, tamanho),
        media_type="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{caminho.name}"',
            "X-Log-Size": str(tamanho),
        },
    )


@router.get("/logs/{log_id}", response_model=ResponseWrapper[LogOut])
async def get_log_by_id(
    log_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    🔍 Retorna um log específico por ID.
    """
    try:
        log = db.query(Log).filter(Log.id == log_id).first()

        if not log:
            raise HTTPException(status_code=404, detail="Log não encontrado.")

        return ResponseWrapper(success=True, data=log)

    except HTTPException:
        raise
    except Exception as e:
        log_message(
            db,
            f"❌ Erro em get_log_by_id: {e}",
            level="error",
            source="get_log_by_id",
            user=user_id,
        )
        raise HTTPException(status_code=500, detail="Erro ao buscar log.")
