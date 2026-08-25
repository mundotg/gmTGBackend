import traceback
from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.config.cache_manager import CACHE_PREFIX, cache_result, clear_cache
from app.schemas.task_schema import (
    TaskCreateSchema,
    TaskSchema,
    TaskStatsSchema,
    TaskUpdateSchema,
)
from app.database import get_db
from app.services import task_service
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

router = APIRouter(tags=["Tasks"])


def _invalidate_task_cache() -> None:
    """
    Descarta o cache das listagens de tarefas depois de uma escrita.

    `list_tasks_cached` guarda 5 minutos; sem isto, criar ou editar uma tarefa
    parecia não ter efeito até o cache expirar.
    """
    try:
        for fn in ("list_tasks_cached", "retrieve_task_cached"):
            clear_cache(f"{CACHE_PREFIX}{fn}:*")
    except Exception as e:  # o cache nunca deve derrubar a operação
        log_message(f"⚠️ Falha ao invalidar cache de tarefas: {e}", level="warning")


# -----------------------------
# 🔧 Função auxiliar centralizada
# -----------------------------
def handle_service_error(context: str, error: Exception, status_code: int = 500):
    """Função utilitária para logar erros e retornar HTTPException."""
    error_trace = traceback.format_exc()
    log_message(f"❌ Erro em {context}: {error}\n{error_trace}", level="error")

    if isinstance(error, HTTPException):
        raise error
    elif isinstance(error, SQLAlchemyError):
        raise HTTPException(status_code=500, detail="Erro de banco de dados")
    else:
        raise HTTPException(
            status_code=status_code, detail=f"Erro interno em {context}"
        )


# -----------------------------
# 💾 Funções com Cache
# -----------------------------
@cache_result(ttl=300, user_id="user_list_tasks_{user_id}")
def list_tasks_cached(db: Session, project_id: str, user_id: int):
    """Lista tarefas com cache."""
    return task_service.list_tasks_service(db, project_id)


@cache_result(ttl=600, user_id="user_retrieve_task_{user_id}")
def retrieve_task_cached(db: Session, project_id: str, task_id: str, user_id: int):
    """Obtém tarefa específica com cache."""
    tasks = task_service.list_tasks_service(db, project_id)
    task = next((t for t in tasks if str(t.id) == task_id), None)
    if not task:
        raise HTTPException(status_code=404, detail="Tarefa não encontrada")
    return task


# -----------------------------
# 🚀 Endpoints de Tarefas
# -----------------------------


@router.get("/task/{project_id}/tasks/", response_model=List[TaskSchema])
async def list_project_tasks(
    project_id: str,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user_id),
):
    """Lista todas as tarefas de um projeto."""
    try:
        print("listar")
        return list_tasks_cached(db, project_id, user)
    except Exception as e:
        handle_service_error(f"listar tarefas do projeto {project_id}", e)


@router.post(
    "/task/{project_id}/tasks/",
    response_model=TaskSchema,
    status_code=status.HTTP_201_CREATED,
)
async def add_new_task(
    project_id: str,
    task: TaskCreateSchema,
    db: Session = Depends(get_db),
    user: int = Depends(get_current_user_id),
):
    """Adiciona uma nova tarefa ao projeto."""
    try:
        result = task_service.add_task_service(db, project_id, task, created_by_id=user)
        _invalidate_task_cache()
        log_message(f"✅ Tarefa adicionada ao projeto {project_id}", level="info")
        return result
    except Exception as e:
        handle_service_error(f"adicionar tarefa no projeto {project_id}", e)


@router.put("/task/{project_id}/tasks/{task_id}", response_model=TaskSchema)
async def update_existing_task(
    project_id: str,
    task_id: str,
    task: TaskUpdateSchema,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user_id),
):
    """Atualiza uma tarefa existente (parcial: só os campos enviados)."""
    try:
        result = task_service.update_task_service(db, project_id, task_id, task)
        _invalidate_task_cache()
        log_message(
            f"✅ Tarefa {task_id} atualizada no projeto {project_id}", level="info"
        )
        return result
    except Exception as e:
        handle_service_error(f"atualizar tarefa {task_id} no projeto {project_id}", e)


@router.delete(
    "/task/{project_id}/tasks/{task_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_existing_task(
    project_id: str,
    task_id: str,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user_id),
):
    """Deleta uma tarefa existente."""
    try:
        task_service.delete_task_service(db, project_id, task_id)
        _invalidate_task_cache()
        log_message(
            f"✅ Tarefa {task_id} deletada do projeto {project_id}", level="info"
        )
        return {"message": "Tarefa deletada com sucesso"}
    except Exception as e:
        handle_service_error(f"deletar tarefa {task_id} do projeto {project_id}", e)


# -----------------------------
# 👥 Delegar Tarefa
# -----------------------------
@router.put("/tasks/delegar/{task_id}")
async def delegar_task(
    task_id: str,
    assigned_to: Optional[str] = Query(
        None, description="ID do usuário para quem delegar a tarefa"
    ),
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user_id),
):
    """
    Delegar (atribuir) uma tarefa a outro utilizador.
    """
    try:
        result = task_service.delegate_task_service(
            db=db, task_id=task_id, assigned_to=assigned_to, user_id=user
        )

        log_message(
            f"👥 Tarefa {task_id} delegada para o usuário {assigned_to}", level="info"
        )
        return {"message": f"Tarefa {task_id} delegada com sucesso!", "data": result}
    except Exception as e:
        handle_service_error(f"delegar tarefa {task_id}", e)


@router.get("/stats/task", response_model=TaskStatsSchema)
async def task_stats_route(
    project_id: Optional[str] = Query(None, description="Filtrar por ID do projeto"),
    sprint_id: Optional[str] = Query(None, description="Filtrar por ID da sprint"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id),  # opcional
):
    """
    Retorna estatísticas das tarefas filtradas por projeto e/ou sprint.
    Caso nenhum filtro seja passado, considera todas as tarefas.
    """
    try:
        stats = task_service.get_task_stats(db, project_id, sprint_id)
        task_service.save_task_stats(db, project_id, sprint_id, stats.model_dump())
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar estatísticas: {e}")


# -----------------------------
# ✅ Validar Tarefa
# -----------------------------
@router.put("/tasks/validar/{task_id}")
async def validar_task(
    task_id: str,
    aprovado: bool = Query(
        True, description="Define se a tarefa foi validada com sucesso"
    ),
    comentario: str | None = Query(
        None, description="Comentário opcional sobre a validação"
    ),
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user_id),
):
    """
    Valida (aprova ou reprova) uma tarefa.
    """
    try:
        result = task_service.validate_task_service(
            db=db,
            task_id=task_id,
            assigned_to=user,
            aprovado=aprovado,
            comentario=comentario,
        )

        status_msg = "aprovada" if aprovado else "reprovada"
        log_message(f"✅ Tarefa {task_id} {status_msg} por {user}", level="info")
        return {"message": f"Tarefa {status_msg} com sucesso!", "data": result}
    except Exception as e:
        handle_service_error(f"validar tarefa {task_id} ", e)


# -----------------------------
# 🔍 Paginação Geral
# -----------------------------
#
# GET /geral/paginate vive em app/routes/geral_routes.py.
#
# Havia aqui um segundo handler com o mesmo método e path. Como geral_routes é
# registado antes de task_routes em main.py, esta versão nunca respondia — o
# único efeito era o aviso "Duplicate Operation ID listar_elementos" no
# arranque e a rota duplicada no Swagger. A versão de geral_routes é a mais
# completa (valida `tipo` contra OptionTipoModel em vez de aceitar str livre).
