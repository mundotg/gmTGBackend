from fastapi import APIRouter, Body, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import List
from app.schemas.sprint_schemas import (
    SprintCreateSchema,
    SprintSchema,
    SprintUpdateSchema,
)
from app.config.dependencies import get_db
from app.services.sprint_services import (
    cancel_sprint,
    create_sprint,
    delete_sprint,
    get_sprints_by_project,
    toggle_sprint_status,
    update_sprint,
)
from app.ultils.get_id_by_token import get_current_user_id

router = APIRouter(prefix="/sprints", tags=["Sprints"])


@router.get(
    "/project/{project_id}",
    response_model=List[SprintSchema],
    status_code=status.HTTP_200_OK,
    summary="Listar sprints de um projeto",
    description="Retorna todas as sprints associadas a um determinado projeto.",
)
async def list_sprints(
    project_id: str,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    sprints = get_sprints_by_project(db, project_id)
    if not sprints:
        raise HTTPException(
            status_code=404, detail="Nenhuma sprint encontrada para este projeto"
        )
    return sprints


@router.patch(
    "/toggle/{sprint_id}/status",
    response_model=SprintSchema,
    summary="Ativar ou desativar sprint",
    description="Ativa ou desativa uma sprint específica, conforme o parâmetro 'activate'.",
)
async def toggle_sprint_status_route(
    sprint_id: str,
    activate: bool = Query(
        ..., description="Define se a sprint será ativada ou desativada."
    ),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    sprint = toggle_sprint_status(db, sprint_id, activate)
    if not sprint:
        raise HTTPException(status_code=404, detail="Sprint não encontrada")
    return sprint


@router.post(
    "/{project_id}",
    response_model=SprintSchema,
    status_code=status.HTTP_201_CREATED,
    summary="Criar nova sprint",
    description="Cria uma nova sprint associada a um projeto.",
)
async def create_sprint_route(
    project_id: str,
    data: SprintCreateSchema,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    sprint = create_sprint(db, project_id, data)
    if not sprint:
        raise HTTPException(status_code=400, detail="Erro ao criar sprint")
    return sprint


@router.put(
    "/{sprint_id}",
    response_model=SprintSchema,
    summary="Atualizar sprint existente",
    description="Atualiza os dados de uma sprint já existente.",
)
async def update_sprint_route(
    sprint_id: str,
    data: SprintUpdateSchema,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    sprint = update_sprint(db, sprint_id, data)
    if not sprint:
        raise HTTPException(status_code=404, detail="Sprint não encontrada")
    return sprint


@router.delete(
    "/{sprint_id}",
    response_model=SprintSchema,
    status_code=status.HTTP_200_OK,
    summary="Excluir sprint",
    description="Remove uma sprint específica do banco de dados.",
)
async def delete_sprint_route(
    sprint_id: str,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    sprint = delete_sprint(db, sprint_id)
    if not sprint:
        raise HTTPException(status_code=404, detail="Sprint não encontrada")
    return sprint


@router.patch(
    "/{sprint_id}/cancel",
    response_model=SprintSchema,
    summary="Cancelar sprint",
    description="Cancela uma sprint ativa, registrando o motivo e a data de cancelamento.",
)
async def cancel_sprint_route(
    sprint_id: str,
    reason: str = Body(
        ..., embed=True, description="Motivo do cancelamento da sprint."
    ),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    sprint = cancel_sprint(db, sprint_id, reason)
    if not sprint:
        raise HTTPException(
            status_code=404, detail="Sprint não encontrada ou erro ao cancelar."
        )
    return sprint
