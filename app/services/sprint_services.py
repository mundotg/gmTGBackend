from typing import List, Optional
from uuid import uuid4
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models.task_models import Sprint as SprintORM, Project as ProjectORM
from app.schemas.sprint_schemas import SprintCreateSchema, SprintSchema, SprintUpdateSchema
from app.ultils.logger import log_message


# -----------------------------
# 🔍 Obter sprint
# -----------------------------
def get_sprint(db: Session, sprint_id: str) -> Optional[SprintORM]:
    sprint = db.query(SprintORM).filter(SprintORM.id == sprint_id).first()
    if not sprint:
        log_message(f"Sprint {sprint_id} não encontrada", "warning")
    return sprint


# -----------------------------
# 🔍 Obter sprints de um projeto
# -----------------------------
def get_sprints_by_project(db: Session, project_id: str) -> List[SprintORM]:
    return db.query(SprintORM).filter(SprintORM.project_id == project_id).all()


# -----------------------------
# ➕ Criar sprint
# -----------------------------
def create_sprint(db: Session, project_id: str, sprint_data: SprintCreateSchema) -> Optional[SprintORM]:
    try:
        project = db.query(ProjectORM).filter(ProjectORM.id == project_id).first()
        if not project:
            log_message(f"Projeto {project_id} não encontrado ao criar sprint", "warning")
            return None

        sprint_dict = sprint_data.model_dump(by_alias=False, exclude_unset=True)
        sprint_dict.setdefault("id", str(uuid4()))
        sprint_dict["project_id"] = project_id
        sprint_dict.setdefault("cancelled", False)

        sprint = SprintORM(**sprint_dict)
        db.add(sprint)
        db.commit()
        db.refresh(sprint)

        log_message(f"Sprint criada com sucesso: {sprint.id}", "info")
        return sprint

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao criar sprint no projeto {project_id}: {e}", "error")
        return None


# -----------------------------
# ✏️ Atualizar sprint
# -----------------------------
def update_sprint(db: Session, sprint_id: str, sprint_data: SprintUpdateSchema) -> Optional[SprintORM]:
    sprint = get_sprint(db, sprint_id)
    if not sprint:
        return None

    try:
        data = sprint_data.model_dump(by_alias=False, exclude_unset=True)
        for key, value in data.items():
            setattr(sprint, key, value)

        sprint.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(sprint)

        log_message(f"Sprint {sprint_id} atualizada com sucesso", "info")
        return sprint
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao atualizar sprint {sprint_id}: {e}", "error")
        return None


# -----------------------------
# 🔁 Alternar status ativo/inativo
# -----------------------------
def toggle_sprint_status(db: Session, sprint_id: str, activate: bool) -> Optional[SprintORM]:
    """
    Alterna o status ativo/inativo de uma sprint.
    Se `activate=True`, desativa todas as outras sprints do mesmo projeto.
    """
    sprint = get_sprint(db, sprint_id)
    if not sprint:
        log_message(f"Sprint {sprint_id} não encontrada", "warning")
        return None

    try:
        if activate:
            # Desativa todas as outras sprints do mesmo projeto
            db.query(SprintORM).filter(
                SprintORM.project_id == sprint.project_id,
                SprintORM.id != sprint.id
            ).update({SprintORM.is_active: False}, synchronize_session=False)

            sprint.is_active = True
            sprint.cancelled = False
            sprint.motivo_cancelamento = None
        else:
            sprint.is_active = False

        sprint.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(sprint)

        estado = "ativada" if activate else "desativada"
        log_message(f"Sprint {sprint_id} {estado} com sucesso", "info")

        return sprint

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao alterar status da sprint {sprint_id}: {e}", "error")
        return None


# -----------------------------
# ❌ Cancelar sprint
# -----------------------------
def cancel_sprint(db: Session, sprint_id: str, motivo: Optional[str] = None) -> Optional[SprintORM]:
    """
    Cancela uma sprint, marcando `cancelled=True`, `is_active=False`,
    e registrando o motivo do cancelamento.
    """
    sprint = get_sprint(db, sprint_id)
    if not sprint:
        return None

    try:
        sprint.cancelled = True
        sprint.is_active = False
        sprint.motivo_cancelamento = motivo or "Cancelamento sem motivo especificado"
        sprint.updated_at = datetime.utcnow()

        db.commit()
        db.refresh(sprint)

        log_message(f"Sprint {sprint_id} cancelada: {sprint.motivo_cancelamento}", "info")
        return sprint

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao cancelar sprint {sprint_id}: {e}", "error")
        return None


# -----------------------------
# 🗑️ Deletar sprint
# -----------------------------
def delete_sprint(db: Session, sprint_id: str) -> Optional[SprintSchema]:
    sprint = get_sprint(db, sprint_id)
    if not sprint:
        return None

    try:
        sprint_data = SprintSchema.model_validate(sprint)  # Salva dados antes da exclusão
        db.delete(sprint)
        db.commit()

        log_message(f"Sprint {sprint_id} deletada com sucesso", "info")
        return sprint_data
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao deletar sprint {sprint_id}: {e}", "error")
        return None
