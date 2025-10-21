from datetime import datetime
import traceback
from typing import Optional, List
from uuid import uuid4
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models.task_models import Project as ProjectORM, Sprint as SprintORM
from app.schemas.sprint_schemas import SprintSchema
from app.ultils.logger import log_message


# ==================================================
# 🔹 CRUD DE SPRINTS
# ==================================================

def get_sprint(db: Session, sprint_id: str) -> Optional[SprintORM]:
    """Obtém uma sprint pelo ID."""
    sprint = db.query(SprintORM).filter(SprintORM.id == sprint_id).first()
    if not sprint:
        log_message(f"Sprint {sprint_id} não encontrada", "warning")
    return sprint


def get_sprints(db: Session) -> List[SprintORM]:
    """Lista todas as sprints."""
    return db.query(SprintORM).all()


def get_sprints_by_project(db: Session, project_id: str) -> List[SprintORM]:
    """Lista todas as sprints de um projeto específico."""
    return db.query(SprintORM).filter(SprintORM.project_id == project_id).all()


def create_sprint(db: Session, project_id: str, sprint_data: SprintSchema) -> Optional[SprintORM]:
    """Cria uma nova sprint associada a um projeto."""
    try:
        project = db.query(ProjectORM).filter(ProjectORM.id == project_id).first()
        if not project:
            log_message(f"❌ Projeto {project_id} não encontrado ao criar sprint", "warning")
            return None

        sprint_dict = sprint_data.model_dump(by_alias=False, exclude_unset=True)
        sprint_dict.setdefault("id", str(uuid4()))
        sprint_dict["project_id"] = project_id

        sprint = SprintORM(**sprint_dict)
        db.add(sprint)
        db.commit()
        db.refresh(sprint)

        log_message(f"✅ Sprint criada com sucesso (projeto={project_id}, sprint={sprint.id})", "info")
        return sprint

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"💥 Erro ao criar sprint no projeto {project_id}: {e}", "error")
        return None


def update_sprint(db: Session, sprint_id: str, sprint_data: SprintSchema) -> Optional[SprintORM]:
    """Atualiza os dados de uma sprint existente."""
    sprint = get_sprint(db, sprint_id)
    if not sprint:
        return None

    try:
        data = sprint_data.model_dump(by_alias=False, exclude_unset=True)
        for key, value in data.items():
            setattr(sprint, key, value)

        sprint.updated_at = datetime.utcnow() if hasattr(sprint, "updated_at") else None
        db.commit()
        db.refresh(sprint)

        log_message(f"🔄 Sprint {sprint_id} atualizada com sucesso", "info")
        return sprint

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"💥 Erro ao atualizar sprint {sprint_id}: {e}", "error")
        return None


def toggle_sprint_status(db: Session, sprint_id: str, activate: bool) -> Optional[SprintORM]:
    """
    Ativa ou desativa uma sprint.
    - Se `activate=True`, ativa a sprint e desativa outras do mesmo projeto.
    - Se `activate=False`, apenas desativa.
    """
    sprint = get_sprint(db, sprint_id)
    if not sprint:
        log_message(f"⚠️ Sprint {sprint_id} não encontrada ao tentar alterar status", "warning")
        return None

    try:
        if activate:
            # Desativa outras sprints do mesmo projeto
            db.query(SprintORM).filter(
                SprintORM.project_id == sprint.project_id,
                SprintORM.id != sprint.id
            ).update({SprintORM.is_active: False})

            sprint.is_active = True
        else:
            sprint.is_active = False

        sprint.updated_at = datetime.utcnow() if hasattr(sprint, "updated_at") else None

        db.commit()
        db.refresh(sprint)

        estado = "ativada" if activate else "desativada"
        log_message(f"⚙️ Sprint {sprint_id} {estado} com sucesso", "info")
        return sprint

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"💥 Erro ao alterar status da sprint {sprint_id}: {e}", "error")
        return None


def delete_sprint(db: Session, sprint_id: str) -> bool:
    """Deleta uma sprint pelo ID."""
    sprint = get_sprint(db, sprint_id)
    if not sprint:
        log_message(f"⚠️ Tentativa de deletar sprint inexistente: {sprint_id}", "warning")
        return False

    try:
        db.delete(sprint)
        db.commit()
        log_message(f"🗑️ Sprint {sprint_id} deletada com sucesso", "info")
        return True

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"💥 Erro ao deletar sprint {sprint_id}: {e}", "error")
        return False
