from datetime import datetime, timezone
from typing import Optional, List
from uuid import uuid4

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import exists

from app.models.task_models import Project as ProjectORM, Sprint as SprintORM
from app.schemas.sprint_schemas import SprintSchema
from app.ultils.logger import log_message


# ==================================================
# 🔹 CRUD DE SPRINTS (otimizado)
# ==================================================

def get_sprint(db: Session, sprint_id: str) -> Optional[SprintORM]:
    """Obtém uma sprint pelo ID."""
    try:
        sprint = db.query(SprintORM).filter(SprintORM.id == sprint_id).first()
        if not sprint:
            log_message(f"Sprint {sprint_id} não encontrada", "warning")
        return sprint
    except SQLAlchemyError as e:
        log_message(f"💥 Erro ao buscar sprint {sprint_id}: {e}", "error")
        return None


def get_sprints(db: Session) -> List[SprintORM]:
    """Lista todas as sprints."""
    try:
        return (
            db.query(SprintORM)
            .order_by(SprintORM.created_at.desc() if hasattr(SprintORM, "created_at") else SprintORM.id.desc())
            .all()
        )
    except SQLAlchemyError as e:
        log_message(f"💥 Erro ao listar sprints: {e}", "error")
        return []


def get_sprints_by_project(db: Session, project_id: str) -> List[SprintORM]:
    """Lista todas as sprints de um projeto específico."""
    try:
        return (
            db.query(SprintORM)
            .filter(SprintORM.project_id == project_id)
            .order_by(
                SprintORM.is_active.desc() if hasattr(SprintORM, "is_active") else SprintORM.id.desc(),
                SprintORM.created_at.desc() if hasattr(SprintORM, "created_at") else SprintORM.id.desc(),
            )
            .all()
        )
    except SQLAlchemyError as e:
        log_message(f"💥 Erro ao listar sprints do projeto {project_id}: {e}", "error")
        return []


def create_sprint(db: Session, project_id: str, sprint_data: SprintSchema) -> Optional[SprintORM]:
    """Cria uma nova sprint associada a um projeto."""
    try:
        # PERFORMANCE: exists() é bem mais barato do que carregar o Project inteiro
        project_exists = db.query(
            exists().where(ProjectORM.id == project_id)
        ).scalar()

        if not project_exists:
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

        changed = False
        for key, value in data.items():
            if hasattr(sprint, key) and getattr(sprint, key) != value:
                setattr(sprint, key, value)
                changed = True

        # só atualiza updated_at se mudou algo
        if changed and hasattr(sprint, "updated_at"):
            sprint.updated_at = datetime.now(timezone.utc)

        if changed:
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
        changed = False

        if activate:
            # Desativa outras sprints do mesmo projeto (bulk update mais rápido)
            db.query(SprintORM).filter(
                SprintORM.project_id == sprint.project_id,
                SprintORM.id != sprint.id,
                SprintORM.is_active.is_(True)
            ).update({SprintORM.is_active: False}, synchronize_session=False)

            if sprint.is_active is not True:
                sprint.is_active = True
                changed = True
        else:
            if sprint.is_active is not False:
                sprint.is_active = False
                changed = True

        if hasattr(sprint, "updated_at") and (changed or activate):
            sprint.updated_at = datetime.now(timezone.utc)

        # Mesmo se "changed" for False, o update em batch pode ter acontecido quando activate=True.
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
    try:
        # PERFORMANCE: delete direto, sem carregar o objeto (mais rápido)
        deleted = (
            db.query(SprintORM)
            .filter(SprintORM.id == sprint_id)
            .delete(synchronize_session=False)
        )

        if not deleted:
            log_message(f"⚠️ Tentativa de deletar sprint inexistente: {sprint_id}", "warning")
            return False

        db.commit()
        log_message(f"🗑️ Sprint {sprint_id} deletada com sucesso", "info")
        return True

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"💥 Erro ao deletar sprint {sprint_id}: {e}", "error")
        return False
