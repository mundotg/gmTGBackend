from datetime import datetime
from typing import List, Optional
from uuid import uuid4
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.schemas.project_schemas import ProjectSchema
from app.ultils.logger import log_message
from app.models.connection_models import DBConnection
from app.models.task_models import TypeProjecto as TypeProjectORM,Usuario as UsuarioORM, Project as ProjectORM


# -----------------------------
# PROJETOS
# -----------------------------
def get_projects(db: Session) -> List[ProjectORM]:
    """
    Retorna todos os projetos cadastrados.
    """
    return db.query(ProjectORM).all()


def get_project(db: Session, project_id: str) -> Optional[ProjectORM]:
    """
    Busca um projeto específico pelo ID.
    """
    project = db.query(ProjectORM).filter(ProjectORM.id == project_id).first()
    if not project:
        log_message(f"⚠️ Projeto {project_id} não encontrado", "warning")
    return project


def create_project(db: Session, project: ProjectSchema) -> Optional[ProjectORM]:
    """
    Cria um novo projeto com dados completos (incluindo type_project, connection e equipe).
    """
    try:
        # Remove campos que não são diretos da tabela principal
        project_data = project.model_dump(
            by_alias=False,
            exclude={"team", "tasks", "sprint", "type_project", "connection"}
        )

        project_data.setdefault("id", str(uuid4()))
        project_data.setdefault("created_at", datetime.utcnow())
        project_data.setdefault("is_active", True)

        # Buscar e associar TypeProject se existir
        type_project_obj = None
        if project.type_project and getattr(project.type_project, "id", None):
            type_project_obj = db.query(TypeProjectORM).filter(
                TypeProjectORM.id == project.type_project.id
            ).first()

        # Buscar e associar Connection se existir
        connection_obj = None
        if project.connection and getattr(project.connection, "id", None):
            connection_obj = db.query(DBConnection).filter(
                DBConnection.id == project.connection.id
            ).first()

        # Criar o projeto
        db_project = ProjectORM(
            **project_data,
            type_project=type_project_obj,
            db_connection=connection_obj
        )
        db.add(db_project)
        db.flush()  # Garante que o ID do projeto já exista antes de associações

        # 🔗 Associa membros da equipe
        if project.team:
            team_ids = [str(uid) for uid in project.team]
            users = db.query(UsuarioORM).filter(UsuarioORM.id.in_(team_ids)).all()
            db_project.team_members.extend(users)

        db.commit()
        db.refresh(db_project)

        log_message(f"✅ Projeto criado com sucesso: {db_project.id}", "info")
        return db_project

    except SQLAlchemyError as e:
        db.rollback()
        import traceback
        log_message(f'{e}{traceback.format_exc()}', "error") 
        return None
    except Exception as e:
        db.rollback()
        import traceback
        log_message(f'{e}{traceback.format_exc()}', "error") 
        return None


def update_project(db: Session, project_id: str, project_data: ProjectSchema) -> Optional[ProjectORM]:
    """
    Atualiza informações de um projeto existente, incluindo relacionamentos.
    """
    project = get_project(db, project_id)
    if not project:
        return None

    try:
        updates = project_data.model_dump(exclude_unset=True, by_alias=False)

        # Atualiza campos diretos
        for attr, value in updates.items():
            if hasattr(project, attr):
                setattr(project, attr, value)

        # Atualiza relacionamentos, se enviados
        if project_data.type_project and getattr(project_data.type_project, "id", None):
            type_project_obj = db.query(TypeProjectORM).filter(
                TypeProjectORM.id == project_data.type_project.id
            ).first()
            project.type_project = type_project_obj

        if project_data.connection and getattr(project_data.connection, "id", None):
            connection_obj = db.query(DBConnection).filter(
                DBConnection.id == project_data.connection.id
            ).first()
            project.connection = connection_obj

        if project_data.team:
            team_ids = [str(uid) for uid in project_data.team]
            users = db.query(UsuarioORM).filter(UsuarioORM.id.in_(team_ids)).all()
            project.team_members = users

        project.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(project)

        log_message(f"🛠️ Projeto atualizado: {project_id}", "info")
        return project

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao atualizar projeto {project_id}: {e}", "error")
        return None


def cancel_project(db: Session, project_id: str, reason: str) -> Optional[ProjectORM]:
    """
    Cancela um projeto (define como inativo e registra o motivo e data do cancelamento).
    """
    project = get_project(db, project_id)
    if not project:
        log_message(f"⚠️ Projeto {project_id} não encontrado para cancelamento", "warning")
        return None

    if not project.is_active:
        log_message(f"⚠️ Projeto {project_id} já está inativo/cancelado", "warning")
        return project

    try:
        project.is_active = False
        project.cancel_reason = reason
        project.cancelled_at = datetime.utcnow()
        db.commit()
        db.refresh(project)

        log_message(f"🚫 Projeto {project_id} cancelado. Motivo: {reason}", "info")
        return project
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao cancelar projeto {project_id}: {e}", "error")
        return None


def delete_project(db: Session, project_id: str) -> bool:
    """
    Exclui um projeto do banco de dados.
    """
    project = get_project(db, project_id)
    if not project:
        log_message(f"⚠️ Tentativa de deletar projeto inexistente: {project_id}", "warning")
        return False

    try:
        db.delete(project)
        db.commit()
        log_message(f"🗑️ Projeto deletado: {project_id}", "info")
        return True
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao deletar projeto {project_id}: {e}", "error")
        return False
