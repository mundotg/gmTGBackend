from datetime import datetime
from typing import List, Optional
from uuid import uuid4
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models.task_models import Project as ProjectORM, Task as TaskORM
from app.schemas.task_schema import ProjectSchema, TaskSchema
from app.ultils.logger import log_message


# -----------------------------
# PROJETOS
# -----------------------------
def get_projects(db: Session) -> List[ProjectORM]:
    return db.query(ProjectORM).all()


def get_project(db: Session, project_id: str) -> Optional[ProjectORM]:
    project = db.query(ProjectORM).filter(ProjectORM.id == project_id).first()
    if not project:
        log_message(f"Projeto {project_id} não encontrado", "warning")
    return project

def create_project(db: Session, project: ProjectSchema) -> ProjectORM:
    try:
        # Preenche campos obrigatórios caso não existam
        project_data = project.model_dump(by_alias=False, exclude_unset=True)
        if "id" not in project_data or not project_data["id"]:
            project_data["id"] = str(uuid4())
        if "owner" not in project_data or not project_data["owner"]:
            project_data["owner"] = "Desconhecido"  # ou pegar do usuário logado
        if "created_at" not in project_data or not project_data["created_at"]:
            project_data["created_at"] = datetime.utcnow()

        # Cria ORM
        db_project = ProjectORM(**project_data)

        db.add(db_project)
        db.commit()
        db.refresh(db_project)
        log_message(f"Projeto criado: {db_project.id}", "info")
        return db_project

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao criar projeto: {e}", "error")
        raise e


def update_project(db: Session, project_id: str, project_data: ProjectORM) -> Optional[ProjectORM]:
    project = get_project(db, project_id)
    if not project:
        log_message(f"Projeto {project_id} não encontrado para atualização", "warning")
        return None
    try:
        data = {k: v for k, v in vars(project_data).items() if not k.startswith("_")}
        for attr, value in data.items():
            setattr(project, attr, value)
        db.commit()
        db.refresh(project)
        log_message(f"Projeto atualizado: {project_id}", "info")
        return project
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao atualizar projeto {project_id}: {e}", "error")
        return None


def delete_project(db: Session, project_id: str) -> bool:
    project = get_project(db, project_id)
    if not project:
        log_message(f"Tentativa de deletar projeto inexistente: {project_id}", "warning")
        return False
    try:
        db.delete(project)
        db.commit()
        log_message(f"Projeto deletado: {project_id}", "info")
        return True
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao deletar projeto {project_id}: {e}", "error")
        return False


# -----------------------------
# TAREFAS
# -----------------------------
def add_task(db: Session, project_id: str, task_data: TaskSchema) -> Optional[TaskORM]:
    project = get_project(db, project_id)
    if not project:
        log_message(f"Projeto {project_id} não encontrado ao adicionar tarefa", "warning")
        return None
    try:
        task_dict = task_data.model_dump(by_alias=False, exclude_unset=True)
        task = TaskORM(**task_dict)
        if not task.id:
            task.id = str(uuid4())
        project.tasks.append(task)
        db.add(task)
        db.commit()
        db.refresh(task)
        log_message(f"Tarefa adicionada ao projeto {project_id}: {task.id}", "info")
        return task
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao adicionar tarefa ao projeto {project_id}: {e}", "error")
        return None


def get_tasks(db: Session, project_id: str) -> Optional[List[TaskORM]]:
    project = get_project(db, project_id)
    if not project:
        log_message(f"Projeto {project_id} não encontrado ao listar tarefas", "warning")
        return None
    return project.tasks


def update_task(db: Session, project_id: str, task_id: str, task_data: TaskSchema) -> Optional[TaskORM]:
    project = get_project(db, project_id)
    if not project:
        log_message(f"Projeto {project_id} não encontrado ao atualizar tarefa", "warning")
        return None
    task = next((t for t in project.tasks if t.id == task_id), None)
    if not task:
        log_message(f"Tarefa {task_id} não encontrada no projeto {project_id}", "warning")
        return None
    try:
        task_dict = task_data.model_dump(by_alias=False, exclude_unset=True)
        for attr, value in task_dict.items():
            setattr(task, attr, value)
        db.commit()
        db.refresh(task)
        log_message(f"Tarefa {task_id} atualizada no projeto {project_id}", "info")
        return task
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao atualizar tarefa {task_id}: {e}", "error")
        return None


def delete_task(db: Session, project_id: str, task_id: str) -> bool:
    project = get_project(db, project_id)
    if not project:
        log_message(f"Projeto {project_id} não encontrado ao deletar tarefa", "warning")
        return False
    task = next((t for t in project.tasks if t.id == task_id), None)
    if not task:
        log_message(f"Tarefa {task_id} não encontrada no projeto {project_id}", "warning")
        return False
    try:
        db.delete(task)
        db.commit()
        log_message(f"Tarefa {task_id} deletada do projeto {project_id}", "info")
        return True
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao deletar tarefa {task_id}: {e}", "error")
        return False
