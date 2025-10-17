import traceback
from typing import List, Optional, Any, Dict, Type, TypeVar
from uuid import uuid4
from sqlalchemy import func, select, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.cruds.project_cruds import get_project
from app.models.task_models import Task as TaskORM
from app.schemas.task_schema import TaskSchema
from app.ultils.logger import log_message

T = TypeVar("T")


# -----------------------------------------------------
# 🧩 ADICIONAR TAREFA
# -----------------------------------------------------
def add_task(db: Session, project_id: str, task_data: TaskSchema) -> Optional[TaskORM]:
    """Cria uma nova tarefa associada a um projeto."""
    try:
        if not get_project(db, project_id):
            log_message(f"Projeto {project_id} não encontrado ao adicionar tarefa", "warning")
            return None

        task_dict = task_data.model_dump(by_alias=False, exclude_unset=True)
        task_dict.setdefault("id", str(uuid4()))
        task_dict.setdefault("project_id",project_id)
        task = TaskORM(**task_dict)

        db.add(task)
        db.commit()

        log_message(f"Tarefa criada com sucesso: {task.id} (Projeto {project_id})", "info")
        return task

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro SQL ao adicionar tarefa no projeto {project_id}: {e}", "error")
        return None


# -----------------------------------------------------
# 📋 LISTAR TAREFAS
# -----------------------------------------------------
def get_tasks(db: Session, project_id: str) -> List[TaskORM]:
    """Lista todas as tarefas de um projeto (sem carregar o Project inteiro)."""
    try:
        return db.query(TaskORM).filter(TaskORM.project_id == project_id).all()
    except SQLAlchemyError as e:
        log_message(f"Erro ao listar tarefas do projeto {project_id}: {e}", "error")
        return []


# -----------------------------------------------------
# 🔍 BUSCAR TAREFA POR ID
# -----------------------------------------------------
def get_task_by_id(db: Session, task_id: str) -> Optional[TaskORM]:
    """Busca uma tarefa pelo ID."""
    try:
        return db.get(TaskORM, task_id)
    except SQLAlchemyError as e:
        log_message(f"Erro ao buscar tarefa {task_id}: {e}", "error")
        return None


# -----------------------------------------------------
# 🔄 ATUALIZAR TAREFA
# -----------------------------------------------------
def update_task(db: Session, project_id: str, task_id: str, task_data: TaskSchema) -> Optional[TaskORM]:
    """Atualiza uma tarefa existente com base nos dados do schema."""
    task = get_task_by_id(db, task_id)
    if not task:
        log_message(f"Tarefa {task_id} não encontrada", "warning")
        return None

    try:
        updates = task_data.model_dump(by_alias=False, exclude_unset=True)
        for attr, value in updates.items():
            setattr(task, attr, value)

        db.commit()
        log_message(f"Tarefa {task_id} atualizada no projeto {project_id}", "info")
        return task

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro SQL ao atualizar tarefa {task_id}: {e}", "error")
        return None

    except Exception as e:
        db.rollback()
        log_message(f"Erro inesperado ao atualizar tarefa {task_id}: {traceback.format_exc()}", "error")
        return None


# -----------------------------------------------------
# ❌ DELETAR TAREFA
# -----------------------------------------------------
def delete_task(db: Session, project_id: str, task_id: str) -> bool:
    """Remove uma tarefa de um projeto."""
    try:
        task = get_task_by_id(db, task_id)
        if not task:
            log_message(f"Tarefa {task_id} não encontrada", "warning")
            return False

        db.delete(task)
        db.commit()
        log_message(f"Tarefa {task_id} deletada do projeto {project_id}", "info")
        return True

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao deletar tarefa {task_id}: {e}", "error")
        return False


# -----------------------------------------------------
# 🧭 DELEGAR TAREFA
# -----------------------------------------------------
def delegate_task(db: Session, task_id: str, new_user_id: str) -> Optional[TaskORM]:
    """Atribui a tarefa a outro usuário (delegação)."""
    try:
        task = get_task_by_id(db, task_id)
        if not task:
            log_message(f"Tarefa {task_id} não encontrada", "warning")
            return None

        if task.delegated_to_id == new_user_id:
            log_message(f"Tarefa {task_id} já está delegada para {new_user_id}", "info")
            return task

        task.delegated_to_id = new_user_id
        db.commit()
        log_message(f"Tarefa {task_id} delegada para usuário {new_user_id}", "info")
        return task

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao delegar tarefa {task_id}: {e}", "error")
        return None


# -----------------------------------------------------
# ✅ VALIDAR TAREFA
# -----------------------------------------------------
def validate_task(db: Session, task_id: str,aprovado:bool=True, validator_id: Optional[str] = None) -> Optional[TaskORM]:
    """Marca uma tarefa como validada e concluída."""
    try:
        task = get_task_by_id(db, task_id)
        if not task:
            log_message(f"Tarefa {task_id} não encontrada", "warning")
            return None

        task.is_validated = aprovado
        task.status = "concluida"
        task.completed_at = func.now()

        # if validator_id:
        #     task.delegated_to_id = validator_id

        db.commit()
        log_message(f"Tarefa {task_id} validada com sucesso", "info")
        return task

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao validar tarefa {task_id}: {e}", "error")
        return None


# -----------------------------------------------------
# 📄 PAGINAÇÃO E FILTROS
# -----------------------------------------------------
def get_paginated_query(
    db: Session,
    model: Type,
    search: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    page: int = 1,
    limit: int = 10,
):
    """Retorna resultados paginados de qualquer modelo."""
    try:
        query = select(model)

        # 🔍 Busca textual (somente em colunas de texto)
        if search:
            or_conditions = [
                col.ilike(f"%{search}%")
                for col in model.__table__.columns
                if hasattr(col.type, "python_type") and col.type.python_type == str
            ]
            if or_conditions:
                query = query.filter(or_(*or_conditions))

        # ⚙️ Filtros dinâmicos
        if filters:
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)

        total = db.scalar(select(func.count()).select_from(query.subquery()))
        offset = (page - 1) * limit
        items = db.scalars(query.offset(offset).limit(limit)).all()

        return {
            "items": items,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit,
        }

    except SQLAlchemyError as e:
        log_message(f"Erro ao executar consulta paginada: {e}", "error")
        return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}
