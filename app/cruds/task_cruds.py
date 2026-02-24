import traceback
from typing import List, Optional, Any, Dict, Type, TypeVar
from uuid import uuid4

from pydantic import ValidationError
from sqlalchemy import func, select, or_, exists
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.sqltypes import String, Text

from app.cruds.project_cruds import get_project
from app.models.connection_models import DBConnection
from app.models.task_models import Project, Sprint, Task as TaskORM, TaskStats, TypeProjecto
from app.models.user_model import Role, User
from app.schemas.connetion_schema import DBConnectionBase
from app.schemas.project_schemas import ProjectResponseSchema, TypeProjectoSchema
from app.schemas.sprint_schemas import SprintSchema
from app.schemas.task_schema import TaskSchema, TaskStatsSchema
from app.schemas.userTask_schemas import RoleSchema
from app.schemas.users_chemas import UserOut
from app.ultils.logger import log_message

T = TypeVar("T")


# -----------------------------------------------------
# 🧩 ADICIONAR TAREFA
# -----------------------------------------------------
def add_task(db: Session, project_id: str, task_data: TaskSchema) -> Optional[TaskORM]:
    """Cria uma nova tarefa associada a um projeto."""
    try:
        # PERFORMANCE: se você só quer saber se existe, use exists()
        # Mantive get_project, mas dá para trocar se quiser.
        if not get_project(db, project_id):
            log_message(f"Projeto {project_id} não encontrado ao adicionar tarefa", "warning")
            return None

        task_dict = task_data.model_dump(by_alias=False, exclude_unset=True)
        task_dict.setdefault("id", str(uuid4()))
        task_dict.setdefault("project_id", project_id)

        task = TaskORM(**task_dict)
        db.add(task)

        db.commit()
        # garante que o objeto está atualizado com defaults do banco
        db.refresh(task)
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
        return (
            db.query(TaskORM)
            .filter(TaskORM.project_id == project_id)
            .order_by(TaskORM.created_at.desc() if hasattr(TaskORM, "created_at") else TaskORM.id.desc())
            .all()
        )
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

        changed = False
        for attr, value in updates.items():
            if hasattr(task, attr) and getattr(task, attr) != value:
                setattr(task, attr, value)
                changed = True

        if changed:
            db.commit()
            db.refresh(task)

        return task

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro SQL ao atualizar tarefa {task_id}: {e}", "error")
        return None

    except Exception:
        db.rollback()
        log_message(f"Erro inesperado ao atualizar tarefa {task_id}: {traceback.format_exc()}", "error")
        return None


# -----------------------------------------------------
# ❌ DELETAR TAREFA
# -----------------------------------------------------
def delete_task(db: Session, project_id: str, task_id: str) -> bool:
    """Remove uma tarefa de um projeto."""
    try:
        # PERFORMANCE: delete direto (sem carregar o objeto) é mais barato
        deleted = (
            db.query(TaskORM)
            .filter(TaskORM.id == task_id, TaskORM.project_id == project_id)
            .delete(synchronize_session=False)
        )
        db.commit()
        return deleted > 0

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
            return task

        task.delegated_to_id = new_user_id
        db.commit()
        db.refresh(task)
        return task

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao delegar tarefa {task_id}: {e}", "error")
        return None


# -----------------------------------------------------
# ✅ VALIDAR TAREFA
# -----------------------------------------------------
def validate_task(
    db: Session,
    task_id: str,
    aprovado: bool = True,
    validator_id: Optional[str] = None
) -> Optional[TaskORM]:
    """Marca uma tarefa como validada e concluída."""
    try:
        task = get_task_by_id(db, task_id)
        if not task:
            log_message(f"Tarefa {task_id} não encontrada", "warning")
            return None

        # Evita commits repetidos se já estiver validado/concluído
        changed = False

        if getattr(task, "is_validated", None) != aprovado:
            task.is_validated = aprovado
            changed = True

        if getattr(task, "status", None) != "concluida":
            task.status = "concluida"
            changed = True

        # Usa func.now() para timestamp do banco
        task.completed_at = func.now()
        changed = True

        # if validator_id:
        #     task.delegated_to_id = validator_id

        if changed:
            db.commit()
            db.refresh(task)

        return task

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao validar tarefa {task_id}: {e}", "error")
        return None


# -----------------------------------------------------
# 📦 PAGINAÇÃO GENÉRICA (otimizada)
# -----------------------------------------------------
def get_paginated_query(
    db: Session,
    model: Type,
    search: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    page: int = 1,
    limit: int = 10,
    relationships: Optional[List[str]] = None,
):
    """
    Retorna resultados paginados de qualquer modelo com suporte a relações e schemas.
    Melhorias:
    - saneamento de page/limit
    - count eficiente (sem ORDER BY e sem subquery pesada desnecessária)
    - search apenas em colunas String/Text
    - filtros com segurança
    - selectinload para relações existentes
    """
    try:
        page = max(int(page or 1), 1)
        limit = min(max(int(limit or 10), 1), 100)
        offset = (page - 1) * limit
        filters = filters or {}

        query = select(model)

        # 🔗 Carregar relações se especificadas
        if relationships:
            for relation in relationships:
                if hasattr(model, relation):
                    query = query.options(selectinload(getattr(model, relation)))
                else:
                    log_message(f"Relação '{relation}' não encontrada no modelo {model.__name__}", "warning")

        # 🔍 Busca textual (somente String/Text)
        if search:
            s = f"%{search.strip()}%"
            str_cols = []
            for col in model.__table__.columns:
                try:
                    if isinstance(col.type, (String, Text)):
                        str_cols.append(col.ilike(s))
                except Exception:
                    pass

            if str_cols:
                query = query.filter(or_(*str_cols))

        # ⚙️ Filtros dinâmicos
        for key, value in filters.items():
            if value is None:
                continue
            if hasattr(model, key):
                query = query.filter(getattr(model, key) == value)

        # ✅ total count eficiente
        total = db.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0

        # Itens
        items = db.scalars(query.offset(offset).limit(limit)).all()

        # 🎯 Converter modelos para schemas
        full_schema_map = {
            User: UserOut,
            Project: ProjectResponseSchema,
            TaskORM: TaskSchema,
            Sprint: SprintSchema,
            TypeProjecto: TypeProjectoSchema,
            Role: RoleSchema,
            TaskStats: TaskStatsSchema,
            DBConnection: DBConnectionBase,
        }

        schema_class = full_schema_map.get(model)

        if schema_class and items:
            try:
                resultado = [schema_class.model_validate(item) for item in items]
            except ValidationError:
                # Se falhar, devolve raw
                resultado = items
        else:
            resultado = items

        return {
            "items": resultado,
            "total": int(total),
            "page": page,
            "limit": limit,
            "pages": (int(total) + limit - 1) // limit if limit > 0 else 0,
        }

    except SQLAlchemyError as e:
        log_message(f"Erro ao executar consulta paginada: {e}", "error")
        return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}

    except Exception:
        log_message(f"Erro inesperado na consulta paginada: {traceback.format_exc()}", "error")
        return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}
