import traceback
from datetime import datetime
from typing import List, Optional, Any, Dict, Type, TypeVar

from pydantic import ValidationError
from sqlalchemy import func, select, or_, exists
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.sqltypes import String, Text

from app.cruds.project_cruds import get_project
from app.models.connection_models import DBConnection
from app.models.task_models import (
    Project,
    Sprint,
    Task as TaskORM,
    TaskStats,
    TypeProjecto,
)
from app.models.user_model import Role, User
from app.schemas.connetion_schema import DBConnectionBase
from app.schemas.project_schemas import ProjectResponseSchema, TypeProjectoSchema
from app.schemas.sprint_schemas import SprintSchema
from app.schemas.task_schema import (
    TaskCreateSchema,
    TaskSchema,
    TaskStatsSchema,
    TaskUpdateSchema,
)
from app.schemas.userTask_schemas import RoleSchema
from app.schemas.users_schemas import UserOut
from app.ultils.logger import log_message

T = TypeVar("T")


# -----------------------------------------------------
# 🧩 ADICIONAR TAREFA
# -----------------------------------------------------
def _to_int_id(value: Any) -> Optional[int]:
    """
    Converte um ID vindo do frontend (string) para o inteiro da chave primária.
    Devolve None quando o valor está vazio ou não é numérico, para o campo
    ficar a NULL em vez de rebentar a query.
    """
    if value in (None, "", "null", "undefined"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def add_task(
    db: Session,
    project_id: str,
    task_data: TaskCreateSchema,
    created_by_id: Optional[int] = None,
) -> Optional[TaskORM]:
    """Cria uma nova tarefa associada a um projeto."""
    try:
        if not get_project(db, project_id):
            log_message(
                f"Projeto {project_id} não encontrado ao adicionar tarefa", "warning"
            )
            return None

        task_dict = task_data.model_dump(by_alias=False, exclude_unset=True)

        # ⚠️ Não gerar `id`: a chave primária é um Integer com autoincremento.
        # O código antigo fazia `setdefault("id", str(uuid4()))`, o que tentava
        # escrever um UUID numa coluna inteira.
        task_dict.pop("id", None)

        # O agendamento é um sub-schema; a coluna é JSON.
        schedule = task_dict.get("schedule")
        if hasattr(schedule, "to_json"):
            task_dict["schedule"] = schedule.to_json()

        # IDs chegam como string do frontend, mas as FKs são Integer.
        for campo in ("assigned_to_id", "delegated_to_id", "sprint_id"):
            if campo in task_dict:
                task_dict[campo] = _to_int_id(task_dict[campo])

        task_dict["project_id"] = _to_int_id(project_id)
        task_dict["created_by_id"] = created_by_id

        task = TaskORM(**task_dict)
        db.add(task)

        db.commit()
        # garante que o objeto está atualizado com defaults do banco
        db.refresh(task)
        return task

    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"Erro SQL ao adicionar tarefa no projeto {project_id}: {e}", "error"
        )
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
            .order_by(
                TaskORM.created_at.desc()
                if hasattr(TaskORM, "created_at")
                else TaskORM.id.desc()
            )
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
def update_task(
    db: Session, project_id: str, task_id: str, task_data: TaskUpdateSchema
) -> Optional[TaskORM]:
    """Atualiza uma tarefa existente com base nos dados do schema."""
    task = get_task_by_id(db, task_id)
    if not task:
        log_message(f"Tarefa {task_id} não encontrada", "warning")
        return None

    try:
        updates = task_data.model_dump(by_alias=False, exclude_unset=True)

        # A chave primária e o projeto nunca são alterados por um update.
        for imutavel in ("id", "project_id", "created_by_id"):
            updates.pop(imutavel, None)

        schedule = updates.get("schedule")
        if hasattr(schedule, "to_json"):
            updates["schedule"] = schedule.to_json()

        for campo in ("assigned_to_id", "delegated_to_id", "sprint_id"):
            if campo in updates:
                updates[campo] = _to_int_id(updates[campo])

        changed = False
        for attr, value in updates.items():
            if hasattr(task, attr) and getattr(task, attr) != value:
                setattr(task, attr, value)
                changed = True

        # Concluir a tarefa carimba a data; reabri-la limpa-a.
        if "status" in updates:
            if updates["status"] == "concluida" and not task.completed_at:
                task.completed_at = datetime.utcnow()
                changed = True
            elif updates["status"] != "concluida" and task.completed_at:
                task.completed_at = None
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
        log_message(
            f"Erro inesperado ao atualizar tarefa {task_id}: {traceback.format_exc()}",
            "error",
        )
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
    """
    Delega a tarefa a outro utilizador.

    Passa a ser essa pessoa o responsável (`assigned_to_id`) e guarda-se em
    `delegated_to_id` o registo de que a tarefa foi delegada. Antes só se
    escrevia `delegated_to_id`, pelo que a tarefa continuava a aparecer para
    o responsável antigo.
    """
    try:
        task = get_task_by_id(db, task_id)
        if not task:
            log_message(f"Tarefa {task_id} não encontrada", "warning")
            return None

        destino = _to_int_id(new_user_id)
        if destino is None:
            log_message(f"ID de utilizador inválido na delegação: {new_user_id}", "warning")
            return None

        if not db.get(User, destino):
            log_message(f"Utilizador {destino} não existe (delegação)", "warning")
            return None

        if task.assigned_to_id == destino and task.delegated_to_id == destino:
            return task

        task.delegated_to_id = destino
        task.assigned_to_id = destino

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
    validator_id: Optional[str] = None,
    comentario: Optional[str] = None,
) -> Optional[TaskORM]:
    """
    Aprova ou reprova uma tarefa.

    Aprovar conclui a tarefa. **Reprovar devolve-a ao trabalho** (`em_andamento`)
    e limpa a data de conclusão — antes, reprovar marcava-a igualmente como
    "concluida", pelo que uma tarefa rejeitada contava como feita nas
    estatísticas.
    """
    try:
        task = get_task_by_id(db, task_id)
        if not task:
            log_message(f"Tarefa {task_id} não encontrada", "warning")
            return None

        task.is_validated = aprovado
        task.comentario_is_validated = comentario or None
        task.validated_by_id = _to_int_id(validator_id)
        task.validated_at = datetime.utcnow()

        if aprovado:
            task.status = "concluida"
            task.completed_at = task.completed_at or datetime.utcnow()
        else:
            task.status = "em_andamento"
            task.completed_at = None

        db.commit()
        db.refresh(task)

        log_message(
            f"Tarefa {task_id} {'aprovada' if aprovado else 'reprovada'}"
            f"{f' — {comentario}' if comentario else ''}",
            "info",
        )
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
                    log_message(
                        f"Relação '{relation}' não encontrada no modelo {model.__name__}",
                        "warning",
                    )

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
            if not hasattr(model, key):
                continue

            coluna = getattr(model, key)

            # Sentinela para "sem valor" (ex.: tarefas no backlog, sem sprint).
            # Um `None` no JSON não serve: o `continue` acima descartava-o e o
            # filtro era ignorado em silêncio.
            if value == "__null__":
                query = query.filter(coluna.is_(None))
            else:
                query = query.filter(coluna == value)

        # ✅ total count eficiente
        total = (
            db.scalar(select(func.count()).select_from(query.order_by(None).subquery()))
            or 0
        )

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
        log_message(
            f"Erro inesperado na consulta paginada: {traceback.format_exc()}", "error"
        )
        return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}
