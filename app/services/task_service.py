import traceback
from typing import Any, Dict, List, Optional, Type
from sqlalchemy import Numeric, and_, case, cast, func, select
from typing_extensions import Literal
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.cruds.task_cruds import delegate_task, get_paginated_query, get_tasks, add_task,  update_task, delete_task, validate_task
from app.models.connection_models import DBConnection
from app.models.task_models import AuditLog, Project, Sprint, Task, TaskStats, TypeProjecto, project_team_association
from app.models.user_model import Role, User
from app.schemas.task_schema import  TaskSchema, TaskStatsSchema
from app.ultils.logger import log_message


def list_tasks_service(db: Session, project_id: Optional[str]) -> List[TaskSchema]:
    """
    Lista todas as tarefas de um projeto específico.
    Inclui logs detalhados e tratamento de exceções.
    """
    try:
        if not project_id:
            log_message("Tentativa de listar tarefas com project_id vazio", level="warning")
            raise HTTPException(status_code=400, detail="O ID do projeto é obrigatório")

        tasks = get_tasks(db, project_id)

        if tasks is None:
            log_message(f"Projeto {project_id} não encontrado ao listar tarefas", level="warning")
            raise HTTPException(status_code=404, detail="Projeto não encontrado")

        log_message(f"Tarefas listadas com sucesso para o projeto {project_id}", level="info")
        return tasks

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        log_message(
            f"Erro de banco de dados ao listar tarefas do projeto {project_id}: {str(e)}\n{traceback.format_exc()}",
            level="error"
        )
        raise HTTPException(status_code=500, detail="Erro ao acessar o banco de dados")
    except Exception as e:
        log_message(
            f"Erro inesperado ao listar tarefas do projeto {project_id}: {str(e)}\n{traceback.format_exc()}",
            level="critical"
        )
        raise HTTPException(status_code=500, detail="Erro interno ao listar tarefas")


def add_task_service(db: Session, project_id: Optional[str], task: TaskSchema) -> TaskSchema:
    """
    Adiciona uma nova tarefa ao projeto especificado.
    """
    try:
        if not project_id:
            log_message("Tentativa de adicionar tarefa sem project_id", level="warning")
            raise HTTPException(status_code=400, detail="O ID do projeto é obrigatório")

        if not task or not task.title:
            log_message("Tentativa de adicionar tarefa inválida (campos obrigatórios ausentes)", level="warning")
            raise HTTPException(status_code=400, detail="Dados da tarefa inválidos")

        new_task = add_task(db, project_id, task)

        if not new_task:
            log_message(f"Projeto {project_id} não encontrado ao adicionar tarefa", level="warning")
            raise HTTPException(status_code=404, detail="Projeto não encontrado")

        log_message(f"Tarefa '{task.title}' adicionada ao projeto {project_id}", level="info")
        return new_task

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        log_message(
            f"Erro de banco de dados ao adicionar tarefa em {project_id}: {str(e)}\n{traceback.format_exc()}",
            level="error"
        )
        raise HTTPException(status_code=500, detail="Erro ao salvar tarefa no banco de dados")
    except Exception as e:
        log_message(
            f"Erro inesperado ao adicionar tarefa no projeto {project_id}: {str(e)}\n{traceback.format_exc()}",
            level="critical"
        )
        raise HTTPException(status_code=500, detail="Erro interno ao adicionar tarefa")


def update_task_service(db: Session, project_id: Optional[str], task_id: Optional[str], task: TaskSchema) -> TaskSchema:
    """
    Atualiza uma tarefa existente dentro de um projeto.
    """
    try:
        if not project_id or not task_id:
            log_message("Tentativa de atualizar tarefa com ID ausente", level="warning")
            raise HTTPException(status_code=400, detail="O ID do projeto e da tarefa são obrigatórios")

        updated_task = update_task(db, project_id, task_id, task)

        if not updated_task:
            log_message(f"Tarefa {task_id} ou projeto {project_id} não encontrado ao atualizar", level="warning")
            raise HTTPException(status_code=404, detail="Tarefa ou projeto não encontrado")

        log_message(f"Tarefa {task_id} atualizada com sucesso no projeto {project_id}", level="info")
        return updated_task

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        log_message(
            f"Erro de banco de dados ao atualizar tarefa {task_id} em {project_id}: {str(e)}\n{traceback.format_exc()}",
            level="error"
        )
        raise HTTPException(status_code=500, detail="Erro ao atualizar tarefa no banco de dados")
    except Exception as e:
        log_message(
            f"Erro inesperado ao atualizar tarefa {task_id} no projeto {project_id}: {str(e)}\n{traceback.format_exc()}",
            level="critical"
        )
        raise HTTPException(status_code=500, detail="Erro interno ao atualizar tarefa")


def delete_task_service(db: Session, project_id: Optional[str], task_id: Optional[str]) -> dict:
    """
    Exclui uma tarefa de um projeto.
    """
    try:
        if not project_id or not task_id:
            log_message("Tentativa de exclusão de tarefa com ID ausente", level="warning")
            raise HTTPException(status_code=400, detail="O ID do projeto e da tarefa são obrigatórios")

        success = delete_task(db, project_id, task_id)

        if not success:
            log_message(f"Tarefa {task_id} ou projeto {project_id} não encontrado ao excluir", level="warning")
            raise HTTPException(status_code=404, detail="Tarefa ou projeto não encontrado")

        log_message(f"Tarefa {task_id} deletada com sucesso do projeto {project_id}", level="info")
        return {"detail": f"Tarefa {task_id} deletada com sucesso"}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        log_message(
            f"Erro de banco de dados ao deletar tarefa {task_id} do projeto {project_id}: {str(e)}\n{traceback.format_exc()}",
            level="error"
        )
        raise HTTPException(status_code=500, detail="Erro ao excluir tarefa do banco de dados")
    except Exception as e:
        log_message(
            f"Erro inesperado ao deletar tarefa {task_id} do projeto {project_id}: {str(e)}\n{traceback.format_exc()}",
            level="critical"
        )
        raise HTTPException(status_code=500, detail="Erro interno ao deletar tarefa")
    
    



# -----------------------------------------------------
# 🧭 DELEGAR TAREFA - SERVICE
# -----------------------------------------------------
def delegate_task_service(db: Session, task_id: str, new_user_id: str,assigned_to:Optional[str]=None) -> TaskSchema:
    """
    Serviço para delegar uma tarefa para outro usuário.
    """
    try:
        if not task_id or not new_user_id:
            log_message("Tentativa de delegar tarefa com parâmetros ausentes", level="warning")
            raise HTTPException(status_code=400, detail="O ID da tarefa e do novo usuário são obrigatórios")

        delegated_task = delegate_task(db, task_id, new_user_id)

        if not delegated_task:
            log_message(f"Falha ao delegar tarefa {task_id}", level="warning")
            raise HTTPException(status_code=404, detail="Tarefa não encontrada para delegação")

        log_message(f"Tarefa {task_id} delegada com sucesso para o usuário {new_user_id}", level="info")
        return delegated_task

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"Erro de banco de dados ao delegar tarefa {task_id} para usuário {new_user_id}: {str(e)}\n{traceback.format_exc()}",
            level="error"
        )
        raise HTTPException(status_code=500, detail="Erro ao delegar tarefa no banco de dados")
    except Exception as e:
        db.rollback()
        log_message(
            f"Erro inesperado ao delegar tarefa {task_id} para {new_user_id}: {str(e)}\n{traceback.format_exc()}",
            level="critical"
        )
        raise HTTPException(status_code=500, detail="Erro interno ao delegar tarefa")
    
# Após calcular as estatísticas com get_task_stats, podemos gravar ou atualizar o registro correspondente: 

# -------------------------------------------
# 💾 SALVAR / ATUALIZAR ESTATÍSTICAS
# -------------------------------------------
def save_task_stats(db: Session, project_id: Optional[str], sprint_id: Optional[str], stats_data: dict):
    """Cria ou atualiza o registro de estatísticas."""
    try:
        existing = db.query(TaskStats).filter(
            TaskStats.project_id == project_id,
            TaskStats.sprint_id == sprint_id
        ).first()

        if existing:
            for key, value in stats_data.items():
                setattr(existing, key, value)
            log_message(f"🔄 Estatísticas atualizadas para projeto={project_id} sprint={sprint_id}", "info")
        else:
            new_stats = TaskStats(**stats_data)
            db.add(new_stats)
            log_message(f"✅ Estatísticas criadas para projeto={project_id} sprint={sprint_id}", "success")

        db.commit()

    except Exception as e:
        db.rollback()
        log_message(f"Erro ao salvar estatísticas de tarefas: {e}", "error")
    
 

def get_task_stats(
    db: Session,
    project_id: Optional[str] = None,
    sprint_id: Optional[str] = None
) -> TaskStatsSchema:
    """
    Calcula estatísticas agregadas de tarefas, filtrando opcionalmente
    por project_id e/ou sprint_id.
    Compatível com SQLAlchemy 2.x e otimizado.
    """

    filters = []
    if project_id:
        filters.append(Task.project_id == project_id)
    if sprint_id:
        filters.append(Task.sprint_id == sprint_id)

    # Expressões CASE (forma correta para SQLAlchemy 2.x)
    completed_case = func.sum(case((Task.status == "concluida", 1), else_=0))
    in_progress_case = func.sum(case((Task.status == "em_andamento", 1), else_=0))
    pending_case = func.sum(case((Task.status == "pendente", 1), else_=0))
    in_review_case = func.sum(case((Task.status == "em_revisao", 1), else_=0))
    blocked_case = func.sum(case((Task.status == "bloqueada", 1), else_=0))
    cancelled_case = func.sum(case((Task.status == "cancelada", 1), else_=0))

    # COUNT total e soma de horas estimadas (com tipo correto)
    total_count = func.count(Task.id)
    total_hours = func.coalesce(func.sum(cast(Task.estimated_hours, Numeric)), 0)

    # Query principal
    stmt = select(
        total_count.label("total"),
        completed_case.label("completed"),
        in_progress_case.label("in_progress"),
        pending_case.label("pending"),
        in_review_case.label("in_review"),
        blocked_case.label("blocked"),
        cancelled_case.label("cancelled"),
        total_hours.label("total_estimated_hours"),
    )

    if filters:
        stmt = stmt.where(and_(*filters))

    try:
        row = db.execute(stmt).mappings().first()
        total = int(row["total"] or 0)
        completed = int(row["completed"] or 0)
        progress = int((completed / total) * 100) if total > 0 else 0

        return TaskStatsSchema(
            total=total,
            completed=completed,
            in_progress=int(row["in_progress"] or 0),
            pending=int(row["pending"] or 0),
            in_review=int(row["in_review"] or 0),
            blocked=int(row["blocked"] or 0),
            cancelled=int(row["cancelled"] or 0),
            total_estimated_hours=float(row["total_estimated_hours"] or 0),
            progress_percent=progress,
        )

    except Exception as e:
        log_message(f"Erro ao calcular estatísticas de tarefas: {e}", "error")
        raise


# -----------------------------------------------------
# ✅ VALIDAR TAREFA - SERVICE
# -----------------------------------------------------
def validate_task_service(db: Session, task_id: str, aprovado: bool=True,comentario:str= "",assigned_to:Optional[str]=None) -> TaskSchema:
    """
    Serviço para validar (aprovar/concluir) uma tarefa.
    """
    try:
        if not task_id:
            log_message("Tentativa de validar tarefa sem ID", level="warning")
            raise HTTPException(status_code=400, detail="O ID da tarefa é obrigatório")

        validated_task = validate_task(db, task_id,aprovado=aprovado, validator_id=assigned_to)

        if not validated_task:
            log_message(f"Falha ao validar tarefa {task_id}", level="warning")
            raise HTTPException(status_code=404, detail="Tarefa não encontrada para validação")

        log_message(f"Tarefa {task_id} validada com sucesso", level="info")
        return validated_task

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"Erro de banco de dados ao validar tarefa {task_id}: {str(e)}\n{traceback.format_exc()}",
            level="error"
        )
        raise HTTPException(status_code=500, detail="Erro ao validar tarefa no banco de dados")
    except Exception as e:
        db.rollback()
        log_message(
            f"Erro inesperado ao validar tarefa {task_id}: {str(e)}\n{traceback.format_exc()}",
            level="critical"
        )
        raise HTTPException(status_code=500, detail="Erro interno ao validar tarefa")

def get_paginacao_service(
    db: Session,
    search: Optional[str] = None,
    page: int = 1,
    limit: int = 10,
    options: Literal["user", "project", "task", "sprint", "type_project", "Role", "project_team_association", "AuditLog", "TaskStats", "DBConnection"] = "user",
    filters: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    load_relations: bool = False,
):
    """
    Serviço genérico para retornar dados paginados com suporte a busca, filtros e relações.

    Args:
        db (Session): Sessão do banco de dados.
        search (str, opcional): Texto para pesquisa nas colunas string.
        page (int, opcional): Página atual. Padrão é 1.
        limit (int, opcional): Quantidade de itens por página. Padrão é 10.
        options (Literal): Define qual modelo será consultado.
        filters (dict, opcional): Filtros adicionais, ex: {"status": "ativo"}
        user_id (str, opcional): ID do usuário para filtros específicos
        load_relations (bool, opcional): Se deve carregar relações automaticamente

    Returns:
        dict: Resultado contendo items, total, página e total de páginas.
    """

    model_map: Dict[str, Type] = {
        "user": User,
        "project": Project,
        "task": Task,
        "sprint": Sprint,
        "type_project": TypeProjecto,
        "Role": Role,
        "project_team_association": project_team_association,
        "AuditLog": AuditLog,
        "TaskStats": TaskStats,
        "DBConnection": DBConnection
    }

    # ✅ Verificação de tipo válido
    if options not in model_map:
        raise ValueError(f"Opção inválida: '{options}'. Use: {', '.join(model_map.keys())}")

    model = model_map[options]

    # 🔗 Definir relações para cada modelo
    relation_map = {
        "user": ["role_ref", "created_projects", "assigned_tasks", "projects_participating"],
        "project": ["owner_user", "team_members",  "task_stats", "type_project", "db_connection"],
        "task": ["assigned_user", "delegated_user", "creator_user", "project", "sprint"],
        "sprint": ["created_by", "project", "task_stats"],
        "type_project": [],  # Sem relações
        "Role": ["users"],
        "AuditLog": ["user"],
        # "TaskStats": ["project", "sprint"],
        "DBConnection": ["projects"],
        "project_team_association": []  # Tabela de associação, sem relações
    }

    # Preparar relações para carregamento
    relationships = []
    if load_relations and options in relation_map:
        relationships = relation_map[options]

    # 🔍 Chama o método genérico de paginação e busca
    return get_paginated_query(
        db=db,
        model=model,
        search=search,
        filters=filters,
        page=page,
        limit=limit,
        relationships=relationships if load_relations else None,
    )