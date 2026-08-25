import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Type
from sqlalchemy import Numeric, and_, case, cast, func, select
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.cruds.task_cruds import (
    _to_int_id,
    add_task,
    delegate_task,
    delete_task,
    get_tasks,
    update_task,
    validate_task,
)
from app.models.task_models import Task, TaskStats
from app.schemas.task_schema import (
    TaskCreateSchema,
    TaskSchema,
    TaskStatsSchema,
    TaskUpdateSchema,
)
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


def add_task_service(
    db: Session,
    project_id: Optional[str],
    task: TaskCreateSchema,
    created_by_id: Optional[int] = None,
) -> TaskSchema:
    """
    Adiciona uma nova tarefa ao projeto especificado.

    `created_by_id` vem da sessão autenticada — o criador não é aceite do
    corpo do pedido.
    """
    try:
        if not project_id:
            log_message("Tentativa de adicionar tarefa sem project_id", level="warning")
            raise HTTPException(status_code=400, detail="O ID do projeto é obrigatório")

        if not task or not task.title:
            log_message("Tentativa de adicionar tarefa inválida (campos obrigatórios ausentes)", level="warning")
            raise HTTPException(status_code=400, detail="Dados da tarefa inválidos")

        new_task = add_task(db, project_id, task, created_by_id=created_by_id)

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
def delegate_task_service(
    db: Session,
    task_id: str,
    assigned_to: Optional[str] = None,
    user_id: Optional[str] = None,
) -> TaskSchema:
    """
    Serviço para delegar uma tarefa a outro utilizador.

    ⚠️ A assinatura antiga era `(db, task_id, new_user_id, assigned_to=None)`,
    mas a rota chamava `(db=, task_id=, assigned_to=, user_id=)`: faltava o
    argumento obrigatório `new_user_id` e `user_id` não existia. Qualquer
    tentativa de delegar rebentava com TypeError antes de tocar na base.

    `user_id` é quem está a delegar (fica registado no log).
    """
    try:
        if not task_id or not assigned_to:
            log_message("Tentativa de delegar tarefa com parâmetros ausentes", level="warning")
            raise HTTPException(
                status_code=400,
                detail="O ID da tarefa e do utilizador de destino são obrigatórios",
            )

        delegated_task = delegate_task(db, task_id, assigned_to)

        if not delegated_task:
            log_message(f"Falha ao delegar tarefa {task_id}", level="warning")
            raise HTTPException(
                status_code=404,
                detail="Tarefa não encontrada, ou utilizador de destino inexistente",
            )

        log_message(
            f"Tarefa {task_id} delegada para o utilizador {assigned_to}"
            f"{f' por {user_id}' if user_id else ''}",
            level="info",
        )
        return delegated_task

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(
            f"Erro de banco de dados ao delegar tarefa {task_id} para usuário {assigned_to}: {str(e)}\n{traceback.format_exc()}",
            level="error"
        )
        raise HTTPException(status_code=500, detail="Erro ao delegar tarefa no banco de dados")
    except Exception as e:
        db.rollback()
        log_message(
            f"Erro inesperado ao delegar tarefa {task_id} para {assigned_to}: {str(e)}\n{traceback.format_exc()}",
            level="critical"
        )
        raise HTTPException(status_code=500, detail="Erro interno ao delegar tarefa")
    
# Após calcular as estatísticas com get_task_stats, podemos gravar ou atualizar o registro correspondente: 

# -------------------------------------------
# 💾 SALVAR / ATUALIZAR ESTATÍSTICAS
# -------------------------------------------
def save_task_stats(db: Session, project_id: Optional[str], sprint_id: Optional[str], stats_data: dict):
    """
    Cria ou atualiza o registo de estatísticas de um projeto/sprint.

    Sem filtro nenhum (project_id e sprint_id ambos a None) não há nada para
    guardar: seria uma linha órfã com o total global, e era isso que acontecia
    antes — `TaskStats(**stats_data)` era criado sem project_id nem sprint_id,
    acumulando linhas anónimas a cada chamada de /stats/task.
    """
    pid = _to_int_id(project_id)
    sid = _to_int_id(sprint_id)

    if pid is None and sid is None:
        return

    # Só as colunas que a tabela tem (o schema traz priority_counts, que não é
    # coluna).
    colunas = {c.name for c in TaskStats.__table__.columns}
    dados = {k: v for k, v in stats_data.items() if k in colunas}
    dados.pop("id", None)

    try:
        existing = db.query(TaskStats).filter(
            TaskStats.project_id == pid,
            TaskStats.sprint_id == sid,
        ).first()

        if existing:
            for key, value in dados.items():
                setattr(existing, key, value)
            log_message(f"🔄 Estatísticas atualizadas para projeto={pid} sprint={sid}", "info")
        else:
            db.add(TaskStats(project_id=pid, sprint_id=sid, **dados))
            log_message(f"✅ Estatísticas criadas para projeto={pid} sprint={sid}", "success")

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

    pid = _to_int_id(project_id)
    sid = _to_int_id(sprint_id)

    filters = []
    if pid is not None:
        filters.append(Task.project_id == pid)
    if sid is not None:
        filters.append(Task.sprint_id == sid)

    def conta_status(valor: str):
        return func.sum(case((Task.status == valor, 1), else_=0))

    agora = datetime.utcnow()

    # COUNT total e soma de horas estimadas
    total_count = func.count(Task.id)
    total_hours = func.coalesce(func.sum(cast(Task.estimated_hours, Numeric)), 0)

    # Atrasadas: prazo passado e ainda não concluídas/canceladas.
    overdue_case = func.sum(
        case(
            (
                and_(
                    Task.end_date < agora,
                    Task.status.notin_(["concluida", "cancelada"]),
                ),
                1,
            ),
            else_=0,
        )
    )
    validated_case = func.sum(case((Task.is_validated.is_(True), 1), else_=0))

    stmt = select(
        total_count.label("total"),
        conta_status("concluida").label("completed"),
        conta_status("em_andamento").label("in_progress"),
        conta_status("pendente").label("pending"),
        conta_status("em_revisao").label("in_review"),
        conta_status("bloqueada").label("blocked"),
        conta_status("cancelada").label("cancelled"),
        validated_case.label("validated"),
        overdue_case.label("overdue_tasks"),
        total_hours.label("total_estimated_hours"),
    )

    if filters:
        stmt = stmt.where(and_(*filters))

    try:
        row = db.execute(stmt).mappings().first()
        total = int(row["total"] or 0)
        completed = int(row["completed"] or 0)
        progress = int((completed / total) * 100) if total > 0 else 0

        # Contagem por prioridade, numa query agrupada.
        prio_stmt = select(Task.priority, func.count(Task.id)).group_by(Task.priority)
        if filters:
            prio_stmt = prio_stmt.where(and_(*filters))

        priority_counts = {
            p: 0 for p in ("baixa", "media", "alta", "urgente", "critica")
        }
        for prioridade, quantos in db.execute(prio_stmt).all():
            priority_counts[prioridade or "media"] = int(quantos or 0)

        return TaskStatsSchema(
            total=total,
            completed=completed,
            in_progress=int(row["in_progress"] or 0),
            pending=int(row["pending"] or 0),
            in_review=int(row["in_review"] or 0),
            blocked=int(row["blocked"] or 0),
            cancelled=int(row["cancelled"] or 0),
            validated=int(row["validated"] or 0),
            overdue_tasks=int(row["overdue_tasks"] or 0),
            total_estimated_hours=float(row["total_estimated_hours"] or 0),
            progress_percent=progress,
            priority_counts=priority_counts,
        )

    except Exception as e:
        log_message(f"Erro ao calcular estatísticas de tarefas: {e}", "error")
        raise


# -----------------------------------------------------
# ✅ VALIDAR TAREFA - SERVICE
# -----------------------------------------------------
def validate_task_service(db: Session, task_id: str, aprovado: bool=True,comentario:str= "",assigned_to:Optional[str]=None) -> TaskSchema:
    """
    Serviço para validar (aprovar/reprovar) uma tarefa.

    `assigned_to` é quem está a validar — fica registado em `validated_by_id`.
    """
    try:
        if not task_id:
            log_message("Tentativa de validar tarefa sem ID", level="warning")
            raise HTTPException(status_code=400, detail="O ID da tarefa é obrigatório")

        if not aprovado and not (comentario or "").strip():
            raise HTTPException(
                status_code=400,
                detail="Ao reprovar uma tarefa, o comentário é obrigatório.",
            )

        validated_task = validate_task(
            db,
            task_id,
            aprovado=aprovado,
            validator_id=assigned_to,
            comentario=comentario,
        )

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

# ⚠️ `get_paginacao_service` vivia aqui, com um mapa de modelos próprio.
#
# A rota `/geral/paginate` chama a versão de `app/services/geral_services.py`,
# e os dois mapas divergiram: este tinha project/task/sprint, o outro não. O
# resultado era 403 "Opção inválida" em todo o módulo de gestão de projetos.
# Ficou uma só implementação, em geral_services, com a união dos dois mapas.
#
# Se precisares de paginação aqui, importa-a de lá:
#     from app.services.geral_services import get_paginacao_service