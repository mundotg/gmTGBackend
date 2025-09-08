from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import List, Optional

from app.models.queryhistory_models import QueryHistory
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryHistoryUpdate
from app.ultils.logger import log_message


# ─────────────── Criar ───────────────

def create_query_history(db: Session, data: QueryHistoryCreate) -> QueryHistory:
    """
    Cria um novo registro de histórico de consulta.
    """
    new_query = QueryHistory(**data.dict())
    db.add(new_query)
    db.commit()
    db.refresh(new_query)
    log_message(
        f"Consulta criada com sucesso (UserID={new_query.user_id}, ConnID={new_query.db_connection_id})",
        level="success"
    )
    return new_query

def get_query_history_by_user_and_query(db: Session, user_id: int, connection_id: int, query_string: str) -> QueryHistory | None:
    return (
        db.query(QueryHistory)
        .filter(
            QueryHistory.user_id == user_id,
            QueryHistory.db_connection_id == connection_id,
            QueryHistory.query == query_string
        )
        .first()
    )


# ─────────────── Obter por ID ───────────────

def get_query_history(db: Session, query_id: int) -> Optional[QueryHistory]:
    """
    Retorna um registro de consulta específico pelo ID.
    """
    return db.query(QueryHistory).filter(QueryHistory.id == query_id).first()


# ─────────────── Listar por Usuário ───────────────

def get_all_queries_by_user(db: Session, user_id: int, limit: int = 50) -> List[QueryHistory]:
    """
    Lista as últimas consultas realizadas por um usuário.
    """
    return (
        db.query(QueryHistory)
        .filter(QueryHistory.user_id == user_id)
        .order_by(desc(QueryHistory.executed_at))
        .limit(limit)
        .all()
    )


# ─────────────── Listar por Conexão ───────────────

def get_queries_by_connection(db: Session, connection_id: int, limit: int = 50) -> List[QueryHistory]:
    """
    Lista as últimas consultas feitas em uma conexão de banco de dados específica.
    """
    return (
        db.query(QueryHistory)
        .filter(QueryHistory.db_connection_id == connection_id)
        .order_by(desc(QueryHistory.executed_at))
        .limit(limit)
        .all()
    )


# ─────────────── Última Consulta ───────────────

def get_ultima_consulta(db: Session, connection_id: int, user_id: int) -> Optional[QueryHistory]:
    """
    Retorna a última consulta executada por um usuário em uma conexão específica.
    """
    return (
        db.query(QueryHistory)
        .filter(QueryHistory.user_id == user_id, QueryHistory.db_connection_id == connection_id)
        .order_by(QueryHistory.executed_at.desc())
        .first()
    )


# ─────────────── Atualizar ───────────────

def update_query_history(db: Session, query_id: int, data: QueryHistoryUpdate) -> Optional[QueryHistory]:
    """
    Atualiza os campos de um registro de consulta específico.
    """
    query = db.query(QueryHistory).filter(QueryHistory.id == query_id).first()
    if not query:
        log_message(f"Tentativa de atualizar consulta inexistente (ID={query_id})", level="warning")
        return None

    for field, value in data.dict(exclude_unset=True).items():
        setattr(query, field, value)

    db.commit()
    db.refresh(query)
    log_message(f"Consulta atualizada com sucesso (ID={query_id})", level="info")
    return query


# ─────────────── Deletar ───────────────

def delete_query_history(db: Session, query_id: int) -> bool:
    """
    Remove um registro de histórico de consulta pelo ID.
    """
    query = db.query(QueryHistory).filter(QueryHistory.id == query_id).first()
    if not query:
        log_message(f"Tentativa de deletar consulta inexistente (ID={query_id})", level="error")
        return False

    db.delete(query)
    db.commit()
    log_message(f"Consulta deletada com sucesso (ID={query_id})", level="warning")
    return True
