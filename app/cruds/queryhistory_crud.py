from sqlalchemy.orm import Session
from sqlalchemy import desc, select
from typing import List, Optional

from app.models.queryhistory_models import QueryHistory
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryHistoryCreateAsync, QueryHistoryUpdate, QueryHistoryUpdateAsync
from app.ultils.logger import log_message
    
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload


async def create_query_history_async(
    db: AsyncSession, 
    data: QueryHistoryCreateAsync
) -> QueryHistory:
    """
    Cria um novo registro de histórico de consulta
    Compatível com SQLAlchemy 2 e async
    """
    try:
        # Converte para dict e remove valores None para campos opcionais
        query_data = data.model_dump(exclude_unset=True)
        
        # Cria a instância do modelo
        new_query = QueryHistory(**query_data)
        
        # Adiciona à sessão
        db.add(new_query)
        
        # Commit com tratamento de exceções
        await db.commit()
        await db.refresh(new_query)
        
        log_message(
            f"Consulta criada com sucesso (ID={new_query.id}, "
            f"UserID={new_query.user_id}, ConnID={new_query.db_connection_id})",
            "success"
        )
        
        return new_query
        
    except Exception as e:
        await db.rollback()
        log_message(f"Erro ao criar histórico async: {str(e)}", "error")
        raise

async def get_query_history_async(
    db: AsyncSession, 
    query_id: int
) -> Optional[QueryHistory]:
    """Obtém um histórico por ID"""
    try:
        stmt = (
            select(QueryHistory)
            .where(QueryHistory.id == query_id)
            .options(selectinload(QueryHistory.user))
            .options(selectinload(QueryHistory.connection))
        )
        
        result = await db.execute(stmt)
        return result.scalar_one_or_none()
        
    except Exception as e:
        logger.error(f"Erro ao buscar histórico {query_id}: {str(e)}")
        return None

async def update_query_history_async(
    db: AsyncSession, 
    query_id: int, 
    data: QueryHistoryUpdateAsync
) -> Optional[QueryHistory]:
    """Atualiza um histórico existente"""
    try:
        # Busca o registro
        stmt = select(QueryHistory).where(QueryHistory.id == query_id)
        result = await db.execute(stmt)
        query_history = result.scalar_one_or_none()
        
        if not query_history:
            return None
        
        # Atualiza os campos
        update_data = data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(query_history, field, value)
        
        await db.commit()
        await db.refresh(query_history)
        
        logger.info(f"Histórico {query_id} atualizado com sucesso")
        return query_history
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Erro ao atualizar histórico {query_id}: {str(e)}")
        raise

async def delete_query_history_async(
    db: AsyncSession, 
    query_id: int
) -> bool:
    """Deleta um histórico"""
    try:
        stmt = select(QueryHistory).where(QueryHistory.id == query_id)
        result = await db.execute(stmt)
        query_history = result.scalar_one_or_none()
        
        if not query_history:
            return False
        
        await db.delete(query_history)
        await db.commit()
        
        logger.info(f"Histórico {query_id} deletado com sucesso")
        return True
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Erro ao deletar histórico {query_id}: {str(e)}")
        raise

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


async def get_query_history_by_user_and_query_async(
    db: AsyncSession, 
    user_id: int, 
    connection_id: int, 
    query_string: str
) -> Optional[QueryHistory]:
    """
    Busca o histórico de query mais recente para um usuário, conexão e query específicos.
    Retorna o registro mais recente em caso de duplicatas.
    """
    try:
        stmt = (
            select(QueryHistory)
            .filter(
                QueryHistory.user_id == user_id,
                QueryHistory.db_connection_id == connection_id,
                QueryHistory.query == query_string,
                QueryHistory.error_message.is_(None)  # ✅ Opcional: excluir queries com erro
            )
            .order_by(
                QueryHistory.executed_at.desc(),  # ✅ Data mais recente primeiro
                QueryHistory.id.desc()  # ✅ Em caso de mesma data, pega o ID maior
            )
        )
        result = await db.execute(stmt)
        return result.scalars().first()  # ✅ Retorna o mais recente ou None
        
    except Exception as e:
        log_message(f"Erro ao buscar histórico de query: {str(e)}", "error")
        return None



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
