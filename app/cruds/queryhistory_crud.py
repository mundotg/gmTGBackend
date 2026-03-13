import traceback
from typing import List, Optional

from sqlalchemy.orm import Session, selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import desc, select
from sqlalchemy.exc import SQLAlchemyError

from app.models.queryhistory_models import QueryHistory
from app.schemas.queryhistory_schemas import (
    QueryHistoryCreate,
    QueryHistoryCreateAsync,
    QueryHistoryUpdate,
    QueryHistoryUpdateAsync,
)
from app.ultils.logger import log_message


# ============================================================
# Helpers internos (não mudam API)
# ============================================================
def _sanitize_limit(limit: int, default: int = 50, max_limit: int = 200) -> int:
    try:
        limit = int(limit)
    except Exception:
        return default
    return min(max(limit, 1), max_limit)


def _apply_common_order(stmt):
    # ordenação estável (mais recente primeiro; empate pelo id)
    return stmt.order_by(QueryHistory.executed_at.desc(), QueryHistory.id.desc())


# ============================================================
# ASYNC CRUD
# ============================================================
async def create_query_history_async(
    db: AsyncSession,
    data: QueryHistoryCreateAsync
) -> QueryHistory:
    """
    Cria um novo registro de histórico de consulta (async).
    """
    try:
        query_data = data.model_dump(exclude_unset=True)
        new_query = QueryHistory(**query_data)

        db.add(new_query)
        await db.commit()
        await db.refresh(new_query)

        log_message(
            f"✅ Consulta criada (ID={new_query.id}, UserID={new_query.user_id}, ConnID={new_query.db_connection_id})",
            "success",
        )
        return new_query

    except SQLAlchemyError as e:
        await db.rollback()
        log_message(f"❌ Erro SQL ao criar histórico async: {str(e)}", "error")
        raise

    except Exception as e:
        await db.rollback()
        log_message(f"❌ Erro inesperado ao criar histórico async: {str(e)}\n{traceback.format_exc()}", "error")
        raise


async def get_query_history_async(
    db: AsyncSession,
    query_id: int
) -> Optional[QueryHistory]:
    """Obtém um histórico por ID (async)."""
    try:
        stmt = (
            select(QueryHistory)
            .where(QueryHistory.id == query_id)
            # Carrega relações SOMENTE quando você precisa.
            # Se isso for usado em listagem leve, considere remover.
            .options(selectinload(QueryHistory.user))
            .options(selectinload(QueryHistory.connection))
        )

        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    except SQLAlchemyError as e:
        log_message(f"❌ Erro SQL ao buscar histórico {query_id}: {str(e)}", "error")
        return None

    except Exception as e:
        log_message(f"❌ Erro inesperado ao buscar histórico {query_id}: {str(e)}", "error")
        return None


async def update_query_history_async(
    db: AsyncSession,
    query_id: int,
    data: QueryHistoryUpdateAsync
) -> Optional[QueryHistory]:
    """Atualiza um histórico existente (async)."""
    try:
        stmt = select(QueryHistory).where(QueryHistory.id == query_id)
        result = await db.execute(stmt)
        query_history = result.scalar_one_or_none()

        if not query_history:
            return None

        update_data = data.model_dump(exclude_unset=True)

        changed = False
        for field, value in update_data.items():
            if hasattr(query_history, field) and getattr(query_history, field) != value:
                setattr(query_history, field, value)
                changed = True

        if changed:
            await db.commit()
            await db.refresh(query_history)

        log_message(f"ℹ️ Histórico {query_id} atualizado", "info")
        return query_history

    except SQLAlchemyError as e:
        await db.rollback()
        log_message(f"❌ Erro SQL ao atualizar histórico {query_id}: {str(e)}", "error")
        raise

    except Exception:
        await db.rollback()
        log_message(f"❌ Erro inesperado ao atualizar histórico {query_id}:\n{traceback.format_exc()}", "error")
        raise


async def delete_query_history_async(
    db: AsyncSession,
    query_id: int
) -> bool:
    """Deleta um histórico (async)."""
    try:
        stmt = select(QueryHistory).where(QueryHistory.id == query_id)
        result = await db.execute(stmt)
        query_history = result.scalar_one_or_none()

        if not query_history:
            return False

        await db.delete(query_history)
        await db.commit()
        return True

    except SQLAlchemyError as e:
        await db.rollback()
        log_message(f"❌ Erro SQL ao deletar histórico {query_id}: {str(e)}", "error")
        raise

    except Exception:
        await db.rollback()
        log_message(f"❌ Erro inesperado ao deletar histórico {query_id}:\n{traceback.format_exc()}", "error")
        raise


# ============================================================
# SYNC CRUD
# ============================================================
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models.queryhistory_models import QueryHistory
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryHistoryUpdate
from app.ultils.logger import log_message

# ============================================================
# ➕ CRIAR HISTÓRICO DE CONSULTA
# ============================================================

def create_query_history(db: Session, user_id: int, data: QueryHistoryCreate) -> QueryHistory:
    """
    Cria um novo registro de histórico de consulta.
    Força o user_id recebido pelo token para evitar spoofing (falsidade ideológica).
    """
    try:
        # Extrai os dados e injeta o dono real (proteção contra manipulação de payload)
        dump_data = data.model_dump(exclude_unset=True)
        dump_data["user_id"] = user_id
        dump_data["executed_by"] = f"user_{user_id}"

        new_query = QueryHistory(**dump_data)
        db.add(new_query)
        db.commit()
        db.refresh(new_query)

        log_message(
            f"✅ Histórico registrado (ID={new_query.id}, UserID={user_id}, ConnID={new_query.db_connection_id}, Type={new_query.query_type})",
            level="success",
        )
        return new_query

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Falha no banco ao criar histórico de consulta (User {user_id}): {str(e)}", level="error")
        raise
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro inesperado ao criar histórico de consulta: {str(e)}", level="error")
        raise


# ============================================================
# ✏️ EDITAR HISTÓRICO DE CONSULTA (Metadados apenas)
# ============================================================

def update_query_history_v2(
    db: Session, 
    query_id: int, 
    user_id: int, 
    data: QueryHistoryUpdate
) -> Optional[QueryHistory]:
    """
    Atualiza metadados organizacionais de uma consulta histórica (Favoritos e Tags).
    Bloqueia a edição da string da query ou tempo de execução por motivos de auditoria.
    """
    try:
        # 1. Busca garantindo que o registro pertence ao usuário que solicitou
        query_obj = db.query(QueryHistory).filter(
            QueryHistory.id == query_id,
            QueryHistory.user_id == user_id
        ).first()

        if not query_obj:
            log_message(f"⚠️ Tentativa de edição negada ou histórico não encontrado (ID={query_id}, UserID={user_id})", level="warning")
            return None

        # 2. Atualiza apenas os campos permitidos
        update_data = data.model_dump(exclude_unset=True)
        
        # Blindagem: Apenas estes campos podem ser editados pelo usuário no histórico
        allowed_updates = {"is_favorite", "tags", "meta_info"}

        has_changes = False
        for key, value in update_data.items():
            if key in allowed_updates:
                setattr(query_obj, key, value)
                has_changes = True

        # Se houver mudanças, registra a auditoria da modificação
        if has_changes:
            query_obj.modified_by = f"user_{user_id}" # type: ignore
            db.commit()
            db.refresh(query_obj)
            log_message(f"✅ Histórico {query_id} atualizado com sucesso (UserID={user_id})", level="success")
        
        return query_obj

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Falha no banco ao atualizar histórico {query_id}: {str(e)}", level="error")
        raise
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro inesperado ao atualizar histórico {query_id}: {str(e)}", level="error")
        raise


def get_query_history_by_user_and_query(
    db: Session,
    user_id: int,
    connection_id: int,
    query_string: str
) -> QueryHistory | None:
    return (
        db.query(QueryHistory)
        .filter(
            QueryHistory.user_id == user_id,
            QueryHistory.db_connection_id == connection_id,
            QueryHistory.query == query_string,
        )
        .order_by(QueryHistory.executed_at.desc(), QueryHistory.id.desc())
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
            .where(
                QueryHistory.user_id == user_id,
                QueryHistory.db_connection_id == connection_id,
                QueryHistory.query == query_string,
                QueryHistory.error_message.is_(None),
            )
        )
        stmt = _apply_common_order(stmt)

        result = await db.execute(stmt)
        return result.scalars().first()

    except SQLAlchemyError as e:
        log_message(f"❌ Erro SQL ao buscar histórico de query: {str(e)}", "error")
        return None

    except Exception as e:
        log_message(f"❌ Erro inesperado ao buscar histórico de query: {str(e)}", "error")
        return None


# ─────────────── Obter por ID ───────────────
def get_query_history(db: Session, query_id: int) -> Optional[QueryHistory]:
    """Retorna um registro de consulta específico pelo ID."""
    return (
        db.query(QueryHistory)
        .filter(QueryHistory.id == query_id)
        .first()
    )


# ─────────────── Listar por Usuário ───────────────
def get_all_queries_by_user(db: Session, user_id: int, limit: int = 50) -> List[QueryHistory]:
    """Lista as últimas consultas realizadas por um usuário."""
    limit = _sanitize_limit(limit)
    return (
        db.query(QueryHistory)
        .filter(QueryHistory.user_id == user_id)
        .order_by(desc(QueryHistory.executed_at), desc(QueryHistory.id))
        .limit(limit)
        .all()
    )


# ─────────────── Listar por Conexão ───────────────
def get_queries_by_connection(db: Session, connection_id: int, limit: int = 50) -> List[QueryHistory]:
    """Lista as últimas consultas feitas em uma conexão de banco de dados específica."""
    limit = _sanitize_limit(limit)
    return (
        db.query(QueryHistory)
        .filter(QueryHistory.db_connection_id == connection_id)
        .order_by(desc(QueryHistory.executed_at), desc(QueryHistory.id))
        .limit(limit)
        .all()
    )


# ─────────────── Última Consulta ───────────────
def get_ultima_consulta(db: Session, connection_id: int, user_id: int) -> Optional[QueryHistory]:
    """Retorna a última consulta executada por um usuário em uma conexão específica."""
    return (
        db.query(QueryHistory)
        .filter(
            QueryHistory.user_id == user_id,
            QueryHistory.db_connection_id == connection_id,
        )
        .order_by(QueryHistory.executed_at.desc(), QueryHistory.id.desc())
        .first()
    )


# ─────────────── Atualizar ───────────────
def update_query_history(db: Session, query_id: int, data: QueryHistoryUpdate) -> Optional[QueryHistory]:
    """Atualiza os campos de um registro de consulta específico."""
    query = db.query(QueryHistory).filter(QueryHistory.id == query_id).first()
    if not query:
        log_message(f"⚠️ Tentativa de atualizar consulta inexistente (ID={query_id})", level="warning")
        return None

    update_data = data.model_dump(exclude_unset=True)

    changed = False
    for field, value in update_data.items():
        if hasattr(query, field) and getattr(query, field) != value:
            setattr(query, field, value)
            changed = True

    if changed:
        db.commit()
        db.refresh(query)

    log_message(f"ℹ️ Consulta atualizada (ID={query_id})", level="info")
    return query


# ─────────────── Deletar ───────────────
def delete_query_history(db: Session, query_id: int) -> bool:
    """Remove um registro de histórico de consulta pelo ID."""
    deleted = (
        db.query(QueryHistory)
        .filter(QueryHistory.id == query_id)
        .delete(synchronize_session=False)
    )
    if not deleted:
        log_message(f"⚠️ Tentativa de deletar consulta inexistente (ID={query_id})", level="warning")
        return False

    db.commit()
    log_message(f"🗑️ Consulta deletada (ID={query_id})", level="warning")
    return True
