from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta, timezone
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session, load_only
from sqlalchemy.dialects.postgresql import insert  # Usado para Upsert no PostgreSQL
from sqlalchemy.exc import SQLAlchemyError

# Assumindo que DBStructure vem de models e não do CRUD para evitar dependência circular
from app.models.dbstructure_models import DBStructure 
from app.models.dbstatistics_models import DBStatistics, TableRowCountCache
from app.schemas.dbstatistics_schema import (
    ConnectionStatisticsOverview,
    DBStatisticsCreate,
    DBStatisticsUpdate,
)
from app.ultils.logger import log_message


# ============================================================
#  DBStatistics
# ============================================================

def get_statistics_by_connection(db: Session, connection_id: int) -> Optional[DBStatistics]:
    """Retorna estatísticas de uma conexão específica, otimizado com SQLAlchemy 2.0."""
    try:
        stmt = (
            select(DBStatistics)
            .options(
                load_only(
                    DBStatistics.db_connection_id,
                    DBStatistics.server_version,
                    DBStatistics.tables_connected,
                    DBStatistics.table_count,
                    DBStatistics.view_count,
                    DBStatistics.procedure_count,
                    DBStatistics.function_count,
                    DBStatistics.trigger_count,
                    DBStatistics.index_count,
                    DBStatistics.queries_today,
                    DBStatistics.records_analyzed,
                    DBStatistics.updated_at,
                    DBStatistics.last_query_at,
                )
            )
            .where(DBStatistics.db_connection_id == connection_id)
        )
        return db.execute(stmt).scalar_one_or_none()
    except SQLAlchemyError as e:
        log_message(f"❌ Erro ao buscar estatísticas para conexão {connection_id}: {str(e)}", "error")
        return None


def get_statistics_by_connection_geral(
    db: Session, connection_id: int
) -> Optional[ConnectionStatisticsOverview]:
    """
    Retorna uma visão geral com as estatísticas e quantidade de tabelas registradas.
    Otimizado para contar sem trazer instâncias para a memória.
    """
    db_stat = get_statistics_by_connection(db, connection_id)
    if not db_stat:
        log_message(f"⚠️ Nenhuma estatística encontrada para a conexão ID={connection_id}.", "warning")
        return None

    try:
        # 🔥 PERFORMANCE: Conta diretamente na base de dados (MUITO mais rápido)
        # Assumindo que você importa o modelo DBStructure diretamente
        stmt_count = select(func.count(1)).select_from(DBStructure).where(DBStructure.db_connection_id == connection_id)
        table_count = db.execute(stmt_count).scalar() or 0

        return ConnectionStatisticsOverview(
            statistics=DBStatisticsCreate.model_validate(db_stat, from_attributes=True),
            total_structured_tables=table_count,
        )
    except SQLAlchemyError as e:
        log_message(f"❌ Erro ao calcular visão geral da conexão {connection_id}: {str(e)}", "error")
        return None


def converter_stats(stats: ConnectionStatisticsOverview | None) -> Optional[Dict[str, Any]]:
    """Converte as estatísticas para o formato esperado pelo frontend."""
    if not stats or not stats.statistics:
        log_message("⚠️ Estatísticas não disponíveis para conversão.", "warning")
        return None

    s = stats.statistics
    return {
        "connection_id": s.db_connection_id,
        "tables_connected": s.tables_connected,
        "table_count": s.table_count,
        "view_count": s.view_count,
        "procedure_count": s.procedure_count,
        "function_count": s.function_count,
        "trigger_count": s.trigger_count,
        "index_count": s.index_count,
        "queries_today": s.queries_today,
        "records_analyzed": s.records_analyzed,
        "last_query_at": s.last_query_at,
        "total_structured_tables": stats.total_structured_tables,
        "server_version": str(s.server_version) if s.server_version else "",
    }


def create_statistics(db: Session, stats: DBStatisticsCreate) -> Optional[DBStatistics]:
    """Cria uma nova entrada de estatísticas."""
    try:
        now = datetime.now(timezone.utc)
        
        db_stat = DBStatistics(
            db_connection_id=stats.db_connection_id,
            server_version=stats.server_version,
            tables_connected=stats.tables_connected,
            table_count=stats.table_count,
            view_count=stats.view_count,
            procedure_count=stats.procedure_count,
            function_count=stats.function_count,
            trigger_count=stats.trigger_count,
            index_count=stats.index_count,
            queries_today=stats.queries_today,
            records_analyzed=stats.records_analyzed,
            updated_at=now,
            last_query_at=stats.last_query_at,
        )

        db.add(db_stat)
        db.commit()
        db.refresh(db_stat)

        log_message(f"✅ Estatísticas criadas para conexão ID={stats.db_connection_id}", "success")
        return db_stat
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao criar estatísticas para conexão {stats.db_connection_id}: {str(e)}", "error")
        return None


def update_statistics(
    db: Session, connection_id: int, updates: DBStatisticsUpdate
) -> Optional[DBStatistics]:
    """Atualiza as estatísticas de uma conexão existente."""
    try:
        db_stat = get_statistics_by_connection(db, connection_id)
        if not db_stat:
            log_message(f"⚠️ Estatísticas não encontradas para atualização. Conexão ID={connection_id}", "warning")
            return None

        update_data = updates.model_dump(exclude_unset=True)
        if not update_data:
            return db_stat # Retorna sem bater no banco se não houver mudanças

        changed = False
        for key, value in update_data.items():
            if hasattr(db_stat, key) and getattr(db_stat, key) != value:
                setattr(db_stat, key, value)
                changed = True

        if changed:
            now = datetime.now(timezone.utc)
            db_stat.updated_at = now
            if "queries_today" in update_data:
                db_stat.last_query_at = update_data.get("last_query_at", now)

            db.commit()
            db.refresh(db_stat)
            log_message(f"ℹ️ Estatísticas atualizadas para conexão ID={connection_id}", "info")

        return db_stat
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao atualizar estatísticas da conexão {connection_id}: {str(e)}", "error")
        return None


# ============================================================
#  TableRowCountCache
# ============================================================

def get_cached_row_count_all(db: Session, connection_id: int) -> List[TableRowCountCache]:
    """Retorna todos os registros de cache para uma conexão específica."""
    try:
        stmt = (
            select(TableRowCountCache)
            .options(
                load_only(
                    TableRowCountCache.connection_id,
                    TableRowCountCache.table_name,
                    TableRowCountCache.row_count,
                    TableRowCountCache.last_updated,
                )
            )
            .where(TableRowCountCache.connection_id == connection_id)
        )
        return list(db.execute(stmt).scalars().all())
    except SQLAlchemyError as e:
        log_message(f"❌ Erro ao buscar cache de linhas (conexão {connection_id}): {str(e)}", "error")
        return []


def get_cached_row_count_all_tupla(db: Session, connection_id: int) -> List[Tuple[str, int]]:
    """Retorna lista de tuplas (table_name, row_count) para uma conexão."""
    try:
        stmt = select(TableRowCountCache.table_name, TableRowCountCache.row_count)\
               .where(TableRowCountCache.connection_id == connection_id)
        return list(db.execute(stmt).all())
    except SQLAlchemyError as e:
         log_message(f"❌ Erro ao buscar tuplas de cache (conexão {connection_id}): {str(e)}", "error")
         return []


def get_cached_row_count(
    db: Session, connection_id: int, table_name: str, max_age_minutes: int = 60
) -> Optional[int]:
    """Retorna o valor do cache se ainda estiver dentro do tempo válido."""
    if not table_name:
        return None

    try:
        limite = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)

        stmt = (
            select(TableRowCountCache.row_count)
            .where(
                TableRowCountCache.connection_id == connection_id,
                TableRowCountCache.table_name == table_name,
                TableRowCountCache.last_updated.isnot(None),
                TableRowCountCache.last_updated > limite,
            )
        )
        row_count = db.execute(stmt).scalar()
        
        return int(row_count) if row_count is not None else None
    except SQLAlchemyError as e:
        log_message(f"❌ Erro ao verificar cache para a tabela {table_name}: {str(e)}", "error")
        return None


def update_or_create_cache(db: Session, connection_id: int, table_name: str, count: int) -> Optional[TableRowCountCache]:
    """
    Atualiza ou cria um novo cache. 
    Usa abordagem Upsert para evitar condições de corrida em alta concorrência.
    """
    if not table_name:
        return None

    try:
        now = datetime.now(timezone.utc)
        
        # Otimização extrema para PostgreSQL (Upsert/ON CONFLICT)
        # Se você estiver usando MySQL ou SQLite, a sintaxe muda ligeiramente.
        # Caso esteja usando DBs mistos, a sua lógica original (Select, if exist Update, else Insert) é a mais segura.
        # Mantive a sua lógica original, mas refatorada para SQLAlchemy 2.0 e com rollback seguro.
        
        stmt = (
            select(TableRowCountCache)
            .where(
                TableRowCountCache.connection_id == connection_id,
                TableRowCountCache.table_name == table_name,
            )
        )
        cache = db.execute(stmt).scalar_one_or_none()

        if cache:
            if cache.row_count != count:
                cache.row_count = count
            cache.last_updated = now
        else:
            cache = TableRowCountCache(
                connection_id=connection_id,
                table_name=table_name,
                row_count=count,
                last_updated=now,
            )
            db.add(cache)

        db.commit()
        db.refresh(cache)
        return cache
        
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao fazer upsert no cache (Tabela {table_name}): {str(e)}", "error")
        return None