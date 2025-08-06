from typing import List, Optional
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone

from app.cruds.dbstructure_crud import get_db_structures
from app.models.dbstatistics_models import DBStatistics, TableRowCountCache
from app.schemas.dbstatistics_schema import (
    ConnectionStatisticsOverview,
    DBStatisticsCreate,
    DBStatisticsUpdate,
)
from app.ultils.logger import log_message


def get_statistics_by_connection(db: Session, connection_id: int) -> Optional[DBStatistics]:
    """Retorna estatísticas de uma conexão específica."""
    return db.query(DBStatistics).filter(DBStatistics.db_connection_id == connection_id).first()


def get_statistics_by_connection_geral(
    db: Session, connection_id: int
) -> Optional[ConnectionStatisticsOverview]:
    """
    Retorna uma visão geral com as estatísticas e quantidade de tabelas registradas na estrutura.
    """
    db_stat = get_statistics_by_connection(db, connection_id)
    if not db_stat:
        log_message(f"⚠️ Nenhuma estatística encontrada para a conexão ID={connection_id}.", "warning")
        return None

    estruturas = get_db_structures(db, connection_id=connection_id)
    table_count = len(estruturas or [])

    return ConnectionStatisticsOverview(
                statistics = DBStatisticsCreate.model_validate(db_stat, from_attributes=True),
                total_structured_tables=table_count
            )

def converter_stats(stats: ConnectionStatisticsOverview | None) -> dict | None:
    """
    Converte as estatísticas para o formato esperado pelo frontend.
    """

    if not stats:
        return None
    if not stats.statistics:
        log_message("⚠️ Estatísticas não disponíveis.", "warning")
        return None
    return  {
        "connection_id": stats.statistics.db_connection_id,
        "tables_connected": stats.statistics.tables_connected,
        "table_count": stats.statistics.table_count,
        "view_count": stats.statistics.view_count,
        "procedure_count": stats.statistics.procedure_count,
        "function_count": stats.statistics.function_count,
        "trigger_count": stats.statistics.trigger_count,
        "index_count": stats.statistics.index_count,
        "queries_today": stats.statistics.queries_today,
        "records_analyzed": stats.statistics.records_analyzed,
        "last_query_at": stats.statistics.last_query_at,
        "total_structured_tables": stats.total_structured_tables,
        "server_version": str(stats.statistics.server_version) if stats.statistics.server_version else ""
    }
 

def create_statistics(db: Session, stats: DBStatisticsCreate) -> DBStatistics:
    """Cria uma nova entrada de estatísticas."""
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
        updated_at=datetime.now(timezone.utc),
        last_query_at=stats.last_query_at or None,
    )

    db.add(db_stat)
    db.commit()
    db.refresh(db_stat)

    log_message(f"✅ Estatísticas criadas para conexão ID={stats.db_connection_id}", "success")
    return db_stat


def update_statistics(
    db: Session, connection_id: int, updates: DBStatisticsUpdate
) -> Optional[DBStatistics]:
    """Atualiza as estatísticas de uma conexão existente."""
    db_stat = get_statistics_by_connection(db, connection_id)
    if not db_stat:
        log_message(f"⚠️ Estatísticas não encontradas para conexão ID={connection_id}", "warning")
        return None

    update_data = updates.model_dump(exclude_unset=True)

    # Atualiza campos dinamicamente
    for key, value in update_data.items():
        setattr(db_stat, key, value)

    # Sempre atualiza a data
    db_stat.updated_at = datetime.now(timezone.utc)
    if "queries_today" in update_data:
        db_stat.last_query_at = update_data.get("last_query_at", datetime.now(timezone.utc))

    db.commit()
    db.refresh(db_stat)

    log_message(f"ℹ️ Estatísticas atualizadas para conexão ID={connection_id}", "info")
    return db_stat

# cruds/table_row_count_cache.py

def get_cached_row_count_all(
    db: Session, connection_id: int
) -> List[TableRowCountCache]:
    """
    Retorna todos os registros de cache para uma conexão específica.
    """
    return db.query(TableRowCountCache).filter_by(
        connection_id=connection_id
    ).all()


def get_cached_row_count(
    db: Session, connection_id: int, table_name: str, max_age_minutes: int = 60
) -> int | None:
    """
    Retorna o valor do cache se ainda estiver dentro do tempo válido.
    Caso contrário, retorna None.
    """
    cache = db.query(TableRowCountCache).filter_by(
        connection_id=connection_id,
        table_name=table_name
    ).first()

    if cache and cache.last_updated:
        limite = datetime.utcnow() - timedelta(minutes=max_age_minutes)
        if cache.last_updated > limite:
            return cache.row_count

    return None

def update_or_create_cache(db: Session, connection_id: int, table_name: str, count: int):
    """
    Atualiza ou cria um novo cache com a contagem de registros.
    """
    cache = db.query(TableRowCountCache).filter_by(
        connection_id=connection_id,
        table_name=table_name
    ).first()

    if cache:
        cache.row_count = count
        cache.last_updated = datetime.utcnow()
    else:
        cache = TableRowCountCache(
            connection_id=connection_id,
            table_name=table_name,
            row_count=count,
            last_updated=datetime.utcnow()
        )
        db.add(cache)

    db.commit()
    return cache
