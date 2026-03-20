from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

from sqlalchemy import func, select
from sqlalchemy.orm import load_only
from sqlalchemy.exc import SQLAlchemyError

from app.database import SessionLocal
from app.models.dbstructure_models import DBStructure
from app.models.dbstatistics_models import DBStatistics, TableRowCountCache
from app.schemas.dbstatistics_schema import (
    ConnectionStatisticsOverview,
    DBStatisticsCreate,
    DBStatisticsUpdate,
)
from app.ultils.logger import log_message


# ============================================================
# 🔒 SESSION MANAGER (ANTI-LEAK)
# ============================================================

@contextmanager
def get_db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============================================================
#  DBStatistics
# ============================================================

def get_statistics_by_connection(connection_id: int) -> Optional[DBStatistics]:
    with get_db_session() as db:
        try:
            stmt = (
                select(DBStatistics)
                .options(load_only(
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
                ))
                .where(DBStatistics.db_connection_id == connection_id)
            )

            return db.execute(stmt).scalar_one_or_none()

        except SQLAlchemyError as e:
            log_message(f"❌ Erro ao buscar estatísticas {connection_id}: {str(e)}", "error")
            return None


def get_statistics_by_connection_geral(connection_id: int) -> Optional[ConnectionStatisticsOverview]:
    with get_db_session() as db:
        try:
            db_stat = get_statistics_by_connection(connection_id)

            if not db_stat:
                log_message(f"⚠️ Nenhuma estatística para conexão {connection_id}", "warning")
                return None

            stmt_count = (
                select(func.count(1))
                .select_from(DBStructure)
                .where(DBStructure.db_connection_id == connection_id)
            )

            table_count = db.execute(stmt_count).scalar() or 0

            return ConnectionStatisticsOverview(
                statistics=DBStatisticsCreate.model_validate(db_stat, from_attributes=True),
                total_structured_tables=table_count,
            )

        except SQLAlchemyError as e:
            log_message(f"❌ Erro visão geral {connection_id}: {str(e)}", "error")
            return None


def converter_stats(stats: ConnectionStatisticsOverview | None) -> Optional[Dict[str, Any]]:
    if not stats or not stats.statistics:
        log_message("⚠️ Estatísticas não disponíveis.", "warning")
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


def create_statistics(stats: DBStatisticsCreate) -> Optional[DBStatistics]:
    with get_db_session() as db:
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

            return db_stat

        except SQLAlchemyError as e:
            db.rollback()
            log_message(f"❌ Erro ao criar stats: {str(e)}", "error")
            return None


def update_statistics(connection_id: int, updates: DBStatisticsUpdate) -> Optional[DBStatistics]:
    with get_db_session() as db:
        try:
            stmt = select(DBStatistics).where(DBStatistics.db_connection_id == connection_id)
            db_stat = db.execute(stmt).scalar_one_or_none()

            if not db_stat:
                return None

            update_data = updates.model_dump(exclude_unset=True)

            if not update_data:
                return db_stat

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

            return db_stat

        except SQLAlchemyError as e:
            db.rollback()
            log_message(f"❌ Erro ao atualizar stats {connection_id}: {str(e)}", "error")
            return None


# ============================================================
#  TableRowCountCache
# ============================================================

def get_cached_row_count_all(connection_id: int) -> List[TableRowCountCache]:
    with get_db_session() as db:
        try:
            stmt = (
                select(TableRowCountCache)
                .options(load_only(
                    TableRowCountCache.connection_id,
                    TableRowCountCache.table_name,
                    TableRowCountCache.row_count,
                    TableRowCountCache.last_updated,
                ))
                .where(TableRowCountCache.connection_id == connection_id)
            )

            return list(db.execute(stmt).scalars().all())

        except SQLAlchemyError as e:
            log_message(f"❌ Erro cache all {connection_id}: {str(e)}", "error")
            return []


def get_cached_row_count_all_tupla(connection_id: int) -> List[Tuple[str, int]]:
    with get_db_session() as db:
        try:
            stmt = select(
                TableRowCountCache.table_name,
                TableRowCountCache.row_count
            ).where(TableRowCountCache.connection_id == connection_id)

            return list(db.execute(stmt).all())

        except SQLAlchemyError as e:
            log_message(f"❌ Erro cache tupla {connection_id}: {str(e)}", "error")
            return []


def get_cached_row_count(connection_id: int, table_name: str, max_age_minutes: int = 60) -> Optional[int]:
    if not table_name:
        return None

    with get_db_session() as db:
        try:
            limite = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)

            stmt = (
                select(TableRowCountCache.row_count)
                .where(
                    TableRowCountCache.connection_id == connection_id,
                    TableRowCountCache.table_name == table_name,
                    TableRowCountCache.last_updated > limite,
                )
            )

            row_count = db.execute(stmt).scalar()
            return int(row_count) if row_count is not None else None

        except SQLAlchemyError as e:
            log_message(f"❌ Erro cache {table_name}: {str(e)}", "error")
            return None


def update_or_create_cache(connection_id: int, table_name: str, count: int) -> Optional[TableRowCountCache]:
    if not table_name:
        return None

    with get_db_session() as db:
        try:
            now = datetime.now(timezone.utc)

            stmt = select(TableRowCountCache).where(
                TableRowCountCache.connection_id == connection_id,
                TableRowCountCache.table_name == table_name,
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
            log_message(f"❌ Erro upsert cache {table_name}: {str(e)}", "error")
            return None