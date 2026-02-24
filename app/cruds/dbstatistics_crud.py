from typing import List, Optional, Tuple
from sqlalchemy.orm import Session, load_only
from sqlalchemy import func
from datetime import datetime, timedelta, timezone

from app.cruds.dbstructure_crud import get_db_structures
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
    """Retorna estatísticas de uma conexão específica."""
    # Leve: pega só as colunas necessárias (evita carregar relação/lixo)
    return (
        db.query(DBStatistics)
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
        .filter(DBStatistics.db_connection_id == connection_id)
        .first()
    )


def get_statistics_by_connection_geral(
    db: Session, connection_id: int
) -> Optional[ConnectionStatisticsOverview]:
    """
    Retorna uma visão geral com as estatísticas e quantidade de tabelas registradas na estrutura.
    Otimização: evita carregar todas estruturas só pra contar.
    """
    db_stat = get_statistics_by_connection(db, connection_id)
    if not db_stat:
        log_message(f"⚠️ Nenhuma estatística encontrada para a conexão ID={connection_id}.", "warning")
        return None

    # 🔥 PERFORMANCE: contar tabelas sem carregar todas as estruturas
    # Se o teu get_db_structures já está otimizado pra retornar leve, beleza,
    # mas ainda assim len(...) carrega tudo. Aqui vai melhor:
    table_count = (
        db.query(func.count(1))
        .select_from(get_db_structures.__globals__.get("DBStructure"))  # fallback se DBStructure estiver no módulo
        .filter(get_db_structures.__globals__.get("DBStructure").db_connection_id == connection_id)  # type: ignore
        .scalar()
        if get_db_structures.__globals__.get("DBStructure") is not None
        else len(get_db_structures(db, connection_id=connection_id) or [])
    )

    return ConnectionStatisticsOverview(
        statistics=DBStatisticsCreate.model_validate(db_stat, from_attributes=True),
        total_structured_tables=int(table_count or 0),
    )


def converter_stats(stats: ConnectionStatisticsOverview | None) -> dict | None:
    """
    Converte as estatísticas para o formato esperado pelo frontend.
    """
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


def create_statistics(db: Session, stats: DBStatisticsCreate) -> DBStatistics:
    """Cria uma nova entrada de estatísticas."""
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

    # Atualiza somente se mudou (evita write desnecessário)
    changed = False
    for key, value in update_data.items():
        if hasattr(db_stat, key) and getattr(db_stat, key) != value:
            setattr(db_stat, key, value)
            changed = True

    # Sempre atualiza updated_at se houve alteração
    if changed:
        now = datetime.now(timezone.utc)
        db_stat.updated_at = now

        # Se queries_today foi alterado, atualiza last_query_at (mantém tua regra)
        if "queries_today" in update_data:
            db_stat.last_query_at = update_data.get("last_query_at", now)

        db.commit()
        db.refresh(db_stat)

        log_message(f"ℹ️ Estatísticas atualizadas para conexão ID={connection_id}", "info")

    return db_stat


# ============================================================
#  TableRowCountCache
# ============================================================
def get_cached_row_count_all(db: Session, connection_id: int) -> List[TableRowCountCache]:
    """Retorna todos os registros de cache para uma conexão específica."""
    return (
        db.query(TableRowCountCache)
        .options(
            load_only(
                TableRowCountCache.connection_id,
                TableRowCountCache.table_name,
                TableRowCountCache.row_count,
                TableRowCountCache.last_updated,
            )
        )
        .filter(TableRowCountCache.connection_id == connection_id)
        .all()
    )


def get_cached_row_count_all_tupla(db: Session, connection_id: int) -> List[Tuple[str, int]]:
    return (
        db.query(TableRowCountCache.table_name, TableRowCountCache.row_count)
        .filter(TableRowCountCache.connection_id == connection_id)
        .all()
    )


def get_cached_row_count(
    db: Session, connection_id: int, table_name: str, max_age_minutes: int = 60
) -> int | None:
    """
    Retorna o valor do cache se ainda estiver dentro do tempo válido.
    Otimização: valida no banco (menos lógica em Python + menos dados carregados).
    """
    if not table_name:
        return None

    now = datetime.now(timezone.utc)
    limite = now - timedelta(minutes=max_age_minutes)

    row_count = (
        db.query(TableRowCountCache.row_count)
        .filter(
            TableRowCountCache.connection_id == connection_id,
            TableRowCountCache.table_name == table_name,
            TableRowCountCache.last_updated.isnot(None),
            TableRowCountCache.last_updated > limite,
        )
        .scalar()
    )

    return int(row_count) if row_count is not None else None


def update_or_create_cache(db: Session, connection_id: int, table_name: str, count: int):
    """
    Atualiza ou cria um novo cache com a contagem de registros.
    Otimização: atualiza apenas se mudou.
    """
    if not table_name:
        return None

    now = datetime.now(timezone.utc)

    cache = (
        db.query(TableRowCountCache)
        .options(
            load_only(
                TableRowCountCache.connection_id,
                TableRowCountCache.table_name,
                TableRowCountCache.row_count,
                TableRowCountCache.last_updated,
            )
        )
        .filter(
            TableRowCountCache.connection_id == connection_id,
            TableRowCountCache.table_name == table_name,
        )
        .first()
    )

    if cache:
        # evita writes desnecessários
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
