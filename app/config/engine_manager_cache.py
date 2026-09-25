#app/config/engine_manager_cache.py
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy import Engine, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.ultils.conect_database import close_engine, is_mongo
from app.ultils.logger import log_message


def _verify_sqlite_connection(
    engine: Engine,
    expected_table: Optional[str] = None,
) -> dict:
    """
    Verifica se a conexão SQLite está realmente funcional e se o ficheiro/tabela existem.
    Retorna detalhes úteis para debug.
    """
    db_url = str(engine.url)
    db_file = db_url.replace("sqlite:///", "", 1)
    db_path = Path(db_file)

    info = {
        "dialect": engine.dialect.name,
        "engine_url": db_url,
        "db_file_exists": db_path.exists(),
        "db_file_size": db_path.stat().st_size if db_path.exists() else 0,
        "database_list": [],
        "tables": [],
        "views": [],
        "sqlite_master_objects": [],
        "expected_table": expected_table,
        "expected_table_exists": False,
    }

    with engine.connect() as conn:
        # teste básico
        conn.execute(text("SELECT 1"))

        # mostra em que ficheiro o SQLite diz que está ligado
        db_list = conn.execute(text("PRAGMA database_list")).mappings().all()
        info["database_list"] = [dict(row) for row in db_list]

        # lista real do schema
        schema_rows = conn.execute(
            text("""
                SELECT name, type
                FROM sqlite_master
                WHERE type IN ('table', 'view')
                ORDER BY type, name
            """)
        ).mappings().all()

        info["sqlite_master_objects"] = [dict(row) for row in schema_rows]
        info["tables"] = [row["name"] for row in schema_rows if row["type"] == "table"]
        info["views"] = [row["name"] for row in schema_rows if row["type"] == "view"]

        if expected_table:
            exists = conn.execute(
                text("""
                    SELECT 1
                    FROM sqlite_master
                    WHERE type = 'table' AND name = :table_name
                    LIMIT 1
                """),
                {"table_name": expected_table},
            ).scalar()

            info["expected_table_exists"] = bool(exists)

    return info
# app/config/engine_manager_cache.py




class EngineManager:
    __engines: Dict[int, Engine] = {}
    __engine_connections: Dict[int, int] = {}
    _async_engines: Dict[int, AsyncEngine] = {}
    _async_engine_connections: Dict[int, int] = {}

    # -------------------------
    # SYNC ENGINE
    # -------------------------

    @classmethod
    def set(cls, engine: Engine, id_user: int, connection_id: Optional[int] = None):
        cls.__engines[id_user] = engine
        if connection_id is not None:
            cls.__engine_connections[id_user] = connection_id

    @classmethod
    def get_connection_id(cls, id_user: int) -> Optional[int]:
        return cls.__engine_connections.get(id_user)

    @classmethod
    def get(cls, id_user: int) -> Optional[Engine]:
        return cls.__engines.get(id_user)

    @classmethod
    def remove(cls, id_user: int):
        """Remove engine do usuário e fecha conexão."""
        engine = cls.__engines.pop(id_user, None)
        cls.__engine_connections.pop(id_user, None)

        if engine:
            try:
                # O cache guarda também MongoClient, que não tem .dispose().
                close_engine(engine)
                log_message(f"Engine removido e fechado para usuário {id_user}")
            except Exception as e:
                log_message(f"Erro ao fechar engine {id_user}: {e}", "error")

        else:
            log_message(
                f"Nenhum engine encontrado para remover do usuário {id_user}"
            )

    # -------------------------
    # ASYNC ENGINE
    # -------------------------

    @classmethod
    def async_get(cls, id_user: int) -> Optional[AsyncEngine]:
        return cls._async_engines.get(id_user)

    @classmethod
    def async_get_connection_id(cls, id_user: int) -> Optional[int]:
        return cls._async_engine_connections.get(id_user)

    @classmethod
    def async_set(cls, id_user: int, engine: AsyncEngine, connection_id: Optional[int] = None) -> None:
        cls._async_engines[id_user] = engine
        if connection_id is not None:
            cls._async_engine_connections[id_user] = connection_id

    @classmethod
    async def async_remove(cls, id_user: int):
        """Remove async engine do usuário e fecha pool."""
        engine = cls._async_engines.pop(id_user, None)
        cls._async_engine_connections.pop(id_user, None)

        if engine:
            try:
                # O cache async também guarda MongoClient (não há engine
                # async do SQLAlchemy para MongoDB): `await .dispose()`
                # resolveria ".dispose" como o nome de uma base de dados.
                if is_mongo(engine):
                    close_engine(engine)
                else:
                    await engine.dispose()

                log_message(f"Async engine removido e fechado para usuário {id_user}")
            except Exception as e:
                log_message(f"Erro ao fechar async engine {id_user}: {e}", "error")

    # -------------------------
    # SHUTDOWN
    # -------------------------

    @classmethod
    async def dispose_all(cls):
        """Fecha todas engines no shutdown da aplicação."""

        for engine in cls._async_engines.values():
            try:
                if is_mongo(engine):
                    close_engine(engine)
                else:
                    await engine.dispose()
            except Exception as e:
                log_message(f"Erro ao fechar async engine: {e}", "error")

        cls._async_engines.clear()
        cls._async_engine_connections.clear()

        for engine in cls.__engines.values():
            try:
                close_engine(engine)
            except Exception as e:
                log_message(f"Erro ao fechar engine: {e}", "error")

        cls.__engines.clear()
        cls.__engine_connections.clear()