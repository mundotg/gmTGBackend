#app/config/engine_manager_cache.py
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy import Engine, text
from sqlalchemy.ext.asyncio import AsyncEngine

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
    _async_engines: Dict[int, AsyncEngine] = {}

    # -------------------------
    # SYNC ENGINE
    # -------------------------

    @classmethod
    def set(cls, engine: Engine, id_user: int):
        cls.__engines[id_user] = engine

    @classmethod
    def get(cls, id_user: int) -> Optional[Engine]:
        engine = cls.__engines.get(id_user)

        if not engine:
            log_message(
                f"Nenhum engine ativo para o usuário ID {id_user}",
                "error",
            )

        return engine

    @classmethod
    def remove(cls, id_user: int):
        """Remove engine do usuário e fecha conexão."""
        engine = cls.__engines.pop(id_user, None)

        if engine:
            try:
                engine.dispose()
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
    def async_set(cls, id_user: int, engine: AsyncEngine) -> None:
        cls._async_engines[id_user] = engine

    @classmethod
    async def async_remove(cls, id_user: int):
        """Remove async engine do usuário e fecha pool."""
        engine = cls._async_engines.pop(id_user, None)

        if engine:
            try:
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
                await engine.dispose()
            except Exception as e:
                log_message(f"Erro ao fechar async engine: {e}", "error")

        cls._async_engines.clear()

        for engine in cls.__engines.values():
            try:
                engine.dispose()
            except Exception as e:
                log_message(f"Erro ao fechar engine: {e}", "error")

        cls.__engines.clear()