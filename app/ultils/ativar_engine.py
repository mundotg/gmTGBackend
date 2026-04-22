import ssl  # 👈 ADICIONADO: Necessário para o ssl_context
import traceback
from typing import Tuple

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from app.config.dependencies import defaults, get_session_by_connection
from app.config.engine_manager_cache import EngineManager
from app.models.connection_models import DBConnection
from app.services.crypto_utils import aes_decrypt
from app.ultils.ativar_session_bd import (
    get_connection_by_id,
    get_connection_current,
    get_connection_current_async,
    get_connection_id_async,
    reativar_connection,
)
from app.ultils.conect_database import DatabaseManager
from app.ultils.logger import log_message


async def test_session_connection(async_session: AsyncSession):
    """Testa se uma sessão AsyncSession está funcional."""
    try:
        result = await async_session.execute(text("SELECT 1"))
        value = result.scalar_one()
        log_message(f"✅ Conexão bem-sucedida! Retorno: {value}", "info")
    except Exception as e:
        log_message(f"❌ Erro ao testar sessão: {e}", "error")


class ConnectionManager:
    """Gerenciador de conexões com o banco de dados."""

    @staticmethod
    def ensure_connection(db: Session, user_id: int):
        """
        Garante que existe uma conexão ativa para o usuário.
        Retorna: (engine, connection)
        """
        try:
            if user_id <= 0:
                raise HTTPException(status_code=400, detail="user_id inválido")

            engine = EngineManager.get(user_id)

            # Se não existir engine ativa tenta reativar
            if not engine:
                log_message(f"Tentando reativar conexão | user_id={user_id}", "info")
                result = reativar_connection(db=db, id_user=user_id)

                if not result or not result.get("success"):
                    log_message(
                        f"Falha ao reativar conexão | user_id={user_id}", "warning"
                    )
                    raise HTTPException(
                        status_code=400,
                        detail="Conexão do banco de dados não encontrada",
                    )

                engine = EngineManager.get(user_id)

                if not engine:
                    log_message(
                        f"Engine não criada após reativação | user_id={user_id}",
                        "error",
                    )
                    raise HTTPException(
                        status_code=500,
                        detail="Falha ao inicializar engine da conexão",
                    )

            connection, _ = get_connection_current(db, user_id)

            if connection is None:
                log_message(
                    f"Conexão atual não encontrada | user_id={user_id}", "warning"
                )
                raise HTTPException(
                    status_code=400,
                    detail="ID da conexão não está disponível",
                )

            return engine, connection

        except HTTPException:
            raise
        except Exception as e:
            log_message(
                f"Erro inesperado em ensure_connection | user_id={user_id} | "
                f"erro={str(e)} | trace={traceback.format_exc()}",
                "error",
            )
            raise HTTPException(
                status_code=500,
                detail="Erro interno ao verificar conexão",
            )

    @staticmethod
    def ensure_idConn_connection(db: Session, user_id: int, id_connection: int):
        """Garante que existe uma conexão válida por ID."""
        connection = get_connection_by_id(db, user_id, id_connection)
        engine = get_session_by_connection(connection)

        if not engine:
            log_message(f"Reativando conexão para usuário {user_id}", "error")
            raise HTTPException(
                status_code=400, detail="Conexão do banco de dados não encontrada"
            )

        return engine, connection

    # =====================================================
    # 🔄 MÉTODO ASSÍNCRONO POR ID
    # =====================================================
    @staticmethod
    async def get_engine_idconn_async(
        db: AsyncSession, user_id: int, id_connection: int
    ) -> Tuple[AsyncEngine, DBConnection]:

        connection = await get_connection_id_async(db, user_id, id_connection)

        if not connection:
            raise HTTPException(status_code=400, detail="Conexão não encontrada")

        engine = EngineManager.async_get(user_id)
        if engine:
            return engine, connection

        engine = await ConnectionManager._create_async_engine(connection)
        # 👈 AJUSTE: Opcionalmente, podes querer guardar na cache aqui também.
        # EngineManager.async_set(user_id, engine)

        return engine, connection

    # =====================================================
    # 🧩 MÉTODO INTERNO DE CRIAÇÃO DE ENGINE
    # =====================================================
    @staticmethod
    async def get_engine_async(
        db: AsyncSession, user_id: int
    ) -> Tuple[AsyncEngine, DBConnection]:

        connection, _ = await get_connection_current_async(db, user_id)

        if connection is None:
            raise HTTPException(status_code=400, detail="Conexão não encontrada")

        # 🔎 verifica se engine já existe na cache
        engine = EngineManager.async_get(user_id)
        if engine:
            return engine, connection

        # cria engine nova
        engine = await ConnectionManager._create_async_engine(connection)

        # 👈 CORREÇÃO: Guardar a engine na cache (antes estava '(user_id, engine)')
        EngineManager.async_set(user_id, engine)

        return engine, connection

    @staticmethod
    async def _create_async_engine(connection: DBConnection) -> AsyncEngine:
        db_type = (connection.type or "").lower()
        engineManager = DatabaseManager()

        try:
            if db_type == "sqlite":
                db_path = aes_decrypt(connection.host)
                uri = f"sqlite+aiosqlite:///{db_path}"
                engine = create_async_engine(uri, echo=False, pool_pre_ping=True)
            else:
                config = {
                    "user": (
                        aes_decrypt(connection.username) if connection.username else ""
                    ),
                    "password": (
                        aes_decrypt(connection.password) if connection.password else ""
                    ),
                    "host": aes_decrypt(connection.host),
                    "port": connection.port,
                    "database": connection.database_name,
                    "service": connection.service or "",
                    "sslmode": connection.sslmode or "disable",
                    "trustServerCertificate": connection.trustServerCertificate
                    or "yes",
                }

                uri_template = engineManager.DB_URIS_ASYNC.get(
                    defaults.get(connection.type)
                )

                if not uri_template:
                    raise ValueError(f"Banco não suportado: {connection.type}")

                # PostgreSQL
                if db_type in ["postgresql", "pg"]:
                    uri = uri_template.format(**config)

                    if config["host"] in ["localhost", "127.0.0.1"]:
                        connect_args = {"ssl": False}
                    else:
                        ssl_context = None
                        if config["sslmode"].lower() != "disable":
                            ssl_context = ssl.create_default_context()
                        connect_args = (
                            {"ssl": ssl_context} if ssl_context else {"ssl": False}
                        )

                # SQL Server
                elif db_type in ["mssql", "sqlserver"]:
                    uri = uri_template.format(**config)
                    connect_args = {}

                # Outros (ex: MySQL, Oracle)
                else:
                    uri = uri_template.format(**config)
                    connect_args = {}
                engine = create_async_engine(
                    uri,
                    echo=False,
                    pool_pre_ping=True,
                    pool_size=3,
                    max_overflow=5,
                    pool_timeout=30,
                    pool_recycle=1800,
                    connect_args=connect_args,
                )

            # 🔎 teste de conexão (com tratamento para garantir que a ligação devolve à pool)
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
                await conn.commit()  # Assegura a libertação limpa

            log_message(f"✅ Engine criada ({connection.type})", "info")
            return engine

        except SQLAlchemyError as e:
            log_message(
                f"❌ erro criando engine: {e}\n{traceback.format_exc()}", "error"
            )
            raise HTTPException(status_code=500, detail="Erro ao conectar ao banco")
