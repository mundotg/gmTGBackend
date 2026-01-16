from fastapi import HTTPException
from typing import Any, Dict, Tuple
from app.config.dependencies import EngineManager, defaults, get_session_by_connection
from app.models.connection_models import DBConnection
from app.ultils.ativar_session_bd import (
    get_connection_by_id, get_connection_current, get_connection_current_async,
    get_connection_id_async, reativar_connection
)
from app.ultils.conect_database import DatabaseManager
from app.ultils.logger import log_message
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


async def test_session_connection(async_session: AsyncSession):
    """Testa se uma sessão AsyncSession está funcional."""
    try:
        result = await async_session.execute(text("SELECT 1"))
        value = result.scalar_one()
        print("✅ Conexão bem-sucedida! Retorno:", value)
    except Exception as e:
        print("❌ Erro ao testar sessão:", e)


class ConnectionManager:
    """Gerenciador de conexões com o banco de dados."""

    @staticmethod
    def ensure_connection(db: Session, user_id: int):
        """Garante que existe uma conexão válida para o usuário."""
        engine = EngineManager.get(user_id)

        if not engine:
            log_message(f"Reativando conexão para usuário {user_id}", "info")
            result = reativar_connection(db=db, id_user=user_id)
            if not result["success"]:
                raise HTTPException(status_code=400, detail="Conexão do banco de dados não encontrada")
            engine = EngineManager.get(user_id)

        connection, _ = get_connection_current(db, user_id)
        if connection is None:
            raise HTTPException(status_code=400, detail="ID da conexão não está disponível")

        return engine, connection

    @staticmethod
    def ensure_idConn_connection(db: Session, user_id: int, id_connection: int):
        """Garante que existe uma conexão válida por ID."""
        connection = get_connection_by_id(db, user_id, id_connection)
        engine = get_session_by_connection(connection)
        if not engine:
            log_message(f"Reativando conexão para usuário {user_id}", "info")
            raise HTTPException(status_code=400, detail="Conexão do banco de dados não encontrada")

        return engine, connection

    # =====================================================
    # 🔄 MÉTODO ASSÍNCRONO PADRÃO
    # =====================================================
    @staticmethod
    async def get_engine_async(db: AsyncSession, user_id: int) -> Tuple[AsyncEngine, DBConnection]:
        engineManager = DatabaseManager()
        connection, _ = await get_connection_current_async(db, user_id)
        if connection is None:
            raise HTTPException(status_code=400, detail="ID da conexão não está disponível")

        return await ConnectionManager._create_async_engine(connection, engineManager)

    # =====================================================
    # 🔄 MÉTODO ASSÍNCRONO POR ID
    # =====================================================
    @staticmethod
    async def get_engine_idconn_async(db: AsyncSession, user_id: int, id_connection: int) -> Tuple[AsyncEngine, DBConnection]:
        engineManager = DatabaseManager()
        connection = await get_connection_id_async(db, user_id, id_connection)

        if not connection:
            raise HTTPException(status_code=400, detail="Conexão não encontrada")

        # print(f"✅ Conexão encontrada (ID={connection.id}, Tipo={connection.type}, Host={connection.host})")

        return await ConnectionManager._create_async_engine(connection, engineManager)

    # =====================================================
    # 🧩 MÉTODO INTERNO DE CRIAÇÃO DE ENGINE
    # =====================================================
    @staticmethod
    async def _create_async_engine(connection: DBConnection, engineManager: DatabaseManager):
        """Cria e testa uma engine assíncrona segura."""
        config = {
            "user": connection.username,
            "password": connection.password,
            "host": connection.host,
            "port": connection.port,
            "database": connection.database_name,
            "service": connection.service or "",
            "sslmode": connection.sslmode or "disable",
            "trustServerCertificate": connection.trustServerCertificate or "yes",
        }

        uri_template = engineManager.DB_URIS_ASYNC.get(defaults.get(connection.type))
        if not uri_template:
            raise ValueError(f"Tipo de banco de dados não suportado: {connection.type}")

        try:
            if connection.type.lower() in ["postgresql", "pg"]:
                uri = uri_template.format(
                    user=config["user"],
                    password=config["password"],
                    host=config["host"],
                    port=config["port"],
                    database=config["database"]
                )

                # Configura SSL apenas se for remoto
                if config["host"] in ["localhost", "127.0.0.1"]:
                    connect_args = { "ssl": False,}  # ❌ não incluir "ssl" de forma alguma
                    # print("🔒 Conexão PostgreSQL local detectada — SSL totalmente desativado")
                else:
                    import ssl
                    ssl_context = None
                    if config["sslmode"].lower() != "disable":
                        ssl_context = ssl.create_default_context()
                        if config["sslmode"].lower() == "require":
                            ssl_context.check_hostname = False
                            ssl_context.verify_mode = ssl.CERT_NONE
                    connect_args = {"ssl": ssl_context} if ssl_context else {}

            elif connection.type.lower() in ["mssql", "sqlserver"]:
                uri = uri_template.format(
                    user=config["user"],
                    password=config["password"],
                    host=config["host"],
                    port=config["port"],
                    database=config["database"],
                    trustServerCertificate=config["trustServerCertificate"]
                )
                connect_args = {}

            else:
                uri = uri_template.format(
                    user=config["user"],
                    password=config["password"],
                    host=config["host"],
                    port=config["port"],
                    database=config["database"],
                    service=config["service"]
                )
                connect_args = {}

            # Argumentos extras
            extra_args: Dict[str, Any] = {
                "echo": False,
                "pool_pre_ping": True,
                "connect_args": connect_args if connect_args else {} 
            }

            if connection.type.lower() != "sqlite":
                extra_args.update({
                    "pool_size": 5,
                    "max_overflow": 10,
                    "pool_timeout": 30,
                    "pool_recycle": 1800,
                })

            # Criação da engine
            engine = create_async_engine(uri, **extra_args)

            # Teste da conexão
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

            log_message(f"✅ Engine criada e testada com sucesso ({connection.type})", "info")
            return engine, connection

        except SQLAlchemyError as e:
            log_message(f"❌ Erro ao criar ou testar engine: {e}", "error")
            raise HTTPException(status_code=500, detail=f"Erro ao criar engine ({connection.type})")
