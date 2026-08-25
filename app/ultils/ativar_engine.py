import ssl  # 👈 ADICIONADO: Necessário para o ssl_context
import traceback
from urllib.parse import quote_plus
from typing import Tuple

from fastapi import HTTPException
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from app.config.dependencies import (
    DATABASE_TYPES,
    _maybe_remap_host,
    get_session_by_connection,
)
from app.config.engine_manager_cache import EngineManager
from app.models.connection_models import DBConnection
from app.services.crypto_utils import secret_decrypt
from app.ultils.ativar_session_bd import (
    get_connection_by_id, get_connection_current, get_connection_current_async,
    get_connection_id_async, reativar_connection
)
from app.ultils.conect_database import DatabaseManager
from app.ultils.db_url import async_url, engine_from_url, is_mongo_url, remap_url_host
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

            connection, _ = get_connection_current(db, user_id)

            if connection is None:
                log_message(f"Conexão atual não encontrada | user_id={user_id}", "warning")
                raise HTTPException(
                    status_code=400,
                    detail="ID da conexão não está disponível",
                )

            cached_connection_id = EngineManager.get_connection_id(user_id)
            engine = EngineManager.get(user_id)

            if engine and cached_connection_id is not None and cached_connection_id != connection.id:
                log_message(
                    f"Cache stale detectado | user_id={user_id} | engine_conn_id={cached_connection_id} | current_conn_id={connection.id}",
                    "warning",
                )
                EngineManager.remove(user_id)
                engine = None

            # Se não existir engine ativa tenta reativar
            if not engine:
                log_message(f"Tentando reativar conexão | user_id={user_id}", "info")
                result = reativar_connection(db=db, id_user=user_id)

                if not result or not result.get("success"):
                    log_message(f"Falha ao reativar conexão | user_id={user_id}", "warning")
                    raise HTTPException(
                        status_code=400,
                        detail="Conexão do banco de dados não encontrada",
                    )

                engine = EngineManager.get(user_id)

                if not engine:
                    log_message(f"Engine não criada após reativação | user_id={user_id}", "error")
                    raise HTTPException(
                        status_code=500,
                        detail="Falha ao inicializar engine da conexão",
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
            raise HTTPException(status_code=400, detail="Conexão do banco de dados não encontrada")

        return engine, connection

    # =====================================================
    # 🔄 MÉTODO ASSÍNCRONO POR ID
    # =====================================================
    @staticmethod
    async def get_engine_idconn_async(db: AsyncSession, user_id: int, id_connection: int) -> Tuple[AsyncEngine, DBConnection]:
        """
        Obtém ou cria uma AsyncEngine reutilizável para a conexão solicitada.
        Garante que a engine tenha cache por usuário e que pools antigos não
        permaneçam vivos quando a conexão é trocada.
        """
        connection = await get_connection_id_async(db, user_id, id_connection)

        if not connection:
            raise HTTPException(status_code=400, detail="Conexão não encontrada")

        cached_connection_id = EngineManager.async_get_connection_id(user_id)
        engine = EngineManager.async_get(user_id)

        if engine and cached_connection_id is not None and cached_connection_id != connection.id:
            log_message(
                f"Async cache stale detectado | user_id={user_id} | engine_conn_id={cached_connection_id} | current_conn_id={connection.id}",
                "warning",
            )
            await EngineManager.async_remove(user_id)
            engine = None

        if engine:
            return engine, connection

        engine = await ConnectionManager._create_async_engine(connection)
        EngineManager.async_set(user_id, engine, connection_id=connection.id)

        return engine, connection

    # =====================================================
    # 🧩 MÉTODO INTERNO DE CRIAÇÃO DE ENGINE
    # =====================================================
    @staticmethod
    async def get_engine_async(
        db: AsyncSession,
        user_id: int
    ) -> Tuple[AsyncEngine, DBConnection]:

        connection, _ = await get_connection_current_async(db, user_id)

        if connection is None:
            raise HTTPException(status_code=400, detail="Conexão não encontrada")

        cached_connection_id = EngineManager.async_get_connection_id(user_id)
        engine = EngineManager.async_get(user_id)

        if engine and cached_connection_id is not None and cached_connection_id != connection.id:
            log_message(
                f"Async cache stale detectado | user_id={user_id} | engine_conn_id={cached_connection_id} | current_conn_id={connection.id}",
                "warning",
            )
            await EngineManager.async_remove(user_id)
            engine = None

        # 🔎 verifica se engine já existe na cache
        if engine:
            return engine, connection

        # cria engine nova
        engine = await ConnectionManager._create_async_engine(connection)

        # 👈 CORREÇÃO: Guardar a engine na cache (antes estava '(user_id, engine)')
        EngineManager.async_set(user_id, engine, connection_id=connection.id)

        return engine, connection

    @staticmethod
    async def _create_async_engine(connection: DBConnection) -> AsyncEngine | MongoClient:
        db_type = (connection.type or "").lower()
        engineManager = DatabaseManager()

        try:
            # ------------------------------------------------------------
            # Ligação por URL (connection string completa)
            # ------------------------------------------------------------
            # Tem de vir primeiro: com URL não há campos separados de onde
            # remontar a URI, e o esquema da própria URL é que decide se isto é
            # Mongo ou SQL. `async_url` troca o driver pelo assíncrono
            # (psycopg2 → asyncpg) e tira da query os parâmetros que o asyncpg
            # não conhece (sslmode, channel_binding), devolvendo-os como
            # connect_args.
            url_cifrada = getattr(connection, "url", None)

            if url_cifrada:
                url = remap_url_host(secret_decrypt(url_cifrada), _maybe_remap_host)

                if is_mongo_url(url):
                    client = engine_from_url(url)
                    client.admin.command("ping")
                    log_message("✅ MongoClient criado a partir de URL", "info")
                    return client

                uri, connect_args = async_url(url)

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

                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))

                log_message(f"✅ Engine criada a partir de URL ({db_type})", "info")
                return engine

            # ------------------------------------------------------------
            # MongoDB
            # ------------------------------------------------------------
            # Não existe dialecto SQLAlchemy para MongoDB: entregar a URI
            # "mongodb://…" ao create_async_engine dá
            # NoSuchModuleError: Can't load plugin: sqlalchemy.dialects:mongodb
            #
            # Devolve-se o mesmo MongoClient síncrono usado no resto do
            # código, e não um AsyncMongoClient, de propósito: manter um só
            # tipo de objeto é o que faz `is_mongo`, `close_engine` e toda a
            # introspeção continuarem a funcionar sobre este engine.
            # As operações SQL que o receberem são recusadas por
            # `assert_sql_engine`, não por um erro de driver.
            if db_type == "mongodb":
                config = {
                    "user": secret_decrypt(connection.username)
                    if connection.username
                    else "",
                    "password": secret_decrypt(connection.password)
                    if connection.password
                    else "",
                    # Remap localhost→host.docker.internal em contentor.
                    "host": _maybe_remap_host(secret_decrypt(connection.host)),
                    "port": connection.port,
                    "database": connection.database_name,
                    "service": connection.service or "",
                }

                client = engineManager.get_engine("MongoDB", config)

                # Equivalente ao "SELECT 1" das ligações SQL.
                client.admin.command("ping")

                log_message("✅ MongoClient criado (MongoDB)", "info")
                return client

            if db_type == "sqlite":
                db_path = secret_decrypt(connection.host)
                uri = f"sqlite+aiosqlite:///{db_path}"
                engine = create_async_engine(uri, echo=False, pool_pre_ping=True)
            else:
                config = {
                    "user": secret_decrypt(connection.username) if connection.username else "",
                    "password": secret_decrypt(connection.password) if connection.password else "",
                    # Remap localhost→host.docker.internal em contentor.
                    "host": _maybe_remap_host(secret_decrypt(connection.host)),
                    "port": connection.port,
                    "database": connection.database_name,
                    "service": connection.service or "",
                    "sslmode": connection.sslmode or "disable",
                    "trustServerCertificate": connection.trustServerCertificate or "yes",
                }

                uri_template = engineManager.DB_URIS_ASYNC.get(DATABASE_TYPES.get(connection.type))

                if not uri_template:
                    raise ValueError(f"Banco não suportado: {connection.type}")

                # URL-encode user/password nas URIs de "userinfo" (mesma razão
                # do builder síncrono: uma password com @ : / ? # % partia a URI
                # e dava "password authentication failed" falso). SQL Server
                # (ODBC) não usa userinfo → fica em cru.
                _userinfo = db_type not in ["mssql", "sqlserver"]
                uri_config = {
                    **config,
                    "user": quote_plus(str(config["user"])) if (_userinfo and config["user"]) else config["user"],
                    "password": quote_plus(str(config["password"])) if (_userinfo and config["password"]) else config["password"],
                }

                # PostgreSQL
                if db_type in ["postgresql", "pg"]:
                    uri = uri_template.format(**uri_config)

                    # `host.docker.internal` é o loopback do host (para onde
                    # `localhost` foi remapeado): é local e, tal como localhost,
                    # normalmente NÃO fala SSL. Se aqui não desligássemos o SSL,
                    # o asyncpg tentava o upgrade e o servidor recusava
                    # ("rejected SSL upgrade").
                    local_hosts = {"localhost", "127.0.0.1", "host.docker.internal"}
                    sslmode = (config.get("sslmode") or "disable").lower()

                    if config["host"] in local_hosts or sslmode == "disable":
                        # ssl=False → asyncpg nem tenta o upgrade (sem fallback
                        # ao modo "prefer", que também tentaria SSL primeiro).
                        connect_args = {"ssl": False}
                    else:
                        connect_args = {"ssl": ssl.create_default_context()}

                # SQL Server
                elif db_type in ["mssql", "sqlserver"]:
                    uri = uri_template.format(**config)
                    connect_args = {}

                # Outros (ex: MySQL, Oracle)
                else:
                    uri = uri_template.format(**uri_config)
                    connect_args = {}

                engine = create_async_engine(
                    uri,
                    echo=False,
                    pool_pre_ping=True,
                    pool_size=3,
                    max_overflow=5,
                    pool_timeout=30,
                    pool_recycle=1800,
                    connect_args=connect_args
                )

            # 🔎 teste de conexão (com tratamento para garantir que a ligação devolve à pool)
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

            log_message(f"✅ Engine criada ({connection.type})", "info")
            return engine

        except SQLAlchemyError as e:
            log_message(f"❌ erro criando engine: {e}\n{traceback.format_exc()}", "error")
            raise HTTPException(status_code=500, detail="Erro ao conectar ao banco")

        except PyMongoError as e:
            # Só SQLAlchemyError era apanhado: uma falha do pymongo (auth,
            # servidor inacessível) escapava em cru para o cliente.
            log_message(
                f"❌ erro criando MongoClient: {e}\n{traceback.format_exc()}", "error"
            )
            raise HTTPException(
                status_code=503, detail="Não foi possível ligar ao MongoDB"
            )