from http.client import HTTPException
from typing import Any, Dict, Tuple
from app.config.dependencies import EngineManager,defaults
from app.models.connection_models import DBConnection
from app.ultils.ativar_session_bd import get_connection_current, get_connection_current_async, reativar_connection
from app.ultils.conect_database import DatabaseManager
from app.ultils.logger import log_message
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession,AsyncEngine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


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
                raise HTTPException(
                    status_code=400, 
                    detail="Conexão do banco de dados não encontrada"
                )
            engine = EngineManager.get(user_id)
        
        connection, _ = get_connection_current(db, user_id)
        if connection is None:
            raise HTTPException(
                status_code=400, 
                detail="ID da conexão não está disponível"
            )
        
        return engine, connection
    
    @staticmethod
    async def get_engine_async(db: AsyncSession, user_id: int) -> Tuple[AsyncEngine, DBConnection ]:
        engineManager = DatabaseManager()
        connection, _ = await get_connection_current_async(db, user_id)
        if connection is None:
            raise HTTPException(status_code=400, detail="ID da conexão não está disponível")

        config = {
            "user": connection.username,
            "password": connection.password,
            "host": connection.host,
            "port": connection.port,
            "database": connection.database_name,
            "service": connection.service,
            "sslmode": connection.sslmode,
            "trustServerCertificate": connection.trustServerCertificate
        }

        uri_template = engineManager.DB_URIS_ASYNC.get(defaults.get(connection.type))
        if not uri_template:
            raise ValueError(f"Tipo de banco de dados não suportado: {connection.type}")

        try:
            # Para PostgreSQL async, removemos sslmode da URL
            if connection.type.lower() in ["postgresql", "pg"]:
                uri = uri_template.format(
                    user=config.get("user", "root"),
                    password=config.get("password", ""),
                    host=config.get("host", "localhost"),
                    port=config.get("port", DatabaseManager.get_default_port(connection.type)),
                    database=config.get("database", "")
                )
                import ssl
                # Configura SSL se necessário
                ssl_context = None
                if connection.sslmode and connection.sslmode.lower() != "disable":
                    ssl_context = ssl.create_default_context()
                    if connection.sslmode.lower() == "require":
                        ssl_context.check_hostname = False
                        ssl_context.verify_mode = ssl.CERT_NONE

                connect_args = {"ssl": ssl_context} if ssl_context else {}

            else:
                # Para outros SGBDs, mantém os parâmetros normais
                uri = uri_template.format(
                    user=config.get("user", "root"),
                    password=config.get("password", ""),
                    host=config.get("host", "localhost"),
                    port=config.get("port", DatabaseManager.get_default_port(connection.type)),
                    database=config.get("database", ""),
                    service=config.get("service", "xe"),
                    sslmode=config.get("sslmode", "disable"),
                    trustServerCertificate=config.get("trustServerCertificate", "yes")
                )
                connect_args = config.get("connect_args", {})

            extra_args: Dict[str, Any] = {
                "echo": config.get("debug_sql", False),
                "pool_pre_ping": True,
                "connect_args": connect_args
            }

            # Só aplica pool para bancos que suportam (não para SQLite)
            if connection.type.lower() != "sqlite":
                extra_args.update({
                    "pool_size": config.get("pool_size", 5),
                    "max_overflow": config.get("max_overflow", 10),
                    "pool_timeout": config.get("pool_timeout", 30),
                    "pool_recycle": config.get("pool_recycle", 1800),
                })

            engine = create_async_engine(uri, **extra_args)

            # Testa conexão
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

            return engine, connection

        except SQLAlchemyError as e:
            log_message(f"❌ Erro ao criar engine ou testar conexão: {e}", "error")
            raise HTTPException(status_code=500, detail="Erro interno ao criar conexão com o banco de dados")