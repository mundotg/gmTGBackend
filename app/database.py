
from typing import Any
from urllib.parse import parse_qsl, urlparse, urlunparse

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from app.config.dotenv import get_env, get_env_int
from app.ultils.db_ssl import asyncpg_ssl_arg
from app.ultils.logger import log_message

# ============================================================
# 📦 URL BASE (do .env)
# ============================================================
DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./test.db")


# ============================================================
# ⚙️ CONFIGURAÇÃO SYNC
# ============================================================
IS_SQLITE = DATABASE_URL.startswith("sqlite")

connect_args = {"check_same_thread": False} if IS_SQLITE else {}

# Parâmetros de pool só se aplicam a bases de dados de rede.
# O SQLite usa um pool próprio e rejeita estas opções.
if IS_SQLITE:
    pool_kwargs = {}
else:
    pool_kwargs = {
        # Valida a ligação antes de a entregar. Sem isto, uma conexão
        # fechada pelo servidor (timeout, restart, firewall) só é detetada
        # quando a query rebenta — resultando em 500 intermitentes.
        "pool_pre_ping": True,
        # Recicla ligações antes do wait_timeout típico do MySQL/Postgres.
        "pool_recycle": get_env_int("DB_POOL_RECYCLE", 1800),
        "pool_size": get_env_int("DB_POOL_SIZE", 5),
        "max_overflow": get_env_int("DB_MAX_OVERFLOW", 10),
        # Falha rápido em vez de bloquear o worker indefinidamente à espera
        # de uma ligação livre.
        "pool_timeout": get_env_int("DB_POOL_TIMEOUT", 30),
    }

sync_db_url = DATABASE_URL
if sync_db_url.startswith("postgres://"):
    sync_db_url = sync_db_url.replace("postgres://", "postgresql://", 1)

sync_engine = create_engine(sync_db_url, connect_args=connect_args, **pool_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)


# ============================================================
# ⚡ CONFIGURAÇÃO ASYNC
# ============================================================
def convert_to_asyncpg_url(url: str) -> str:
    """Converte URL síncrona para asyncpg (sem query string para asyncpg)"""
    parsed = urlparse(url)

    clean_url = urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            "",  # query string vazia
            parsed.fragment,
        )
    )

    log_message(
        "🔧 URL convertida para asyncpg (sem query parameters)",
        source="database.py",
        withBd=True,
    )

    if clean_url.startswith("postgres://"):
        return clean_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif clean_url.startswith("postgresql+psycopg2://"):
        return clean_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
    elif clean_url.startswith("postgresql://"):
        return clean_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return clean_url


if IS_SQLITE:
    ASYNC_DATABASE_URL = DATABASE_URL.replace("sqlite://", "sqlite+aiosqlite://", 1)
    async_connect_args: dict[str, Any] = {"check_same_thread": False}
    async_pool_kwargs: dict[str, Any] = {}
elif DATABASE_URL.startswith("postgresql") or DATABASE_URL.startswith("postgres"):
    ASYNC_DATABASE_URL = convert_to_asyncpg_url(DATABASE_URL)

    parsed = urlparse(DATABASE_URL)
    query_params = dict(parse_qsl(parsed.query))
    sslmode = query_params.get("sslmode") or query_params.get("ssl_mode")

    async_connect_args = {
        "ssl": asyncpg_ssl_arg(parsed.hostname, sslmode),
    }

    # Desativa prepared statements no asyncpg se estiver atrás do PgBouncer
    # (PgBouncer em modo transação não suporta prepared statements com nome)
    is_pgbouncer = (
        query_params.get("pgbouncer", "").lower() in ("1", "true", "yes", "on")
        or parsed.port in (6432, 6543)
        or get_env("DB_PG_BOUNCER", "false").lower() in ("1", "true", "yes", "on")
    )
    if is_pgbouncer:
        async_connect_args["statement_cache_size"] = 0

    async_pool_kwargs = pool_kwargs
else:
    raise ValueError("❌ Driver não suportado para ASYNC.")

async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=False,
    future=True,
    connect_args=async_connect_args,
    **async_pool_kwargs,
)
AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


# ============================================================
# 🧱 BASE DECLARATIVA
# ============================================================
Base = declarative_base()


# ============================================================
# 🔁 DEPENDÊNCIAS DE SESSÃO
# ============================================================


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def get_db_async():
    async with AsyncSessionLocal() as session:
        yield session


# ============================================================
# 🩺 HEALTH CHECK
# ============================================================


def check_database_health() -> tuple[bool, str]:
    """
    Verifica se a base de dados da aplicação responde.

    Devolve (alcançável, detalhe). Nunca levanta exceção — é chamada por
    endpoints de health, que têm de responder mesmo com a BD em baixo.
    """
    try:
        with sync_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "ok"

    except Exception as exc:
        # A mensagem pode conter o host/utilizador da ligação, por isso
        # devolve-se apenas o tipo do erro; o detalhe fica no log.
        log_message(f"🩺 Health check da BD falhou: {exc}", "error", withBd=True)
        return False, type(exc).__name__
