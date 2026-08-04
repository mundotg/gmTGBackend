
from urllib.parse import urlparse, urlunparse

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from app.config.dotenv import get_env, get_env_int
from app.ultils.logger import log_message

# ============================================================
# 📦 URL BASE (do .env)
# ============================================================
DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./test.db")


# ============================================================
# 🧩 AJUSTA SSL PARA ASYNC (PostgreSQL)
# ============================================================
def convert_to_asyncpg_url(url: str) -> str:
    """Converte URL síncrona para asyncpg - versão simplificada"""
    if not url.startswith("postgresql://"):
        return url

    # Para asyncpg, é melhor usar parâmetros de conexão via connect_args
    # do que via query string. Vamos remover a query string completamente.
    parsed = urlparse(url)

    # Constrói a URL sem query parameters
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

    # troca o driver psycopg2 → asyncpg
    return clean_url.replace("postgresql://", "postgresql+asyncpg://")


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

sync_engine = create_engine(DATABASE_URL, connect_args=connect_args, **pool_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)


# ============================================================
# ⚡ CONFIGURAÇÃO ASYNC
# ============================================================
if DATABASE_URL.startswith("sqlite"):
    ASYNC_DATABASE_URL = DATABASE_URL.replace("sqlite://", "sqlite+aiosqlite://")
elif DATABASE_URL.startswith("postgresql"):
    ASYNC_DATABASE_URL = convert_to_asyncpg_url(DATABASE_URL)
else:
    raise ValueError("❌ Driver não suportado para ASYNC.")

# Configuração do engine async
# ============================================================
# ⚡ CONFIGURAÇÃO ASYNC
# ============================================================
if DATABASE_URL.startswith("sqlite"):
    async_engine_kwargs = {
        "echo": False,
        "future": True,
        "connect_args": {"check_same_thread": False},  # ✅ apenas para sqlite
    }
else:
    async_engine_kwargs = {
        "echo": False,
        "future": True,
        "connect_args": {
            "ssl": False,  # ou um contexto SSL válido se precisar
            "server_settings": {"jit": "off"},
        },
    }


async_engine = create_async_engine(ASYNC_DATABASE_URL, **async_engine_kwargs)
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
