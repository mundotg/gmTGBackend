from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from app.config.dotenv import get_env
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
    clean_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        "",  # query string vazia
        parsed.fragment
    ))
    
    log_message(f"🔧 URL convertida para asyncpg (sem query parameters)")
    
    # troca o driver psycopg2 → asyncpg
    return clean_url.replace("postgresql://", "postgresql+asyncpg://")


# ============================================================
# ⚙️ CONFIGURAÇÃO SYNC
# ============================================================
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
sync_engine = create_engine(DATABASE_URL, connect_args=connect_args)
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
async_engine_kwargs = {
    "echo": False,
    "future": True,
    "connect_args": {
        "ssl": False,  # 👈 Desativa SSL (resolve o erro)
        "server_settings": {
            "jit": "off"
        }
    }
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
# 🧩 DEBUG OPCIONAL
# ============================================================
def print_db_config():
    log_message("🔗 URL base", DATABASE_URL)
    log_message("⚡ URL assíncrona", ASYNC_DATABASE_URL)