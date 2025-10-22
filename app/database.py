from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from urllib.parse import quote_plus
from app.config.dotenv import get_env
import sqlalchemy.engine.url as sa_url

from app.ultils.logger import log_message

# ============================================================
# 🌍 DETECÇÃO DO AMBIENTE (DEV / PROD)
# ============================================================
APP_ENV = get_env("APP_ENV", "development").lower()  # valores: development | production

# ============================================================
# 🔧 MONTAGEM DA DATABASE_URL
# ============================================================
if APP_ENV == "development":
    # 🔹 Usa URL completa do .env
    DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./dev.db")

else:
    # 🔹 Modo produção - monta com variáveis individuais
    PGUSER = get_env("PGUSER", "")
    PGPASSWORD = get_env("PGPASSWORD", "")
    PGHOST = get_env("PGHOST", "localhost")
    PGPORT = get_env("PGPORT", "5432")
    PGDATABASE = get_env("PGDATABASE", "defaultdb")
    PGSSLMODE = get_env("PGSSLMODE", "require")

    if PGUSER and PGPASSWORD:
        DATABASE_URL = (
            f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}?sslmode={PGSSLMODE}"
        )
    else:
        raise ValueError("Variáveis de ambiente PostgreSQL ausentes em produção.")

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
    ASYNC_DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

elif DATABASE_URL.startswith("mssql+pyodbc"):
    url_obj = sa_url.make_url(DATABASE_URL)
    odbc_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={url_obj.host},{url_obj.port or 1433};"
        f"DATABASE={url_obj.database};"
        f"UID={url_obj.username};"
        f"PWD={url_obj.password}"
    )
    ASYNC_DATABASE_URL = f"mssql+aioodbc:///?odbc_connect={quote_plus(odbc_str)}"

else:
    raise ValueError(f"Driver não suportado para ASYNC: {DATABASE_URL}")

async_engine = create_async_engine(ASYNC_DATABASE_URL, echo=False, future=True)

AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

# ============================================================
# 🧱 BASE E DEPENDÊNCIAS
# ============================================================
Base = declarative_base()


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
# 🧩 DEBUG OPCIONAL (LOG)
# ============================================================
def print_db_config():
    log_message("🔧 Ambiente:", APP_ENV)
    log_message("🔗 URL de conexão:", DATABASE_URL)
    log_message("⚡ URL assíncrona:", ASYNC_DATABASE_URL)
