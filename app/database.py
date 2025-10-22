from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from urllib.parse import quote_plus
import sqlalchemy.engine.url as sa_url

from app.config.dotenv import get_env
from app.ultils.logger import log_message

# ============================================================
# 🌍 DETECÇÃO DO AMBIENTE (DEV / PROD)
# ============================================================
APP_ENV = get_env("APP_ENV", "development").lower()  # Valores: development | production

# ============================================================
# 🔧 MONTAGEM DA DATABASE_URL
# ============================================================
if APP_ENV == "azure":
    PGUSER = get_env("PGUSER", "")
    PGPASSWORD = get_env("PGPASSWORD", "")
    PGHOST = get_env("PGHOST", "localhost")
    PGPORT = get_env("PGPORT", "5432")
    PGDATABASE = get_env("PGDATABASE", "defaultdb")
    PGSSLMODE = get_env("PGSSLMODE", "require")

    if not (PGUSER and PGPASSWORD):
        raise ValueError("❌ Variáveis de ambiente PostgreSQL ausentes em produção.")

    # 🔍 Detecta localhost e desativa SSL automaticamente
    if PGHOST in ("localhost", "127.0.0.1"):
        PGSSLMODE = "disable"

    DATABASE_URL = (
        f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}?sslmode={PGSSLMODE}"
    )
else:
    DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./dev.db")

# ============================================================
# ⚙️ CONFIGURAÇÃO SYNC
# ============================================================
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
sync_engine = create_engine(DATABASE_URL, connect_args=connect_args, echo=False, future=True)

SessionLocal = sessionmaker(bind=sync_engine, autocommit=False, autoflush=False)

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
    raise ValueError(f"❌ Driver não suportado para conexão assíncrona: {DATABASE_URL}")

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
    """Sessão síncrona"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def get_db_async():
    """Sessão assíncrona"""
    async with AsyncSessionLocal() as session:
        yield session

# ============================================================
# 🧩 DEBUG OPCIONAL (LOG)
# ============================================================
def print_db_config():
    """Exibe no log as configurações de banco em uso"""
    log_message("🔧 Ambiente", APP_ENV)
    log_message("🔗 URL de conexão", DATABASE_URL)
    log_message("⚡ URL assíncrona", ASYNC_DATABASE_URL)
