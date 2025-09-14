from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from urllib.parse import quote_plus
from app.config.dotenv import get_env

# URL base (lida do .env)
DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./test.db")

# --- CONFIGURAÇÃO SYNC ---
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
sync_engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)

# --- CONFIGURAÇÃO ASYNC ---
if DATABASE_URL.startswith("sqlite"):
    ASYNC_DATABASE_URL = DATABASE_URL.replace("sqlite://", "sqlite+aiosqlite://")

elif DATABASE_URL.startswith("postgresql"):
    # Troca psycopg2 -> asyncpg
    ASYNC_DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

elif DATABASE_URL.startswith("mssql+pyodbc"):
    # Para SQL Server com aioodbc
    # Extrair credenciais da URL
    import sqlalchemy.engine.url as sa_url
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

# Base declarativa
Base = declarative_base()

# Dependência para endpoints síncronos
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Dependência para endpoints assíncronos
async def get_db_async():
    async with AsyncSessionLocal() as session:
        yield session
