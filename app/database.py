from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config.dotenv import get_env

# Define a URL padrão (usando SQLite local se não definido)
DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./test.db")

# Define configurações adicionais dependendo do tipo de banco
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

# Cria o engine
engine = create_engine(DATABASE_URL, connect_args=connect_args)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base declarativa
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
