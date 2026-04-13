from typing import Dict, Any, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from app.ultils.logger import log_message

class DatabaseManager:
    """Gerencia conexões com diferentes bancos de dados usando SQLAlchemy."""
    DB_URIS_ASYNC = {
        "MySQL": "mysql+aiomysql://{user}:{password}@{host}:{port}/{database}",
        "MariaDB": "mariadb+aiomysql://{user}:{password}@{host}:{port}/{database}",

        "PostgreSQL": "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}",
        "pg": "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}",

        "SQLite": "sqlite+aiosqlite:///{database}",  # pode usar ":memory:" para testes

       "SQL Server": (
            "mssql+aioodbc:///?odbc_connect="
            "DRIVER={{ODBC Driver 17 for SQL Server}};"
            "SERVER={host},{port};"
            "DATABASE={database};"
            "UID={user};PWD={password};"
            "TrustServerCertificate={trustServerCertificate};"
        ),
        "Oracle": "oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}",
    }



    DB_URIS = {
        "MySQL": "mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        "PostgreSQL": "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}",
        "pg": "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
        "SQLite": "sqlite:///{database}",
        "SQL Server": (
            "mssql+pyodbc:///?odbc_connect="
            "DRIVER={{ODBC Driver 17 for SQL Server}};"
            "SERVER={host},{port};"
            "DATABASE={database};"
            "UID={user};PWD={password};TrustServerCertificate={TrustServerCertificate}"
        ),
        "Oracle": "oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}",
        "MariaDB": "mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}",
    }
    
    def get_default_port(db_type: str) -> str:
        defaults = {
            "MySQL": "3306",
            "PostgreSQL": "5432",
            "SQLite": "",
            "SQL Server": "1433",
            "Oracle": "1521",
            "MariaDB": "3306"
        }
        return defaults.get(db_type, "")

    @staticmethod
    def get_engine(db_type: str, config: Dict[str, Any]):
        """Cria e retorna um SQLAlchemy engine configurado para o tipo de banco."""
        uri_template = DatabaseManager.DB_URIS.get(db_type)
        if not uri_template:
            log_message(f"❌ Tipo de banco de dados não suportado: {db_type}", level="error")
            raise ValueError(f"Tipo de banco de dados não suportado: {db_type}")

        try:
            uri = uri_template.format(
                user=config.get("user", "root"),
                password=config.get("password", ""),
                host=config.get("host", "localhost"),
                port=config.get("port", DatabaseManager.get_default_port(db_type)),
                database=config.get("database", ""),
                service=config.get("service", "xe"),  # Apenas Oracle
                sslmode=config.get("sslmode", "disable"),
                TrustServerCertificate=config.get("TrustServerCertificate", "yes"),
            )

            log_message(f"🔌 Criando engine para {db_type} em {config.get('host')}:{config.get('port')}", level="debug")

            # Configurações padrão
            extra_args: Dict[str, Any] = {
                "echo": config.get("debug_sql", False),
                "pool_pre_ping": True,
            }

            # Só aplica pool para bancos que suportam (não para SQLite)
            if db_type != "sqlite":
                extra_args.update({
                    "pool_size": config.get("pool_size", 5),
                    "max_overflow": config.get("max_overflow", 10),
                    "pool_timeout": config.get("pool_timeout", 30),
                    "pool_recycle": config.get("pool_recycle", 1800),
                })

            # Se precisar SSL ou args extras
            if "connect_args" in config:
                extra_args["connect_args"] = config["connect_args"]

            engine = create_engine(uri, **extra_args)
            return engine

        except Exception as e:
            log_message(f"❌ Erro ao montar URI de conexão para {db_type}: {e}", level="error")
            raise

    @staticmethod
    def connect(db_type: str, config: Dict[str, Any]) -> Tuple[Session, Any]:
        """
        Estabelece a conexão e retorna a session + engine.
        Faz fallback para 'pg' se necessário (SSL PostgreSQL).
        """
        try:
            engine = DatabaseManager.get_engine(db_type, config)
            SessionLocal = sessionmaker(bind=engine)
            session = SessionLocal()

            # Testar conexão
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            log_message(f"✅ Conexão estabelecida com sucesso ({db_type})", level="info")
            return session, engine

        except Exception as e:
            log_message(f"❌ Falha ao conectar ao {db_type}: {e}", level="error")
            if db_type == "PostgreSQL" and "SSL" in str(e):
                log_message("🔁 Tentando fallback com tipo 'pg' (PostgreSQL sem SSL)", level="warning")
                return DatabaseManager.connect("pg", config)
            raise

    @staticmethod
    def test_connection(db_type: str, config: Dict[str, Any]) -> bool:
        """Testa a conexão com base nas configurações informadas."""
        try:
            session, engine = DatabaseManager.connect(db_type, config)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            session.close()
            log_message(f"✅ Teste de conexão bem-sucedido com {db_type}", level="info")
            return True
        except Exception as e:
            log_message(f"❌ Teste de conexão falhou para {db_type}: {e}", level="error")
            return False
