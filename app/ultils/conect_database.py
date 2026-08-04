from typing import Dict, Any, Tuple
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import (
    SQLAlchemyError,
    OperationalError,
    DBAPIError,
)
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

from app.ultils.logger import log_message


def close_engine(engine: Any) -> None:
    """
    Fecha uma conexão, seja ela SQLAlchemy ou MongoDB.

    Existe porque as duas APIs divergem e a confusão é silenciosa e
    traiçoeira: `MongoClient.dispose()` não rebenta com AttributeError.
    O pymongo interpreta qualquer atributo desconhecido como o nome de
    uma base de dados, devolve um objeto `Database` chamado "dispose" e
    só o `()` final falha, com a mensagem enganadora
    "'Database' object is not callable".

    MongoClient fecha-se com .close(); Engine do SQLAlchemy com .dispose().
    """
    if engine is None:
        return

    if isinstance(engine, MongoClient):
        engine.close()
        return

    engine.dispose()


class DatabaseManager:
    """Gerencia conexões SQL e NoSQL."""

    DB_URIS_ASYNC = {
        "MySQL": "mysql+aiomysql://{user}:{password}@{host}:{port}/{database}",
        "MariaDB": "mariadb+aiomysql://{user}:{password}@{host}:{port}/{database}",
        "PostgreSQL": "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}",
        "pg": "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}",
        "SQLite": "sqlite+aiosqlite:///{database}",
        "SQL Server": (
            "mssql+aioodbc:///?odbc_connect="
            "DRIVER={{ODBC Driver 17 for SQL Server}};"
            "SERVER={host},{port};"
            "DATABASE={database};"
            "UID={user};PWD={password};"
            "TrustServerCertificate={trustServerCertificate};"
        ),
        "Oracle": "oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}",
        "MongoDB": "mongodb://{user}:{password}@{host}:{port}/{database}?authSource=admin",
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
            "UID={user};PWD={password};"
            "TrustServerCertificate={TrustServerCertificate}"
        ),
        "Oracle": "oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}",
        "MariaDB": "mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}",
        "MongoDB": "mongodb://{user}:{password}@{host}:{port}/{database}?authSource=admin",
    }

    @staticmethod
    def get_default_port(db_type: str) -> str:
        defaults = {
            "MySQL": "3306",
            "MariaDB": "3306",
            "PostgreSQL": "5432",
            "SQLite": "",
            "SQL Server": "1433",
            "Oracle": "1521",
            "MongoDB": "27017",
        }
        return defaults.get(db_type, "")

    @staticmethod
    def get_engine(db_type: str, config: Dict[str, Any]):
        """
        Cria o Engine SQLAlchemy ou MongoClient.
        """

        uri_template = DatabaseManager.DB_URIS.get(db_type)

        if not uri_template:
            raise ValueError(f"Banco '{db_type}' não suportado.")

        try:

            user = config.get("user", "root")
            password = config.get("password", "")
            host = config.get("host", "localhost")
            port = config.get("port", DatabaseManager.get_default_port(db_type))
            database = config.get("database", "")

            # ------------------------
            # Mongo
            # ------------------------

            if db_type == "MongoDB":

                # authSource: a base onde o utilizador foi CRIADO, que muitas
                # vezes não é "admin". Estava fixo em "admin", o que fazia
                # falhar com code 18 qualquer utilizador criado na própria
                # base de dados. Reutiliza-se o campo `service`, livre para
                # MongoDB (só o Oracle lhe dá outro uso), evitando migração.
                auth_source = (
                    config.get("authSource") or config.get("service") or "admin"
                )

                if user and password:
                    # quote_plus é obrigatório: o pymongo rejeita credenciais
                    # com caracteres reservados na URI (@ : / ? # % e acentos).
                    # Sem isto, uma password com um "@" parte a URI e o erro
                    # devolvido é "Authentication failed", que aponta para o
                    # lado errado do problema.
                    uri = (
                        f"mongodb://{quote_plus(str(user))}:"
                        f"{quote_plus(str(password))}@{host}:{port}/{database}"
                        f"?authSource={quote_plus(str(auth_source))}"
                    )
                else:
                    # Sem credenciais não faz sentido indicar authSource.
                    uri = f"mongodb://{host}:{port}/{database}"

                log_message(
                    f"🔌 Criando MongoClient ({host}:{port}) db={database} "
                    f"authSource={auth_source if (user and password) else 'n/a'}",
                    level="debug",
                )

                return MongoClient(
                    uri,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=5000,
                )

            # ------------------------
            # SQL
            # ------------------------

            uri = uri_template.format(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database,
                service=config.get("service", "xe"),
                sslmode=config.get("sslmode", "disable"),
                TrustServerCertificate=config.get(
                    "TrustServerCertificate",
                    "yes",
                ),
            )

            extra_args = {
                "echo": config.get("debug_sql", False),
                "pool_pre_ping": True,
                "pool_recycle": config.get("pool_recycle", 1800),
            }

            if db_type != "SQLite":
                extra_args.update({
                    "pool_size": config.get("pool_size", 5),
                    "max_overflow": config.get("max_overflow", 10),
                    "pool_timeout": config.get("pool_timeout", 30),
                })

            if "connect_args" in config:
                extra_args["connect_args"] = config["connect_args"]

            log_message(
                f"🔌 Criando Engine {db_type} ({host}:{port})",
                level="debug",
            )

            return create_engine(uri, **extra_args)

        except Exception as e:
            log_message(
                f"❌ Erro ao criar engine {db_type}: {e}",
                level="error",
            )
            raise

    @staticmethod
    def connect(db_type: str, config: Dict[str, Any]) -> Tuple[Any, Any]:
        """
        Cria sessão e testa conexão.
        """

        session = None
        engine = None

        try:

            engine = DatabaseManager.get_engine(db_type, config)

            # ------------------------
            # Mongo
            # ------------------------

            if db_type == "MongoDB":

                engine.admin.command("ping")

                log_message(
                    "✅ MongoDB conectado.",
                    level="info",
                )

                return engine, engine

            # ------------------------
            # SQL
            # ------------------------

            SessionLocal = sessionmaker(bind=engine)

            session = SessionLocal()

            with engine.connect() as conn:

                conn.execute(text("SELECT 1"))

            log_message(
                f"✅ Conexão com {db_type} realizada.",
                level="info",
            )

            return session, engine

        except ServerSelectionTimeoutError as e:

            log_message(
                f"❌ MongoDB indisponível: {e}",
                level="error",
            )

            raise

        except OperationalError as e:

            log_message(
                f"❌ Erro operacional em {db_type}: {e}",
                level="error",
            )

            if (
                db_type == "PostgreSQL"
                and "ssl" in str(e).lower()
            ):
                log_message(
                    "🔁 Tentando PostgreSQL sem SSL...",
                    level="warning",
                )
                return DatabaseManager.connect("pg", config)

            raise

        except (SQLAlchemyError, DBAPIError, PyMongoError) as e:

            log_message(
                f"❌ Falha na conexão ({db_type}): {e}",
                level="error",
            )
            raise

        finally:

            if session:
                try:
                    session.close()
                except Exception:
                    pass

    @staticmethod
    def test_connection(db_type: str, config: Dict[str, Any]) -> bool:
        """
        Apenas testa a conectividade.
        """

        engine = None
        session = None

        try:

            engine = DatabaseManager.get_engine(db_type, config)

            if db_type == "MongoDB":

                engine.admin.command("ping")

            else:

                SessionLocal = sessionmaker(bind=engine)
                session = SessionLocal()

                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

            log_message(
                f"✅ Teste de conexão com {db_type} executado com sucesso.",
                level="info",
            )

            return True

        except Exception as e:

            log_message(
                f"❌ Teste de conexão falhou ({db_type}): {e}",
                level="error",
            )

            return False

        finally:

            if session:
                try:
                    session.close()
                except Exception:
                    pass

            if engine:

                try:
                    close_engine(engine)
                except Exception:
                    pass