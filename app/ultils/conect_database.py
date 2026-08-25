from typing import Dict, Any, Optional, Tuple
from urllib.parse import quote_plus

from fastapi import HTTPException, status
from pymongo.database import Database as MongoDatabase
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import (
    SQLAlchemyError,
    OperationalError,
    DBAPIError,
)
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

from app.ultils.db_url import engine_from_url
from app.ultils.logger import log_message


def is_mongo(engine: Any) -> bool:
    """
    True se a ligação for MongoDB em vez de um Engine do SQLAlchemy.

    Vive aqui, e não no database_inspector, porque é preciso tanto na
    introspeção como na leitura de campos — e o inspector já importa o
    field_info, pelo que o caminho inverso criaria um ciclo.
    """
    return isinstance(engine, MongoClient)


def get_mongo_database(engine: MongoClient) -> Optional[MongoDatabase]:
    """
    Devolve a base de dados a inspecionar.

    Usa a que vem na URI da conexão. Só se ela não existir é que recorre a
    `list_database_names()`, que exige privilégios sobre o cluster inteiro —
    um utilizador limitado a uma base recebe erro de autorização e ficaria
    sem qualquer resultado.
    """
    try:
        database = engine.get_default_database()
        if database is not None and database.name:
            return database
    except Exception:  # noqa: S110 - URI sem base é normal; descobre-se abaixo
        pass

    try:
        nomes = [
            n
            for n in engine.list_database_names()
            if n not in ("admin", "config", "local")
        ]
        if nomes:
            return engine[nomes[0]]
    except Exception as e:
        log_message(f"⚠️ Não foi possível listar bases MongoDB: {e}", "warning")

    return None


def assert_sql_engine(engine: Any, operacao: str) -> None:
    """
    Recusa, com mensagem clara, operações SQL sobre uma ligação MongoDB.

    Sem isto o erro que chega ao utilizador é opaco: `engine.connect()` num
    MongoClient não falha com AttributeError — o pymongo devolve um objeto
    Database chamado "connect" e o erro final é
    "'Database' object is not callable", que não diz nada sobre a causa.

    A introspeção (listar coleções, campos, estatísticas) já funciona em
    MongoDB. O que ainda não tem equivalente implementado são as operações
    que dependem de sintaxe SQL (consultar, inserir, editar, alterar
    schema) ou de ferramentas próprias do SGBD (pg_dump, mysqldump), e é
    isso que esta função sinaliza.
    """
    if is_mongo(engine):
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=(
                f"{operacao} ainda não está disponível para conexões MongoDB. "
                "A leitura de estruturas, campos e estatísticas funciona; "
                "esta operação depende de SQL ou de ferramentas específicas "
                "do SGBD e precisa de um equivalente MongoDB."
            ),
        )


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
        # ⚠️ Não usar: não existe dialecto SQLAlchemy para MongoDB, e entregar
        # esta URI ao create_async_engine dá NoSuchModuleError. O caminho
        # async trata MongoDB antes de chegar aqui (ver
        # ConnectionManager._create_async_engine), devolvendo um MongoClient.
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

        Se `config["url"]` vier preenchido, é essa a ligação: a URL do
        fornecedor é usada tal como foi colada, sem ser remontada a partir dos
        campos (ver app/ultils/db_url.py). É o modo "ligar por URL" do
        formulário.
        """

        url = (config.get("url") or "").strip()
        if url:
            return engine_from_url(
                url,
                echo=config.get("debug_sql", False),
                pool_recycle=config.get("pool_recycle", 1800),
            )

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

            # URL-encode user/password nas URIs de "userinfo"
            # (postgres://user:pass@host). Sem isto, uma password com um
            # caractere reservado (@ : / ? # %) parte a URI e o driver
            # autentica com credenciais erradas — o servidor devolve
            # "password authentication failed", que aponta para o lado
            # errado do problema. SQL Server (ODBC) e SQLite não usam
            # userinfo, por isso ficam com os valores em cru.
            uses_userinfo = db_type not in ("SQL Server", "SQLite", "MongoDB")
            uri_user = quote_plus(str(user)) if (uses_userinfo and user) else (user or "")
            uri_pw = quote_plus(str(password)) if (uses_userinfo and password) else (password or "")

            uri = uri_template.format(
                user=uri_user,
                password=uri_pw,
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

            # `is_mongo(engine)` em vez de `db_type == "MongoDB"`: no modo
            # "ligar por URL" o tipo vem do esquema da URL, não do argumento.
            if is_mongo(engine):

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
        """Apenas testa a conectividade (compatibilidade — devolve só bool)."""
        ok, _ = DatabaseManager.test_connection_detailed(db_type, config)
        return ok

    @staticmethod
    def test_connection_detailed(
        db_type: str, config: Dict[str, Any]
    ) -> "tuple[bool, Optional[str]]":
        """
        Testa a conectividade e devolve `(ok, motivo)`.

        Ao contrário de `test_connection` (que devolvia só um bool e perdia o
        erro), aqui traduz-se a exceção do driver numa mensagem acionável —
        conexão recusada, host desconhecido, autenticação falhada, timeout —
        para o utilizador saber o que corrigir em vez de um genérico
        "Falha ao testar conexão".
        """
        engine = None
        session = None

        try:
            engine = DatabaseManager.get_engine(db_type, config)

            if is_mongo(engine):
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
            return True, None

        except Exception as e:
            motivo = DatabaseManager._explain_connection_error(db_type, config, e)
            log_message(
                f"❌ Teste de conexão falhou ({db_type}): {e}", level="error"
            )
            return False, motivo

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

    @staticmethod
    def _explain_connection_error(
        db_type: str, config: Dict[str, Any], error: Exception
    ) -> str:
        """Traduz a exceção do driver numa mensagem legível e acionável."""
        host = config.get("host", "?")
        port = config.get("port", "?")
        alvo = f"{host}:{port}"
        raw = str(error).lower()

        if any(t in raw for t in ("could not translate host", "name or service not known",
                                  "getaddrinfo", "nodename nor servname", "unknown host")):
            return f"Host desconhecido: não foi possível resolver '{host}'."
        if any(t in raw for t in ("connection refused", "actively refused", "connect call failed",
                                  "econnrefused")):
            return (
                f"Ligação recusada a {alvo}. O servidor pode estar desligado, a porta "
                f"errada, ou inacessível a partir do backend (em Docker, 'localhost' é o "
                f"próprio contentor — use o host real ou 'host.docker.internal')."
            )
        if any(t in raw for t in ("timeout", "timed out")):
            return f"Tempo esgotado a ligar a {alvo}. Verifique firewall/rede."
        if any(t in raw for t in ("password authentication failed", "authentication failed",
                                  "auth failed", "access denied", "role \"", "login failed")):
            return "Autenticação falhou: utilizador ou password incorretos."
        if any(t in raw for t in ("database", "does not exist", "unknown database")):
            if "does not exist" in raw or "unknown database" in raw:
                return f"A base de dados '{config.get('database')}' não existe no servidor."
        if "ssl" in raw:
            return f"Erro de SSL ao ligar a {alvo}. Reveja o modo SSL."

        # Fallback: primeira linha da exceção, já sem stack.
        primeira = str(error).strip().splitlines()[0]
        return f"Falha ao ligar a {alvo}: {primeira[:200]}"