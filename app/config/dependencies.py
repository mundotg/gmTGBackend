import os
import socket
import traceback

from fastapi import Depends, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import (
    OperationalError,
    SQLAlchemyError,
)
from sqlalchemy.orm import Session

from app.config.dotenv import get_env
from app.database import get_db
from app.models.connection_models import DBConnection
from app.services.crypto_utils import secret_decrypt
from app.ultils.conect_database import DatabaseManager
from app.ultils.db_url import InvalidDatabaseUrl, parse_db_url, remap_url_host
from app.ultils.logger import log_message


DATABASE_TYPES = {
    "mysql": "MySQL",
    "postgresql": "PostgreSQL",
    "sqlite": "SQLite",
    "sqlserver": "SQL Server",
    "oracle": "Oracle",
    "mariadb": "MariaDB",
    "mongodb": "MongoDB",
}

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def _in_container() -> bool:
    """Deteta se o backend corre dentro de um contentor Docker."""
    return os.path.exists("/.dockerenv") or (
        get_env("IN_DOCKER", "false").lower() == "true"
    )


def _maybe_remap_host(host: str) -> str:
    """
    Dentro de um contentor, `localhost` aponta para o próprio contentor, não
    para a máquina anfitriã onde a base de dados corre. Se a conexão foi criada
    com `localhost`, remapeia-se para o alias que alcança o anfitrião
    (`host.docker.internal`), tal como o compose já faz para a BD da app.

    Só remapeia se: estamos em contentor, o host é local, e o alias resolve.
    Desligável com DB_LOCALHOST_ALIAS="".
    """
    alias = get_env("DB_LOCALHOST_ALIAS", "host.docker.internal")
    if not alias or not _in_container():
        return host
    if (host or "").strip().lower() not in _LOCAL_HOSTS:
        return host

    try:
        socket.getaddrinfo(alias, None)
    except socket.gaierror:
        return host  # o alias não resolve — não arriscar

    log_message(
        f"🔀 Host '{host}' remapeado para '{alias}' (backend em contentor).",
        level="info",
    )
    return alias


def _build_config(connection: DBConnection) -> dict:
    """
    Monta a configuração utilizada pelo DatabaseManager.

    Aceita tanto o modelo ORM como o schema que chega na rota `/conn/connect/`
    — daí os `getattr`.
    """

    host = secret_decrypt(connection.host) if connection.host else ""

    # Modo "ligar por URL": a connection string manda, e os campos servem
    # apenas para as mensagens de erro (host:porta). Ao testar uma conexão nova
    # ainda não há host derivado, por isso tira-se da própria URL.
    url_cifrada = getattr(connection, "url", None)
    url = secret_decrypt(url_cifrada) if url_cifrada else ""

    if url:
        url = remap_url_host(url, _maybe_remap_host)

        if not host:
            try:
                dados = parse_db_url(url)
                host = dados["host"]
                connection_port = dados["port"]
            except InvalidDatabaseUrl:
                connection_port = connection.port or 0
        else:
            connection_port = connection.port or 0

        return {
            "url": url,
            "user": "",
            "password": "",
            "host": host,
            "port": connection_port,
            "database": connection.database_name or "",
            "service": getattr(connection, "service", "") or "",
            "sslmode": getattr(connection, "sslmode", "disable") or "disable",
            "TrustServerCertificate": getattr(
                connection, "trustServerCertificate", "yes"
            )
            or "yes",
        }

    return {
        "user": secret_decrypt(connection.username)
        if connection.username else "",

        "password": secret_decrypt(connection.password)
        if connection.password else "",

        "host": _maybe_remap_host(host),

        "port": connection.port,
        "database": connection.database_name,
        "service": getattr(connection, "service", "xe"),
        "sslmode": getattr(connection, "sslmode", "disable"),
        "TrustServerCertificate": getattr(
            connection,
            "trustServerCertificate",
            "yes",
        ),
    }


def _validate_sqlite(db_path: str):

    engine = create_engine(f"sqlite:///{db_path}")

    try:

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return engine

    except Exception:

        engine.dispose()
        raise


# ---------------------------------------------------------
# API
# ---------------------------------------------------------

def get_session_by_connection_id(
    connection_id: int,
    db: Session = Depends(get_db),
):

    connection = (
        db.query(DBConnection)
        .filter(DBConnection.id == connection_id)
        .first()
    )

    if not connection:
        raise HTTPException(
            status_code=404,
            detail="Conexão não encontrada.",
        )

    return get_session_by_connection(connection)


def get_session_by_connection(connection: DBConnection):

    if not connection:
        raise HTTPException(
            status_code=404,
            detail="Conexão não encontrada.",
        )

    db_type = (connection.type or "").lower()

    try:

        # --------------------------------------
        # URL (connection string completa)
        # --------------------------------------
        # Antes do ramo SQLite de propósito: com URL, o caminho do ficheiro vem
        # dentro dela e não na coluna `host`.

        config = _build_config(connection)

        if config.get("url"):

            ok, motivo = DatabaseManager.test_connection_detailed(
                DATABASE_TYPES.get(db_type) or db_type, config
            )

            if not ok:
                raise HTTPException(
                    status_code=503,
                    detail=motivo or "Falha ao ligar com a URL indicada.",
                )

            return DatabaseManager.get_engine(
                DATABASE_TYPES.get(db_type) or db_type, config
            )

        # --------------------------------------
        # SQLite
        # --------------------------------------

        if db_type == "sqlite":

            db_path = secret_decrypt(connection.host)

            if not db_path:
                raise HTTPException(
                    status_code=400,
                    detail="Caminho SQLite inválido.",
                )

            return _validate_sqlite(db_path)

        # --------------------------------------
        # Outros Bancos
        # --------------------------------------

        database = DATABASE_TYPES.get(db_type)

        if not database:
            raise HTTPException(
                status_code=400,
                detail=f"Banco '{connection.type}' não suportado.",
            )

        ok, motivo = DatabaseManager.test_connection_detailed(database, config)
        if not ok:
            # 503: o servidor de destino está indisponível (não é culpa do
            # nosso backend). O `detail` traz o motivo real e acionável.
            raise HTTPException(
                status_code=503,
                detail=motivo or "Falha ao testar conexão.",
            )

        return DatabaseManager.get_engine(
            database,
            config,
        )

    except HTTPException:
        raise

    except OperationalError as e:

        log_message(
            f"❌ Erro operacional ({db_type}): {e}",
            level="error",
        )

        raise HTTPException(
            status_code=503,
            detail="Banco indisponível.",
        )

    except SQLAlchemyError as e:

        log_message(
            f"❌ SQLAlchemy ({db_type}): {e}",
            level="error",
        )

        raise HTTPException(
            status_code=503,
            detail="Falha na conexão com banco.",
        )

    except Exception as e:

        log_message(
            f"❌ Erro inesperado ({db_type})",
            level="error",
        )

        log_message(traceback.format_exc(), level="error")

        raise HTTPException(
            status_code=500,
            detail="Erro interno.",
        )


def get_test_by_connection(
    conn_data: DBConnection,
    db: Session,
):

    db_type = DATABASE_TYPES.get(
        (conn_data.type or "").lower()
    )

    if not db_type:

        raise HTTPException(
            status_code=400,
            detail="Tipo de banco inválido.",
        )

    config = {
        "user": conn_data.username,
        "password": conn_data.password,
        "host": conn_data.host,
        "port": conn_data.port,
        "database": conn_data.database_name,
        "service": conn_data.service,
        "sslmode": conn_data.sslmode,
        "TrustServerCertificate": conn_data.trustServerCertificate,
    }

    if not DatabaseManager.test_connection(
        db_type,
        config,
    ):
        raise HTTPException(
            status_code=503,
            detail="Falha ao conectar ao banco.",
        )

    return True