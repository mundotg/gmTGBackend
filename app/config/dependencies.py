import traceback

from fastapi import Depends, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import (
    OperationalError,
    SQLAlchemyError,
)
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.connection_models import DBConnection
from app.services.crypto_utils import aes_decrypt
from app.ultils.conect_database import DatabaseManager
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


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def _build_config(connection: DBConnection) -> dict:
    """
    Monta a configuração utilizada pelo DatabaseManager.
    """

    return {
        "user": aes_decrypt(connection.username)
        if connection.username else "",

        "password": aes_decrypt(connection.password)
        if connection.password else "",

        "host": aes_decrypt(connection.host)
        if connection.host else "",

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
        # SQLite
        # --------------------------------------

        if db_type == "sqlite":

            db_path = aes_decrypt(connection.host)

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

        config = _build_config(connection)

        if not DatabaseManager.test_connection(
            database,
            config,
        ):
            raise HTTPException(
                status_code=503,
                detail="Falha ao testar conexão.",
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