
import traceback
from aiosqlite import OperationalError
from fastapi import Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.connection_models import DBConnection
from app.services.crypto_utils import aes_decrypt
from app.ultils.conect_database import DatabaseManager
from sqlalchemy.exc import SQLAlchemyError

from app.ultils.logger import log_message

defaults = {
    "mysql": "MySQL",
    "postgresql": "PostgreSQL",
    "sqlite": "SQLite",
    "SQLite": "SQLite",
    "sqlserver": "SQL Server",
    "oracle": "Oracle",
    "mariadb": "MariaDB"
}


def get_session_by_connection_id(connection_id: int, db: Session = Depends(get_db)):
    # Buscar a conexão salva
    # print(f"connection_id: {connection_id}")
    connection = db.query(DBConnection).filter(DBConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Conexão não encontrada")

    config = {
        "user": aes_decrypt(connection.username),
        "password": aes_decrypt(connection.password),
        "host": aes_decrypt(connection.host),
        "port": connection.port,
        "database": connection.database_name,
        "service": connection.service if hasattr(connection, "service_name") else "xe",
        "sslmode"       : connection.sslmode,
        "trustServerCertificate" : connection.trustServerCertificate
    }

    try:
        db_type = (connection.type or "").lower()
        # print(f"db_type: {db_type}")
        if db_type == "sqlite":
            db_path = aes_decrypt(connection.host)

            if not db_path:
                raise HTTPException(
                    status_code=400,
                    detail="Caminho do ficheiro SQLite não encontrado."
                )

            engine = create_engine(f"sqlite:///{db_path}")
            rs= ""
            with engine.connect() as conn:
                rs=conn.execute(text("SELECT 1"))
                
            # info = _verify_sqlite_connection(engine,connection.database_name)
            # print(rs)
            # print("dialect:", info["dialect"])
            # print("engine_url:", info["engine_url"])
            # print("db_file_exists:", info["db_file_exists"])
            # print("db_file_size:", info["db_file_size"])
            # print("database_list:", info["database_list"])
            # print("tables:", info["tables"])
            # print("views:", info["views"])
            # print("expected_table:", info["expected_table"])
            # print("expected_table_exists:", info["expected_table_exists"])

            return engine
        # Obter o engine com base nas configs
        engine = DatabaseManager.get_engine(defaults[connection.type], config)

        return engine

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar ao banco de dados: {str(e)} {traceback.format_exc()}")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}{traceback.format_exc()}")
    
from sqlalchemy import create_engine, text
from fastapi import HTTPException

def get_session_by_connection(connection: DBConnection):
    if not connection:
        raise HTTPException(status_code=404, detail="Conexão não encontrada")

    try:
        db_type = (connection.type or "").lower()

        if db_type == "sqlite":
            db_path = aes_decrypt(connection.host)

            if not db_path:
                raise HTTPException(
                    status_code=400,
                    detail="Caminho do ficheiro SQLite não encontrado."
                )

            engine = create_engine(f"sqlite:///{db_path}")

            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            return engine

        config = {
            "user": aes_decrypt(connection.username) if connection.username else "",
            "password": aes_decrypt(connection.password) if connection.password else "",
            "host": aes_decrypt(connection.host),
            "port": connection.port,
            "database": connection.database_name,
            "service": connection.service,
            "sslmode": connection.sslmode,
            "trustServerCertificate": connection.trustServerCertificate,
        }

        db_config = defaults.get(connection.type)
        if not db_config:
            raise HTTPException(
                status_code=400,
                detail=f"Tipo de banco de dados '{connection.type}' não é suportado."
            )

        engine = DatabaseManager.get_engine(db_config, config)

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return engine

    except OperationalError as e:
        log_message("❌ Falha de conexão (OperationalError) ao conectar ao banco", level="error")
        log_message(str(e), level="error")
        raise HTTPException(
            status_code=503,
            detail="Não foi possível conectar ao banco (timeout/host/porta/instância/firewall)."
        )

    except SQLAlchemyError as e:
        log_message("❌ Erro SQLAlchemy ao conectar ao banco", level="error")
        log_message(str(e), level="error")
        raise HTTPException(
            status_code=503,
            detail="Falha ao conectar ao banco de dados."
        )

    except Exception as e:
        log_message("❌ Erro inesperado ao conectar ao banco", level="error")
        log_message(str(e), level="error")
        raise HTTPException(
            status_code=500,
            detail="Erro interno ao criar/testar engine."
        )
    
def get_test_by_connection(conn_data: DBConnection, db: Session):
    config = {
        "user": conn_data.username,
        "password": conn_data.password,
        "host": conn_data.host,
        "port": conn_data.port,
        "database": conn_data.database_name,
        "service": conn_data.service,  # mais seguro
        "sslmode"       : conn_data.sslmode,
        "trustServerCertificate" : conn_data.trustServerCertificate
    }
    session, engine = DatabaseManager.connect(conn_data.type, config)
    session.close()