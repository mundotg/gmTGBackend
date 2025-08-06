from fastapi import Depends, HTTPException
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.connection_models import DBConnection
from app.ultils.conect_database import DatabaseManager
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from app.ultils.logger import log_message

defaults = {
    "mysql": "MySQL",
    "postgresql": "PostgreSQL",
    "sqlite": "SQLite",
    "sqlserver": "SQL Server",
    "oracle": "Oracle",
    "mariadb": "MariaDB"
}


def get_session_by_connection_id(connection_id: int, db: Session = Depends(get_db)):
    # Buscar a conexão salva
    connection = db.query(DBConnection).filter(DBConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Conexão não encontrada")

    config = {
        "user": connection.username,
        "password": connection.password,
        "host": connection.host,
        "port": connection.port,
        "database": connection.database_name,
        "service": connection.service if hasattr(connection, "service_name") else "xe",
        "sslmode"       : connection.sslmode,
        "trustServerCertificate" : connection.trustServerCertificate
    }

    try:
        # Obter o engine com base nas configs
        engine = DatabaseManager.get_engine(defaults[connection.type], config)

        return engine

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar ao banco de dados: {str(e)}")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")

def get_session_by_connection(connection: DBConnection):
    """
    Cria e testa uma conexão com o banco de dados com base nas configurações fornecidas.

    Retorna:
        sqlalchemy.engine.Engine ou None em caso de erro.
    """

    # 1. Montar a configuração da conexão
    config = {
        "user": connection.username,
        "password": connection.password,
        "host": connection.host,
        "port": connection.port,
        "database": connection.database_name,
        "service": connection.service,  # Para bancos que requerem
        "sslmode": connection.sslmode,
        "trustServerCertificate": connection.trustServerCertificate
    }

    # 2. Verificar se o tipo do banco é suportado
    db_config = defaults.get(connection.type)
    if not db_config:
        raise HTTPException(
            status_code=400,
            detail=f"Tipo de banco de dados '{connection.type}' não é suportado."
        )

    try:
        # 3. Criar o engine
        engine = DatabaseManager.get_engine(db_config, config)

        # 4. Criar a sessão local para teste
        SessionLocal = sessionmaker(bind=engine)

        # 5. Testar a conexão
        with SessionLocal() as session:
            session.execute(text("SELECT 1"))

        return engine

    except SQLAlchemyError as e:
        log_message("❌ Erro SQLAlchemy ao conectar ao banco", level="error")
        log_message(str(e), level="error")

    except Exception as e:
        log_message("❌ Erro inesperado ao conectar ao banco", level="error")
        log_message(str(e), level="error")

    return None

    
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

class EngineManager:
    __engines: dict[int, Engine] = {}

    @classmethod
    def set(cls, engine: Engine, id_user: int):
        cls.__engines[id_user] = engine

    @classmethod
    def get(cls, id_user: int) -> Engine:
        engine = cls.__engines.get(id_user)
        if not engine:
            log_message(f"Nenhum engine ativo para o usuário ID {id_user} em EngineManager dependecia","error")
        return engine
    @classmethod
    def remove(cls, id_user:int):
        engine = cls.__engines.pop(id_user, None)
        if engine:
            log_message(f"Engine removido para o usuário {id_user}  em EngineManager dependecia")
        else:
            log_message(f"Nenhum engine encontrado para remover do usuário {id_user}  em EngineManager dependecia")
