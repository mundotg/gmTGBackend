import traceback
from typing import Optional, Tuple
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.config.dependencies import EngineManager, get_session_by_connection
from app.cruds.connection_cruds import create_db_connection, disconnect_active_connection, get_db_connection_by_id
from app.cruds.dbstatistics_crud import get_statistics_by_connection_geral
from app.cruds.queryhistory_crud import get_ultima_consulta
from app.models.connection_models import ActiveConnection, DBConnection
from app.schemas.connetion_schema import DBConnectionBase
from app.schemas.users_chemas import DbInfoSchema
from app.ultils.logger import log_message
from datetime import datetime

from app.ultils.socket_connection import is_port_open
def reativar_connection(id_user: int, db: Session) -> dict:
    """
    Reativa a conexão de banco de dados para um usuário específico.

    Args:
        id_user (int): ID do usuário.
        db (Session): Sessão do SQLAlchemy.

    Returns:
        dict: {
            "success": bool,
            "config": DbInfoSchema | None
        }
    """
    try:
        conexao, activated_at = get_connection_current(db, id_user)
        if not conexao:
            return {"success": False, "config": None}

        stats = get_statistics_by_connection_geral(db, conexao.id)
        num_tabelas = 0
        consultas_hoje = 0
        registros = 0
        if stats:
            num_tabelas = stats.total_structured_tables or 0
            consultas_hoje = stats.statistics.queries_today or 0
            registros = stats.statistics.records_analyzed or 0

        ultima_consulta = get_ultima_consulta(db, conexao.id, id_user)
        data_ultima = ultima_consulta.executed_at if ultima_consulta else None
        duracao_ultima = ultima_consulta.duration_ms if ultima_consulta else None

        db_on = DbInfoSchema(
            id_connection=conexao.id,
            name_db=conexao.database_name,
            data=activated_at,
            type=conexao.type,
            num_table=num_tabelas,
            num_consultas=consultas_hoje,
            ultima_execucao_ms=duracao_ultima,
            ultima_consulta_em=data_ultima,
            registros_analizados=registros
        )

        # Se não houver engine ativa, tenta criar
        if not EngineManager.get(id_user):
            # 🔍 Checa antes de criar engine se o host:porta está acessível
            if not is_port_open(conexao.host, conexao.port):
                log_message(
                    f"❌ Banco {conexao.host}:{conexao.port} inacessível para o usuário {id_user}",
                    "error"
                )
                desativar_connection(id_user, conexao.id, db)
                return {"success": False, "config": db_on}

            engine = get_session_by_connection(conexao)
            if engine:
                EngineManager.set(engine, id_user)
            else:
                log_message(
                    f"⚠️ Falha ao criar engine para o usuário {id_user}",
                    "warning"
                )
                desativar_connection(id_user, conexao.id, db)
                return {"success": False, "config": db_on}

        return {"success": True, "config": db_on}

    except Exception as e:
        log_message(
            f"❌ Erro em reativar_connection:"
            f"\nTipo: {type(e).__name__}"
            f"\nMensagem: {str(e)}"
            f"\nStackTrace:\n{traceback.format_exc()}",
            "error"
        )
        return {"success": False, "config": None}


def desativar_connection(id_user: int, conn: int, db: Session) -> dict:
    """
    Desativa a conexão de banco de dados para um usuário específico.

    Args:
        id_user (int): ID do usuário.
        db (Session): Sessão do SQLAlchemy.

    Returns:
        dict: {
            "success": bool,
            "message": str
        }
    """
    try:
        conexao_ativa = disconnect_active_connection(db, conn)
        conn_data: DBConnectionBase = get_db_connection_by_id(db, conn)
        conn_data.status = "disconnected"
        create_db_connection(db, id_user, conn_data)

        if not conexao_ativa or not conexao_ativa.status:
            return {"success": False, "message": "Nenhuma conexão ativa encontrada para o usuário."}
        disconnect_active_connection(db, conexao_ativa.connection_id)
        if EngineManager.get(id_user):
            EngineManager.remove(id_user)
            log_message(f"🔌 Conexão desativada para o usuário {id_user}", "info")
            return {"success": True, "message": "Conexão desativada com sucesso."}
        else:
            return {"success": False, "message": "Engine não encontrada para o usuário."}

    except Exception as e:
        log_message(f"❌ Erro em desativar_connection:"
                    f"Tipo: { type(e).__name__}\n"
        f"Mensagem: {str(e)}\n"
        f"StackTrace:\n{traceback.format_exc()}", "error")
        return {"success": False, "message": "Erro ao desativar a conexão."}



def get_connection_current(db: Session, id_user: int) -> Tuple[Optional[DBConnection], Optional[datetime]]:
    """
    Retorna a conexão ativa do usuário, se existir, e a data de ativação.
    Agora com apenas 1 consulta ao banco.
    """
    connection = (
        db.query(DBConnection, ActiveConnection.activated_at)
        .join(ActiveConnection, ActiveConnection.connection_id == DBConnection.id)
        .filter(DBConnection.user_id == id_user, ActiveConnection.status == True)
        .first()
    )

    if not connection:
        return None, None
    connection, activated_at = connection
    return connection, activated_at

def get_connection_by_id(db: Session, id_user: int, id_conn:int) -> Optional[DBConnection]:
    """
    Retorna a conexão ativa do usuário, se existir, e a data de ativação.
    Agora com apenas 1 consulta ao banco.
    """
    return (
        db.query(DBConnection)
        .filter(DBConnection.user_id == id_user, DBConnection.id == id_conn)
        .first()
    )

from sqlalchemy.ext.asyncio import AsyncSession
async def get_connection_current_async(
    db: AsyncSession, id_user: int
) -> Tuple[Optional[DBConnection], Optional[datetime]]:
    """
    Retorna a conexão ativa do usuário, se existir, e a data de ativação.
    Agora usando AsyncSession corretamente (sem .query).
    """
    stmt = (
        select(DBConnection, ActiveConnection.activated_at)
        .join(ActiveConnection, ActiveConnection.connection_id == DBConnection.id)
        .filter(DBConnection.user_id == id_user, ActiveConnection.status == True)
    )

    result = await db.execute(stmt)
    connection = result.first()  # pega a primeira tupla (DBConnection, activated_at)

    if not connection:
        return None, None

    db_connection, activated_at = connection
    return db_connection, activated_at


from sqlalchemy.exc import SQLAlchemyError, OperationalError

async def get_connection_id_async(
    db: AsyncSession,
    id_user: int,
    id_connection: int
) -> Optional[DBConnection]:
    """
    Retorna uma conexão específica do usuário, se existir.
    Valida se o AsyncSession está funcional antes de executar a consulta.
    """
    try:
        # 1️⃣ Verifica se o db está funcional
        try:
            from sqlalchemy import text
            await db.execute(text("SELECT 1"))
        except OperationalError as e:
            log_message(f"[ERRO] Sessão de banco inválida ou desconectada: {e}", "error")
            return None
        except Exception as e:
            log_message(f"[ERRO] Falha ao validar AsyncSession: {e}{traceback.format_exc()}", "error")
            return None

        # 2️⃣ Busca a conexão no banco
        stmt = (
            select(DBConnection)
            .where(
                DBConnection.id == id_connection,
                DBConnection.user_id == id_user
            )
        )
        result = await db.execute(stmt)
        connection = result.scalar_one_or_none()

        if connection is None:
            log_message(f"[WARN] Conexão {id_connection} não encontrada para o usuário {id_user}")
            return None
        # print("existe")
        return connection

    except SQLAlchemyError as db_err:
        log_message(f"[ERRO] Erro SQL ao buscar conexão {id_connection}: {db_err}{traceback.format_exc()}", "error")
        return None
    except Exception as e:
        log_message(f"[ERRO] Erro inesperado em get_connection_id_async: {e}{traceback.format_exc()}", "error")
        return None

   