import traceback
from typing import Optional, Tuple
from sqlalchemy.orm import Session
from app.config.dependencies import EngineManager, get_session_by_connection
from app.cruds.connection_cruds import disconnect_active_connection, get_active_connection_by_userid, get_db_connection_by_id
from app.cruds.dbstatistics_crud import get_statistics_by_connection_geral
from app.cruds.queryhistory_crud import get_ultima_consulta
from app.models.connection_models import DBConnection
from app.schemas.users_chemas import Db_on
from app.ultils.logger import log_message
from datetime import datetime

def reativar_connection(id_user: int, db: Session) -> dict:
    """
    Reativa a conexão de banco de dados para um usuário específico.

    Args:
        id_user (int): ID do usuário.
        db (Session): Sessão do SQLAlchemy.

    Returns:
        dict: {
            "success": bool,
            "config": Db_on | None
        }
    """
    try:
        

        conexao,activated_at = get_connection_current(db,id_user)
        if not conexao:
            # print("conexao=",conexao)
            return {"success": False, "config": None}

        stats = get_statistics_by_connection_geral(db, conexao.id)
        # print("stats: ",stats)
        num_tabelas =  0
        consultas_hoje = 0
        registros = 0
        if stats:
            num_tabelas = stats.total_structured_tables or 0
            consultas_hoje = stats.statistics.queries_today or 0
            registros = stats.statistics.records_analyzed or 0

        

        ultima_consulta = get_ultima_consulta(db, conexao.id, id_user)
        data_ultima = ultima_consulta.executed_at if ultima_consulta else None
        duracao_ultima = ultima_consulta.duration_ms if ultima_consulta else None

        db_on = Db_on(
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
        
        # print("db_on:", db_on)

        if not EngineManager.get(id_user):
            engine = get_session_by_connection(conexao)
            if engine:
                EngineManager.set(engine, id_user)
            else:
                log_message(f"⚠️ Falha ao criar engine para o usuário {id_user}", "warning")
                return {"success": False, "config": db_on}

        return {"success": True, "config": db_on}

    except Exception as e:
        log_message(f"❌ Erro em reativar_connection:"
        f"Tipo: { type(e).__name__}\n"
        f"Mensagem: {str(e)}\n"
        f"StackTrace:\n{traceback.format_exc()}","error")
        return {"success": False, "config": None}


def desativar_connection(id_user: int, db: Session) -> dict:
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
        conexao_ativa = disconnect_active_connection(db, id_user)
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

    :param db: Sessão do banco de dados
    :param id_user: ID do usuário
    :return: Uma tupla contendo (conexão, data de ativação) ou (None, None)
    """
    conexao_ativa = get_active_connection_by_userid(db, id_user)

    if not conexao_ativa or not conexao_ativa.status:
        return None, None

    conexao = get_db_connection_by_id(db, conexao_ativa.connection_id)
    
    if not conexao:
        return None, None

    return conexao, conexao_ativa.activated_at