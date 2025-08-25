from datetime import datetime, timezone
from typing import Optional
from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.models.connection_models import ActiveConnection, ConnectionLog, DBConnection
from app.schemas.connetion_schema import DBConnectionBase
from app.schemas.users_chemas import PagitacionOutput
from app.ultils.logger import log_message


def map_status(status: str,id_conn1:Optional[int],id_conn2:Optional[int]) -> str:
    if id_conn1 == id_conn2:
        return "connected"
    if status.lower() == "error":
        return "error"
    return "disconnected"

def get_active_connection_by_userid(db: Session, user_id: int):
    log_message(f"📡 Verificando conexão ativa do usuário {user_id}", "info")
    return (
        db.query(ActiveConnection)
        .join(ActiveConnection.connection)
        .filter(ActiveConnection.connection.has(user_id=user_id),ActiveConnection.status == True)
        .first()
    )
def get_active_connection_by_connid(db: Session, conn_id: int):
    log_message(f"📡 Verificando conexão ativa do conexão {conn_id}", "info")
    conn_ativa = db.query(ActiveConnection).filter(ActiveConnection.connection_id == conn_id, ActiveConnection.status == True).first()
    return conn_ativa

def delete_active_connection(db: Session, conn_id: int):
    active = get_active_connection_by_userid(db, conn_id)
    if active:
        db.delete(active)
        db.commit()
    return active

def disconnect_active_connection(db: Session, conn_id: int):
    active = get_active_connection_by_connid(db, conn_id)
    if active:
        active.status = False
        db.add(active)
        db.commit()
        db.refresh(active)
        log_message(f"🔌 Conexão desativada para o usuário {conn_id}", "info")
    else:
        log_message(f"⚠️ Nenhuma conexão ativa encontrada para o usuário {conn_id}", "warning")
    return active

def connect_active_connection(db: Session, conn_id: int):
    
    active = get_active_connection_by_connid(db, conn_id)
    if not active:
        return None
    desactivate_all_connections(db, active.connection.user_id)  # Desativa todas as conexões ativas do usuário
    if active:
        active.status = True
        db.add(active)
        db.commit()
        db.refresh(active)
        log_message(f"✅ Conexão reativada para o usuário {conn_id}", "info")
    else:
        
        log_message(f"⚠️ Nenhuma conexão ativa encontrada para reativar (usuário {conn_id})", "warning")
    return active

# === ActiveConnection ===
def set_active_connection(db: Session,user_id:int, id_conn: int):
    db.query(ActiveConnection).filter(ActiveConnection.connection_id == id_conn).delete()
    db.commit()
    log_message(f"🔁 Definindo nova conexão ativa para o usuário {user_id}: conexão {id_conn}", "info")

    active = ActiveConnection(
        connection_id=id_conn,
        status=True,
        activated_at=datetime.now(timezone.utc)
    )
    db.add(active)
    db.commit()
    db.refresh(active)
    log_message(f"✅ Conexão ativa definida: user={user_id}, conn={id_conn}", "success")
    return active

def desactivate_all_connections(db: Session, user_id: int):
    # Subquery para obter os IDs das conexões associadas ao usuário
    subquery = (
        db.query(ActiveConnection.connection_id)
        .join(ActiveConnection.connection)
        .filter(DBConnection.user_id == user_id, ActiveConnection.status == True)
        .subquery()
    )

    # Atualiza diretamente as conexões usando os IDs
    updated = db.query(ActiveConnection).filter(
        ActiveConnection.connection_id.in_(subquery)
    ).update(
        {"status": False},
        synchronize_session=False
    )
    

    db.commit()
    log_message(f"🔒 {updated} conexões desativadas para o usuário {user_id}", "info")


# === DBConnection ===
def create_db_connection(db: Session, user_id: int, conn_data: DBConnectionBase):
    exist = db.query(DBConnection).filter(
        DBConnection.user_id == user_id,
        DBConnection.name == conn_data.name
    ).first()

    if exist:
        exist.status = conn_data.status  # ✅ Atualiza só o status
        db_conn = exist
        log_message(f"🔄 Status da conexão '{db_conn.name}' atualizado para o usuário {user_id}", "info")
    else:
        db_conn = DBConnection(**conn_data.model_dump(), user_id=user_id)
        db.add(db_conn)
        log_message(f"✅ Conexão '{db_conn.name}' criada para o usuário {user_id}", "success")

    db.commit()
    db.refresh(db_conn)
    return db_conn


def get_db_connections(db: Session, user_id: int):
    log_message(f"🔍 Buscando conexões do usuário {user_id}", "info")
    return db.query(DBConnection).filter(DBConnection.user_id == user_id).all()

def get_db_connections_pagination(
    db: Session, user_id: int,
    page: int = 1, limit: int = 10) ->PagitacionOutput:
    log_message(f"🔍 Buscando conexões do usuário {user_id} | Página {page}, Limite {limit}", "info")
    
    offset = (page - 1) * limit

    total = db.query(DBConnection).filter(DBConnection.user_id == user_id).count()
    sub_last_used = (
        db.query(
            ConnectionLog.connection_id,
            func.max(ConnectionLog.timestamp).label("last_used")
        )
        .group_by(ConnectionLog.connection_id)
        .subquery()
    )

    connections = (
        db.query(DBConnection, sub_last_used.c.last_used)
        .outerjoin(sub_last_used, DBConnection.id == sub_last_used.c.connection_id)
        .filter(DBConnection.user_id == user_id)
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "results": connections
    }


def get_db_connection_by_id(db: Session, connection_id: int)->DBConnection|None:
    log_message(f"🔍 Buscando conexão com ID {connection_id}", "info")
    conn = db.query(DBConnection).filter(DBConnection.id == connection_id).first()
    if not conn:
        log_message(f"❌ Conexão ID {connection_id} não encontrada", "error")
        raise HTTPException(status_code=404, detail="Conexão não encontrada")
    return conn


def get_db_connection_by_name(db: Session, name: str):
    log_message(f"🔍 Buscando conexão com ID {name}", "info")
    return db.query(DBConnection).filter(DBConnection.database_name == name).first()

def delete_connection(db: Session, id_conn: int):
    try:
        log_message(f"🗑️ Deletando conexão com ID {id_conn}", "info")

        connection = get_db_connection_by_id(db, id_conn)

        if not connection:
            log_message(f"❌ Conexão {id_conn} não encontrada", "error")
            return None

        # Remove dependências (relacionamentos em ActiveConnection)
        db.query(ActiveConnection).filter(
            ActiveConnection.connection_id == connection.id
        ).delete()

        # Remove a conexão principal
        db.delete(connection)

        # Commit final
        db.commit()

        log_message(f"✅ Conexão {id_conn} deletada com sucesso", "success")
        return connection

    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao deletar conexão {id_conn}: {str(e)}", "error")
        raise


def get_connection_logs(db: Session, connection_id: int):
    log_message(f"📜 Buscando logs da conexão {connection_id}", "info")
    return db.query(ConnectionLog).filter(ConnectionLog.connection_id == connection_id).all()

def get_connection_logs_pagination(
    db: Session,user_id: int ,
    connection_id: int = None,
    page: int = 1, limit: int = 10)->PagitacionOutput:
    log_message(f"📜 Buscando logs | Conexão: {connection_id or 'todas'} | Página {page}, Limite {limit}", "info")

    query = db.query(ConnectionLog).join(DBConnection).filter(DBConnection.user_id == user_id)

    if connection_id is not None:
        query = query.filter(ConnectionLog.connection_id == connection_id)

    total = query.count()
    results = query.offset((page - 1) * limit).limit(limit).all()

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "results": results
    }


def create_connection_log(db: Session, connection_id: int, action: str, status: str):
    log_entry = ConnectionLog(
        connection_id=connection_id,
        action=action,
        status=status,
        timestamp=datetime.now(timezone.utc)
    )
    db.add(log_entry)
    db.commit()
    db.refresh(log_entry)
    log_message(f"📑 Log criado para conexão {connection_id}: ação='{action}', status='{status}'", "info")
    return log_entry
