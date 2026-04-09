from datetime import datetime, timezone
import traceback
from typing import Any, Dict, Optional

from fastapi import HTTPException
from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import Session
from sqlalchemy.orm import load_only, noload

from app.models.user_model import User
from app.models.connection_models import ActiveConnection, ConnectionLog, DBConnection
from app.schemas.connetion_schema import DBConnectionBase
from app.schemas.users_schemas import PaginationOutput
from app.ultils.logger import log_message


def map_status(status: str, id_conn1: Optional[int], id_conn2: Optional[int]) -> str:
    if id_conn1 == id_conn2:
        return "connected"
    if status.lower() == "error":
        return "error"
    return "disconnected"


def get_active_connection_by_userid(db: Session, user_id: int):
    """
    Busca conexão ativa do usuário SEM carregar relações.
    Mais rápido: join direto no DBConnection e load_only.
    """
    log_message(f"📡 Verificando conexão ativa do usuário {user_id}", "info")

    return (
        db.query(ActiveConnection)
        .join(DBConnection, DBConnection.id == ActiveConnection.connection_id)
        .options(
            load_only(
                ActiveConnection.connection_id,
                ActiveConnection.status,
                ActiveConnection.activated_at,
            ),
            # evita lazy-load acidental
            noload(ActiveConnection.connection),
        )
        .filter(
            DBConnection.user_id == user_id,
            ActiveConnection.status.is_(True),
        )
        .first()
    )


def get_active_connection_by_connid(
    db: Session, conn_id: int
) -> Optional[ActiveConnection]:
    """
    Busca conexão ativa por connection_id SEM relações.
    """
    log_message(f"📡 Verificando conexão ativa do conexão {conn_id}", "info")

    return (
        db.query(ActiveConnection)
        .options(
            load_only(
                ActiveConnection.connection_id,
                ActiveConnection.status,
                ActiveConnection.activated_at,
            ),
            noload(ActiveConnection.connection),
        )
        .filter(
            ActiveConnection.connection_id == conn_id,
            ActiveConnection.status.is_(True),
        )
        .first()
    )


def delete_active_connection(db: Session, conn_id: int):
    """
    BUG FIX + performance:
    Antes chamava get_active_connection_by_userid(db, conn_id) (errado).
    Agora busca por conn_id como o nome sugere.
    """
    active = get_active_connection_by_connid(db, conn_id)
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
        log_message(
            f"⚠️ Nenhuma conexão ativa encontrada para o usuário {conn_id}", "warning"
        )
    return active


def connect_active_connection(db: Session, conn_id: int):
    """
    Mantém a mesma assinatura e comportamento:
    - encontra ActiveConnection
    - desativa todas do usuário
    - reativa a conexão escolhida
    """
    active = get_active_connection_by_connid(db, conn_id)
    if not active:
        return None

    # Precisamos do user_id: pega via join com DBConnection (1 query leve)
    user_id = (
        db.query(DBConnection.user_id)
        .filter(DBConnection.id == active.connection_id)
        .scalar()
    )

    if user_id is None:
        return None

    desactivate_all_connections(db, user_id)

    # Reativa a escolhida (se existir)
    active.status = True
    db.add(active)
    db.commit()
    db.refresh(active)
    log_message(f"✅ Conexão reativada para o usuário {conn_id}", "info")
    return active


# === ActiveConnection ===
def set_active_connection(db: Session, user_id: int, id_conn: int):
    """
    Otimização:
    - em vez de delete + insert sempre, você pode manter como está (sem mudar regra).
    - mas vamos evitar carregar relação e manter o fluxo.
    """
    db.query(ActiveConnection).filter(ActiveConnection.connection_id == id_conn).delete(
        synchronize_session=False
    )
    db.commit()

    log_message(
        f"🔁 Definindo nova conexão ativa para o usuário {user_id}: conexão {id_conn}",
        "info",
    )

    active = ActiveConnection(
        connection_id=id_conn,
        status=True,
        activated_at=datetime.now(timezone.utc),
    )
    db.add(active)
    db.commit()
    db.refresh(active)
    log_message(f"✅ Conexão ativa definida: user={user_id}, conn={id_conn}", "success")
    return active


def desactivate_all_connections(db: Session, user_id: int):
    # subquery: pega ids de conexões do usuário
    conn_ids_subq = select(DBConnection.id).where(DBConnection.user_id == user_id)

    stmt = (
        update(ActiveConnection)
        .where(
            ActiveConnection.connection_id.in_(conn_ids_subq),
            ActiveConnection.status.is_(True),
        )
        .values(status=False)
    )

    result = db.execute(stmt)
    db.commit()

    updated = result.rowcount or 0
    log_message(f"🔒 {updated} conexões desativadas para o usuário {user_id}", "info")
    return updated


# === DBConnection ===
def create_db_connection(db: Session, user_id: int, conn_data: DBConnectionBase):
    """
    Mantém o comportamento:
    - se existe, atualiza status
    - senão cria
    Performance: evita carregar relações e atualiza só o necessário.
    """
    exist = (
        db.query(DBConnection)
        .options(
            load_only(
                DBConnection.id,
                DBConnection.user_id,
                DBConnection.name,
                DBConnection.status,
            ),
            # se tiver relações pesadas, bloqueia aqui (remova se não existir)
            (
                noload(DBConnection.structures)
                if hasattr(DBConnection, "structures")
                else ()
            ),
        )
        .filter(DBConnection.user_id == user_id, DBConnection.name == conn_data.name)
        .first()
    )

    if exist:
        exist.status = conn_data.status
        db_conn = exist
        log_message(
            f"🔄 Status da conexão '{db_conn.name}' atualizado para o usuário {user_id}",
            "info",
        )
    else:
        db_conn = DBConnection(**conn_data.model_dump(), user_id=user_id)
        db.add(db_conn)
        log_message(
            f"✅ Conexão '{db_conn.name}' criada para o usuário {user_id}", "success"
        )

    db.commit()
    db.refresh(db_conn)
    return db_conn


def upsert_db_connection(db: Session, user_id: int, conn_data: DBConnectionBase):
    """
    Cria ou atualiza uma conexão de banco de dados para o usuário.
    Performance:
    - usa exclude_unset=True pra atualizar só campos enviados
    - evita writes desnecessários (compara antes de setar)
    """
    db_conn = (
        db.query(DBConnection)
        .options(
            load_only(
                DBConnection.id,
                DBConnection.user_id,
                DBConnection.name,
                DBConnection.status,
            ),
            (
                noload(DBConnection.structures)
                if hasattr(DBConnection, "structures")
                else ()
            ),
        )
        .filter(DBConnection.user_id == user_id, DBConnection.name == conn_data.name)
        .first()
    )

    payload = conn_data.model_dump(exclude_unset=True)

    if db_conn:
        for field, value in payload.items():
            # evita dirty write desnecessário
            if hasattr(db_conn, field) and getattr(db_conn, field) != value:
                setattr(db_conn, field, value)

        log_message(
            f"🔄 Conexão '{db_conn.name}' atualizada para o usuário {user_id}", "info"
        )
    else:
        db_conn = DBConnection(**payload, user_id=user_id)
        db.add(db_conn)
        log_message(
            f"✅ Nova conexão '{db_conn.name}' criada para o usuário {user_id}",
            "success",
        )

    db.commit()
    db.refresh(db_conn)
    return db_conn


# ============================================================
#  Helpers internos (não mudam API pública, só performance)
# ============================================================
def _safe_page_limit(page: int, limit: int, max_limit: int = 100):
    page = max(int(page or 1), 1)
    limit = min(max(int(limit or 10), 1), max_limit)
    offset = (page - 1) * limit
    return page, limit, offset


def _count_fast(query):
    """
    Conta de forma mais eficiente:
    - remove ORDER BY
    - evita overhead quando o query tem joins/columns extras
    """
    return query.order_by(None).count()


# ============================================================
#  Connections
# ============================================================
def get_db_connections(db: Session, user_id: int):
    log_message(f"🔍 Buscando conexões do usuário {user_id}", "info")

    # Performance: retorna modelo, mas sem relações e com colunas essenciais
    return (
        db.query(DBConnection)
        .options(
            load_only(
                DBConnection.id,
                DBConnection.user_id,
                DBConnection.name,
                DBConnection.type,
                DBConnection.database_name,
                DBConnection.status,
                DBConnection.is_encrypted,
                DBConnection.created_at,
                DBConnection.updated_at,
            ),
            # bloqueia relações acidentais (remova se essas relações não existirem)
            (
                noload(DBConnection.structures)
                if hasattr(DBConnection, "structures")
                else ()
            ),
            noload(DBConnection.user) if hasattr(DBConnection, "user") else (),
        )
        .filter(DBConnection.user_id == user_id)
        .order_by(DBConnection.updated_at.desc())
        .all()
    )


def get_db_connections_pagination_v1(
    db: Session,
    user_id: int,
    page: int = 1,
    limit: int = 10,
):
    log_message(
        f"🔍 Buscando conexões | user={user_id} | page={page} | limit={limit}",
        "info",
    )

    page, limit, offset = _safe_page_limit(page, limit, max_limit=100)

    # 🔸 Evita carregar o user inteiro; pega só o que precisa
    user_row = db.query(User.id, User.empresa_id).filter(User.id == user_id).first()
    if not user_row:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    # Se role for relationship, evitar carregar tudo — pega só o nome da role via join (se existir)
    # Caso seu model tenha User.role_id / Role, ajuste aqui conforme seu schema.
    # Vou manter a lógica original com fallback seguro:
    user_obj = db.query(User).filter(User.id == user_id).first()
    is_admin = bool(getattr(getattr(user_obj, "role", None), "name", "") == "admin")

    # 🔹 Subquery: último uso da conexão
    sub_last_used = (
        db.query(
            ConnectionLog.connection_id,
            func.max(ConnectionLog.timestamp).label("last_used"),
        )
        .group_by(ConnectionLog.connection_id)
        .subquery()
    )

    # 🔹 Query base: carrega DBConnection “leve” + last_used
    query = (
        db.query(DBConnection, sub_last_used.c.last_used)
        .options(
            load_only(
                DBConnection.id,
                DBConnection.user_id,
                DBConnection.name,
                DBConnection.type,
                DBConnection.database_name,
                DBConnection.status,
                DBConnection.is_encrypted,
                DBConnection.created_at,
                DBConnection.updated_at,
            ),
            (
                noload(DBConnection.structures)
                if hasattr(DBConnection, "structures")
                else ()
            ),
            noload(DBConnection.user) if hasattr(DBConnection, "user") else (),
        )
        .outerjoin(sub_last_used, DBConnection.id == sub_last_used.c.connection_id)
        .join(User, DBConnection.user_id == User.id)
    )

    # 🔐 Regra de permissão (mesma regra, só mais limpa)
    if is_admin:
        query = query.filter(User.empresa_id == user_row.empresa_id)
    else:
        query = query.filter(
            DBConnection.user_id == user_row.id,
            User.empresa_id == user_row.empresa_id,
        )

    total = _count_fast(query)

    results = (
        query.order_by(DBConnection.updated_at.desc()).offset(offset).limit(limit).all()
    )

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "results": results,
    }


def get_db_connection_by_id(db: Session, connection_id: int) -> DBConnection | None:
    log_message(f"🔍 Buscando conexão com ID {connection_id}", "info")

    conn = (
        db.query(DBConnection)
        .options(
            load_only(
                DBConnection.id,
                DBConnection.user_id,
                DBConnection.name,
                DBConnection.type,
                DBConnection.database_name,
                DBConnection.status,
                DBConnection.is_encrypted,
                DBConnection.created_at,
                DBConnection.updated_at,
            ),
            (
                noload(DBConnection.structures)
                if hasattr(DBConnection, "structures")
                else ()
            ),
            noload(DBConnection.user) if hasattr(DBConnection, "user") else (),
        )
        .filter(DBConnection.id == connection_id)
        .first()
    )

    if not conn:
        log_message(f"❌ Conexão ID {connection_id} não encontrada", "error")
        raise HTTPException(status_code=404, detail="Conexão não encontrada")

    return conn


def get_db_connection_by_name(db: Session, name: str):
    # corrigindo log (não era ID, era name)
    log_message(f"🔍 Buscando conexão com NAME {name}", "info")

    return (
        db.query(DBConnection)
        .options(
            load_only(
                DBConnection.id,
                DBConnection.user_id,
                DBConnection.name,
                DBConnection.type,
                DBConnection.database_name,
                DBConnection.status,
                DBConnection.is_encrypted,
            ),
            (
                noload(DBConnection.structures)
                if hasattr(DBConnection, "structures")
                else ()
            ),
            noload(DBConnection.user) if hasattr(DBConnection, "user") else (),
        )
        .filter(DBConnection.database_name == name)
        .first()
    )


def delete_connection(db: Session, id_conn: int):
    try:
        log_message(f"🗑️ Deletando conexão com ID {id_conn}", "info")

        connection = get_db_connection_by_id(db, id_conn)

        # Remove dependências (bulk delete)
        db.query(ActiveConnection).filter(
            ActiveConnection.connection_id == connection.id
        ).delete(synchronize_session=False)

        db.delete(connection)
        db.commit()

        log_message(f"✅ Conexão {id_conn} deletada com sucesso", "success")
        return connection

    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao deletar conexão {id_conn}: {str(e)}", "error")
        raise e


# ============================================================
#  Logs
# ============================================================
def get_connection_logs(db: Session, connection_id: int):
    log_message(f"📜 Buscando logs da conexão {connection_id}", "info")

    return (
        db.query(ConnectionLog)
        .options(
            load_only(
                ConnectionLog.id,
                ConnectionLog.connection_id,
                ConnectionLog.action,
                ConnectionLog.status,
                ConnectionLog.timestamp,
                ConnectionLog.details,
            ),
            (
                noload(ConnectionLog.connection)
                if hasattr(ConnectionLog, "connection")
                else ()
            ),
        )
        .filter(ConnectionLog.connection_id == connection_id)
        .order_by(ConnectionLog.timestamp.desc())
        .all()
    )


def get_connection_logs_pagination(
    db: Session, user_id: int, connection_id: int = None, page: int = 1, limit: int = 10
) -> PaginationOutput:
    log_message(
        f"📜 Buscando logs | Conexão: {connection_id or 'todas'} | Página {page}, Limite {limit}",
        "info",
    )

    page, limit, offset = _safe_page_limit(page, limit, max_limit=200)

    # Join existe por permissão (filtra logs por conexões do user)
    query = (
        db.query(ConnectionLog)
        .join(DBConnection, DBConnection.id == ConnectionLog.connection_id)
        .options(
            load_only(
                ConnectionLog.id,
                ConnectionLog.connection_id,
                ConnectionLog.action,
                ConnectionLog.status,
                ConnectionLog.timestamp,
                ConnectionLog.details,
            ),
            (
                noload(ConnectionLog.connection)
                if hasattr(ConnectionLog, "connection")
                else ()
            ),
        )
        .filter(DBConnection.user_id == user_id)
    )

    if connection_id is not None:
        query = query.filter(ConnectionLog.connection_id == connection_id)

    total = _count_fast(query)

    results = (
        query.order_by(ConnectionLog.timestamp.desc()).offset(offset).limit(limit).all()
    )

    return {"page": page, "limit": limit, "total": total, "results": results}


def create_connection_log(
    db: Session,
    connection_id: Optional[int],
    action: str,
    status: str = "success",
    details: Optional[Dict[str, Any]] = None,
    user_id: Optional[int] = None,
):
    details = details or {}
    timestamp = datetime.now(timezone.utc)

    try:
        log_entry = ConnectionLog(
            connection_id=connection_id,
            action=action,
            status=status,
            timestamp=timestamp,
            details=details,
        )

        db.add(log_entry)
        db.commit()
        db.refresh(log_entry)

        log_message(
            f"📑 Log criado → conexão={connection_id or 'N/A'}, ação='{action}', status='{status}', usuário={user_id or 'anon'}",
            level="info",
        )

        return log_entry

    except Exception as e:
        db.rollback()
        error_info = traceback.format_exc()

        log_message(
            f"❌ Falha ao criar log: ação='{action}', status='{status}', conexão={connection_id or 'N/A'}, erro={e}\n{error_info}",
            level="error",
        )

        fallback = {
            "connection_id": connection_id,
            "action": action,
            "status": "error",
            "timestamp": timestamp.isoformat(),
            "details": {"fallback_error": str(e)},
        }
        return fallback


# ============================================================
#  Query “leve” (já estava boa — só ajustes finos)
# ============================================================
def query_connections_simple(
    db: Session,
    *,
    user_id: int,
    search: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    page: int = 1,
    limit: int = 10,
):
    """
    Consulta simples em DBConnection
    - sem JOIN
    - sem relationships
    - filtros direto no banco
    - retorno leve
    """
    page, limit, offset = _safe_page_limit(page, limit, max_limit=100)
    filters = filters or {}

    query = db.query(
        DBConnection.id,
        DBConnection.user_id,
        DBConnection.name,
        DBConnection.type,
        DBConnection.database_name,
        DBConnection.is_encrypted,
    ).filter(DBConnection.user_id == user_id)

    if search:
        s = f"%{search.strip()}%"
        query = query.filter(
            or_(
                DBConnection.name.ilike(s),
                DBConnection.database_name.ilike(s),
                DBConnection.type.ilike(s),
            )
        )

    if "type" in filters and filters["type"] is not None:
        query = query.filter(DBConnection.type == filters["type"])

    if "is_encrypted" in filters and filters["is_encrypted"] is not None:
        query = query.filter(
            DBConnection.is_encrypted.is_(bool(filters["is_encrypted"]))
        )

    total = _count_fast(query)

    rows = query.order_by(DBConnection.id.desc()).offset(offset).limit(limit).all()

    results = [
        {
            "id": r.id,
            "user_id": r.user_id,
            "name": r.name,
            "type": r.type,
            "database_name": r.database_name,
            "is_encrypted": r.is_encrypted,
        }
        for r in rows
    ]

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "items": results,
        "pages": (total + limit - 1) // limit if limit > 0 else 1,
    }
