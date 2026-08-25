from datetime import datetime, timezone
import traceback
from typing import Any, Dict, Optional

from fastapi import HTTPException, status
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session
from sqlalchemy.orm import joinedload, load_only, noload

from app.models.user_model import User
from app.models.connection_models import (
    ActiveConnection,
    ConnectionLog,
    DBConnection,
    DBConnectionShare,
)
from app.schemas.connetion_schema import (
    ConnectionAccessLevel,
    ConnectionAccessOut,
    ConnectionShareOut,
    DBConnectionBase,
)
from app.schemas.users_schemas import PaginationOutput
from app.services.crypto_utils import reencrypt_at_rest, secret_decrypt, secret_encrypt
from app.ultils.db_url import InvalidDatabaseUrl, parse_db_url
from app.ultils.logger import log_message
from app.ultils.permissions import is_superadmin


# Campos que chegam ofuscados pelo frontend e têm de ser recifrados
# com a chave-mestra antes de tocar na base de dados.
# A `url` entra aqui porque leva as credenciais todas dentro dela.
SECRET_FIELDS = ("host", "username", "password", "url")


def _secure_secrets(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte os campos sensíveis do payload para cifra em repouso.

    O frontend envia estes valores no envelope antigo (que não protege
    nada — a chave viaja junto). Aqui recifram-se com AES-256-GCM e a
    chave-mestra em ENCRYPTION_KEY, para que um dump da BD não exponha
    as credenciais dos clientes.

    É idempotente: valores já em "v2." passam intactos.
    """
    for field in SECRET_FIELDS:
        value = payload.get(field)
        if value:
            payload[field] = reencrypt_at_rest(str(value))

    return payload


def _fill_from_url(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    No modo "ligar por URL", deriva host/porta/base/utilizador da própria URL.

    A ligação usa sempre a URL como está (ver app/ultils/db_url.py) — isto
    serve só para as colunas: `host`, `port` e `database_name` são NOT NULL e a
    listagem de conexões mostra host e base de dados. Sem isto, uma conexão
    criada por URL aparecia na lista sem qualquer identificação.

    Corre DEPOIS de `_secure_secrets`, portanto a URL já está em repouso (v2) e
    os valores derivados são cifrados com `secret_encrypt` directamente.
    """
    cifrada = payload.get("url")
    if not cifrada:
        return payload

    try:
        dados = parse_db_url(secret_decrypt(str(cifrada)))
    except (InvalidDatabaseUrl, ValueError) as e:
        # Não deve acontecer (a ligação já foi testada antes de persistir), mas
        # é preferível gravar sem os derivados do que perder a conexão.
        log_message(f"⚠️ Não foi possível derivar campos da URL: {e}", "warning")
        return payload

    if not payload.get("host"):
        payload["host"] = secret_encrypt(dados["host"])
    if not payload.get("port"):
        payload["port"] = dados["port"]
    if not payload.get("database_name"):
        payload["database_name"] = dados["database"]
    if not payload.get("username") and dados["username"]:
        payload["username"] = secret_encrypt(dados["username"])
    if not payload.get("password") and dados["password"]:
        payload["password"] = secret_encrypt(dados["password"])
    if not payload.get("service") and dados["service"]:
        payload["service"] = dados["service"]
    if not payload.get("sslmode") and dados["sslmode"]:
        payload["sslmode"] = dados["sslmode"]

    return payload


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
        db_conn = DBConnection(
            **_fill_from_url(_secure_secrets(conn_data.model_dump())),
            user_id=user_id,
            is_encrypted=True,
        )
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

    payload = _fill_from_url(_secure_secrets(conn_data.model_dump(exclude_unset=True)))

    if db_conn:
        for field, value in payload.items():
            # evita dirty write desnecessário
            if hasattr(db_conn, field) and getattr(db_conn, field) != value:
                setattr(db_conn, field, value)

        db_conn.is_encrypted = True

        log_message(
            f"🔄 Conexão '{db_conn.name}' atualizada para o usuário {user_id}", "info"
        )
    else:
        db_conn = DBConnection(**payload, user_id=user_id, is_encrypted=True)
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
    superadmin = bool(user_obj and is_superadmin(user_obj))

    # Conexões de outros que foram partilhadas com este utilizador.
    shared_ids = get_shared_connection_ids(db, user_row.id)

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

    # 🔐 Regra de visibilidade:
    #   super admin → todas as conexões
    #   admin       → todas as da sua empresa
    #   restantes   → as suas + as que lhe foram partilhadas
    if superadmin:
        pass
    elif is_admin:
        query = query.filter(User.empresa_id == user_row.empresa_id)
    else:
        proprias = and_(
            DBConnection.user_id == user_row.id,
            User.empresa_id == user_row.empresa_id,
        )
        query = query.filter(
            or_(proprias, DBConnection.id.in_(shared_ids)) if shared_ids else proprias
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
            user_id=user_id,
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


# =========================================================
# 🤝 Partilha de conexões
# =========================================================

# Ordem de força dos níveis. Serve para comparar ("write chega para ler?").
ACCESS_ORDER = {
    ConnectionAccessLevel.read: 1,
    ConnectionAccessLevel.write: 2,
    ConnectionAccessLevel.manage: 3,
}


def _share_out(share: DBConnectionShare) -> ConnectionShareOut:
    return ConnectionShareOut(
        id=share.id,
        connection_id=share.connection_id,
        user_id=share.user_id,
        user_nome=share.user.nome if share.user else None,
        user_email=share.user.email if share.user else None,
        access_level=ConnectionAccessLevel(share.access_level),
        granted_by_id=share.granted_by_id,
        granted_by_nome=share.granted_by.nome if share.granted_by else None,
        created_at=share.created_at,
    )


def get_connection_or_404(db: Session, connection_id: int) -> DBConnection:
    conn = (
        db.query(DBConnection)
        .options(joinedload(DBConnection.owner))
        .filter(DBConnection.id == connection_id)
        .first()
    )
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Conexão com ID {connection_id} não encontrada.",
        )
    return conn


def get_share(
    db: Session, connection_id: int, user_id: int
) -> Optional[DBConnectionShare]:
    return (
        db.query(DBConnectionShare)
        .filter(
            DBConnectionShare.connection_id == connection_id,
            DBConnectionShare.user_id == user_id,
        )
        .first()
    )


def get_connection_access(
    db: Session, conn: DBConnection, user: User
) -> ConnectionAccessOut:
    """
    Resolve o que `user` pode fazer em `conn`, juntando as três origens de
    acesso: ser dono, ser super admin, ou ter uma partilha.
    """
    is_owner = conn.user_id == user.id
    superadmin = is_superadmin(user)

    nivel: Optional[ConnectionAccessLevel] = None
    if is_owner or superadmin:
        nivel = ConnectionAccessLevel.manage
    else:
        share = get_share(db, conn.id, user.id)
        if share:
            nivel = ConnectionAccessLevel(share.access_level)

    forca = ACCESS_ORDER.get(nivel, 0) if nivel else 0

    # Quem tem "manage" pode repartilhar; apagar continua reservado ao dono
    # e ao super admin.
    pode_partilhar = forca >= ACCESS_ORDER[ConnectionAccessLevel.manage]

    shares: list[ConnectionShareOut] = []
    if pode_partilhar:
        registos = (
            db.query(DBConnectionShare)
            .options(
                joinedload(DBConnectionShare.user),
                joinedload(DBConnectionShare.granted_by),
            )
            .filter(DBConnectionShare.connection_id == conn.id)
            .order_by(DBConnectionShare.created_at.desc())
            .all()
        )
        shares = [_share_out(s) for s in registos]

    return ConnectionAccessOut(
        connection_id=conn.id,
        connection_name=conn.name,
        owner_id=conn.user_id,
        owner_nome=conn.owner.nome if conn.owner else None,
        is_owner=is_owner,
        access_level=nivel,
        can_read=forca >= ACCESS_ORDER[ConnectionAccessLevel.read],
        can_write=forca >= ACCESS_ORDER[ConnectionAccessLevel.write],
        can_share=pode_partilhar,
        can_delete=is_owner or superadmin,
        shares=shares,
    )


def assert_connection_access(
    db: Session,
    conn: DBConnection,
    user: User,
    required: ConnectionAccessLevel = ConnectionAccessLevel.read,
) -> ConnectionAccessOut:
    """Levanta 403 se `user` não tiver pelo menos o nível `required` em `conn`."""
    acesso = get_connection_access(db, conn, user)
    forca = ACCESS_ORDER.get(acesso.access_level, 0) if acesso.access_level else 0

    if forca < ACCESS_ORDER[required]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Sem acesso de nível '{required.value}' à conexão '{conn.name}'. "
                "Peça ao dono da conexão para lhe conceder acesso."
            ),
        )

    return acesso


def list_connection_shares(
    db: Session, connection_id: int, actor: User
) -> ConnectionAccessOut:
    conn = get_connection_or_404(db, connection_id)
    acesso = assert_connection_access(db, conn, actor, ConnectionAccessLevel.read)

    # Quem só tem read/write vê o seu próprio acesso, não a lista de quem mais
    # tem acesso — `get_connection_access` já devolve `shares` vazio nesse caso.
    return acesso


def share_connection(
    db: Session,
    connection_id: int,
    actor: User,
    target_user_id: int,
    access_level: ConnectionAccessLevel,
) -> ConnectionShareOut:
    """Concede (ou atualiza) o acesso de outro utilizador a uma conexão."""
    conn = get_connection_or_404(db, connection_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    if target_user_id == conn.user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="O dono da conexão já tem acesso total.",
        )

    alvo = db.get(User, target_user_id)
    if not alvo:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Utilizador não encontrado.",
        )

    if not alvo.is_active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"A conta de {alvo.email} está desativada.",
        )

    # Isolamento entre empresas: não se partilha para fora da organização.
    dono = conn.owner
    if (
        dono is not None
        and dono.empresa_id is not None
        and alvo.empresa_id != dono.empresa_id
        and not is_superadmin(actor)
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Só é possível partilhar com membros da mesma empresa.",
        )

    share = get_share(db, conn.id, target_user_id)
    criou = share is None

    if share:
        share.access_level = access_level.value
    else:
        share = DBConnectionShare(
            connection_id=conn.id,
            user_id=target_user_id,
            access_level=access_level.value,
            granted_by_id=actor.id,
        )
        db.add(share)

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao partilhar conexão {conn.id}: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro ao guardar a partilha.")

    db.refresh(share)

    create_connection_log(
        db,
        connection_id=conn.id,
        action="share_granted" if criou else "share_updated",
        status="success",
        details={
            "target_user_id": target_user_id,
            "target_email": alvo.email,
            "access_level": access_level.value,
        },
        user_id=actor.id,
    )

    log_message(
        f"🤝 Conexão '{conn.name}' partilhada com {alvo.email} "
        f"(nível={access_level.value}) por {actor.email}",
        "success",
    )

    # Recarrega as relações para preencher os nomes no output.
    share = (
        db.query(DBConnectionShare)
        .options(
            joinedload(DBConnectionShare.user),
            joinedload(DBConnectionShare.granted_by),
        )
        .filter(DBConnectionShare.id == share.id)
        .first()
    )
    return _share_out(share)


def revoke_connection_share(
    db: Session,
    connection_id: int,
    actor: User,
    target_user_id: int,
) -> None:
    """Retira o acesso de um utilizador a uma conexão."""
    conn = get_connection_or_404(db, connection_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    share = get_share(db, conn.id, target_user_id)
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Este utilizador não tem acesso partilhado a esta conexão.",
        )

    email_alvo = share.user.email if share.user else str(target_user_id)

    db.delete(share)
    db.commit()

    create_connection_log(
        db,
        connection_id=conn.id,
        action="share_revoked",
        status="success",
        details={"target_user_id": target_user_id, "target_email": email_alvo},
        user_id=actor.id,
    )

    log_message(
        f"🚫 Acesso de {email_alvo} à conexão '{conn.name}' revogado por {actor.email}",
        "success",
    )


def get_shared_connection_ids(db: Session, user_id: int) -> list[int]:
    """IDs das conexões partilhadas com este utilizador (não as que ele possui)."""
    rows = (
        db.query(DBConnectionShare.connection_id)
        .filter(DBConnectionShare.user_id == user_id)
        .all()
    )
    return [row[0] for row in rows]
