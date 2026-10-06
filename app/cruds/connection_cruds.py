from datetime import datetime, timezone
import traceback
from typing import Any, Dict, Optional

from fastapi import HTTPException, status
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session
from sqlalchemy.orm import joinedload, load_only, noload

from app.models.user_model import Empresa, Permission, User
from app.models.connection_models import (
    ActiveConnection,
    ConnectionLog,
    ConnectionRole,
    DBConnection,
    DBConnectionShare,
    EmpresaConnection,
    empresa_connections,
)
from app.schemas.connetion_schema import (
    ConnectionAccessLevel,
    ConnectionAccessOut,
    ConnectionEmpresaCreate,
    ConnectionEmpresaOut,
    ConnectionEmpresaUpdate,
    ConnectionRoleCreate,
    ConnectionRoleOut,
    ConnectionRolePermissionOut,
    ConnectionRoleUpdate,
    ConnectionShareOut,
    DBConnectionBase,
    EffectiveConnectionRules,
    EmpresaConnectionOut,
)
from app.schemas.users_schemas import PaginationOutput
from app.services.crypto_utils import reencrypt_at_rest, secret_decrypt, secret_encrypt
from app.ultils.connection_access import (
    ACCESS_ORDER,
    assert_connection_level,
    forca_do_nivel,
    get_effective_connection_rules,
    negar_acesso,
    resolve_access_level,
)
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
            # O estado de ligação já é do utilizador (chave composta), portanto
            # não se filtra mais por dono da conexão: quem tem a conexão só por
            # partilha também tem direito ao seu próprio estado de ligação.
            ActiveConnection.user_id == user_id,
            ActiveConnection.status.is_(True),
        )
        .first()
    )


def get_active_connection_by_connid(
    db: Session, conn_id: int, user_id: int
) -> Optional[ActiveConnection]:
    """
    Busca a ligação ativa de UM utilizador a uma conexão, sem relações.

    `user_id` passou a ser obrigatório: sem ele, desligar devolvia a linha de
    outra pessoa que estivesse ligada à mesma conexão partilhada.
    """
    log_message(
        f"📡 Verificando conexão ativa | conn={conn_id} | user={user_id}", "info"
    )

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
            ActiveConnection.user_id == user_id,
            ActiveConnection.status.is_(True),
        )
        .first()
    )


def delete_active_connection(db: Session, conn_id: int, user_id: int):
    """
    BUG FIX + performance:
    Antes chamava get_active_connection_by_userid(db, conn_id) (errado).
    Agora busca por conn_id como o nome sugere.
    """
    active = get_active_connection_by_connid(db, conn_id, user_id)
    if active:
        db.delete(active)
        db.commit()
    return active


def disconnect_active_connection(db: Session, conn_id: int, user_id: int):
    active = get_active_connection_by_connid(db, conn_id, user_id)
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


def connect_active_connection(db: Session, conn_id: int, user_id: int):
    """
    - encontra a ligação do utilizador àquela conexão
    - desativa todas as ligações dele
    - reativa a conexão escolhida

    O `user_id` era antes deduzido do dono da conexão, o que dava a pessoa
    errada assim que a conexão fosse partilhada. Agora vem de quem pede.
    """
    active = get_active_connection_by_connid(db, conn_id, user_id)
    if not active:
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
    # Apaga só a linha DESTE utilizador para esta conexão. Antes apagava as de
    # toda a gente, o que expulsava quem mais estivesse ligado à mesma conexão
    # partilhada.
    db.query(ActiveConnection).filter(
        ActiveConnection.connection_id == id_conn,
        ActiveConnection.user_id == user_id,
    ).delete(synchronize_session=False)
    db.commit()

    log_message(
        f"🔁 Definindo nova conexão ativa para o usuário {user_id}: conexão {id_conn}",
        "info",
    )

    active = ActiveConnection(
        user_id=user_id,
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
    # Direto pelo `user_id` da própria linha. A subquery pelas conexões que o
    # utilizador possui deixava de fora as que ele tem por partilha — essas
    # ficavam ativas para sempre, mesmo depois de ele ligar a outra.
    stmt = (
        update(ActiveConnection)
        .where(
            ActiveConnection.user_id == user_id,
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

# `ACCESS_ORDER` e a resolução do nível vivem em `app.ultils.connection_access`:
# são as mesmas regras usadas no caminho de execução, e ter duas cópias era o
# caminho mais curto para elas divergirem.


def _share_out(share: DBConnectionShare) -> ConnectionShareOut:
    return ConnectionShareOut(
        id=share.id,
        connection_id=share.connection_id,
        user_id=share.user_id,
        user_nome=share.user.nome if share.user else None,
        user_email=share.user.email if share.user else None,
        access_level=ConnectionAccessLevel(share.access_level),
        role_id=share.role_id,
        role_name=share.role.name if share.role else None,
        allowed_tables=list(share.allowed_tables or []),
        blocked_tables=list(share.blocked_tables or []),
        allowed_columns=dict(share.allowed_columns or {}),
        blocked_columns=dict(share.blocked_columns or {}),
        allowed_query_types=list(share.allowed_query_types or []),
        max_rows=share.max_rows,
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


def _connection_role_out(role: ConnectionRole) -> ConnectionRoleOut:
    return ConnectionRoleOut(
        id=role.id,
        connection_id=role.connection_id,
        name=role.name,
        description=role.description,
        is_default=bool(role.is_default),
        created_at=role.created_at,
        allowed_tables=list(role.allowed_tables or []),
        blocked_tables=list(role.blocked_tables or []),
        allowed_columns=dict(role.allowed_columns or {}),
        blocked_columns=dict(role.blocked_columns or {}),
        allowed_query_types=list(role.allowed_query_types or []),
        max_rows=role.max_rows,
        permissions=[
            ConnectionRolePermissionOut(
                id=p.id,
                name=p.name,
                description=p.description,
                category=p.name.split(":", 1)[0] if ":" in p.name else "outros",
            )
            for p in (role.permissions or [])
        ],
    )


def _connection_empresa_out(assoc: EmpresaConnection) -> ConnectionEmpresaOut:
    return ConnectionEmpresaOut(
        id=assoc.empresa.id,
        nome=assoc.empresa.nome,
        tamanho=assoc.empresa.tamanho,
        nif=assoc.empresa.nif,
        access_level=ConnectionAccessLevel(assoc.access_level or "read"),
        role_id=assoc.role_id,
        role_name=assoc.role.name if assoc.role else None,
        created_at=assoc.created_at or assoc.empresa.criado_em,
    )


def _get_connection_empresas(db: Session, conn_id: int) -> list[ConnectionEmpresaOut]:
    assocs = (
        db.query(EmpresaConnection)
        .options(joinedload(EmpresaConnection.empresa), joinedload(EmpresaConnection.role))
        .filter(EmpresaConnection.connection_id == conn_id)
        .all()
    )
    return [_connection_empresa_out(a) for a in assocs if a.empresa]


DEFAULT_CONN_ROLES = [
    {
        "name": "Leitor",
        "description": "Apenas leitura de dados e consulta de estruturas.",
        "is_default": True,
        "permissions": ["query:execute", "query:read_history", "table:read", "table:describe", "db_connection:read_own"],
    },
    {
        "name": "Operador de Dados",
        "description": "Consulta e inserção/atualização de dados, sem permissões de DDL.",
        "is_default": True,
        "permissions": ["query:execute", "query:read_history", "query:export", "table:read", "table:describe", "data:write", "db_connection:read_own"],
    },
    {
        "name": "DBA / Estrutura",
        "description": "Controlo total de dados e esquemas (DDL, DML, transferências).",
        "is_default": True,
        "permissions": ["query:execute", "query:read_history", "query:export", "table:read", "table:describe", "table:stats", "data:write", "data:delete", "schema:manage", "data:transfer", "db_connection:read_own"],
    },
    {
        "name": "Admin da Conexão",
        "description": "Gestão completa da conexão, parâmetros e partilhas.",
        "is_default": True,
        "permissions": ["query:execute", "query:read_history", "query:export", "table:read", "table:describe", "table:stats", "data:write", "data:delete", "schema:manage", "data:transfer", "db_connection:read_own", "db_connection:update", "db_connection:test", "backup:read", "backup:execute"],
    },
]


def _ensure_default_connection_roles(db: Session, conn: DBConnection) -> None:
    """Garante a existência das roles padrão nesta conexão se ainda não existirem."""
    existing_roles = {
        r.name for r in db.query(ConnectionRole.name).filter(ConnectionRole.connection_id == conn.id).all()
    }

    all_perms = {p.name: p for p in db.query(Permission).all()}

    criou_alguma = False
    for spec in DEFAULT_CONN_ROLES:
        if spec["name"] in existing_roles:
            continue
        role_perms = []
        for p_name in spec["permissions"]:
            if p_name in all_perms:
                role_perms.append(all_perms[p_name])
            else:
                new_perm = Permission(name=p_name, description=f"Permissão {p_name}")
                db.add(new_perm)
                db.flush()
                all_perms[p_name] = new_perm
                role_perms.append(new_perm)

        c_role = ConnectionRole(
            connection_id=conn.id,
            name=spec["name"],
            description=spec["description"],
            is_default=True,
            permissions=role_perms,
        )
        db.add(c_role)
        criou_alguma = True

    if criou_alguma:
        try:
            db.commit()
        except Exception as e:
            db.rollback()
            log_message(f"⚠️ Erro ao semear roles padrão para conexão #{conn.id}: {e}", "warning")


def get_connection_access(
    db: Session, conn: DBConnection, user: User
) -> ConnectionAccessOut:
    """
    Resolve o que `user` pode fazer em `conn`, juntando as três origens de
    acesso: ser dono, ser super admin, ou ter uma partilha.
    """
    is_owner = conn.user_id == user.id
    superadmin = is_superadmin(user)

    nivel = resolve_access_level(db, conn, user)
    forca = forca_do_nivel(nivel)

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
                joinedload(DBConnectionShare.role),
            )
            .filter(DBConnectionShare.connection_id == conn.id)
            .order_by(DBConnectionShare.created_at.desc())
            .all()
        )
        shares = [_share_out(s) for s in registos]

    connection_roles: list[ConnectionRoleOut] = []
    if pode_partilhar or is_owner or superadmin:
        conn_roles = (
            db.query(ConnectionRole)
            .options(joinedload(ConnectionRole.permissions))
            .filter(ConnectionRole.connection_id == conn.id)
            .order_by(ConnectionRole.name)
            .all()
        )
        if not conn_roles:
            _ensure_default_connection_roles(db, conn)
            conn_roles = (
                db.query(ConnectionRole)
                .options(joinedload(ConnectionRole.permissions))
                .filter(ConnectionRole.connection_id == conn.id)
                .order_by(ConnectionRole.name)
                .all()
            )
        connection_roles = [_connection_role_out(r) for r in conn_roles]

    user_role_id = None
    user_role_name = None
    if is_owner:
        user_role_name = "Proprietário"
    elif superadmin:
        user_role_name = "Super Admin"
    else:
        u_share = (
            db.query(DBConnectionShare)
            .options(joinedload(DBConnectionShare.role))
            .filter(
                DBConnectionShare.connection_id == conn.id,
                DBConnectionShare.user_id == user.id,
            )
            .first()
        )
        if u_share and u_share.role:
            user_role_id = u_share.role_id
            user_role_name = u_share.role.name
        elif getattr(user, "empresa_id", None):
            emp_assoc = (
                db.query(EmpresaConnection)
                .options(joinedload(EmpresaConnection.role))
                .filter(
                    EmpresaConnection.connection_id == conn.id,
                    EmpresaConnection.empresa_id == user.empresa_id,
                )
                .first()
            )
            if emp_assoc and emp_assoc.role:
                user_role_id = emp_assoc.role_id
                user_role_name = emp_assoc.role.name

    empresas_list: list[ConnectionEmpresaOut] = []
    if pode_partilhar or is_owner or superadmin:
        empresas_list = _get_connection_empresas(db, conn.id)
    effective_rules = get_effective_connection_rules(db, conn, user)

    return ConnectionAccessOut(
        connection_id=conn.id,
        connection_name=conn.name,
        owner_id=conn.user_id,
        owner_nome=conn.owner.nome if conn.owner else None,
        is_owner=is_owner,
        access_level=nivel,
        role_id=user_role_id,
        role_name=user_role_name,
        can_read=forca >= ACCESS_ORDER[ConnectionAccessLevel.read],
        can_write=forca >= ACCESS_ORDER[ConnectionAccessLevel.write],
        can_share=pode_partilhar,
        can_delete=is_owner or superadmin,
        shares=shares,
        roles=connection_roles,
        empresas=empresas_list,
        effective_rules=effective_rules,
    )


def assert_connection_access(
    db: Session,
    conn: DBConnection,
    user: User,
    required: ConnectionAccessLevel = ConnectionAccessLevel.read,
) -> ConnectionAccessOut:
    """
    Levanta 403 se `user` não tiver pelo menos o nível `required` em `conn`.

    Devolve o `ConnectionAccessOut` completo porque as rotas de gestão precisam
    dos flags (`can_delete`) e da lista de partilhas. No caminho de execução usa-se
    antes `assert_connection_level`, que resolve o nível sem montar essa lista.
    """
    acesso = get_connection_access(db, conn, user)

    if forca_do_nivel(acesso.access_level) < ACCESS_ORDER[required]:
        negar_acesso(conn, user.id, required)

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
    access_level: Optional[ConnectionAccessLevel] = None,
    role_id: Optional[int] = None,
    allowed_tables: Optional[list[str]] = None,
    blocked_tables: Optional[list[str]] = None,
    allowed_columns: Optional[dict[str, list[str]]] = None,
    blocked_columns: Optional[dict[str, list[str]]] = None,
    allowed_query_types: Optional[list[str]] = None,
    max_rows: Optional[int] = None,
) -> ConnectionShareOut:
    """Concede (ou atualiza) o acesso de outro utilizador a uma conexão com regras opcionais."""
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

    # Validar role_id se fornecido
    if role_id is not None:
        c_role = (
            db.query(ConnectionRole)
            .filter(
                ConnectionRole.id == role_id,
                ConnectionRole.connection_id == conn.id,
            )
            .first()
        )
        if not c_role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="A função de conexão especificada não pertence a esta conexão.",
            )

    share = get_share(db, conn.id, target_user_id)
    criou = share is None

    if share:
        if access_level is not None:
            share.access_level = access_level.value
        if role_id is not None:
            share.role_id = role_id
        if allowed_tables is not None:
            share.allowed_tables = allowed_tables
        if blocked_tables is not None:
            share.blocked_tables = blocked_tables
        if allowed_columns is not None:
            share.allowed_columns = allowed_columns
        if blocked_columns is not None:
            share.blocked_columns = blocked_columns
        if allowed_query_types is not None:
            share.allowed_query_types = allowed_query_types
        if max_rows is not None:
            share.max_rows = max_rows
    else:
        share = DBConnectionShare(
            connection_id=conn.id,
            user_id=target_user_id,
            access_level=access_level.value if access_level else "read",
            role_id=role_id,
            allowed_tables=allowed_tables or [],
            blocked_tables=blocked_tables or [],
            allowed_columns=allowed_columns or {},
            blocked_columns=blocked_columns or {},
            allowed_query_types=allowed_query_types or [],
            max_rows=max_rows,
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
            "access_level": share.access_level,
            "role_id": role_id,
        },
        user_id=actor.id,
    )

    log_message(
        f"🤝 Conexão '{conn.name}' partilhada com {alvo.email} "
        f"(nível={access_level.value}, role_id={role_id}) por {actor.email}",
        "success",
    )

    # Recarrega as relações para preencher os nomes no output.
    share = (
        db.query(DBConnectionShare)
        .options(
            joinedload(DBConnectionShare.user),
            joinedload(DBConnectionShare.granted_by),
            joinedload(DBConnectionShare.role),
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
    """IDs das conexões partilhadas com este utilizador ou com a sua empresa."""
    user = db.query(User.empresa_id).filter(User.id == user_id).first()
    empresa_id = user[0] if user else None

    rows = (
        db.query(DBConnectionShare.connection_id)
        .filter(DBConnectionShare.user_id == user_id)
        .all()
    )
    conn_ids = {row[0] for row in rows}

    if empresa_id:
        emp_rows = (
            db.query(EmpresaConnection.connection_id)
            .filter(EmpresaConnection.empresa_id == empresa_id)
            .all()
        )
        conn_ids.update(row[0] for row in emp_rows)

    return list(conn_ids)


# =========================================================
# 🔑 Gestão de Funções (Roles) por Conexão
# =========================================================

def list_connection_roles(
    db: Session, connection_id: int, actor: User
) -> list[ConnectionRoleOut]:
    """Lista as funções RBAC disponíveis nesta conexão (semeando as padrões se não existirem)."""
    conn = get_connection_or_404(db, connection_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.read)

    roles = (
        db.query(ConnectionRole)
        .options(joinedload(ConnectionRole.permissions))
        .filter(ConnectionRole.connection_id == conn.id)
        .order_by(ConnectionRole.name)
        .all()
    )

    if not roles:
        _ensure_default_connection_roles(db, conn)
        roles = (
            db.query(ConnectionRole)
            .options(joinedload(ConnectionRole.permissions))
            .filter(ConnectionRole.connection_id == conn.id)
            .order_by(ConnectionRole.name)
            .all()
        )

    return [_connection_role_out(r) for r in roles]


def create_connection_role(
    db: Session, connection_id: int, actor: User, data: ConnectionRoleCreate
) -> ConnectionRoleOut:
    """Cria uma nova função RBAC isolada nesta conexão."""
    conn = get_connection_or_404(db, connection_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    existente = (
        db.query(ConnectionRole)
        .filter(
            ConnectionRole.connection_id == conn.id,
            ConnectionRole.name == data.name.strip(),
        )
        .first()
    )
    if existente:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Já existe uma função com o nome '{data.name.strip()}' nesta conexão.",
        )

    perms = []
    if data.permission_ids:
        perms = (
            db.query(Permission)
            .filter(Permission.id.in_(data.permission_ids))
            .all()
        )

    c_role = ConnectionRole(
        connection_id=conn.id,
        name=data.name.strip(),
        description=data.description.strip() if data.description else None,
        is_default=False,
        permissions=perms,
        allowed_tables=data.allowed_tables or [],
        blocked_tables=data.blocked_tables or [],
        allowed_columns=data.allowed_columns or {},
        blocked_columns=data.blocked_columns or {},
        allowed_query_types=data.allowed_query_types or [],
        max_rows=data.max_rows,
    )
    db.add(c_role)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao criar função de conexão: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro ao criar a função da conexão.")

    db.refresh(c_role)
    log_message(f"🔑 Função de conexão '{c_role.name}' criada para conexão #{conn.id} por {actor.email}", "success")
    return _connection_role_out(c_role)


def update_connection_role(
    db: Session,
    connection_id: int,
    role_id: int,
    actor: User,
    data: ConnectionRoleUpdate,
) -> ConnectionRoleOut:
    """Atualiza uma função RBAC da conexão."""
    conn = get_connection_or_404(db, connection_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    c_role = (
        db.query(ConnectionRole)
        .options(joinedload(ConnectionRole.permissions))
        .filter(
            ConnectionRole.id == role_id,
            ConnectionRole.connection_id == conn.id,
        )
        .first()
    )
    if not c_role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Função da conexão não encontrada.",
        )

    if data.name is not None and data.name.strip() != c_role.name:
        dup = (
            db.query(ConnectionRole)
            .filter(
                ConnectionRole.connection_id == conn.id,
                ConnectionRole.name == data.name.strip(),
                ConnectionRole.id != c_role.id,
            )
            .first()
        )
        if dup:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Já existe uma função com o nome '{data.name.strip()}' nesta conexão.",
            )
        c_role.name = data.name.strip()

    if data.description is not None:
        c_role.description = data.description.strip() if data.description else None

    if data.permission_ids is not None:
        novas = (
            db.query(Permission)
            .filter(Permission.id.in_(data.permission_ids))
            .all()
        )
        c_role.permissions = novas

    if data.allowed_tables is not None:
        c_role.allowed_tables = data.allowed_tables
    if data.blocked_tables is not None:
        c_role.blocked_tables = data.blocked_tables
    if data.allowed_columns is not None:
        c_role.allowed_columns = data.allowed_columns
    if data.blocked_columns is not None:
        c_role.blocked_columns = data.blocked_columns
    if data.allowed_query_types is not None:
        c_role.allowed_query_types = data.allowed_query_types
    if data.max_rows is not None:
        c_role.max_rows = data.max_rows

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao atualizar função de conexão: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro ao atualizar a função da conexão.")

    db.refresh(c_role)
    return _connection_role_out(c_role)


def delete_connection_role(
    db: Session, connection_id: int, role_id: int, actor: User
) -> None:
    """Remove uma função da conexão, desvinculando membros partilhados."""
    conn = get_connection_or_404(db, connection_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    c_role = (
        db.query(ConnectionRole)
        .filter(
            ConnectionRole.id == role_id,
            ConnectionRole.connection_id == conn.id,
        )
        .first()
    )
    if not c_role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Função da conexão não encontrada.",
        )

    # Desvincular membros partilhados desta role
    db.query(DBConnectionShare).filter(
        DBConnectionShare.role_id == c_role.id
    ).update({"role_id": None}, synchronize_session=False)

    nome = c_role.name
    db.delete(c_role)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao remover função de conexão: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro ao remover a função da conexão.")

    log_message(f"🗑️ Função de conexão '{nome}' (#{role_id}) removida por {actor.email}", "success")


def list_connection_available_permissions(db: Session) -> list[ConnectionRolePermissionOut]:
    """Retorna o catálogo de permissões aplicáveis a conexões de dados."""
    prefixes = ("query:", "table:", "data:", "schema:", "db_connection:", "backup:")
    all_perms = db.query(Permission).order_by(Permission.name).all()
    conn_perms = [p for p in all_perms if any(p.name.startswith(pref) for pref in prefixes)]

    return [
        ConnectionRolePermissionOut(
            id=p.id,
            name=p.name,
            description=p.description,
            category=p.name.split(":", 1)[0] if ":" in p.name else "outros",
        )
        for p in conn_perms
    ]


# =========================================================
# 🏢🔗🔌 Gestão N:N de Conexão com Empresas
# =========================================================

def list_connection_empresas(db: Session, conn_id: int, actor: User) -> list[ConnectionEmpresaOut]:
    """Lista todas as empresas associadas a uma conexão com o seu nível de acesso e role."""
    conn = get_connection_or_404(db, conn_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.read)
    return _get_connection_empresas(db, conn_id)


def list_shareable_users(db: Session, conn_id: int, actor: User) -> list[dict]:
    """
    Colegas a quem esta conexão pode ser partilhada: membros ativos da mesma
    empresa do dono, sem contar com o próprio dono nem com quem já tem acesso.
    """
    conn = get_connection_or_404(db, conn_id)
    assert_connection_level(db, conn, actor, ConnectionAccessLevel.manage)

    ja_com_acesso = {
        r[0] for r in db.query(DBConnectionShare.user_id).filter(DBConnectionShare.connection_id == conn_id).all()
    }
    ja_com_acesso.add(conn.user_id)

    query = db.query(User.id, User.nome, User.apelido, User.email).filter(
        User.is_active.is_(True),
    )
    if ja_com_acesso:
        query = query.filter(User.id.notin_(ja_com_acesso))

    dono = conn.owner
    if dono is not None and dono.empresa_id is not None:
        query = query.filter(User.empresa_id == dono.empresa_id)
    elif not is_superadmin(actor):
        query = query.filter(User.empresa_id == actor.empresa_id)

    candidatos = query.order_by(User.nome).all()

    return [
        {
            "id": u.id,
            "nome": u.nome,
            "apelido": u.apelido,
            "email": u.email,
        }
        for u in candidatos
    ]


def list_shareable_empresas(db: Session, conn_id: int, actor: User) -> list[dict]:
    """Lista empresas disponíveis para adicionar a esta conexão (exclui as já vinculadas)."""
    conn = get_connection_or_404(db, conn_id)
    assert_connection_level(db, conn, actor, ConnectionAccessLevel.manage)

    # IDs já vinculados
    vinculadas_ids = [
        r[0] for r in db.query(EmpresaConnection.empresa_id).filter(EmpresaConnection.connection_id == conn_id).all()
    ]

    query = db.query(Empresa.id, Empresa.nome, Empresa.nif, Empresa.tamanho)
    if not is_superadmin(actor):
        if actor.empresa_id:
            query = query.filter(Empresa.id == actor.empresa_id)
        else:
            return []

    if vinculadas_ids:
        query = query.filter(Empresa.id.notin_(vinculadas_ids))

    empresas = query.order_by(Empresa.nome).all()
    return [
        {
            "id": e.id,
            "nome": e.nome,
            "nif": e.nif,
            "tamanho": e.tamanho,
        }
        for e in empresas
    ]


def add_connection_empresa(
    db: Session,
    conn_id: int,
    empresa_id: int,
    actor: User,
    access_level: Optional[ConnectionAccessLevel] = None,
    role_id: Optional[int] = None,
) -> ConnectionEmpresaOut:
    """Associa uma empresa à conexão (N:N), definindo nível de acesso e função."""
    conn = get_connection_or_404(db, conn_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    empresa = db.query(Empresa).filter(Empresa.id == empresa_id).first()
    if not empresa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Empresa não encontrada.",
        )

    role_obj = None
    if role_id is not None:
        role_obj = (
            db.query(ConnectionRole)
            .filter(ConnectionRole.id == role_id, ConnectionRole.connection_id == conn.id)
            .first()
        )
        if not role_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="A função especificada não pertence a esta conexão.",
            )

    assoc = (
        db.query(EmpresaConnection)
        .filter(
            EmpresaConnection.empresa_id == empresa_id,
            EmpresaConnection.connection_id == conn_id,
        )
        .first()
    )

    nivel_str = access_level.value if access_level else "read"

    if not assoc:
        assoc = EmpresaConnection(
            empresa_id=empresa_id,
            connection_id=conn_id,
            access_level=nivel_str,
            role_id=role_id,
        )
        db.add(assoc)
    else:
        assoc.access_level = nivel_str
        assoc.role_id = role_id

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao vincular empresa à conexão: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro ao vincular empresa à conexão.")

    db.refresh(assoc)
    log_message(
        f"🔗 Empresa '{empresa.nome}' vinculada à conexão '{conn.name}' (nível={nivel_str}, role_id={role_id}) por {actor.email}",
        "success",
    )

    return ConnectionEmpresaOut(
        id=empresa.id,
        nome=empresa.nome,
        tamanho=empresa.tamanho,
        nif=empresa.nif,
        access_level=ConnectionAccessLevel(assoc.access_level or "read"),
        role_id=assoc.role_id,
        role_name=role_obj.name if role_obj else (assoc.role.name if assoc.role else None),
        created_at=assoc.created_at or empresa.criado_em,
    )


def update_connection_empresa(
    db: Session,
    conn_id: int,
    empresa_id: int,
    actor: User,
    data: ConnectionEmpresaUpdate,
) -> ConnectionEmpresaOut:
    """Atualiza o nível de acesso ou role de uma empresa vinculada à conexão."""
    conn = get_connection_or_404(db, conn_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    assoc = (
        db.query(EmpresaConnection)
        .options(joinedload(EmpresaConnection.empresa), joinedload(EmpresaConnection.role))
        .filter(
            EmpresaConnection.empresa_id == empresa_id,
            EmpresaConnection.connection_id == conn_id,
        )
        .first()
    )
    if not assoc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Esta empresa não está vinculada a esta conexão.",
        )

    if data.role_id is not None:
        if data.role_id == 0:
            assoc.role_id = None
        else:
            role_obj = (
                db.query(ConnectionRole)
                .filter(ConnectionRole.id == data.role_id, ConnectionRole.connection_id == conn.id)
                .first()
            )
            if not role_obj:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="A função especificada não pertence a esta conexão.",
                )
            assoc.role_id = data.role_id

    if data.access_level is not None:
        assoc.access_level = data.access_level.value

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro ao atualizar acesso da empresa: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro ao atualizar acesso da empresa.")

    db.refresh(assoc)
    return ConnectionEmpresaOut(
        id=assoc.empresa.id,
        nome=assoc.empresa.nome,
        tamanho=assoc.empresa.tamanho,
        nif=assoc.empresa.nif,
        access_level=ConnectionAccessLevel(assoc.access_level or "read"),
        role_id=assoc.role_id,
        role_name=assoc.role.name if assoc.role else None,
        created_at=assoc.created_at or assoc.empresa.criado_em,
    )


def remove_connection_empresa(db: Session, conn_id: int, empresa_id: int, actor: User) -> None:
    """Desvincula uma empresa da conexão (N:N)."""
    conn = get_connection_or_404(db, conn_id)
    assert_connection_access(db, conn, actor, ConnectionAccessLevel.manage)

    assoc = (
        db.query(EmpresaConnection)
        .filter(
            EmpresaConnection.empresa_id == empresa_id,
            EmpresaConnection.connection_id == conn_id,
        )
        .first()
    )
    if assoc:
        db.delete(assoc)
        try:
            db.commit()
        except Exception as e:
            db.rollback()
            log_message(f"❌ Erro ao desvincular empresa da conexão: {e}", "error")
            raise HTTPException(status_code=500, detail="Erro ao desvincular empresa da conexão.")

        log_message(
            f"✂️ Empresa #{empresa_id} desvinculada da conexão '{conn.name}' por {actor.email}",
            "info",
        )


def list_empresa_connections(db: Session, empresa_id: int, actor: User) -> list[EmpresaConnectionOut]:
    """Lista todas as conexões associadas a uma empresa específica."""
    empresa = db.query(Empresa).filter(Empresa.id == empresa_id).first()
    if not empresa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Empresa não encontrada.",
        )

    if not is_superadmin(actor) and actor.empresa_id != empresa_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para aceder aos recursos desta empresa.",
        )

    out = []
    for c in empresa.connections:
        dec_host = c.host
        try:
            dec_host = secret_decrypt(c.host)
        except Exception:
            pass

        out.append(
            EmpresaConnectionOut(
                id=c.id,
                name=c.name,
                type=c.type,
                host=dec_host,
                database_name=c.database_name,
                status=c.status,
                created_at=c.created_at,
            )
        )
    return out


