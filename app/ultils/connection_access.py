"""
🔐 Nível de acesso a uma conexão, imposto no momento da execução.

`DBConnectionShare.access_level` (read → write → manage) era gravado, gerido no
CRUD e mostrado na interface, mas nunca chegava a decidir nada: `ensure_connection`
apenas confirmava que existia uma conexão ativa para o utilizador. Quem recebia
uma conexão partilhada com nível `read` conseguia na mesma chamar `/update_row`,
apagar colunas por DDL e `/delete_all` — o nível era só uma etiqueta na UI.

Este módulo é o sítio único onde o nível é resolvido e exigido. Fica em `ultils/`
e não em `cruds/connection_cruds.py` porque é chamado a partir do caminho de
execução (`ativar_engine`), e esse caminho não pode depender do módulo de CRUD
sem criar um ciclo de imports. O CRUD passa a importar daqui, para que as regras
de nível existam num sítio só e não derivem com o tempo.

Cada função tem variante síncrona e assíncrona: metade das rotas usa `Session`
e a outra metade `AsyncSession`.
"""

from __future__ import annotations

from typing import Dict, Optional

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, joinedload, selectinload

from app.models.connection_models import (
    ConnectionRole,
    DBConnection,
    DBConnectionShare,
    EmpresaConnection,
)
from app.models.user_model import Role, User
from app.schemas.connetion_schema import ConnectionAccessLevel, EffectiveConnectionRules
from app.ultils.logger import log_message
from app.ultils.permissions import is_superadmin

# Força relativa dos níveis. Serve para responder a "write chega para ler?".
ACCESS_ORDER: Dict[ConnectionAccessLevel, int] = {
    ConnectionAccessLevel.read: 1,
    ConnectionAccessLevel.write: 2,
    ConnectionAccessLevel.manage: 3,
}


def forca_do_nivel(nivel: Optional[ConnectionAccessLevel]) -> int:
    """0 = sem qualquer acesso."""
    return ACCESS_ORDER.get(nivel, 0) if nivel else 0


def _nivel_por_papel(conn: DBConnection, user: User) -> Optional[ConnectionAccessLevel]:
    """
    Acesso que não vem de uma partilha: o dono e o super admin valem sempre
    `manage`. Devolve None quando o acesso, a existir, tem de vir de um share.
    """
    if conn.user_id == user.id or is_superadmin(user):
        return ConnectionAccessLevel.manage
    return None


def negar_acesso(conn: DBConnection, user_id: int, required: ConnectionAccessLevel) -> None:
    # Uma tentativa de escrita sem nível é um evento de segurança, não apenas um
    # 403 de rotina — fica registada para poder ser investigada.
    log_message(
        f"Acesso negado à conexão | conn_id={getattr(conn, 'id', None)} "
        f"| user_id={user_id} | nível exigido={required.value}",
        "warning",
    )
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=(
            f"Sem acesso de nível '{required.value}' à conexão '{conn.name}'. "
            "Peça ao dono da conexão para lhe conceder acesso."
        ),
    )


def _validar_ator(user: Optional[User], user_id: int) -> User:
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sessão inválida: utilizador não encontrado.",
        )

    # `get_current_user_id` só descodifica o token — não olha para o estado da
    # conta. Sem esta verificação, um utilizador desativado continuava a
    # executar até o token expirar.
    if not user.is_active:
        log_message(
            f"Utilizador desativado tentou usar uma conexão | user_id={user_id}",
            "warning",
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Conta desativada. Contacte um administrador.",
        )

    return user


# ══════════════════════════════ síncrono ══════════════════════════════
def load_actor(db: Session, user_id: int) -> User:
    """
    Carrega o utilizador com `role` e `role.permissions` já resolvidos —
    `is_superadmin` lê `user.permissions`, e sem o eager loading isto seria
    uma query por cada verificação.
    """
    user = (
        db.query(User)
        .options(joinedload(User.role).joinedload(Role.permissions))
        .filter(User.id == user_id)
        .first()
    )
    return _validar_ator(user, user_id)


def resolve_access_level(
    db: Session, conn: DBConnection, user: User
) -> Optional[ConnectionAccessLevel]:
    """Nível efetivo de `user` em `conn`, ou None se não tiver acesso nenhum."""
    nivel = _nivel_por_papel(conn, user)
    if nivel:
        return nivel

    # 1. Share direto do utilizador (tem precedência)
    share = (
        db.query(DBConnectionShare)
        .filter(
            DBConnectionShare.connection_id == conn.id,
            DBConnectionShare.user_id == user.id,
        )
        .first()
    )
    if share:
        return ConnectionAccessLevel(share.access_level)

    # 2. Acesso corporativo via Empresa vinculada à conexão (N:N)
    if getattr(user, "empresa_id", None):
        emp_assoc = (
            db.query(EmpresaConnection)
            .filter(
                EmpresaConnection.connection_id == conn.id,
                EmpresaConnection.empresa_id == user.empresa_id,
            )
            .first()
        )
        if emp_assoc:
            return ConnectionAccessLevel(emp_assoc.access_level or "read")

    return None


def get_effective_connection_rules(
    db: Session, conn: DBConnection, user: User
) -> EffectiveConnectionRules:
    """
    Resolve as regras granulares de segurança (tabelas, campos, tipos de consulta, max_rows)
    aplicáveis a `user` na conexão `conn`.
    """
    if conn.user_id == user.id or is_superadmin(user):
        return EffectiveConnectionRules()

    # 1. Share direto
    share = (
        db.query(DBConnectionShare)
        .options(joinedload(DBConnectionShare.role))
        .filter(
            DBConnectionShare.connection_id == conn.id,
            DBConnectionShare.user_id == user.id,
        )
        .first()
    )

    if share:
        rules = EffectiveConnectionRules()
        if share.role:
            r = share.role
            rules.allowed_tables = list(r.allowed_tables or [])
            rules.blocked_tables = list(r.blocked_tables or [])
            rules.allowed_columns = dict(r.allowed_columns or {})
            rules.blocked_columns = dict(r.blocked_columns or {})
            rules.allowed_query_types = list(r.allowed_query_types or [])
            rules.max_rows = r.max_rows

        if share.allowed_tables:
            rules.allowed_tables = list(share.allowed_tables)
        if share.blocked_tables:
            rules.blocked_tables = list(set(rules.blocked_tables + list(share.blocked_tables)))
        if share.allowed_columns:
            rules.allowed_columns.update(share.allowed_columns)
        if share.blocked_columns:
            rules.blocked_columns.update(share.blocked_columns)
        if share.allowed_query_types:
            rules.allowed_query_types = list(share.allowed_query_types)
        if share.max_rows is not None:
            rules.max_rows = share.max_rows if rules.max_rows is None else min(rules.max_rows, share.max_rows)
        return rules

    # 2. Acesso via Empresa
    if getattr(user, "empresa_id", None):
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
            r = emp_assoc.role
            return EffectiveConnectionRules(
                allowed_tables=list(r.allowed_tables or []),
                blocked_tables=list(r.blocked_tables or []),
                allowed_columns=dict(r.allowed_columns or {}),
                blocked_columns=dict(r.blocked_columns or {}),
                allowed_query_types=list(r.allowed_query_types or []),
                max_rows=r.max_rows,
            )

    return EffectiveConnectionRules()


def validate_connection_query_rules(
    rules: EffectiveConnectionRules,
    query_type: Optional[str] = None,
    tables: Optional[list[str]] = None,
    columns_by_table: Optional[dict[str, list[str]]] = None,
) -> None:
    """Valida se uma ação/consulta viola as regras granulares da conexão."""
    # 1. Tipo de consulta (SELECT, INSERT, UPDATE, DELETE, DDL, etc.)
    if query_type and rules.allowed_query_types:
        tipos_permitidos = [t.strip().upper() for t in rules.allowed_query_types if t.strip()]
        if tipos_permitidos and query_type.strip().upper() not in tipos_permitidos:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Operação do tipo '{query_type}' não é permitida pelo seu perfil nesta conexão. Tipos autorizados: {', '.join(tipos_permitidos)}.",
            )

    # 2. Tabelas
    if tables:
        for raw_tbl in tables:
            tbl = raw_tbl.split(".")[-1].strip('`"[]').lower()
            if not tbl:
                continue

            # Bloqueadas
            blocked = [t.split(".")[-1].strip('`"[]').lower() for t in rules.blocked_tables or []]
            if tbl in blocked:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Acesso à tabela '{raw_tbl}' está expressamente bloqueado para o seu perfil nesta conexão.",
                )

            # Permitidas (se lista não estiver vazia, funciona como whitelist)
            allowed = [t.split(".")[-1].strip('`"[]').lower() for t in rules.allowed_tables or []]
            if allowed and tbl not in allowed:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Acesso à tabela '{raw_tbl}' não é permitido pelo seu perfil nesta conexão.",
                )

    # 3. Colunas por tabela
    if columns_by_table:
        for raw_tbl, cols in columns_by_table.items():
            tbl = raw_tbl.split(".")[-1].strip('`"[]').lower()
            if not tbl or not cols:
                continue

            # Colunas bloqueadas
            blocked_cols: list[str] = []
            for b_tbl, b_cols in (rules.blocked_columns or {}).items():
                if b_tbl.split(".")[-1].strip('`"[]').lower() == tbl:
                    blocked_cols.extend([c.strip().lower() for c in b_cols])

            for col in cols:
                clean_col = col.strip('`"[]').lower()
                if clean_col in blocked_cols:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Acesso à coluna '{col}' na tabela '{raw_tbl}' está bloqueado nesta conexão.",
                    )

            # Colunas permitidas (whitelist se definida)
            allowed_cols: list[str] = []
            for a_tbl, a_cols in (rules.allowed_columns or {}).items():
                if a_tbl.split(".")[-1].strip('`"[]').lower() == tbl:
                    allowed_cols.extend([c.strip().lower() for c in a_cols])

            if allowed_cols:
                for col in cols:
                    clean_col = col.strip('`"[]').lower()
                    if clean_col != "*" and clean_col not in allowed_cols:
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"Acesso à coluna '{col}' na tabela '{raw_tbl}' não está autorizado para o seu perfil.",
                        )


def assert_connection_level(
    db: Session,
    conn: DBConnection,
    user: User,
    required: ConnectionAccessLevel = ConnectionAccessLevel.read,
) -> ConnectionAccessLevel:
    """Levanta 403 se `user` não tiver pelo menos `required` em `conn`."""
    nivel = resolve_access_level(db, conn, user)

    if forca_do_nivel(nivel) < ACCESS_ORDER[required]:
        negar_acesso(conn, user.id, required)

    return nivel  # type: ignore[return-value]


def assert_user_connection_level(
    db: Session,
    conn: DBConnection,
    user_id: int,
    required: ConnectionAccessLevel = ConnectionAccessLevel.read,
) -> ConnectionAccessLevel:
    """Igual à anterior, mas partindo do `user_id` que as rotas já têm à mão."""
    return assert_connection_level(db, conn, load_actor(db, user_id), required)


# ══════════════════════════════ assíncrono ══════════════════════════════
async def load_actor_async(db: AsyncSession, user_id: int) -> User:
    resultado = await db.execute(
        select(User)
        .options(selectinload(User.role).selectinload(Role.permissions))
        .where(User.id == user_id)
    )
    return _validar_ator(resultado.scalars().first(), user_id)


async def resolve_access_level_async(
    db: AsyncSession, conn: DBConnection, user: User
) -> Optional[ConnectionAccessLevel]:
    nivel = _nivel_por_papel(conn, user)
    if nivel:
        return nivel

    resultado = await db.execute(
        select(DBConnectionShare).where(
            DBConnectionShare.connection_id == conn.id,
            DBConnectionShare.user_id == user.id,
        )
    )
    share = resultado.scalars().first()
    if share:
        return ConnectionAccessLevel(share.access_level)

    if getattr(user, "empresa_id", None):
        emp_res = await db.execute(
            select(EmpresaConnection).where(
                EmpresaConnection.connection_id == conn.id,
                EmpresaConnection.empresa_id == user.empresa_id,
            )
        )
        emp_assoc = emp_res.scalars().first()
        if emp_assoc:
            return ConnectionAccessLevel(emp_assoc.access_level or "read")

    return None


async def get_effective_connection_rules_async(
    db: AsyncSession, conn: DBConnection, user: User
) -> EffectiveConnectionRules:
    """Versão assíncrona para resolução de regras de conexão."""
    if conn.user_id == user.id or is_superadmin(user):
        return EffectiveConnectionRules()

    resultado = await db.execute(
        select(DBConnectionShare)
        .options(selectinload(DBConnectionShare.role))
        .where(
            DBConnectionShare.connection_id == conn.id,
            DBConnectionShare.user_id == user.id,
        )
    )
    share = resultado.scalars().first()

    if share:
        rules = EffectiveConnectionRules()
        if share.role:
            r = share.role
            rules.allowed_tables = list(r.allowed_tables or [])
            rules.blocked_tables = list(r.blocked_tables or [])
            rules.allowed_columns = dict(r.allowed_columns or {})
            rules.blocked_columns = dict(r.blocked_columns or {})
            rules.allowed_query_types = list(r.allowed_query_types or [])
            rules.max_rows = r.max_rows

        if share.allowed_tables:
            rules.allowed_tables = list(share.allowed_tables)
        if share.blocked_tables:
            rules.blocked_tables = list(set(rules.blocked_tables + list(share.blocked_tables)))
        if share.allowed_columns:
            rules.allowed_columns.update(share.allowed_columns)
        if share.blocked_columns:
            rules.blocked_columns.update(share.blocked_columns)
        if share.allowed_query_types:
            rules.allowed_query_types = list(share.allowed_query_types)
        if share.max_rows is not None:
            rules.max_rows = share.max_rows if rules.max_rows is None else min(rules.max_rows, share.max_rows)
        return rules

    if getattr(user, "empresa_id", None):
        emp_res = await db.execute(
            select(EmpresaConnection)
            .options(selectinload(EmpresaConnection.role))
            .where(
                EmpresaConnection.connection_id == conn.id,
                EmpresaConnection.empresa_id == user.empresa_id,
            )
        )
        emp_assoc = emp_res.scalars().first()
        if emp_assoc and emp_assoc.role:
            r = emp_assoc.role
            return EffectiveConnectionRules(
                allowed_tables=list(r.allowed_tables or []),
                blocked_tables=list(r.blocked_tables or []),
                allowed_columns=dict(r.allowed_columns or {}),
                blocked_columns=dict(r.blocked_columns or {}),
                allowed_query_types=list(r.allowed_query_types or []),
                max_rows=r.max_rows,
            )

    return EffectiveConnectionRules()


async def assert_user_connection_level_async(
    db: AsyncSession,
    conn: DBConnection,
    user_id: int,
    required: ConnectionAccessLevel = ConnectionAccessLevel.read,
) -> ConnectionAccessLevel:
    user = await load_actor_async(db, user_id)
    nivel = await resolve_access_level_async(db, conn, user)

    if forca_do_nivel(nivel) < ACCESS_ORDER[required]:
        negar_acesso(conn, user_id, required)

    return nivel  # type: ignore[return-value]
