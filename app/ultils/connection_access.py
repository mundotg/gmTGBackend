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

from app.models.connection_models import DBConnection, DBConnectionShare
from app.models.user_model import Role, User
from app.schemas.connetion_schema import ConnectionAccessLevel
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

    share = (
        db.query(DBConnectionShare)
        .filter(
            DBConnectionShare.connection_id == conn.id,
            DBConnectionShare.user_id == user.id,
        )
        .first()
    )
    return ConnectionAccessLevel(share.access_level) if share else None


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
    return ConnectionAccessLevel(share.access_level) if share else None


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
