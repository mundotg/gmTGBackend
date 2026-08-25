"""
🔐 Dependências de autorização (RBAC)

Fornece:
  - get_current_user      → carrega o utilizador autenticado (com role + permissões)
  - user_has_permission   → verificação pura, testável, com suporte a wildcards
  - require_permission    → factory de dependência FastAPI para proteger rotas

Suporta wildcards:
  - "admin:*"          → super admin, concede tudo
  - "<prefixo>:*"      → concede tudo dentro do prefixo (ex.: "role:*" cobre "role:delete")

⚠️ "admin:*" é a marca de super admin. Como é uma permissão normal na base de
dados, o CRUD de RBAC impede que quem não é super admin a conceda a alguém
(incluindo a si próprio) — caso contrário qualquer admin com "role:manage"
poderia promover-se.
"""

from __future__ import annotations

from typing import Iterable, Sequence

from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.models import user_model
from app.ultils.get_id_by_token import get_current_user_id

# Permissão mestre: quem a tiver passa em qualquer verificação.
SUPER_PERMISSION = "admin:*"


def get_current_user(
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> user_model.User:
    """
    Carrega o utilizador autenticado já com `role` e `role.permissions`,
    evitando N+1 queries quando a rota consulta `user.permissions`.
    """
    user = (
        db.query(user_model.User)
        .options(
            joinedload(user_model.User.role).joinedload(user_model.Role.permissions)
        )
        .filter(user_model.User.id == user_id)
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sessão inválida: utilizador não encontrado.",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Conta desativada. Contacte um administrador.",
        )

    return user


def user_has_permission(
    user_permissions: Iterable[str],
    required: Sequence[str],
) -> bool:
    """
    True se o utilizador tiver **pelo menos uma** das permissões exigidas.
    Sem permissões exigidas → acesso livre (rota apenas autenticada).
    """
    if not required:
        return True

    granted = set(user_permissions or ())

    if SUPER_PERMISSION in granted:
        return True

    for permission in required:
        if permission in granted:
            return True

        prefix = permission.split(":", 1)[0]
        if f"{prefix}:*" in granted:
            return True

    return False


def is_superadmin(user: user_model.User) -> bool:
    """Super admin = detém a permissão mestra `admin:*`."""
    return SUPER_PERMISSION in set(user.permissions or ())


def require_permission(*required: str):
    """
    Uso:
        @router.delete("/roles/{role_id}")
        def remover(actor = Depends(require_permission("role:delete", "role:manage"))):
            ...

    Devolve o `User` autenticado, para a rota poder registar quem executou a ação.
    """

    def dependency(
        current_user: user_model.User = Depends(get_current_user),
    ) -> user_model.User:
        if not user_has_permission(current_user.permissions, required):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Permissão insuficiente. "
                    f"É necessária uma destas: {', '.join(required)}."
                ),
            )
        return current_user

    return dependency
