from typing import Optional

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.orm import Session

from app import database
from app.cruds import user_crud
from app.models import user_model
from app.schemas import users_schemas
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.permissions import (
    get_current_user,
    is_superadmin,
    require_permission,
    user_has_permission,
)

router = APIRouter(prefix="/users", tags=["Users"])


# 🔒 Listar todos os usuários (apenas se autenticado)
@router.get("/", response_model=list[users_schemas.UserOut])
async def list_all_users(
    current_user: user_model.User = Depends(get_current_user_id),
    db: Session = Depends(database.get_db),
):
    """
    Retorna todos os usuários cadastrados (rota protegida).
    """
    return user_crud.get_users(db)


# 🔒 Atualizar nome do usuário autenticado
@router.put("/update", response_model=users_schemas.UserOut)
async def update_user_name(
    full_name: str,
    current_user: user_model.User = Depends(get_current_user_id),
    db: Session = Depends(database.get_db),
):
    """
    Atualiza o nome completo do usuário logado.
    """
    return user_crud.update_user(db, current_user.id, full_name)


# ==========================================================
# 🔐 RBAC — funções, permissões e membros
#
# Leitura   → "settings:team" ou "team:read"
# Escrita   → "role:manage" (funções) / "user:manage" (membros)
# `admin:*` → super admin, passa em qualquer verificação
# ==========================================================

_CAN_READ = require_permission("settings:team", "team:read", "role:read")
_CAN_MANAGE_ROLES = require_permission("role:manage", "role:update")
_CAN_MANAGE_MEMBERS = require_permission("user:manage", "team:manage")


@router.get("/rbac/me", response_model=dict)
async def rbac_capabilities(
    actor: user_model.User = Depends(get_current_user),
):
    """
    Diz ao frontend o que este utilizador pode fazer nesta área, para a UI
    esconder/desativar controlos em vez de deixar o utilizador bater num 403.
    """
    permissoes = set(actor.permissions or ())

    def pode(*required: str) -> bool:
        return user_has_permission(permissoes, required)

    return {
        "user_id": actor.id,
        "is_superadmin": is_superadmin(actor),
        "can_read": pode("settings:team", "team:read", "role:read"),
        "can_manage_roles": pode("role:manage", "role:update"),
        "can_manage_members": pode("user:manage", "team:manage"),
    }


# -----------------------------
# 📋 Leitura
# -----------------------------
@router.get("/permissions", response_model=list[users_schemas.PermissionSchema])
async def list_permissions(
    actor: user_model.User = Depends(_CAN_READ),
    db: Session = Depends(database.get_db),
):
    """Catálogo de permissões disponíveis, já agrupável por categoria."""
    return user_crud.list_permissions(db)


@router.get("/roles", response_model=list[users_schemas.RoleSchema])
async def list_roles(
    actor: user_model.User = Depends(_CAN_READ),
    db: Session = Depends(database.get_db),
):
    """Funções existentes, com as respetivas permissões e nº de membros."""
    return user_crud.list_roles(db)


@router.get("/members", response_model=list[users_schemas.MemberSchema])
async def list_members(
    actor: user_model.User = Depends(_CAN_READ),
    db: Session = Depends(database.get_db),
):
    """Membros visíveis para quem pede (a sua empresa; super admin vê todos)."""
    return user_crud.list_members(db, actor)


# -----------------------------
# ✍️ Funções
# -----------------------------
@router.post(
    "/roles",
    response_model=users_schemas.RoleSchema,
    status_code=status.HTTP_201_CREATED,
)
async def create_role(
    data: users_schemas.RoleCreateSchema,
    actor: user_model.User = Depends(_CAN_MANAGE_ROLES),
    db: Session = Depends(database.get_db),
):
    return user_crud.create_role(db, actor, data)


@router.patch("/roles/{role_id}", response_model=users_schemas.RoleSchema)
async def update_role(
    role_id: int,
    data: users_schemas.RoleUpdateSchema,
    actor: user_model.User = Depends(_CAN_MANAGE_ROLES),
    db: Session = Depends(database.get_db),
):
    return user_crud.update_role(db, actor, role_id, data)


@router.delete("/roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_role(
    role_id: int,
    reassign_to: Optional[int] = Query(
        None,
        description=(
            "Função para onde transferir os membros. Obrigatória se a função "
            "a remover ainda tiver membros."
        ),
    ),
    actor: user_model.User = Depends(require_permission("role:delete", "role:manage")),
    db: Session = Depends(database.get_db),
):
    user_crud.delete_role(db, actor, role_id, reassign_to)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put(
    "/roles/{role_id}/permissions",
    response_model=users_schemas.RoleSchema,
)
async def set_role_permissions(
    role_id: int,
    data: users_schemas.RolePermissionsUpdateSchema,
    actor: user_model.User = Depends(_CAN_MANAGE_ROLES),
    db: Session = Depends(database.get_db),
):
    """Substitui todas as permissões da função (botão 'Guardar alterações')."""
    return user_crud.set_role_permissions(db, actor, role_id, data.permission_ids)


@router.post(
    "/roles/{role_id}/permissions/{permission_id}",
    response_model=users_schemas.RoleSchema,
)
async def grant_permission(
    role_id: int,
    permission_id: int,
    actor: user_model.User = Depends(_CAN_MANAGE_ROLES),
    db: Session = Depends(database.get_db),
):
    """Concede uma permissão isolada."""
    return user_crud.toggle_role_permission(db, actor, role_id, permission_id, True)


@router.delete(
    "/roles/{role_id}/permissions/{permission_id}",
    response_model=users_schemas.RoleSchema,
)
async def revoke_permission(
    role_id: int,
    permission_id: int,
    actor: user_model.User = Depends(_CAN_MANAGE_ROLES),
    db: Session = Depends(database.get_db),
):
    """Retira uma permissão isolada."""
    return user_crud.toggle_role_permission(db, actor, role_id, permission_id, False)


# -----------------------------
# ✍️ Membros
# -----------------------------
@router.patch("/members/{user_id}/role", response_model=users_schemas.MemberSchema)
async def set_member_role(
    user_id: int,
    data: users_schemas.MemberRoleUpdateSchema,
    actor: user_model.User = Depends(_CAN_MANAGE_MEMBERS),
    db: Session = Depends(database.get_db),
):
    return user_crud.set_member_role(db, actor, user_id, data.role_id)


@router.patch("/members/{user_id}/status", response_model=users_schemas.MemberSchema)
async def set_member_status(
    user_id: int,
    data: users_schemas.MemberStatusUpdateSchema,
    actor: user_model.User = Depends(_CAN_MANAGE_MEMBERS),
    db: Session = Depends(database.get_db),
):
    return user_crud.set_member_status(db, actor, user_id, data.is_active)
