"""
🏢 Gestão da empresa (organização) do utilizador autenticado.

- `GET /empresas/me`  → dados da empresa do utilizador + `can_manage`
  (para a UI decidir se mostra os campos como editáveis). Qualquer membro
  autenticado que pertença a uma empresa pode LER os seus dados.
- `PUT /empresas/me`  → atualiza os dados. Protegido: só quem tiver
  `company:update` ou `company:settings` (o super admin `admin:*` passa sempre).
  Assim **certifica-se quem pode alterar** — um "manager", que só tem
  `company:read`, vê mas não edita.

Não há modelo próprio em NoSQL para a empresa (é uma entidade relacional do
sistema, não dados do cliente), por isso mantém-se em SQL como o restante RBAC.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.cruds.user_crud import _audit
from app.database import get_db
from app.models import user_model
from app.schemas import users_schemas
from app.ultils.logger import log_message
from app.ultils.permissions import (
    get_current_user,
    require_permission,
    user_has_permission,
)

router = APIRouter(prefix="/empresas", tags=["Empresa"])

# Quem pode escrever nos dados da empresa.
_WRITE_PERMS = ("company:update", "company:settings")
_CAN_WRITE = require_permission(*_WRITE_PERMS)


def _empresa_do_ator(actor: user_model.User, db: Session) -> user_model.Empresa:
    """A empresa a que o utilizador pertence — ou 404 amigável."""
    if not actor.empresa_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="A sua conta ainda não está associada a nenhuma empresa.",
        )
    empresa = (
        db.query(user_model.Empresa)
        .filter(user_model.Empresa.id == actor.empresa_id)
        .first()
    )
    if not empresa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Empresa não encontrada.",
        )
    return empresa


@router.get("/me", response_model=users_schemas.EmpresaComPermissoesSchema)
async def obter_minha_empresa(
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    empresa = _empresa_do_ator(actor, db)
    can_manage = user_has_permission(actor.permissions, _WRITE_PERMS)
    return users_schemas.EmpresaComPermissoesSchema(
        id=empresa.id,
        nome=empresa.nome,
        tamanho=empresa.tamanho,
        nif=empresa.nif,
        endereco=empresa.endereco,
        can_manage=can_manage,
    )


@router.put("/me", response_model=users_schemas.EmpresaComPermissoesSchema)
async def atualizar_minha_empresa(
    data: users_schemas.EmpresaUpdateSchema,
    actor: user_model.User = Depends(_CAN_WRITE),
    db: Session = Depends(get_db),
):
    empresa = _empresa_do_ator(actor, db)

    # Atualização parcial: só os campos enviados (e não-nulos).
    alteracoes = data.model_dump(exclude_unset=True, exclude_none=True, by_alias=False)
    if not alteracoes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nada para atualizar.",
        )

    for campo, valor in alteracoes.items():
        setattr(empresa, campo, valor)

    try:
        db.commit()
        db.refresh(empresa)
    except IntegrityError:
        db.rollback()
        # nome e nif são únicos na base.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Já existe uma empresa com esse nome ou NIF.",
        )

    _audit(db, actor, "update", "empresa", empresa.id)
    db.commit()
    log_message(
        f"🏢 Empresa #{empresa.id} atualizada por user #{actor.id}: {list(alteracoes)}",
        "info",
    )

    can_manage = user_has_permission(actor.permissions, _WRITE_PERMS)
    return users_schemas.EmpresaComPermissoesSchema(
        id=empresa.id,
        nome=empresa.nome,
        tamanho=empresa.tamanho,
        nif=empresa.nif,
        endereco=empresa.endereco,
        can_manage=can_manage,
    )
