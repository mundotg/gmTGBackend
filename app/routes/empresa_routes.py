"""
🏢 Gestão de empresas (organizações).

- `GET /empresas`     → lista paginada de empresas com filtro de busca e status.
  - Se for super admin: lista todas as empresas do sistema.
  - Caso contrário: lista apenas a empresa do utilizador autenticado.
- `POST /empresas`    → cria nova empresa (apenas super admin / company:create).
- `PUT /empresas/{id}`→ atualiza empresa específica.
- `GET /empresas/me`  → dados da empresa do utilizador + `can_manage`.
- `PUT /empresas/me`  → atualiza os dados da empresa do utilizador.
"""

from __future__ import annotations

from math import ceil
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from app.auth import hash_password
from app.config.cache_manager import CACHE_PREFIX, cache_result, clear_cache
from app.cruds import user_crud
from app.cruds.connection_cruds import (
    add_connection_empresa,
    list_empresa_connections,
    remove_connection_empresa,
)
from app.cruds.user_crud import (
    _audit,
    _default_signup_plan,
    _member_out,
    get_or_create_cargo,
)
from app.database import get_db
from app.models import user_model
from app.schemas import users_schemas
from app.schemas.connetion_schema import EmpresaConnectionOut
from app.ultils.logger import log_message
from app.ultils.permissions import (
    SUPER_PERMISSION,
    get_current_user,
    is_superadmin,
    require_permission,
    user_has_permission,
)

router = APIRouter(prefix="/empresas", tags=["Empresa"])

# Quem pode escrever nos dados da empresa.
_WRITE_PERMS = ("company:update", "company:settings")
_CAN_WRITE = require_permission(*_WRITE_PERMS)


def invalidate_empresas_cache():
    """Invalida o cache de empresas paginadas e afins."""
    try:
        clear_cache(f"{CACHE_PREFIX}get_empresas_paginadas_cached:*")
        clear_cache(f"{CACHE_PREFIX}get_shareable_empresas_cached:*")
    except Exception as e:
        log_message(f"Erro ao invalidar cache de empresas: {e}", "warning")


@cache_result(ttl=180, user_id="empresas_paginadas")
def get_empresas_paginadas_cached(
    user_id: int,
    is_superadmin: bool,
    actor_empresa_id: Optional[int],
    can_write: bool,
    busca: Optional[str],
    status_filtro: str,
    page: int,
    page_size: int,
    *,
    db: Session,
) -> dict:
    query = db.query(user_model.Empresa)

    if status_filtro == "minha" and actor_empresa_id:
        query = query.filter(user_model.Empresa.id == actor_empresa_id)

    if busca and busca.strip():
        termo = f"%{busca.strip()}%"
        query = query.filter(
            or_(
                user_model.Empresa.nome.ilike(termo),
                user_model.Empresa.nif.ilike(termo),
                user_model.Empresa.endereco.ilike(termo),
            )
        )

    if status_filtro == "ativas":
        query = query.filter(user_model.Empresa.is_active == True)
    elif status_filtro == "inativas":
        query = query.filter(user_model.Empresa.is_active == False)

    total = query.count()
    total_pages = ceil(total / page_size) if total > 0 else 0

    # Contagem de membros por empresa
    user_counts = dict(
        db.query(user_model.User.empresa_id, func.count(user_model.User.id))
        .filter(user_model.User.empresa_id.isnot(None))
        .group_by(user_model.User.empresa_id)
        .all()
    )

    empresas = (
        query.order_by(user_model.Empresa.nome.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    items = []
    for emp in empresas:
        pode_gerir = is_superadmin or (emp.id == actor_empresa_id and can_write)
        items.append({
            "id": emp.id,
            "nome": emp.nome,
            "company": emp.nome,
            "tamanho": emp.tamanho,
            "companySize": emp.tamanho,
            "nif": emp.nif,
            "endereco": emp.endereco,
            "is_active": emp.is_active if emp.is_active is not None else True,
            "users_count": user_counts.get(emp.id, 0),
            "criado_em": emp.criado_em.isoformat() if emp.criado_em else None,
            "can_manage": pode_gerir,
        })

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "is_admin": is_superadmin,
    }


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


@router.get("", response_model=users_schemas.EmpresaPaginadaSchema)
async def listar_empresas(
    busca: Optional[str] = Query(None, description="Pesquisa por nome, NIF ou endereço"),
    status_filtro: Optional[str] = Query("todas", alias="status", description="Filtro de status: todas, ativas, inativas"),
    page: int = Query(1, ge=1, description="Número da página"),
    page_size: int = Query(10, ge=1, le=100, description="Registos por página"),
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Lista empresas com paginação e pesquisa (com cache no backend).
    - Super Admin vê todas as empresas da aplicação.
    - Outros utilizadores veem apenas a(s) empresa(s) a que pertencem.
    """
    admin = is_superadmin(actor)
    can_write = user_has_permission(actor.permissions, _WRITE_PERMS)

    return get_empresas_paginadas_cached(
        user_id=actor.id,
        is_superadmin=admin,
        actor_empresa_id=actor.empresa_id,
        can_write=can_write,
        busca=busca.strip() if busca and busca.strip() else None,
        status_filtro=status_filtro or "todas",
        page=page,
        page_size=page_size,
        db=db,
    )


@router.post("", response_model=users_schemas.EmpresaComPermissoesSchema, status_code=status.HTTP_201_CREATED)
async def criar_empresa(
    data: users_schemas.EmpresaCreateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Cria uma nova organização (apenas administradores)."""
    admin = is_superadmin(actor)
    if not (admin or user_has_permission(actor.permissions, ("company:create", "admin:*"))):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Apenas administradores podem criar novas organizações.",
        )

    nome = data.nome.strip()
    if db.query(user_model.Empresa).filter(user_model.Empresa.nome.ilike(nome)).first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Já existe uma empresa com o nome '{nome}'.",
        )
    if data.nif and db.query(user_model.Empresa).filter(user_model.Empresa.nif == data.nif.strip()).first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Já existe uma empresa com o NIF '{data.nif}'.",
        )

    nova = user_model.Empresa(
        nome=nome,
        tamanho=data.tamanho.strip() if data.tamanho else None,
        nif=data.nif.strip() if data.nif else None,
        endereco=data.endereco.strip() if data.endereco else None,
        is_active=True,
    )
    db.add(nova)
    db.commit()
    db.refresh(nova)

    _audit(db, actor, "create", "empresa", nova.id)
    db.commit()
    invalidate_empresas_cache()
    log_message(f"🏢 Nova empresa criada #{nova.id} ('{nova.nome}') por user #{actor.id}", "info")

    return users_schemas.EmpresaComPermissoesSchema(
        id=nova.id,
        nome=nova.nome,
        tamanho=nova.tamanho,
        nif=nova.nif,
        endereco=nova.endereco,
        is_active=nova.is_active,
        users_count=0,
        criado_em=nova.criado_em,
        can_manage=True,
    )


@router.get("/me", response_model=users_schemas.EmpresaComPermissoesSchema)
async def obter_minha_empresa(
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    empresa = _empresa_do_ator(actor, db)
    can_manage = is_superadmin(actor) or user_has_permission(actor.permissions, _WRITE_PERMS)

    count = (
        db.query(func.count(user_model.User.id))
        .filter(user_model.User.empresa_id == empresa.id)
        .scalar()
        or 0
    )

    return users_schemas.EmpresaComPermissoesSchema(
        id=empresa.id,
        nome=empresa.nome,
        tamanho=empresa.tamanho,
        nif=empresa.nif,
        endereco=empresa.endereco,
        is_active=empresa.is_active if empresa.is_active is not None else True,
        users_count=count,
        criado_em=empresa.criado_em,
        can_manage=can_manage,
    )


@router.put("/me", response_model=users_schemas.EmpresaComPermissoesSchema)
async def atualizar_minha_empresa(
    data: users_schemas.EmpresaUpdateSchema,
    actor: user_model.User = Depends(_CAN_WRITE),
    db: Session = Depends(get_db),
):
    empresa = _empresa_do_ator(actor, db)

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
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Já existe uma empresa com esse nome ou NIF.",
        )

    _audit(db, actor, "update", "empresa", empresa.id)
    db.commit()
    invalidate_empresas_cache()
    log_message(
        f"🏢 Empresa #{empresa.id} atualizada por user #{actor.id}: {list(alteracoes)}",
        "info",
    )

    can_manage = is_superadmin(actor) or user_has_permission(actor.permissions, _WRITE_PERMS)
    count = (
        db.query(func.count(user_model.User.id))
        .filter(user_model.User.empresa_id == empresa.id)
        .scalar()
        or 0
    )

    return users_schemas.EmpresaComPermissoesSchema(
        id=empresa.id,
        nome=empresa.nome,
        tamanho=empresa.tamanho,
        nif=empresa.nif,
        endereco=empresa.endereco,
        is_active=empresa.is_active if empresa.is_active is not None else True,
        users_count=count,
        criado_em=empresa.criado_em,
        can_manage=can_manage,
    )


@router.put("/{empresa_id}", response_model=users_schemas.EmpresaComPermissoesSchema)
async def atualizar_empresa_por_id(
    empresa_id: int,
    data: users_schemas.EmpresaUpdateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    admin = is_superadmin(actor)
    can_write = user_has_permission(actor.permissions, _WRITE_PERMS)

    if not (admin or (actor.empresa_id == empresa_id and can_write)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para alterar os dados desta empresa.",
        )

    empresa = db.query(user_model.Empresa).filter(user_model.Empresa.id == empresa_id).first()
    if not empresa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Empresa não encontrada.",
        )

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
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Já existe uma empresa com esse nome ou NIF.",
        )

    _audit(db, actor, "update", "empresa", empresa.id)
    db.commit()
    invalidate_empresas_cache()
    log_message(
        f"🏢 Empresa #{empresa.id} atualizada por user #{actor.id}: {list(alteracoes)}",
        "info",
    )

    count = (
        db.query(func.count(user_model.User.id))
        .filter(user_model.User.empresa_id == empresa.id)
        .scalar()
        or 0
    )

    return users_schemas.EmpresaComPermissoesSchema(
        id=empresa.id,
        nome=empresa.nome,
        tamanho=empresa.tamanho,
        nif=empresa.nif,
        endereco=empresa.endereco,
        is_active=empresa.is_active if empresa.is_active is not None else True,
        users_count=count,
        criado_em=empresa.criado_em,
        can_manage=True,
    )


# =============================
# 👥 Membros da Empresa
# =============================
_MEMBER_MANAGE_PERMS = ("user:manage", "team:manage", "company:settings", "team:create")
# Cada ação da empresa tem a sua permissão (que um cargo pode dar — ver
# app/ultils/company_permissions.py); a gestão geral continua a valer para todas.
_ADD_MEMBER_PERMS = _MEMBER_MANAGE_PERMS + ("company:invite",)
_REMOVE_MEMBER_PERMS = _MEMBER_MANAGE_PERMS + ("company:remove_member",)
_ASSIGN_CARGO_PERMS = _MEMBER_MANAGE_PERMS + ("company:assign_cargo",)
_MEMBER_READ_PERMS = ("settings:team", "team:read", "company:read", "settings:company", "user:read")


@router.get("/cargos", response_model=list[users_schemas.CargoSchema])
async def listar_cargos(
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Lista todos os cargos registados para seleção."""
    return (
        db.query(user_model.Cargo)
        .filter(user_model.Cargo.is_active == True)
        .order_by(user_model.Cargo.nome.asc())
        .all()
    )


@router.get("/{empresa_id}/members", response_model=users_schemas.MemberPaginadoSchema)
async def listar_membros_empresa(
    empresa_id: int,
    busca: Optional[str] = Query(None, description="Pesquisa por nome, email ou telefone"),
    status_filtro: Optional[str] = Query("todos", alias="status", description="Filtro de status: todos, ativos, inativos"),
    role_id: Optional[int] = Query(None, description="Filtro por ID da função/role"),
    page: int = Query(1, ge=1, description="Número da página"),
    page_size: int = Query(10, ge=1, le=100, description="Registos por página"),
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Lista membros de uma organização com paginação e filtros.
    - Super Admin pode ver os membros de qualquer organização.
    - Outros utilizadores veem apenas se pertencerem a essa empresa e tiverem permissão.
    """
    admin = is_superadmin(actor)
    pertence = actor.empresa_id == empresa_id
    pode_ler = user_has_permission(actor.permissions, _MEMBER_READ_PERMS)

    if not (admin or (pertence and pode_ler) or pertence):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para visualizar os membros desta organização.",
        )

    empresa = db.query(user_model.Empresa).filter(user_model.Empresa.id == empresa_id).first()
    if not empresa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organização não encontrada.",
        )

    query = (
        db.query(user_model.User)
        .options(
            joinedload(user_model.User.role),
            joinedload(user_model.User.empresa_role),
            joinedload(user_model.User.cargo),
        )
        .filter(user_model.User.empresa_id == empresa_id)
    )

    if busca and busca.strip():
        termo = f"%{busca.strip()}%"
        query = query.filter(
            or_(
                user_model.User.nome.ilike(termo),
                user_model.User.apelido.ilike(termo),
                user_model.User.email.ilike(termo),
                user_model.User.telefone.ilike(termo),
            )
        )

    if status_filtro in ("ativos", "ativas"):
        query = query.filter(user_model.User.is_active == True)
    elif status_filtro in ("inativos", "inativas"):
        query = query.filter(user_model.User.is_active == False)

    if role_id is not None:
        query = query.filter(
            or_(user_model.User.role_id == role_id, user_model.User.empresa_role_id == role_id)
        )

    total = query.count()
    total_pages = ceil(total / page_size) if total > 0 else 0

    membros = (
        query.order_by(user_model.User.nome.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    def pode(perms: tuple[str, ...]) -> bool:
        return admin or (pertence and user_has_permission(actor.permissions, perms))

    return users_schemas.MemberPaginadoSchema(
        items=[_member_out(m) for m in membros],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        can_manage_members=pode(_MEMBER_MANAGE_PERMS),
        can_add_members=pode(_ADD_MEMBER_PERMS),
        can_remove_members=pode(_REMOVE_MEMBER_PERMS),
        can_change_cargo=pode(_ASSIGN_CARGO_PERMS),
    )


@router.get("/{empresa_id}/candidate-users", response_model=users_schemas.CandidateUserPaginadoSchema)
async def buscar_usuarios_candidatos(
    empresa_id: int,
    busca: Optional[str] = Query(None, description="Pesquisa por nome, email ou telefone"),
    page: int = Query(1, ge=1, description="Número da página"),
    page_size: int = Query(5, ge=1, le=50, description="Registos por página"),
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Busca utilizadores existentes elegíveis para serem adicionados à organização.
    - Quem já é membro desta empresa NÃO PODE estar na lista.
    - Super Admin pode ver utilizadores sem empresa ou de outras empresas.
    - Gestor de empresa vê apenas utilizadores sem empresa vinculada.
    - Suporta paginação e busca por nome, apelido, e-mail ou telefone.
    """
    admin = is_superadmin(actor)
    pertence = actor.empresa_id == empresa_id
    can_manage = admin or (pertence and user_has_permission(actor.permissions, _ADD_MEMBER_PERMS))

    if not can_manage:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para pesquisar utilizadores candidatos.",
        )

    # Subquery explícita dos membros atuais desta empresa para exclusão absoluta
    membros_subquery = (
        db.query(user_model.User.id)
        .filter(user_model.User.empresa_id == empresa_id)
        .subquery()
    )

    query = (
        db.query(user_model.User)
        .options(
            joinedload(user_model.User.empresa),
            joinedload(user_model.User.cargo),
            joinedload(user_model.User.role),
        )
        .filter(user_model.User.id.not_in(membros_subquery))
        .filter(
            or_(
                user_model.User.empresa_id.is_(None),
                user_model.User.empresa_id != empresa_id,
            )
        )
    )

    # Se não for super admin, só pode vincular quem está sem empresa
    if not admin:
        query = query.filter(user_model.User.empresa_id.is_(None))

    if busca and busca.strip():
        termo = f"%{busca.strip()}%"
        query = query.filter(
            or_(
                user_model.User.nome.ilike(termo),
                user_model.User.apelido.ilike(termo),
                user_model.User.email.ilike(termo),
                user_model.User.telefone.ilike(termo),
            )
        )

    total = query.count()
    total_pages = ceil(total / page_size) if total > 0 else 0

    candidatos = (
        query.order_by(user_model.User.nome.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    items = [
        users_schemas.CandidateUserSchema(
            id=u.id,
            nome=u.nome,
            apelido=u.apelido,
            email=u.email,
            telefone=u.telefone,
            is_active=bool(u.is_active),
            empresa_id=u.empresa_id,
            empresa_nome=u.empresa.nome if u.empresa else None,
            cargo_nome=u.cargo.nome if u.cargo else None,
            role_name=u.role.name if u.role else None,
        )
        for u in candidatos
    ]

    return users_schemas.CandidateUserPaginadoSchema(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.post(
    "/{empresa_id}/members",
    response_model=users_schemas.MemberSchema,
    status_code=status.HTTP_201_CREATED,
)
async def adicionar_membro_empresa(
    empresa_id: int,
    data: users_schemas.MemberCreateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Adiciona um membro à empresa.
    - Permite associar um utilizador existente por ID ou e-mail.
    - Se for novo utilizador, cria conta com senha fornecida ou padrão.
    """
    admin = is_superadmin(actor)
    pertence = actor.empresa_id == empresa_id
    can_manage = admin or (pertence and user_has_permission(actor.permissions, _ADD_MEMBER_PERMS))

    if not can_manage:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para adicionar membros a esta organização.",
        )

    empresa = db.query(user_model.Empresa).filter(user_model.Empresa.id == empresa_id).first()
    if not empresa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organização não encontrada.",
        )

    # Tipo de utilizador (role_id, função global) e cargo (empresa_role_id,
    # função desta empresa). Por compatibilidade, um cargo da empresa enviado
    # em role_id passa para empresa_role_id.
    empresa_role_id = data.empresa_role_id
    if data.role_id:
        role = db.query(user_model.Role).filter(user_model.Role.id == data.role_id).first()
        if not role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="A função especificada não foi encontrada.",
            )
        if role.empresa_id is not None:
            if role.empresa_id != empresa_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Esse cargo pertence a outra empresa.",
                )
            empresa_role_id = empresa_role_id or role.id
            data = data.model_copy(update={"role_id": None})
        elif not admin and any(p.name == SUPER_PERMISSION for p in role.permissions or []):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Apenas super administradores podem atribuir a função com '{SUPER_PERMISSION}'.",
            )
    if empresa_role_id is not None:
        cargo_role = db.query(user_model.Role).filter(user_model.Role.id == empresa_role_id).first()
        if not cargo_role or cargo_role.empresa_id != empresa_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="O cargo tem de ser um dos cargos desta empresa.",
            )

    # Cargo
    cargo_id = None
    if data.cargo and data.cargo.strip():
        cargo_obj = get_or_create_cargo(
            db, users_schemas.CargoSchema(position=data.cargo.strip())
        )
        cargo_id = cargo_obj.id if cargo_obj else None

    # Caso 1: Usuário selecionado por ID existente
    if data.user_id:
        existente = db.query(user_model.User).filter(user_model.User.id == data.user_id).first()
        if not existente:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Utilizador não encontrado.",
            )
        if existente.empresa_id == empresa_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Este utilizador já é membro desta organização.",
            )
        if existente.empresa_id is not None and not admin:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Este utilizador já pertence a outra organização.",
            )

        existente.empresa_id = empresa_id
        # Um cargo de outra empresa não acompanha o utilizador.
        existente.empresa_role_id = empresa_role_id
        if data.role_id is not None:
            existente.role_id = data.role_id
        if cargo_id is not None:
            existente.cargo_id = cargo_id
        if data.telefone:
            existente.telefone = data.telefone.strip()
        existente.is_active = True

        db.commit()
        db.refresh(existente)
        _audit(db, actor, "add_member", "user", existente.id)
        db.commit()
        log_message(
            f"👥 Utilizador existente #{existente.id} associado à empresa #{empresa_id} por #{actor.id}",
            "info",
        )
        return _member_out(existente)

    # Caso 2: Por e-mail (existente ou novo)
    if not data.email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Indique o utilizador existente ou o e-mail para registar um novo membro.",
        )

    email_norm = data.email.strip().lower()
    existente = (
        db.query(user_model.User)
        .filter(func.lower(user_model.User.email) == email_norm)
        .first()
    )

    if existente:
        if existente.empresa_id == empresa_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Este utilizador já é membro desta organização.",
            )
        if existente.empresa_id is not None and not admin:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Este utilizador já pertence a outra organização.",
            )

        existente.empresa_id = empresa_id
        # Um cargo de outra empresa não acompanha o utilizador.
        existente.empresa_role_id = empresa_role_id
        if data.role_id is not None:
            existente.role_id = data.role_id
        if cargo_id is not None:
            existente.cargo_id = cargo_id
        if data.telefone:
            existente.telefone = data.telefone.strip()
        existente.is_active = True

        db.commit()
        db.refresh(existente)
        _audit(db, actor, "add_member", "user", existente.id)
        db.commit()
        invalidate_empresas_cache()
        log_message(
            f"👥 Utilizador existente #{existente.id} associado à empresa #{empresa_id} por #{actor.id}",
            "info",
        )
        return _member_out(existente)

    # Caso 3: Novo utilizador
    if not data.nome or len(data.nome.strip()) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="O nome completo é obrigatório para cadastrar um novo utilizador.",
        )

    senha_raw = data.senha or "Musta@123456"
    hashed_pw = hash_password(senha_raw)
    plan = _default_signup_plan(db)

    novo_user = user_model.User(
        nome=data.nome.strip(),
        apelido=(data.apelido or "").strip() or None,
        email=email_norm,
        telefone=(data.telefone or "").strip() or None,
        hashed_password=hashed_pw,
        is_active=True,
        email_verified=True,
        concorda_termos=True,
        plan_id=plan.id,
        empresa_id=empresa_id,
        cargo_id=cargo_id,
        role_id=data.role_id,
        empresa_role_id=empresa_role_id,
    )
    db.add(novo_user)
    db.commit()
    db.refresh(novo_user)

    _audit(db, actor, "create_member", "user", novo_user.id)
    db.commit()
    invalidate_empresas_cache()
    log_message(
        f"👥 Novo utilizador #{novo_user.id} ({novo_user.email}) criado e associado à empresa #{empresa_id} por #{actor.id}",
        "success",
    )
    return _member_out(novo_user)


@router.delete("/{empresa_id}/members/{user_id}")
async def remover_membro_empresa(
    empresa_id: int,
    user_id: int,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Desvincula um membro da organização."""
    admin = is_superadmin(actor)
    pertence = actor.empresa_id == empresa_id
    can_manage = admin or (pertence and user_has_permission(actor.permissions, _REMOVE_MEMBER_PERMS))

    if not can_manage:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para remover membros desta organização.",
        )

    alvo = (
        db.query(user_model.User)
        .filter(user_model.User.id == user_id, user_model.User.empresa_id == empresa_id)
        .first()
    )
    if not alvo:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Membro não encontrado nesta organização.",
        )

    if alvo.id == actor.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Não pode remover-se a si próprio da sua organização.",
        )

    if is_superadmin(alvo) and not admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para desvincular um super administrador.",
        )

    alvo.empresa_id = None
    alvo.empresa_role_id = None  # o cargo era desta empresa
    db.commit()

    _audit(db, actor, "remove_member", "user", alvo.id)
    db.commit()
    invalidate_empresas_cache()
    log_message(
        f"👥 Utilizador #{alvo.id} desvinculado da empresa #{empresa_id} por #{actor.id}",
        "info",
    )
    return {"detail": "Membro desvinculado com sucesso."}


@router.patch("/{empresa_id}/members/{user_id}/role", response_model=users_schemas.MemberSchema)
async def alterar_funcao_membro(
    empresa_id: int,
    user_id: int,
    data: users_schemas.MemberRoleUpdateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Altera a função RBAC de um membro da organização."""
    admin = is_superadmin(actor)
    pertence = actor.empresa_id == empresa_id
    can_manage = admin or (pertence and user_has_permission(actor.permissions, _MEMBER_MANAGE_PERMS))

    if not can_manage:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para alterar a função de membros desta organização.",
        )
    membro = db.query(user_model.User.empresa_id).filter(user_model.User.id == user_id).first()
    if not membro or membro.empresa_id != empresa_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Membro não encontrado nesta empresa.",
        )
    if data.role_id is not None:
        _assert_role_usable_in_empresa(db, empresa_id, data.role_id)
    return user_crud.set_member_role(db, actor, user_id, data.role_id)


@router.patch("/{empresa_id}/members/{user_id}/cargo", response_model=users_schemas.MemberSchema)
async def alterar_cargo_membro(
    empresa_id: int,
    user_id: int,
    data: users_schemas.MemberRoleUpdateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Define o cargo (função da empresa) de um membro. `role_id = null` retira-o."""
    admin = is_superadmin(actor)
    pertence = actor.empresa_id == empresa_id
    if not (admin or (pertence and user_has_permission(actor.permissions, _ASSIGN_CARGO_PERMS))):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para alterar o cargo de membros desta organização.",
        )
    return user_crud.set_member_cargo(db, actor, empresa_id, user_id, data.role_id)


@router.patch("/{empresa_id}/members/{user_id}/status", response_model=users_schemas.MemberSchema)
async def alterar_status_membro(
    empresa_id: int,
    user_id: int,
    data: users_schemas.MemberStatusUpdateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Ativa ou desativa um membro da organização."""
    admin = is_superadmin(actor)
    pertence = actor.empresa_id == empresa_id
    can_manage = admin or (pertence and user_has_permission(actor.permissions, _MEMBER_MANAGE_PERMS))

    if not can_manage:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para alterar o estado de membros desta organização.",
        )
    return user_crud.set_member_status(db, actor, user_id, data.is_active)


# ---------------------------------------------------------
# 🎭 Gestão de Funções (Roles) da Empresa
# ---------------------------------------------------------
_ROLE_MANAGE_PERMS = ("role:manage", "company:settings", "company:update")


def _assert_empresa_role_access(actor: user_model.User, empresa_id: int, write: bool = False) -> None:
    if is_superadmin(actor):
        return
    if actor.empresa_id != empresa_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para aceder às funções de outra organização.",
        )
    if write and not user_has_permission(actor.permissions, _ROLE_MANAGE_PERMS):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para gerir funções desta organização.",
        )


def _get_empresa_own_role(db: Session, empresa_id: int, role_id: int) -> user_model.Role:
    """
    Função que pode ser ALTERADA a partir do ecrã desta empresa: só as que lhe
    pertencem (`role.empresa_id == empresa_id`).

    Vale também para o super admin. As funções globais (`empresa_id` nulo:
    admin, manager, developer, user) são partilhadas por todas as empresas;
    alterá-las aqui mudaria o acesso de todas. Gerem-se na administração do
    sistema (/users/roles) — para as ajustar a uma empresa, duplica-se.
    """
    role = user_crud.get_role_or_404(db, role_id)
    if role.empresa_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"'{role.name}' é um cargo global do sistema, partilhado por todas as "
                "empresas, e não pode ser alterado a partir de uma empresa. Duplique-o "
                "para criar uma versão só desta empresa."
            ),
        )
    if role.empresa_id != empresa_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cargo não encontrado nesta empresa.",
        )
    return role


def _assert_role_usable_in_empresa(db: Session, empresa_id: int, role_id: int) -> None:
    """Função que pode ser ATRIBUÍDA nesta empresa: global ou da própria empresa."""
    role = user_crud.get_role_or_404(db, role_id)
    if role.empresa_id is not None and role.empresa_id != empresa_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Esse cargo pertence a outra empresa.",
        )


@router.get("/{empresa_id}/roles", response_model=list[users_schemas.RoleSchema])
async def listar_funcoes_empresa(
    empresa_id: int,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Lista as funções RBAC disponíveis para a empresa (funções próprias + funções do sistema)."""
    _assert_empresa_role_access(actor, empresa_id, write=False)
    return user_crud.list_roles(db, empresa_id=empresa_id)


@router.post("/{empresa_id}/roles", response_model=users_schemas.RoleSchema, status_code=status.HTTP_201_CREATED)
async def criar_funcao_empresa(
    empresa_id: int,
    data: users_schemas.RoleCreateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Cria uma nova função RBAC isolada na empresa."""
    _assert_empresa_role_access(actor, empresa_id, write=True)
    # A empresa vem sempre do URL: um `empresa_id` diferente (ou nulo) no corpo
    # criaria a função noutra empresa — ou global.
    data = data.model_copy(update={"empresa_id": empresa_id})
    return user_crud.create_role(db, actor, data, empresa_id=empresa_id)


@router.patch("/{empresa_id}/roles/{role_id}", response_model=users_schemas.RoleSchema)
async def atualizar_funcao_empresa(
    empresa_id: int,
    role_id: int,
    data: users_schemas.RoleUpdateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Atualiza metadados de uma função da empresa."""
    _assert_empresa_role_access(actor, empresa_id, write=True)
    _get_empresa_own_role(db, empresa_id, role_id)
    return user_crud.update_role(db, actor, role_id, data)


@router.put("/{empresa_id}/roles/{role_id}/permissions", response_model=users_schemas.RoleSchema)
async def atualizar_permissoes_funcao_empresa(
    empresa_id: int,
    role_id: int,
    data: users_schemas.RolePermissionsUpdateSchema,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Atualiza a lista completa de permissões de uma função da empresa."""
    _assert_empresa_role_access(actor, empresa_id, write=True)
    _get_empresa_own_role(db, empresa_id, role_id)
    return user_crud.set_role_permissions(db, actor, role_id, data.permission_ids)


@router.delete("/{empresa_id}/roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remover_funcao_empresa(
    empresa_id: int,
    role_id: int,
    reassign_to_id: Optional[int] = Query(None, description="ID da função para onde transferir os membros"),
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Remove uma função da empresa."""
    _assert_empresa_role_access(actor, empresa_id, write=True)
    _get_empresa_own_role(db, empresa_id, role_id)
    if reassign_to_id is not None:
        _assert_role_usable_in_empresa(db, empresa_id, reassign_to_id)
    user_crud.delete_role(db, actor, role_id, reassign_to_id)


# =========================================================
# 🏢🔗🔌 Conexões da Empresa (Relação N:N)
# =========================================================

@router.get("/{empresa_id}/connections", response_model=list[EmpresaConnectionOut])
async def listar_conexoes_da_empresa(
    empresa_id: int,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Lista as conexões de banco de dados vinculadas à empresa."""
    return list_empresa_connections(db, empresa_id, actor)


@router.post("/{empresa_id}/connections/{connection_id}", status_code=status.HTTP_201_CREATED)
async def vincular_conexao_a_empresa(
    empresa_id: int,
    connection_id: int,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Vincula uma conexão de banco de dados a esta empresa (N:N)."""
    _assert_empresa_role_access(actor, empresa_id, write=True)
    add_connection_empresa(db, connection_id, empresa_id, actor)
    return {"message": "Conexão vinculada à empresa com sucesso."}


@router.delete("/{empresa_id}/connections/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def desvincular_conexao_da_empresa(
    empresa_id: int,
    connection_id: int,
    actor: user_model.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Desvincula uma conexão de banco de dados desta empresa (N:N)."""
    _assert_empresa_role_access(actor, empresa_id, write=True)
    remove_connection_empresa(db, connection_id, empresa_id, actor)
    return Response(status_code=status.HTTP_204_NO_CONTENT)



