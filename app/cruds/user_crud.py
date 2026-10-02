from typing import Iterable, Optional, Sequence

from fastapi import HTTPException, status
from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload, load_only
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from app import auth
from app.models import user_model
from app.models.task_models import AuditLog
from app.schemas import users_schemas
from app.ultils.company_permissions import is_company_permission
from app.ultils.logger import log_message
from app.ultils.permissions import SUPER_PERMISSION, is_superadmin


# -----------------------------
# 🔍 Buscar usuário por e-mail
# -----------------------------
def get_user_by_email(db: Session, email: str):
    # Normaliza uma vez e evita chamadas desnecessárias
    normalized_email = (email or "").strip().lower()
    log_message(f"🔍 Buscando usuário com email: {normalized_email}", "info")

    # Melhor prática: guardar email normalizado em coluna (ex: email_lower) e indexar.
    # Como você está usando lower(email), isso pode impedir uso de índice dependendo do DB.
    # Mantive a lógica, mas deixei o código mais enxuto.
    return (
        db.query(user_model.User)
        .filter(func.lower(user_model.User.email) == normalized_email)
        .first()
    )


def get_user_by_id(db: Session, user_id: str):
    return (
        db.query(user_model.User)
        .options(
            load_only(
                user_model.User.id,
                user_model.User.email,
                user_model.User.nome,
            ),
            joinedload(user_model.User.plan),
            joinedload(user_model.User.storage_usage),
            joinedload(user_model.User.network_metrics),
            joinedload(user_model.User.request_usage),
        )
        .filter(user_model.User.id == user_id)
        .first()
    )


# -----------------------------
# 🧩 Criar (ou obter) empresa
# -----------------------------
def get_or_create_empresa(db: Session, empresa_data: users_schemas.EmpresaSchema):
    """Busca ou cria uma empresa caso ainda não exista."""
    if not empresa_data or not (empresa_data.nome or "").strip():
        return None

    nome = empresa_data.nome.strip()

    # Leve: busca só o que precisamos
    empresa = (
        db.query(user_model.Empresa)
        .options(load_only(user_model.Empresa.id, user_model.Empresa.nome))
        .filter(user_model.Empresa.nome == nome)
        .first()
    )

    if empresa:
        log_message(f"🏢 Empresa existente associada: {empresa.nome}", "info")
        return empresa

    # Cria — com proteção contra corrida (2 requests criando ao mesmo tempo)
    try:
        empresa = user_model.Empresa(
            nome=nome,
            tamanho=empresa_data.tamanho,
            nif=empresa_data.nif,
            endereco=empresa_data.endereco,
        )
        db.add(empresa)
        db.commit()
        db.refresh(empresa)
        log_message(f"🏢 Nova empresa criada: {empresa.nome}", "success")
        return empresa

    except IntegrityError:
        # Outro request criou primeiro → faz rollback e busca de novo
        db.rollback()
        empresa = (
            db.query(user_model.Empresa)
            .options(load_only(user_model.Empresa.id, user_model.Empresa.nome))
            .filter(user_model.Empresa.nome == nome)
            .first()
        )
        if empresa:
            log_message(
                f"🏢 Empresa criada por outra transação, usando existente: {empresa.nome}",
                "info",
            )
            return empresa
        raise

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao criar empresa '{nome}': {e}", "error")
        raise


# -----------------------------
# 🧩 Criar (ou obter) cargo
# -----------------------------
def get_or_create_cargo(db: Session, cargo_data: users_schemas.CargoSchema):
    """Busca ou cria um cargo caso ainda não exista."""
    if not cargo_data or not (cargo_data.nome or "").strip():
        return None

    nome = cargo_data.nome.strip()

    cargo = (
        db.query(user_model.Cargo)
        .options(load_only(user_model.Cargo.id, user_model.Cargo.nome))
        .filter(user_model.Cargo.nome == nome)
        .first()
    )

    if cargo:
        log_message(f"💼 Cargo existente associado: {cargo.nome}", "info")
        return cargo

    try:
        cargo = user_model.Cargo(
            nome=nome,
            descricao=cargo_data.descricao,
            nivel=cargo_data.nivel,
        )
        db.add(cargo)
        db.commit()
        db.refresh(cargo)
        log_message(f"💼 Novo cargo criado: {cargo.nome}", "success")
        return cargo

    except IntegrityError:
        db.rollback()
        cargo = (
            db.query(user_model.Cargo)
            .options(load_only(user_model.Cargo.id, user_model.Cargo.nome))
            .filter(user_model.Cargo.nome == nome)
            .first()
        )
        if cargo:
            log_message(
                f"💼 Cargo criado por outra transação, usando existente: {cargo.nome}",
                "info",
            )
            return cargo
        raise

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao criar cargo '{nome}': {e}", "error")
        raise


# Plano atribuído a quem se regista. `users.plan_id` é obrigatório (NOT NULL) e
# o registo não o preenchia: toda a inscrição falhava com IntegrityError — que
# o `except` abaixo reportava como "E-mail já está em uso".
DEFAULT_SIGNUP_PLAN = "free"


def _default_signup_plan(db: Session):
    """
    Plano por omissão para novas contas (por nome, não por id). Os planos são
    criados pelo seed no arranque; se ainda não existir, cria-se com os mesmos
    valores do seed para o registo não depender da ordem de arranque.
    """
    from app.models.clouds_models import Plan
    from app.seed_new import PLANOS_DATA

    plan = db.query(Plan).filter(Plan.name == DEFAULT_SIGNUP_PLAN).first()
    if plan:
        return plan
    _, max_storage, max_requests = next(p for p in PLANOS_DATA if p[0] == DEFAULT_SIGNUP_PLAN)
    plan = Plan(name=DEFAULT_SIGNUP_PLAN, max_storage_mb=max_storage, max_requests_per_day=max_requests)
    db.add(plan)
    db.flush()
    return plan


# -----------------------------
# 🧑 Criar novo usuário
# -----------------------------
def create_user(db: Session, user: users_schemas.UserCreate) -> user_model.User:
    email_norm = (user.email or "").strip().lower()

    try:
        log_message(f"🧑 Criando usuário: {email_norm}", "info")

        # 🚫 validação básica
        if not email_norm:
            raise ValueError("Email é obrigatório")

        if not user.senha:
            raise ValueError("Senha é obrigatória")

        # ⚡ evita query pesada desnecessária (opcional)
        existing_user = (
            db.query(user_model.User.id)
            .filter(user_model.User.email == email_norm)
            .first()
        )

        if existing_user:
            raise ValueError("E-mail já está em uso.")

        # 🔐 hash da senha (só depois da validação)
        hashed_pw = auth.hash_password(user.senha)

        # 🏢 relações (lazy creation)
        empresa = get_or_create_empresa(db, user.empresa)
        cargo = get_or_create_cargo(db, user.cargo)

        # 🎯 criar user
        db_user = user_model.User(
            nome=user.nome.strip(),
            apelido=(user.apelido or "").strip(),
            email=email_norm,
            telefone=(user.telefone or "").strip(),
            empresa_id=empresa.id if empresa else None,
            cargo_id=cargo.id if cargo else None,
            hashed_password=hashed_pw,
            concorda_termos=bool(user.concorda_termos),
            plan_id=_default_signup_plan(db).id,
        )

        db.add(db_user)

        # ⚡ flush antes do commit → pega ID sem fechar transação
        db.flush()

        log_message(f"📌 ID gerado: {db_user.id}", "debug")

        db.commit()
        db.refresh(db_user)

        log_message(f"✅ Usuário criado: {email_norm}", "success")
        return db_user

    except IntegrityError as e:
        db.rollback()
        detalhe = str(getattr(e, "orig", e))
        # Só um conflito no e-mail é "e-mail em uso" (corrida entre a
        # verificação acima e o insert). Qualquer outra restrição (NOT NULL,
        # FK, ...) é um erro nosso: regista-se a causa real em vez de a
        # esconder atrás de uma mensagem falsa ao utilizador.
        if "ix_users_email" in detalhe or "(email)" in detalhe:
            log_message(f"❌ Duplicate email: {email_norm}", "error")
            raise ValueError("E-mail já está em uso.")
        log_message(f"🔥 Registo falhou por restrição da base de dados: {detalhe}", "error")
        raise ValueError("Não foi possível criar a conta. Tente novamente ou contacte o suporte.")

    except Exception as e:
        db.rollback()
        log_message(f"🔥 Erro inesperado: {str(e)}", "error")
        raise


# -----------------------------
# ✏️ Atualizar nome do usuário
# -----------------------------
def update_user(db: Session, user_id: int, full_name: str):
    nome = (full_name or "").strip()
    log_message(f"✏️ Atualizando nome do usuário ID {user_id} para '{nome}'", "info")

    user = db.get(user_model.User, user_id)
    if not user:
        log_message(f"❌ Usuário ID {user_id} não encontrado para atualização", "error")
        return None

    # Evita commit se não mudou
    if user.nome != nome:
        user.nome = nome
        db.commit()
        db.refresh(user)
        log_message(f"✅ Usuário ID {user_id} atualizado com sucesso", "success")
    else:
        log_message(f"ℹ️ Usuário ID {user_id} já estava com o mesmo nome", "info")

    return user


# -----------------------------
# 📄 Listar todos os usuários
# -----------------------------
def get_users(db: Session):
    log_message("📄 Listando todos os usuários", "info")

    # Performance: se for listagem, normalmente não precisa de todos campos / relações
    return (
        db.query(user_model.User)
        .options(
            load_only(user_model.User.id, user_model.User.nome, user_model.User.email)
        )
        .order_by(user_model.User.id.desc())
        .all()
    )


# ==========================================================
# 🔐 RBAC — funções, permissões e membros
# ==========================================================

# Roles criadas pelo seed (`app/seed_new.py`): protegidas contra rename/delete,
# porque o resto do sistema assume que existem.
SYSTEM_ROLE_NAMES = {"admin", "manager", "developer", "user"}

# Rótulos amigáveis por prefixo de permissão, só para a UI agrupar.
PERMISSION_CATEGORIES = {
    "auth": "Autenticação",
    "user": "Utilizadores",
    "role": "Segurança (RBAC)",
    "permission": "Segurança (RBAC)",
    "company": "Empresa",
    "db_connection": "Conexões de BD",
    "query": "Consultas SQL",
    "table": "Tabelas",
    "project": "Projetos",
    "team": "Equipa",
    "integration": "Integrações",
    "settings": "Configurações",
    "audit": "Auditoria",
    "logs": "Logs",
    "backup": "Backups",
    "analytics": "Analytics",
    "admin": "Administração",
}


def _category_of(permission_name: str) -> str:
    prefix = (permission_name or "").split(":", 1)[0]
    return PERMISSION_CATEGORIES.get(prefix, "Outros")


def _permission_out(permission: user_model.Permission) -> users_schemas.PermissionSchema:
    return users_schemas.PermissionSchema(
        id=permission.id,
        name=permission.name,
        description=permission.description,
        category=_category_of(permission.name),
        company_scope=is_company_permission(permission.name),
    )


def _assert_company_role_permissions(
    empresa_id: Optional[int], permissions: Iterable[user_model.Permission]
) -> None:
    """Um cargo da empresa só pode ter permissões relacionadas com a empresa."""
    if empresa_id is None:
        return
    fora = sorted(p.name for p in permissions if not is_company_permission(p.name))
    if fora:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Um cargo da empresa só pode ter acessos relacionados com a empresa. "
                f"Não permitido: {', '.join(fora)}."
            ),
        )


def _role_is_locked(role: user_model.Role) -> bool:
    """
    A role que detém `admin:*` é a de super admin: fica totalmente bloqueada.
    Se fosse editável, bastava um clique para deixar o sistema sem ninguém que
    consiga voltar a conceder permissões.
    """
    return any(p.name == SUPER_PERMISSION for p in (role.permissions or []))


def _role_out(
    role: user_model.Role, users_count: int = 0
) -> users_schemas.RoleSchema:
    return users_schemas.RoleSchema(
        id=role.id,
        name=role.name,
        description=role.description,
        permissions=[_permission_out(p) for p in (role.permissions or [])],
        is_system=role.name in SYSTEM_ROLE_NAMES,
        is_locked=_role_is_locked(role),
        is_active=bool(role.is_active),
        users_count=users_count,
        empresa_id=role.empresa_id,
    )


def _member_out(user: user_model.User) -> users_schemas.MemberSchema:
    return users_schemas.MemberSchema(
        id=user.id,
        nome=user.nome,
        apelido=user.apelido,
        email=user.email,
        telefone=user.telefone,
        is_active=bool(user.is_active),
        role_id=user.role_id,
        role_name=user.role.name if user.role else None,
        empresa_role_id=user.empresa_role_id,
        empresa_role_name=user.empresa_role.name if user.empresa_role else None,
        cargo_id=user.cargo_id,
        cargo_nome=user.cargo.nome if user.cargo else None,
        empresa_id=user.empresa_id,
        is_superadmin=is_superadmin(user),
        created_at=user.created_at,
    )


def _audit(
    db: Session,
    actor: user_model.User,
    action: str,
    entity: str,
    entity_id: object,
) -> None:
    """Regista a ação no audit log. Nunca deve impedir a operação principal."""
    try:
        db.add(
            AuditLog(
                user_id=actor.id,
                action=action,
                entity=entity,
                entity_id=str(entity_id),
            )
        )
    except Exception as e:  # pragma: no cover - auditoria é best-effort
        log_message(f"⚠️ Falha ao registar audit log: {e}", "warning")


def _roles_query(db: Session):
    return db.query(user_model.Role).options(
        joinedload(user_model.Role.permissions)
    )


def get_role_or_404(db: Session, role_id: int) -> user_model.Role:
    role = _roles_query(db).filter(user_model.Role.id == role_id).first()
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Função não encontrada.",
        )
    return role


def _users_count_by_role(db: Session, empresa_id: Optional[int] = None) -> dict[int, int]:
    # Com empresa: só os membros dessa empresa. Sem isto, uma função global
    # mostrava no ecrã de uma empresa o total de TODAS as empresas.
    counts: dict[int, int] = {}
    # Uma função conta quem a tem como tipo de utilizador E quem a tem como cargo.
    for coluna in (user_model.User.role_id, user_model.User.empresa_role_id):
        query = db.query(coluna, func.count(user_model.User.id))
        if empresa_id is not None:
            query = query.filter(user_model.User.empresa_id == empresa_id)
        for role_id, total in query.group_by(coluna).all():
            if role_id is not None:
                counts[role_id] = counts.get(role_id, 0) + total
    return counts


def _assert_can_touch_superpermission(
    actor: user_model.User,
    permissions: Iterable[user_model.Permission],
) -> None:
    """
    Só um super admin pode conceder/retirar a permissão mestra `admin:*`.
    Sem isto, qualquer admin com `role:manage` podia promover-se a super admin.
    """
    if is_superadmin(actor):
        return

    if any(p.name == SUPER_PERMISSION for p in permissions):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Apenas um super admin pode atribuir ou remover '{SUPER_PERMISSION}'."
            ),
        )


def _assert_role_editable(role: user_model.Role) -> None:
    if _role_is_locked(role):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "A função de super admin está bloqueada: alterar as suas permissões "
                "podia deixar o sistema sem ninguém capaz de gerir acessos."
            ),
        )


def _assert_can_manage_role(actor: user_model.User, role: user_model.Role) -> None:
    if is_superadmin(actor):
        return
    if role.empresa_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Apenas super administradores podem modificar funções globais do sistema.",
        )
    if actor.empresa_id != role.empresa_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para gerir funções de outra organização.",
        )


def _get_permissions_or_404(
    db: Session, permission_ids: Sequence[int]
) -> list[user_model.Permission]:
    unique_ids = list(dict.fromkeys(permission_ids or []))
    if not unique_ids:
        return []

    permissions = (
        db.query(user_model.Permission)
        .filter(user_model.Permission.id.in_(unique_ids))
        .all()
    )

    if len(permissions) != len(unique_ids):
        encontrados = {p.id for p in permissions}
        em_falta = [pid for pid in unique_ids if pid not in encontrados]
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Permissões inexistentes: {em_falta}",
        )

    return permissions


# -----------------------------
# 📋 Leitura
# -----------------------------
def list_permissions(db: Session) -> list[users_schemas.PermissionSchema]:
    permissions = (
        db.query(user_model.Permission).order_by(user_model.Permission.name).all()
    )
    return [_permission_out(p) for p in permissions]


def list_roles(
    db: Session, empresa_id: Optional[int] = None
) -> list[users_schemas.RoleSchema]:
    counts = _users_count_by_role(db, empresa_id)
    query = _roles_query(db)
    if empresa_id is not None:
        query = query.filter(
            or_(
                user_model.Role.empresa_id == empresa_id,
                user_model.Role.empresa_id.is_(None),
            )
        )
    roles = query.order_by(user_model.Role.name).all()
    return [_role_out(role, counts.get(role.id, 0)) for role in roles]


def list_members(
    db: Session, actor: user_model.User
) -> list[users_schemas.MemberSchema]:
    """
    Um super admin vê todos os membros; os restantes veem apenas os da sua empresa.
    """
    query = db.query(user_model.User).options(
        joinedload(user_model.User.role).joinedload(user_model.Role.permissions)
    )

    if not is_superadmin(actor) and actor.empresa_id is not None:
        query = query.filter(user_model.User.empresa_id == actor.empresa_id)

    members = query.order_by(user_model.User.nome).all()
    return [_member_out(m) for m in members]


# -----------------------------
# ✍️ Escrita — funções
# -----------------------------
def create_role(
    db: Session,
    actor: user_model.User,
    data: users_schemas.RoleCreateSchema,
    empresa_id: Optional[int] = None,
) -> users_schemas.RoleSchema:
    target_empresa_id = data.empresa_id if data.empresa_id is not None else empresa_id
    if not is_superadmin(actor) and target_empresa_id is None:
        target_empresa_id = actor.empresa_id

    if not is_superadmin(actor) and target_empresa_id != actor.empresa_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Não tem permissão para criar funções para outra organização.",
        )

    dup_query = db.query(user_model.Role.id).filter(user_model.Role.name == data.name)
    if target_empresa_id is not None:
        dup_query = dup_query.filter(
            or_(
                user_model.Role.empresa_id == target_empresa_id,
                user_model.Role.empresa_id.is_(None),
            )
        )
    else:
        dup_query = dup_query.filter(user_model.Role.empresa_id.is_(None))

    existente = dup_query.first()
    if existente:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Já existe uma função chamada '{data.name}'.",
        )

    permissions = _get_permissions_or_404(db, data.permission_ids)
    _assert_can_touch_superpermission(actor, permissions)
    _assert_company_role_permissions(target_empresa_id, permissions)

    role = user_model.Role(
        name=data.name,
        description=data.description,
        empresa_id=target_empresa_id,
        permissions=permissions,
    )

    try:
        db.add(role)
        db.flush()
        _audit(db, actor, f"Criou a função '{role.name}'", "Role", role.id)
        db.commit()
        db.refresh(role)
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Já existe uma função chamada '{data.name}'.",
        )
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao criar função: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro ao criar a função.")

    log_message(f"🎭 Função '{role.name}' criada por {actor.email}", "success")
    return _role_out(role, 0)


def update_role(
    db: Session,
    actor: user_model.User,
    role_id: int,
    data: users_schemas.RoleUpdateSchema,
) -> users_schemas.RoleSchema:
    role = get_role_or_404(db, role_id)
    _assert_can_manage_role(actor, role)
    _assert_role_editable(role)

    if data.name is not None and data.name != role.name:
        if role.name in SYSTEM_ROLE_NAMES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A função de sistema '{role.name}' não pode ser renomeada.",
            )
        dup_query = db.query(user_model.Role.id).filter(
            user_model.Role.name == data.name,
            user_model.Role.id != role.id,
        )
        if role.empresa_id is not None:
            dup_query = dup_query.filter(
                or_(
                    user_model.Role.empresa_id == role.empresa_id,
                    user_model.Role.empresa_id.is_(None),
                )
            )
        else:
            dup_query = dup_query.filter(user_model.Role.empresa_id.is_(None))

        duplicada = dup_query.first()
        if duplicada:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Já existe uma função chamada '{data.name}'.",
            )
        role.name = data.name

    if data.description is not None:
        role.description = data.description

    if data.is_active is not None:
        if not data.is_active and _role_is_locked(role):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A função de super admin não pode ser desativada.",
            )
        role.is_active = data.is_active

    _audit(db, actor, f"Atualizou a função '{role.name}'", "Role", role.id)
    db.commit()
    db.refresh(role)

    counts = _users_count_by_role(db)
    return _role_out(role, counts.get(role.id, 0))


def delete_role(
    db: Session,
    actor: user_model.User,
    role_id: int,
    reassign_to_id: Optional[int] = None,
) -> None:
    role = get_role_or_404(db, role_id)
    _assert_can_manage_role(actor, role)

    if role.name in SYSTEM_ROLE_NAMES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"A função de sistema '{role.name}' não pode ser removida.",
        )

    membros = (
        db.query(user_model.User)
        .filter(
            or_(
                user_model.User.role_id == role.id,
                user_model.User.empresa_role_id == role.id,
            )
        )
        .all()
    )

    if membros:
        if reassign_to_id is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"A função '{role.name}' está atribuída a {len(membros)} "
                    "membro(s). Indique para que função devem ser transferidos."
                ),
            )

        destino = get_role_or_404(db, reassign_to_id)
        if destino.id == role.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A função de destino tem de ser diferente da que vai ser removida.",
            )
        # Os membros só podem passar para uma função global ou da mesma empresa.
        if destino.empresa_id is not None and destino.empresa_id != role.empresa_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A função de destino pertence a outra empresa.",
            )
        _assert_can_touch_superpermission(actor, destino.permissions or [])

        for membro in membros:
            # Mantém o lugar: quem a tinha como cargo fica com o novo cargo.
            if membro.empresa_role_id == role.id:
                membro.empresa_role_id = destino.id
            if membro.role_id == role.id:
                membro.role_id = destino.id

        log_message(
            f"🔁 {len(membros)} membro(s) transferidos de '{role.name}' "
            f"para '{destino.name}'",
            "info",
        )

    nome = role.name
    _audit(db, actor, f"Removeu a função '{nome}'", "Role", role.id)
    db.delete(role)
    db.commit()
    log_message(f"🗑️ Função '{nome}' removida por {actor.email}", "success")


def set_role_permissions(
    db: Session,
    actor: user_model.User,
    role_id: int,
    permission_ids: Sequence[int],
) -> users_schemas.RoleSchema:
    """Substitui integralmente as permissões da função (usado pelo botão Guardar)."""
    role = get_role_or_404(db, role_id)
    _assert_can_manage_role(actor, role)
    _assert_role_editable(role)

    novas = _get_permissions_or_404(db, permission_ids)
    _assert_company_role_permissions(role.empresa_id, novas)

    antigas_nomes = {p.name for p in (role.permissions or [])}
    novas_nomes = {p.name for p in novas}

    concedidas = novas_nomes - antigas_nomes
    retiradas = antigas_nomes - novas_nomes

    if not concedidas and not retiradas:
        counts = _users_count_by_role(db)
        return _role_out(role, counts.get(role.id, 0))

    # Só um super admin pode mexer na permissão mestra, em qualquer direção.
    alteradas = concedidas | retiradas
    if SUPER_PERMISSION in alteradas and not is_superadmin(actor):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Apenas um super admin pode atribuir ou remover '{SUPER_PERMISSION}'.",
        )

    role.permissions = novas

    _audit(
        db,
        actor,
        (
            f"Atualizou permissões de '{role.name}' "
            f"(+{len(concedidas)} / -{len(retiradas)})"
        ),
        "Role",
        role.id,
    )
    db.commit()
    db.refresh(role)

    log_message(
        f"🔐 Permissões de '{role.name}' atualizadas por {actor.email}: "
        f"concedidas={sorted(concedidas)} retiradas={sorted(retiradas)}",
        "success",
    )

    counts = _users_count_by_role(db)
    return _role_out(role, counts.get(role.id, 0))


def toggle_role_permission(
    db: Session,
    actor: user_model.User,
    role_id: int,
    permission_id: int,
    grant: bool,
) -> users_schemas.RoleSchema:
    """Concede (grant=True) ou retira (grant=False) uma permissão isolada."""
    role = get_role_or_404(db, role_id)
    _assert_can_manage_role(actor, role)
    _assert_role_editable(role)

    permission = db.get(user_model.Permission, permission_id)
    if not permission:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Permissão não encontrada.",
        )

    _assert_can_touch_superpermission(actor, [permission])
    if grant:
        _assert_company_role_permissions(role.empresa_id, [permission])

    atuais = list(role.permissions or [])
    ja_tem = any(p.id == permission.id for p in atuais)

    if grant and not ja_tem:
        role.permissions = atuais + [permission]
    elif not grant and ja_tem:
        role.permissions = [p for p in atuais if p.id != permission.id]
    else:
        counts = _users_count_by_role(db)
        return _role_out(role, counts.get(role.id, 0))

    verbo = "Concedeu" if grant else "Retirou"
    _audit(
        db,
        actor,
        f"{verbo} '{permission.name}' à função '{role.name}'",
        "Role",
        role.id,
    )
    db.commit()
    db.refresh(role)

    log_message(
        f"🔐 {verbo} '{permission.name}' em '{role.name}' (por {actor.email})",
        "success",
    )

    counts = _users_count_by_role(db)
    return _role_out(role, counts.get(role.id, 0))


# -----------------------------
# ✍️ Escrita — membros
# -----------------------------
def _get_member_or_404(
    db: Session, actor: user_model.User, user_id: int
) -> user_model.User:
    membro = (
        db.query(user_model.User)
        .options(joinedload(user_model.User.role).joinedload(user_model.Role.permissions))
        .filter(user_model.User.id == user_id)
        .first()
    )

    if not membro:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Membro não encontrado.",
        )

    # Isolamento entre empresas: quem não é super admin só gere a sua.
    if (
        not is_superadmin(actor)
        and actor.empresa_id is not None
        and membro.empresa_id != actor.empresa_id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Este membro pertence a outra empresa.",
        )

    if is_superadmin(membro) and not is_superadmin(actor):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Apenas um super admin pode alterar outro super admin.",
        )

    return membro


def _count_active_superadmins(db: Session, excluding_user_id: int) -> int:
    superadmins = (
        db.query(user_model.User)
        .join(user_model.Role, user_model.User.role_id == user_model.Role.id)
        .join(user_model.Role.permissions)
        .filter(
            user_model.Permission.name == SUPER_PERMISSION,
            user_model.User.is_active.is_(True),
            user_model.User.id != excluding_user_id,
        )
        .count()
    )
    return superadmins


def set_member_role(
    db: Session,
    actor: user_model.User,
    user_id: int,
    role_id: Optional[int],
) -> users_schemas.MemberSchema:
    membro = _get_member_or_404(db, actor, user_id)

    if membro.id == actor.id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Não pode alterar a sua própria função — pediria a outro admin "
                "para o fazer, ou perderia o acesso a esta página."
            ),
        )

    nova_role: Optional[user_model.Role] = None
    if role_id is not None:
        nova_role = get_role_or_404(db, role_id)
        if not nova_role.is_active:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A função '{nova_role.name}' está desativada.",
            )
        _assert_can_touch_superpermission(actor, nova_role.permissions or [])
        # O tipo de utilizador é sempre uma função global; as da empresa são
        # o cargo (set_member_cargo).
        if nova_role.empresa_id is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"'{nova_role.name}' é um cargo da empresa, não um tipo de utilizador. "
                    "Atribua-o no campo Cargo."
                ),
            )

    # Despromover o último super admin ativo deixaria o sistema sem gestor.
    perde_super = is_superadmin(membro) and not (
        nova_role and any(p.name == SUPER_PERMISSION for p in nova_role.permissions)
    )
    if perde_super and _count_active_superadmins(db, membro.id) == 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Este é o último super admin ativo. Promova outro membro antes "
                "de lhe retirar a função."
            ),
        )

    anterior = membro.role.name if membro.role else "sem função"
    membro.role_id = role_id

    _audit(
        db,
        actor,
        f"Alterou a função de {membro.email}: '{anterior}' → "
        f"'{nova_role.name if nova_role else 'sem função'}'",
        "User",
        membro.id,
    )
    db.commit()
    db.refresh(membro)

    log_message(
        f"👤 Função de {membro.email} alterada por {actor.email}",
        "success",
    )
    return _member_out(membro)


def set_member_cargo(
    db: Session,
    actor: user_model.User,
    empresa_id: int,
    user_id: int,
    role_id: Optional[int],
) -> users_schemas.MemberSchema:
    """Define (ou retira, com None) o cargo de um membro na sua empresa."""
    membro = _get_member_or_404(db, actor, user_id)
    if membro.empresa_id != empresa_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Membro não encontrado nesta empresa.",
        )
    # Quem pode mudar cargos não muda o próprio: era uma forma de se promover a
    # um cargo com mais ações (o mesmo vale para o tipo de utilizador).
    if membro.id == actor.id and not is_superadmin(actor):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Não pode alterar o seu próprio cargo — peça a outro gestor da empresa.",
        )

    novo: Optional[user_model.Role] = None
    if role_id is not None:
        novo = get_role_or_404(db, role_id)
        if novo.empresa_id != empresa_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="O cargo tem de ser um dos cargos desta empresa.",
            )
        if not novo.is_active:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"O cargo '{novo.name}' está desativado.",
            )

    anterior = membro.empresa_role.name if membro.empresa_role else "sem cargo"
    membro.empresa_role_id = role_id
    _audit(
        db,
        actor,
        f"Alterou o cargo de {membro.email}: '{anterior}' → '{novo.name if novo else 'sem cargo'}'",
        "User",
        membro.id,
    )
    db.commit()
    db.refresh(membro)
    log_message(f"👤 Cargo de {membro.email} alterado por {actor.email}", "success")
    return _member_out(membro)


def set_member_status(
    db: Session,
    actor: user_model.User,
    user_id: int,
    is_active: bool,
) -> users_schemas.MemberSchema:
    membro = _get_member_or_404(db, actor, user_id)

    if membro.id == actor.id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Não pode desativar a sua própria conta.",
        )

    if (
        not is_active
        and is_superadmin(membro)
        and _count_active_superadmins(db, membro.id) == 0
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Este é o último super admin ativo e não pode ser desativado.",
        )

    membro.is_active = is_active

    _audit(
        db,
        actor,
        f"{'Ativou' if is_active else 'Desativou'} a conta de {membro.email}",
        "User",
        membro.id,
    )
    db.commit()
    db.refresh(membro)

    return _member_out(membro)
