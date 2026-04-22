import secrets
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload, load_only
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from app import auth
from app.models import user_model
from app.schemas import users_schemas
from app.ultils.logger import log_message


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
        .options(
            load_only(user_model.User.id, user_model.User.email, user_model.User.nome)
        )
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
            plan_id=1,
            role_id=4,
            telefone=(user.telefone or "").strip(),
            empresa_id=empresa.id if empresa else None,
            cargo_id=cargo.id if cargo else None,
            hashed_password=hashed_pw,
            concorda_termos=bool(user.concorda_termos),
        )

        db.add(db_user)

        # ⚡ flush antes do commit → pega ID sem fechar transação
        db.flush()

        log_message(f"📌 ID gerado: {db_user.id}", "warning")

        db.commit()
        db.refresh(db_user)

        log_message(f"✅ Usuário criado: {email_norm}", "success")
        return db_user

    except IntegrityError:
        db.rollback()
        log_message(f"❌ Duplicate email: {email_norm}", "error")
        raise ValueError("E-mail já está em uso.")

    except Exception as e:
        db.rollback()
        log_message(f"🔥 Erro inesperado: {str(e)}", "error")
        raise


# -----------------------------
# 🐙 Criar user via social provider
# -----------------------------
SUPPORTED_SOCIAL_PROVIDERS = {
    "github",
    "google",
    "microsoft",
    "gitlab",
}


# -----------------------------
# 🐙 Criar user via social provider
# -----------------------------
def create_social_user(
    db: Session,
    email: str,
    provider_user_id: str,
    provider: str,
    provider_username: Optional[str] = None,
    profile_url: Optional[str] = None,
    avatar_url: Optional[str] = None,
    location: Optional[str] = None,
    bio: Optional[str] = None,
    provider_payload: Optional[dict] = None,
):

    email_norm = (email or "").strip().lower()
    provider = (provider or "").strip().lower()

    if provider not in SUPPORTED_SOCIAL_PROVIDERS:
        raise ValueError(f"Provider inválido: {provider}")

    if not email_norm:
        raise ValueError("Email obrigatório")

    try:

        existing_user = (
            db.query(user_model.User)
            .filter(user_model.User.email == email_norm)
            .first()
        )

        # -------------------------
        # user já existe
        # -------------------------

        if existing_user:

            # preenche só se vazio
            if not existing_user.avatar_url and avatar_url:
                existing_user.avatar_url = avatar_url

            provider_exists = (
                db.query(user_model.UserAuthProvider)
                .filter(
                    user_model.UserAuthProvider.user_id == existing_user.id,
                    user_model.UserAuthProvider.provider == provider,
                )
                .first()
            )

            if not provider_exists:

                social_link = user_model.UserAuthProvider(
                    user_id=existing_user.id,
                    provider=provider,
                    provider_user_id=provider_user_id,
                    provider_email=email_norm,
                    provider_username=provider_username,
                    profile_url=profile_url,
                    provider_payload=provider_payload,
                    location=location,
                    bio=bio,
                )

                db.add(social_link)

            db.commit()

            return existing_user

        # -------------------------
        # criar novo user
        # -------------------------

        random_password = secrets.token_hex(24)

        hashed_pw = auth.hash_password(random_password)

        provider_label = provider.capitalize()

        db_user = user_model.User(
            nome=f"{provider_label}_{provider_username}",
            apelido=provider_username,
            email=email_norm,
            telefone="",
            avatar_url=avatar_url,
            plan_id=1,
            role_id=4,
            hashed_password=hashed_pw,
            concorda_termos=True,
        )

        db.add(db_user)

        db.flush()

        social_link = user_model.UserAuthProvider(
            user_id=db_user.id,
            provider=provider,
            provider_user_id=provider_user_id,
            provider_email=email_norm,
            provider_username=provider_username,
            profile_url=profile_url,
            provider_payload=provider_payload,
            location=location,  # se existir no model
            bio=bio,  # se existir no model
        )

        db.add(social_link)

        db.commit()

        db.refresh(db_user)

        return db_user

    except IntegrityError:
        db.rollback()
        raise ValueError("Erro duplicação user")

    except Exception:
        db.rollback()
        raise


# -----------------------------
# 🔍 Buscar user por provider social
# -----------------------------
def get_user_social(
    db: Session,
    provider: str,
    provider_user_id: str,
):
    try:

        if not provider or not provider_user_id:
            return None

        provider = provider.lower().strip()

        auth_link = (
            db.query(user_model.UserAuthProvider)
            .filter(
                user_model.UserAuthProvider.provider == provider,
                user_model.UserAuthProvider.provider_user_id == provider_user_id,
            )
            .first()
        )

        if not auth_link:

            log_message(f"Social auth não encontrado: {provider}", "warning")

            return None

        return auth_link.user

    except Exception as e:

        log_message(f"Erro get_user_social: {e}", "error")

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
