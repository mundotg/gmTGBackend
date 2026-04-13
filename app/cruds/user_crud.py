from sqlalchemy import func
from sqlalchemy.orm import Session, load_only
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
            telefone=(user.telefone or "").strip(),
            empresa_id=empresa.id if empresa else None,
            cargo_id=cargo.id if cargo else None,
            hashed_password=hashed_pw,
            concorda_termos=bool(user.concorda_termos),
        )

        db.add(db_user)

        # ⚡ flush antes do commit → pega ID sem fechar transação
        db.flush()

        log_message(f"📌 ID gerado: {db_user.id}", "debug")

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
