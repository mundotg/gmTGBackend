from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app import auth
from app.models import user_model
from app.schemas import users_chemas
from app.ultils.logger import log_message


# -----------------------------
# 🔍 Buscar usuário por e-mail
# -----------------------------
# -----------------------------
# 🔍 Buscar usuário por e-mail (com debug)
# -----------------------------
def get_user_by_email(db: Session, email: str):
    log_message(f"🔍 Buscando usuário com email: {email}", "info")

    normalized_email = email.strip()
    user = (
        db.query(user_model.User)
        .filter(func.lower(user_model.User.email) == normalized_email.lower())
        .first()
    )

    return user


# -----------------------------
# 🧩 Criar (ou obter) empresa
# -----------------------------
def get_or_create_empresa(db: Session, empresa_data: users_chemas.EmpresaSchema):
    """Busca ou cria uma empresa caso ainda não exista."""
    if not empresa_data or not empresa_data.nome:
        return None

    empresa = db.query(user_model.Empresa).filter(
        user_model.Empresa.nome == empresa_data.nome
    ).first()

    if not empresa:
        empresa = user_model.Empresa(
            nome=empresa_data.nome,
            tamanho=empresa_data.tamanho,
            nif=empresa_data.nif,
            endereco=empresa_data.endereco
        )
        db.add(empresa)
        db.commit()
        db.refresh(empresa)
        log_message(f"🏢 Nova empresa criada: {empresa.nome}", "success")
    else:
        log_message(f"🏢 Empresa existente associada: {empresa.nome}", "info")

    return empresa


# -----------------------------
# 🧩 Criar (ou obter) cargo
# -----------------------------
def get_or_create_cargo(db: Session, cargo_data: users_chemas.CargoSchema):
    """Busca ou cria um cargo caso ainda não exista."""
    if not cargo_data or not cargo_data.nome:
        return None

    cargo = db.query(user_model.Cargo).filter(
        user_model.Cargo.nome == cargo_data.nome
    ).first()

    if not cargo:
        cargo = user_model.Cargo(
            nome=cargo_data.nome,
            descricao=cargo_data.descricao,
            nivel=cargo_data.nivel
        )
        db.add(cargo)
        db.commit()
        db.refresh(cargo)
        log_message(f"💼 Novo cargo criado: {cargo.nome}", "success")
    else:
        log_message(f"💼 Cargo existente associado: {cargo.nome}", "info")

    return cargo


# -----------------------------
# 🧑 Criar novo usuário
# -----------------------------
def create_user(db: Session, user: users_chemas.UserCreate):
    try:
        log_message(f"🧑 Criando novo usuário: {user.email}", "info")

        # 🔐 Hash da senha
        hashed_pw = auth.hash_password(user.senha)

        # 🏢 Empresa
        empresa = get_or_create_empresa(db, user.empresa)

        # 💼 Cargo
        cargo = get_or_create_cargo(db, user.cargo)

        # 👤 Criação do usuário
        db_user = user_model.User(
            nome=user.nome,
            apelido=user.apelido,
            email=user.email,
            telefone=user.telefone,
            empresa_id=empresa.id if empresa else None,
            cargo_id=cargo.id if cargo else None,
            hashed_password=hashed_pw,
            concorda_termos=user.concorda_termos
        )

        db.add(db_user)
        db.commit()
        db.refresh(db_user)

        log_message(f"✅ Usuário {user.email} criado com sucesso (ID: {db_user.id})", "success")
        return db_user

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao criar usuário: {e}", "error")
        raise


# -----------------------------
# ✏️ Atualizar nome do usuário
# -----------------------------
def update_user(db: Session, user_id: int, full_name: str):
    log_message(f"✏️ Atualizando nome do usuário ID {user_id} para '{full_name}'", "info")
    user = db.get(user_model.User, user_id)
    if user:
        user.nome = full_name
        db.commit()
        db.refresh(user)
        log_message(f"✅ Usuário ID {user_id} atualizado com sucesso", "success")
    else:
        log_message(f"❌ Usuário ID {user_id} não encontrado para atualização", "error")
    return user


# -----------------------------
# 📄 Listar todos os usuários
# -----------------------------
def get_users(db: Session):
    log_message("📄 Listando todos os usuários", "info")
    return db.query(user_model.User).all()
