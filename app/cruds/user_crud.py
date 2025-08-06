from sqlalchemy.orm import Session
from app import auth
from app.models import user_model
from app.schemas import users_chemas
from app.ultils.logger import log_message  # Importa o logger customizado

def get_user_by_email(db: Session, email: str):
    log_message(f"🔍 Buscando usuário com email: {email}", "info")
    return db.query(user_model.User).filter(user_model.User.email == email).first()

def create_user(db: Session, user: users_chemas.UserCreate):
    log_message(f"🧑 Criando novo usuário: {user.email}", "info")
    hashed_pw = auth.hash_password(user.senha)

    db_user = user_model.User(
        nome=user.nome,
        apelido=user.apelido,
        email=user.email,
        telefone=user.telefone,
        nome_empresa=user.nome_empresa,
        cargo=user.cargo,
        tamanho_empresa=user.tamanho_empresa,
        hashed_password=hashed_pw,
        concorda_termos=user.concorda_termos
    )

    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    log_message(f"✅ Usuário {user.email} criado com sucesso (ID: {db_user.id})", "success")
    return db_user

def update_user(db: Session, user_id: int, full_name: str):
    log_message(f"✏️ Atualizando nome do usuário ID {user_id} para '{full_name}'", "info")
    user = db.query(user_model.User).get(user_id)
    if user:
        user.nome = full_name
        db.commit()
        db.refresh(user)
        log_message(f"✅ Usuário ID {user_id} atualizado com sucesso", "success")
    else:
        log_message(f"❌ Usuário ID {user_id} não encontrado para atualização", "error")
    return user

def get_users(db: Session):
    log_message("📄 Listando todos os usuários", "info")
    return db.query(user_model.User).all()
