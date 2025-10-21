from datetime import datetime
import traceback
from typing import List, Optional
from uuid import uuid4
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.models.task_models import Role, Usuario as UsuarioORM
from app.schemas.userTask_schemas import UsuarioCreateSchema, UsuarioSchema
from app.ultils.logger import log_message


# -----------------------------
# USUÁRIOS
# -----------------------------

def get_users(db: Session) -> List[UsuarioORM]:
    """Lista todos os usuários."""
    return db.query(UsuarioORM).all()


def get_user(db: Session, user_id: str) -> Optional[UsuarioORM]:
    """Busca um usuário pelo ID."""
    user = db.query(UsuarioORM).filter(UsuarioORM.id == user_id).first()
    if not user:
        log_message(f"Usuário {user_id} não encontrado", "warning")
    return user


def create_user(db: Session, user: UsuarioORM) -> UsuarioORM:
    """Cria usuário diretamente via ORM."""
    try:
        if not user.id:
            user.id = str(uuid4())
        db.add(user)
        db.commit()
        db.refresh(user)
        log_message(f"Usuário criado: {user.id}", "info")
        return user
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao criar usuário: {e}", "error")
        raise e



# -----------------------------
# 🧱 Função interna: Inserção no banco
# -----------------------------
def create_userTask(db: Session, user_data: UsuarioCreateSchema, user_id_principal: Optional[str]) -> UsuarioORM:
    """Cria um único usuário no banco de dados com role padrão."""
    try:
        # ✅ Converte para dict se for Pydantic
        if not isinstance(user_data, dict):
            user_data = (
                user_data.model_dump(exclude_unset=True)
                if hasattr(user_data, "model_dump")
                else vars(user_data)
            )

        # ⚙️ Remove campos imutáveis e não pertencentes ao ORM
        campos_validos = {
            "avatarUrl", "user_id", "nome", "email", "senha", "role_id", 
            "is_active", "email_verified"
        }
        user_data = {k: v for k, v in user_data.items() if k in campos_validos}

        # 🧱 Criação do usuário ORM
        user = UsuarioORM(**user_data)
        user.id = str(uuid4())
        user.user_id = user_id_principal

        # 🔧 Define role padrão (user) caso não informado
        if not getattr(user, "role_id", None):
            default_role = db.query(Role).filter(Role.name == "user").first()
            if default_role:
                user.role_id = default_role.id
            else:
                log_message("⚠️ Role 'user' não encontrada. Criando usuário sem role associada.", "warning")

        # 💾 Commit
        db.add(user)
        db.commit()
        db.refresh(user)

        return user

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro ao criar usuário no banco: {e}\n{traceback.format_exc()}", "error")
        raise



def create_users_bulk(db: Session, users_data: UsuarioSchema) -> List[UsuarioORM]:
    """Cria vários usuários de uma só vez (modo otimizado)."""
    try:
        db_users: List[UsuarioORM] = []

        for item in users_data:
            if not isinstance(item, dict):
                item = item.model_dump(exclude_unset=True) if hasattr(item, "model_dump") else vars(item)

            item.setdefault("id", str(uuid4()))
            item.setdefault("created_at", datetime.utcnow())
            item.setdefault("updated_at", datetime.utcnow())

            # ⚙️ Se não houver role_id informado, define None
            if "role_id" not in item or not item["role_id"]:
                item["role_id"] = None

            db_users.append(UsuarioORM(**item))

        db.bulk_save_objects(db_users)
        db.commit()
        log_message(f"{len(db_users)} usuários criados com sucesso", "info")
        return db_users
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao criar múltiplos usuários: {e}", "error")
        raise e


def delete_user(db: Session, user_id: str) -> bool:
    """Deleta um usuário pelo ID."""
    user = get_user(db, user_id)
    if not user:
        log_message(f"Tentativa de deletar usuário inexistente: {user_id}", "warning")
        return False
    try:
        db.delete(user)
        db.commit()
        log_message(f"Usuário deletado: {user_id}", "info")
        return True
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro ao deletar usuário {user_id}: {e}", "error")
        return False


def get_usuario_by_email(db: Session, email: str) -> Optional[UsuarioORM]:
    """Busca um usuário pelo e-mail."""
    user = db.query(UsuarioORM).filter(UsuarioORM.email == email).first()
    if not user:
        log_message(f"Usuário com e-mail {email} não encontrado", "warning")
    return user
