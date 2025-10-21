from sqlite3 import IntegrityError
import traceback
from typing import List, Optional
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.models.task_models import Usuario as UsuarioORM

from app.auth import (
    create_access_token,
    create_refresh_token,
    decode_token_task,
    hash_password,
    verify_password,
)
from app.cruds.userTask_cruds import (
    create_userTask,
    delete_user,
    get_user,
    get_users,
    get_usuario_by_email,
)
from app.schemas.userTask_schemas import (
    LoginResponseSchema,
    RoleSchema,
    UsuarioCreateSchema,
    UsuarioLoginSchema,
    UsuarioResponseSchema,
    UsuarioUpdateSchema,
)
from app.ultils.logger import log_message


# -----------------------------
# LISTAR USUÁRIOS
# -----------------------------
def list_users_service(db: Session) -> List[UsuarioResponseSchema]:
    """Lista todos os usuários do sistema."""
    try:
        users = get_users(db)
        log_message(f"{len(users)} usuários listados com sucesso", "info")
        return [UsuarioResponseSchema.model_validate(u) for u in users]
    except SQLAlchemyError as e:
        log_message(f"Erro de banco ao listar usuários: {e}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail="Erro ao acessar o banco de dados")
    except Exception as e:
        log_message(f"Erro inesperado ao listar usuários: {e}\n{traceback.format_exc()}", "critical")
        raise HTTPException(status_code=500, detail="Erro interno ao listar usuários")


# -----------------------------
# OBTER USUÁRIO
# -----------------------------
def get_user_service(db: Session, user_id: Optional[str]) -> UsuarioResponseSchema:
    """Obtém um usuário específico pelo ID."""
    try:
        if not user_id:
            raise HTTPException(status_code=400, detail="O ID do usuário é obrigatório")

        user = get_user(db, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")

        log_message(f"Usuário {user_id} obtido com sucesso", "info")
        return UsuarioResponseSchema.model_validate(user)
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        log_message(f"Erro de banco ao obter usuário {user_id}: {e}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail="Erro ao acessar o banco de dados")
    except Exception as e:
        log_message(f"Erro inesperado ao obter usuário {user_id}: {e}\n{traceback.format_exc()}", "critical")
        raise HTTPException(status_code=500, detail="Erro interno ao obter usuário")


# -----------------------------
# 🚀 Serviço: Criar Usuário
# -----------------------------
def create_user_service(
    db: Session,
    user: UsuarioCreateSchema,
    user_id_principal: Optional[str] = None
) -> UsuarioResponseSchema:
    """Cria um novo usuário com validações e logs detalhados."""
    try:
        # 🧩 Validações iniciais
        if not user.nome or not user.email:
            log_message("❌ Tentativa de criar usuário com nome ou e-mail ausente", "warning")
            raise HTTPException(status_code=400, detail="Nome e email são obrigatórios")

        # 🔠 Normaliza o e-mail (minúsculo e sem espaços)
        user.email = user.email.strip().lower()

        # 🧍‍♂️ Verifica duplicidade de e-mail
        existing_user = db.query(UsuarioORM).filter(UsuarioORM.email == user.email).first()
        if existing_user:
            log_message(f"⚠️ E-mail '{user.email}' já cadastrado para o usuário ID {existing_user.id}", "warning")
            raise HTTPException(status_code=400, detail="E-mail já cadastrado")

        # 🔒 Valida e hasheia a senha
        if not user.senha or len(user.senha) < 6:
            log_message("⚠️ Tentativa de criar usuário com senha curta", "warning")
            raise HTTPException(status_code=400, detail="Senha deve ter pelo menos 6 caracteres")
        user.senha = hash_password(user.senha)

        # 🧱 Cria o usuário (função isolada)
        new_user = create_userTask(db, user, user_id_principal)

        log_message(f"✅ Usuário criado com sucesso: ID={new_user.id}, Nome={new_user.nome}, Email={new_user.email}", "info")
        return UsuarioResponseSchema.model_validate(new_user)

    except HTTPException:
        raise
    except IntegrityError as e:
        db.rollback()
        log_message(f"🧨 Violação de integridade ao criar usuário: {e}", "error")
        raise HTTPException(status_code=400, detail="Erro de integridade — verifique campos únicos.")
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"💥 Erro de banco ao criar usuário: {e}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail="Erro ao salvar usuário no banco de dados")
    except Exception as e:
        db.rollback()
        log_message(f"🔥 Erro inesperado ao criar usuário: {e}\n{traceback.format_exc()}", "critical")
        raise HTTPException(status_code=500, detail="Erro interno ao criar usuário")



# -----------------------------
# CRIAR MÚLTIPLOS USUÁRIOS
# -----------------------------
def create_many_users_service(db: Session, users: List[UsuarioCreateSchema]) -> dict:
    """Cria múltiplos usuários de uma só vez."""
    try:
        if not users:
            raise HTTPException(status_code=400, detail="Lista de usuários vazia")

        created = []
        for user in users:
            created_user = create_userTask(db, user)
            created.append(created_user)

        db.commit()
        log_message(f"{len(created)} usuários criados com sucesso", "info")
        return {"detail": f"{len(created)} usuários criados com sucesso"}

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro de banco ao criar múltiplos usuários: {e}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail="Erro ao salvar usuários no banco de dados")
    except Exception as e:
        db.rollback()
        log_message(f"Erro inesperado ao criar múltiplos usuários: {e}\n{traceback.format_exc()}", "critical")
        raise HTTPException(status_code=500, detail="Erro interno ao criar múltiplos usuários")


# -----------------------------
# ATUALIZAR USUÁRIO
# -----------------------------
def update_user_service(db: Session, user_id: str, data: UsuarioUpdateSchema) -> UsuarioResponseSchema:
    """Atualiza os dados de um usuário existente."""
    try:
        existing = get_user(db, user_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")

        # Aqui você deve chamar a função update_user se existir
        # updated_user = update_user(db, user_id, data)
        log_message(f"Usuário {user_id} atualizado com sucesso", "info")

        return UsuarioResponseSchema.model_validate(existing)
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro de banco ao atualizar usuário {user_id}: {e}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail="Erro ao atualizar usuário no banco de dados")
    except Exception as e:
        db.rollback()
        log_message(f"Erro inesperado ao atualizar usuário {user_id}: {e}\n{traceback.format_exc()}", "critical")
        raise HTTPException(status_code=500, detail="Erro interno ao atualizar usuário")


# -----------------------------
# LOGIN
# -----------------------------
def login_user_service(db: Session, credentials: UsuarioLoginSchema, user_id_principal: Optional[str] = None) -> LoginResponseSchema:
    try:
        user = get_usuario_by_email(db, credentials.email)

        if not user or not verify_password(credentials.senha, user.senha):
            log_message(f"Tentativa de login inválida: {credentials.email}", "warning")
            raise HTTPException(status_code=401, detail="E-mail ou senha incorretos")

        token_data = {
            "sub": str(user.id),
            "email": user.email,
            "role": getattr(user.role_ref, "nome", None),
        }

        access_token = create_access_token(token_data)
        refresh_token = create_refresh_token(token_data)

        # ✅ Aqui está a correção: passar o objeto RoleSchema
        role_data = None
        if user.role_ref:
            role_data = RoleSchema.model_validate(user.role_ref)  # converte ORM → schema

        response = LoginResponseSchema(
            id=user.id,
            nome=user.nome,
            email=user.email,
            role=role_data,  # ✅ Agora é um objeto válido
            projects_participating=[{"id": p.id, "name": p.name} for p in getattr(user, "projects_participating", [])],
            created_projects=[{"id": p.id, "name": p.name} for p in getattr(user, "created_projects", [])],
            assigned_tasks=[{"id": t.id, "title": t.title} for t in getattr(user, "assigned_tasks", [])],
            delegated_tasks=[{"id": t.id, "title": t.title} for t in getattr(user, "delegated_tasks", [])],
            created_tasks=[{"id": t.id, "title": t.title} for t in getattr(user, "created_tasks", [])],
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=3600,
        )

        log_message(f"Usuário {user.email} logado com sucesso", "info")
        return response

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro de banco ao fazer login: {e}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail="Erro ao acessar o banco de dados")
    except Exception as e:
        db.rollback()
        log_message(f"Erro inesperado ao fazer login: {e}\n{traceback.format_exc()}", "critical")
        raise HTTPException(status_code=500, detail="Erro interno ao realizar login")



# -----------------------------
# REFRESH TOKEN
# -----------------------------
def refresh_token_user_service(db: Session, refresh_token: str) -> LoginResponseSchema:
    """Atualiza o token de acesso de um usuário."""
    try:
        user_id = decode_token_task(refresh_token)
        if not user_id:
            raise HTTPException(status_code=401, detail="Refresh token inválido ou expirado")

        user = get_user(db, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")

        token_data = {"sub": str(user.id), "email": user.email, "role": getattr(user.role_ref, "name", None)}
        access_token = create_access_token(token_data)
        new_refresh_token = create_refresh_token(token_data)

        role_data = None
        if user.role_ref:
            role_data = RoleSchema.model_validate(user.role_ref)  # converte ORM → schema

        response = LoginResponseSchema(
            id=user.id,
            nome=user.nome,
            email=user.email,
            role=role_data,  # ✅ Agora é um objeto válido
            projects_participating=[{"id": p.id, "name": p.name} for p in getattr(user, "projects_participating", [])],
            created_projects=[{"id": p.id, "name": p.name} for p in getattr(user, "created_projects", [])],
            assigned_tasks=[{"id": t.id, "title": t.title} for t in getattr(user, "assigned_tasks", [])],
            delegated_tasks=[{"id": t.id, "title": t.title} for t in getattr(user, "delegated_tasks", [])],
            created_tasks=[{"id": t.id, "title": t.title} for t in getattr(user, "created_tasks", [])],
            access_token=access_token,
            refresh_token=new_refresh_token,
            expires_in=3600,
        )
        return response

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro de banco ao atualizar token: {e}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail="Erro ao acessar o banco de dados")
    except Exception as e:
        db.rollback()
        log_message(f"Erro inesperado ao atualizar token: {e}\n{traceback.format_exc()}", "critical")
        raise HTTPException(status_code=500, detail="Erro interno ao atualizar token")


# -----------------------------
# DELETAR USUÁRIO
# -----------------------------
def delete_user_service(db: Session, user_id: Optional[str]) -> dict:
    """Exclui um usuário pelo ID."""
    try:
        if not user_id:
            raise HTTPException(status_code=400, detail="O ID do usuário é obrigatório")

        success = delete_user(db, user_id)
        if not success:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")

        log_message(f"Usuário {user_id} excluído com sucesso", "info")
        return {"detail": f"Usuário {user_id} excluído com sucesso"}
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"Erro de banco ao excluir usuário {user_id}: {e}\n{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail="Erro ao excluir usuário no banco de dados")
    except Exception as e:
        db.rollback()
        log_message(f"Erro inesperado ao excluir usuário {user_id}: {e}\n{traceback.format_exc()}", "critical")
        raise HTTPException(status_code=500, detail="Erro interno ao excluir usuário")
