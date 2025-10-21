import traceback
from fastapi import APIRouter, Depends, HTTPException, Header,status
from typing import List, Optional
from sqlalchemy.orm import Session
from app.database import get_db

from app.schemas.userTask_schemas import LoginResponseSchema, UsuarioCreateSchema, UsuarioLoginSchema, UsuarioResponseSchema, UsuarioUpdateSchema
from app.services.userTask_services import create_many_users_service, create_user_service, delete_user_service, get_user_service, list_users_service, login_user_service, refresh_token_user_service, update_user_service
from app.ultils.get_current_user_id_task import get_current_user_id_task
from app.ultils.logger import log_message
from app.ultils.get_id_by_token import get_current_user_id

router = APIRouter(prefix="/usuario", tags=["Usuários"])

# -----------------------------
# Função utilitária de erro
# -----------------------------
def handle_service_error(context: str, error: Exception):
    error_trace = traceback.format_exc()
    log_message(f"❌ Erro em {context}: {error}\n{error_trace}", level="error")
    if isinstance(error, HTTPException):
        raise error
    raise HTTPException(status_code=500, detail=f"Erro interno em {context}")

# -----------------------------
# LISTAR USUÁRIOS
# -----------------------------
@router.get("/", response_model=List[UsuarioResponseSchema])
def list_users(db: Session = Depends(get_db), user_id: str = Depends(get_current_user_id_task)):
    """Lista todos os usuários cadastrados no sistema."""
    try:
        return list_users_service(db)
    except Exception as e:
        handle_service_error("listar usuários", e)

  
@router.get("/refresh", response_model=LoginResponseSchema)
def refresh_token(
    db: Session = Depends(get_db),
    refresh_token: Optional[str] = Header(None, convert_underscores=False),
    user_principal: int = Depends(get_current_user_id_task)
):
    """
    Atualiza o token de acesso usando o refresh_token.
    O token antigo não é necessário, apenas o refresh_token válido.
    """
    # print("Iniciando processo de refresh token...")
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token não fornecido"
        )

    try:
        response = refresh_token_user_service(db, refresh_token)
        log_message(f"Token atualizado com sucesso para o usuário {response.email}", "info")
        return response
    except Exception as e:
        handle_service_error("atualizar token", e)
# -----------------------------
# OBTER USUÁRIO POR ID
# -----------------------------
@router.get("/byid/{user_id}", response_model=UsuarioResponseSchema)
def get_user(user_id: str, db: Session = Depends(get_db), _: str = Depends(get_current_user_id_task)):
    """Obtém um usuário específico pelo ID."""
    try:
        return get_user_service(db, user_id)
    except Exception as e:
        handle_service_error(f"obter usuário {user_id}", e)


# -----------------------------
# CRIAR NOVO USUÁRIO
# -----------------------------
@router.post("/", response_model=UsuarioResponseSchema)
def create_user(user: UsuarioCreateSchema, db: Session = Depends(get_db)):
    """Cria um novo usuário no sistema."""
    try:
        return create_user_service(db, user)
    except Exception as e:
        handle_service_error("criar usuário", e)


# -----------------------------
# CRIAR MÚLTIPLOS USUÁRIOS
# -----------------------------
@router.post("/bulk", response_model=dict)
def create_many_users(users: List[UsuarioCreateSchema], db: Session = Depends(get_db),user_id_principal: int = Depends(get_current_user_id_task)):
    """Cria múltiplos usuários de uma vez."""
    try:
        return create_many_users_service(db, users)
    except Exception as e:
        handle_service_error("criar múltiplos usuários", e)


# -----------------------------
# ATUALIZAR USUÁRIO
# -----------------------------
@router.put("/byid/{user_id}", response_model=UsuarioResponseSchema)
def update_user(user_id: str, data: UsuarioUpdateSchema, db: Session = Depends(get_db), user_id_principal: int = Depends(get_current_user_id_task)):
    """Atualiza os dados de um usuário existente."""
    try:
        return update_user_service(db, user_id, data)
    except Exception as e:
        handle_service_error(f"atualizar usuário {user_id}", e)


# -----------------------------
# LOGIN
# -----------------------------
@router.post("/login", response_model=LoginResponseSchema)
def login_user(credentials: UsuarioLoginSchema, db: Session = Depends(get_db),user_id_principal: int = Depends(get_current_user_id_task)):
    """Realiza login e retorna token de acesso."""
    try:
        return login_user_service(db, credentials,user_id_principal)
    except Exception as e:
        handle_service_error("login", e)
  


# -----------------------------
# DELETAR USUÁRIO
# -----------------------------
@router.delete("/byid/{user_id}", response_model=dict)
def delete_user(user_id: str, db: Session = Depends(get_db), _: str = Depends(get_current_user_id_task)):
    """Remove um usuário pelo ID."""
    try:
        return delete_user_service(db, user_id)
    except Exception as e:
        handle_service_error(f"deletar usuário {user_id}", e)
        
