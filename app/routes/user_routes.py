from fastapi import APIRouter, Depends, HTTPException, Cookie, status
from sqlalchemy.orm import Session
from app import database, auth
from app.config.dotenv import get_env
from app.cruds import user_crud
from app.models import user_model
from app.schemas import users_chemas
from app.ultils.get_id_by_token import get_current_user_id

router = APIRouter(prefix="/users", tags=["Users"])


# 🔒 Listar todos os usuários (apenas se autenticado)
@router.get("/", response_model=list[users_chemas.UserOut])
def list_all_users(
    current_user: user_model.User = Depends(get_current_user_id),
    db: Session = Depends(database.get_db)
):
    """
    Retorna todos os usuários cadastrados (rota protegida).
    """
    return user_crud.get_users(db)

# 🔒 Atualizar nome do usuário autenticado
@router.put("/update", response_model=users_chemas.UserOut)
def update_user_name(
    full_name: str,
    current_user: user_model.User = Depends(get_current_user_id),
    db: Session = Depends(database.get_db)
):
    """
    Atualiza o nome completo do usuário logado.
    """
    return user_crud.update_user(db, current_user.id, full_name)
