from fastapi import APIRouter, Depends, HTTPException, status, Response, Cookie
from sqlalchemy.orm import Session
from app import database, auth
from app.cruds import user_crud
from app.models import user_model
from app.schemas import users_chemas
from app.token_storage import store_refresh_token, is_refresh_token_valid, revoke_token
from app.config.dotenv import get_env
from app.ultils.ativar_session_bd import desativar_connection, reativar_connection
from app.ultils.logger import log_message

router = APIRouter(prefix="/auth", tags=["Auth"])

# Configurações via .env
COOKIE_SECURE = get_env("COOKIE_SECURE", "false").lower() == "true"
COOKIE_SAMESITE = get_env("COOKIE_SAMESITE", "lax")
COOKIE_DOMAIN = get_env("COOKIE_DOMAIN")
ACCESS_TOKEN_EXPIRE = 15 * 60
REFRESH_TOKEN_EXPIRE = 7 * 24 * 60 * 60


def set_cookie(response: Response, key: str, value: str, path="/"):
    response.set_cookie(
        key=key,
        value=value,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        max_age=REFRESH_TOKEN_EXPIRE if key == "refresh_token" else ACCESS_TOKEN_EXPIRE,
        path=path,
        domain=COOKIE_DOMAIN if COOKIE_DOMAIN != "localhost" else None
    )


@router.post("/register", response_model=users_chemas.UserOut, status_code=status.HTTP_201_CREATED)
def register_user(user: users_chemas.UserCreate, db: Session = Depends(database.get_db)):
    if user_crud.get_user_by_email(db, user.email):
        raise HTTPException(status_code=400, detail="Email já cadastrado")
    if user.senha != user.confirmar_senha:
        raise HTTPException(status_code=400, detail="Senhas não coincidem.")
    if not user.concorda_termos:
        raise HTTPException(status_code=400, detail="Aceitação dos termos é obrigatória.")
    return user_crud.create_user(db, user)


@router.post("/login", response_model=users_chemas.LoginResponse)
def login_user(credentials: users_chemas.UserLogin, response: Response, db: Session = Depends(database.get_db)):
    user = user_crud.get_user_by_email(db, credentials.email)
    if not user or not auth.verify_password(credentials.senha, user.hashed_password):
        raise HTTPException(status_code=401, detail="Credenciais inválidas")

    access_token = auth.create_access_token({"sub": str(user.id)})
    refresh_token = auth.create_refresh_token({"sub": str(user.id)})
    store_refresh_token(db, refresh_token, user.id)

    set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh")
    set_cookie(response, "access_token", access_token)
    
    rep =reativar_connection(user.id, db)
    if rep["success"]:
        user.InfPlus = rep["config"]

    return {"user": user}


@router.post("/refresh", response_model=users_chemas.AccessTokenOut)
def refresh_access_token(
    response: Response,
    refresh_token: str = Cookie(None),
    db: Session = Depends(database.get_db)
):
    if not refresh_token or not is_refresh_token_valid(db, refresh_token):
        raise HTTPException(status_code=422, detail="Refresh token inválido ou expirado")
    # print(f"[TOKEN] Refresh token válido: {refresh_token}")
    payload = auth.decode_token(refresh_token)
    if not payload:
        raise HTTPException(status_code=402, detail="Token inválido")

    user_id = payload if isinstance(payload, str) else payload.get("sub")
    access_token = auth.create_access_token({"sub": str(user_id)})

    set_cookie(response, "access_token", access_token)

    return users_chemas.AccessTokenOut(access_token="", token_type="") 


@router.get("/me", response_model=users_chemas.UserOut)
def get_logged_user(
    access_token: str = Cookie(None),
    db: Session = Depends(database.get_db)
):
    # print(f"[TOKEN] auth/me Access token recebido: {access_token}")
    # if not access_token or not is_refresh_token_valid(db, access_token):
        # raise HTTPException(status_code=401, detail="Refresh token inválido ou expirado")
    if access_token is None:
        raise HTTPException(status_code=401, detail="Access token não fornecido")
    
    payload = auth.decode_token(access_token)
    if not payload:
        raise HTTPException(status_code=401, detail="Token inválido")

    user_id = payload if isinstance(payload, str) else payload.get("sub")
    user = db.query(user_model.User).get(int(user_id))
    if not user:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    rep =reativar_connection(user.id, db)
    # print(rep)
    if rep["success"]:
        user.InfPlus = rep["config"]

    return user


@router.post("/logout")
def logout_user(
    response: Response,
    refresh_token: str = Cookie(None),
    db: Session = Depends(database.get_db)
):
    if refresh_token:
        revoke_token(db, refresh_token)
    payload = auth.decode_token(refresh_token)
    if payload:
        user_id = payload if isinstance(payload, str) else payload.get("sub")
        resp =desativar_connection(db,user_id)
        log_message("{resp}","info")

    response.delete_cookie("refresh_token", path="/auth/refresh")
    response.delete_cookie("access_token", path="/")

    return {"message": "Logout efetuado com sucesso."}
