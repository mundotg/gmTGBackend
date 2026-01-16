import traceback
from fastapi import APIRouter, Depends, HTTPException, status, Response, Cookie
from sqlalchemy.orm import Session

from app import database, auth
from app.cruds import user_crud
from app.models import user_model
from app.schemas import users_chemas
from app.token_storage import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_DAYS,
    refresh_token_time_left,
    store_refresh_token,
    is_refresh_token_valid,
    revoke_token,
    update_refresh_token,
)
from app.config.dotenv import get_env
from app.ultils.ativar_session_bd import desativar_connection, reativar_connection
from app.ultils.logger import log_message

router = APIRouter(prefix="/auth", tags=["Auth"])

# 🔐 Configurações via .env
COOKIE_SECURE = get_env("COOKIE_SECURE", "false").lower() == "true"
COOKIE_SAMESITE = get_env("COOKIE_SAMESITE", "lax")
COOKIE_DOMAIN = get_env("COOKIE_DOMAIN")


# 🍪 Helper para cookies
def set_cookie(response: Response, key: str, value: str, path: str = "/"):
    response.set_cookie(
        key=key,
        value=value,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        max_age=(
            REFRESH_TOKEN_EXPIRE_DAYS
            if key == "refresh_token"
            else ACCESS_TOKEN_EXPIRE_MINUTES
        ),
        path=path,
        domain=COOKIE_DOMAIN if COOKIE_DOMAIN and COOKIE_DOMAIN != "localhost" else None,
    )


# 🚨 Helper padrão de erro
def internal_error(e: Exception):
    log_message(
        f"❌ Erro interno: {str(e)}\n{traceback.format_exc()}",
        "error",
    )
    raise HTTPException(status_code=500, detail="Erro interno no servidor.")


# 📝 REGISTRO
@router.post(
    "/register",
    response_model=users_chemas.UserOut,
    status_code=status.HTTP_201_CREATED,
)
def register_user(
    user: users_chemas.UserCreate,
    db: Session = Depends(database.get_db),
):
    try:
        if user_crud.get_user_by_email(db, user.email):
            raise HTTPException(status_code=400, detail="Email já cadastrado")

        if user.senha != user.confirmar_senha:
            raise HTTPException(status_code=400, detail="Senhas não coincidem")

        if not user.concorda_termos:
            raise HTTPException(
                status_code=400,
                detail="Aceitação dos termos é obrigatória",
            )

        return user_crud.create_user(db, user)

    except HTTPException:
        raise
    except Exception as e:
        internal_error(e)


# 🔐 LOGIN
@router.post("/login", response_model=users_chemas.LoginResponse)
def login_user(
    credentials: users_chemas.UserLogin,
    response: Response,
    db: Session = Depends(database.get_db),
):
    try:
        user = user_crud.get_user_by_email(db, credentials.email)
        if not user:
            raise HTTPException(status_code=401, detail="E-mail não encontrado")

        if not auth.verify_password(credentials.senha, user.hashed_password):
            raise HTTPException(status_code=401, detail="Senha incorreta")

        # 🎟️ Tokens
        access_token = auth.create_access_token({"sub": str(user.id)})
        refresh_token = auth.create_refresh_token({"sub": str(user.id)})

        store_refresh_token(db, refresh_token, user.id)

        set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh")
        set_cookie(response, "access_token", access_token)

        # 🔁 Reativar conexão
        rep = reativar_connection(user.id, db)
        info_extra = rep.get("config") if rep.get("success") else None

        return users_chemas.LoginResponse(
        user=users_chemas.UserOut(
            id=str(user.id),
            nome=user.nome,
            apelido=user.apelido,
            email=user.email,
            telefone=user.telefone,
            empresa=(
                users_chemas.EmpresaSchema.model_validate(user.empresa)
                if user.empresa else None
            ),
            cargo=(
                users_chemas.CargoSchema.model_validate(user.cargo)
                if user.cargo else None
            ),
            role=(
                users_chemas.RoleSimpleSchema.model_validate(user.role)
                if user.role else None
            ),
            permissions=list(user.permissions),  # 👈 property do model User
            info_extra=info_extra,
        ),
    )


    except HTTPException:
        raise
    except Exception as e:
        internal_error(e)


# 🔁 REFRESH TOKEN
@router.post("/refresh", response_model=users_chemas.AccessTokenOut)
def refresh_access_token(
    response: Response,
    refresh_token: str = Cookie(None),
    db: Session = Depends(database.get_db),
):
    try:
        if not refresh_token or not is_refresh_token_valid(db, refresh_token):
            raise HTTPException(
                status_code=422,
                detail="Refresh token inválido ou expirado",
            )

        payload = auth.decode_token(refresh_token)
        if not payload:
            raise HTTPException(status_code=401, detail="Token inválido")

        user_id = payload.get("sub") if isinstance(payload, dict) else payload

        access_token = auth.create_access_token({"sub": str(user_id)})

        _, is_expiring = refresh_token_time_left(db, refresh_token)
        if is_expiring:
            new_refresh = auth.create_refresh_token({"sub": str(user_id)})
            update_refresh_token(db, refresh_token, new_refresh)
            refresh_token = new_refresh

        set_cookie(response, "access_token", access_token)
        set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh")

        return users_chemas.AccessTokenOut(
            access_token=access_token,
            token_type="bearer",
        )

    except HTTPException:
        raise
    except Exception as e:
        internal_error(e)


# 👤 USUÁRIO LOGADO
@router.get("/me", response_model=users_chemas.UserOut)
def get_logged_user(
    access_token: str = Cookie(None),
    db: Session = Depends(database.get_db),
):
    try:
        if not access_token:
            raise HTTPException(status_code=401, detail="Access token não fornecido")

        payload = auth.decode_token(access_token)
        if not payload:
            raise HTTPException(status_code=401, detail="Token inválido ou expirado")

        user_id = payload.get("sub") if isinstance(payload, dict) else payload
        user = db.get(user_model.User, int(user_id))

        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")

        rep = reativar_connection(user.id, db)
        info_extra = rep.get("config") if rep.get("success") else None

        return users_chemas.UserOut(
            id=str(user.id),
            nome=user.nome,
            apelido=user.apelido,
            email=user.email,
            telefone=user.telefone,
            empresa=(
                users_chemas.EmpresaSchema.model_validate(user.empresa)
                if user.empresa else None
            ),
            cargo=(
                users_chemas.CargoSchema.model_validate(user.cargo)
                if user.cargo else None
            ),
            role=(
                users_chemas.RoleSimpleSchema.model_validate(user.role)
                if user.role else None
            ),
            permissions=list(user.permissions),  # 👈 property do model User
            info_extra=info_extra,
        )

    except HTTPException:
        raise
    except Exception as e:
        internal_error(e)


# 🚪 LOGOUT
@router.post("/logout")
def logout_user(
    response: Response,
    refresh_token: str = Cookie(None),
    db: Session = Depends(database.get_db),
):
    try:
        if refresh_token:
            revoke_token(db, refresh_token)
            payload = auth.decode_token(refresh_token)
            if payload:
                user_id = payload.get("sub") if isinstance(payload, dict) else payload
                desativar_connection(db, user_id)

        response.delete_cookie("refresh_token", path="/auth/refresh")
        response.delete_cookie("access_token", path="/")

        return {"message": "Logout efetuado com sucesso."}

    except Exception as e:
        internal_error(e)
