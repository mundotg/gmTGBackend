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

# Configurações via .env
COOKIE_SECURE = get_env("COOKIE_SECURE", "false").lower() == "true"
COOKIE_SAMESITE = get_env("COOKIE_SAMESITE", "lax")
COOKIE_DOMAIN = get_env("COOKIE_DOMAIN")


def set_cookie(response: Response, key: str, value: str, path="/"):
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
        domain=COOKIE_DOMAIN if COOKIE_DOMAIN != "localhost" else None,
    )


@router.post(
    "/register",
    response_model=users_chemas.UserOut,
    status_code=status.HTTP_201_CREATED,
)
def register_user(
    user: users_chemas.UserCreate, db: Session = Depends(database.get_db)
):
    if user_crud.get_user_by_email(db, user.email):
        raise HTTPException(status_code=400, detail="Email já cadastrado")
    if user.senha != user.confirmar_senha:
        raise HTTPException(status_code=400, detail="Senhas não coincidem.")
    if not user.concorda_termos:
        raise HTTPException(
            status_code=400, detail="Aceitação dos termos é obrigatória."
        )
    return user_crud.create_user(db, user)


# def login_user(credentials: users_chemas.UserLogin, response: Response, db: Session = Depends(database.get_db)):
#     user = user_crud.get_user_by_email(db, credentials.email)
#     print(user.email, "; ",user.hashed_password)
#     if not user or not auth.verify_password(credentials.senha, user.hashed_password):
#         raise HTTPException(status_code=401, detail="Credenciais inválidas")
#     access_token = auth.create_access_token({"sub": str(user.id)})
#     refresh_token = auth.create_refresh_token({"sub": str(user.id)})
#     store_refresh_token(db, refresh_token, user.id)
#     set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh")
#     set_cookie(response, "access_token", access_token)
#     rep = reativar_connection(user.id, db)
#     if rep["success"]: user.InfPlus = rep["config"]
#     return {"user": user}


@router.post("/login", response_model=users_chemas.LoginResponse)
def login_user(
    credentials: users_chemas.UserLogin,
    response: Response,
    db: Session = Depends(database.get_db),
):
    # 🔍 Buscar usuário pelo e-mail
    print(credentials)
    user = user_crud.get_user_by_email(db, credentials.email)

    print(user)
    if not user:
        raise HTTPException(status_code=401, detail="E-mail não encontrado")

    # 🔑 Verificar senha
    print(user.email, user.hashed_password)
    if not auth.verify_password(credentials.senha, user.hashed_password):
        raise HTTPException(status_code=401, detail="Senha incorreta")

    # 🪪 Gerar tokens JWT
    access_token = auth.create_access_token({"sub": str(user.id)})
    refresh_token = auth.create_refresh_token({"sub": str(user.id)})

    # 💾 Salvar refresh token no banco
    store_refresh_token(db, refresh_token, user.id)

    # 🍪 Setar cookies seguros
    # set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh", httponly=True, secure=True)
    # set_cookie(response, "access_token", access_token, httponly=True, secure=True)

    # setar normal
    set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh")
    set_cookie(response, "access_token", access_token)

    # 🔁 Tentar reativar conexão de banco do usuário
    rep = reativar_connection(user.id, db)
    InfPlus = rep["config"] if rep.get("success") else None

    # 📦 Retorno formatado
    return users_chemas.LoginResponse(
        user=users_chemas.UserOut(
            id=user.id,
            apelido=user.apelido,
            empresa=(
                users_chemas.EmpresaSchema.model_validate(user.empresa_ref)
                if user.empresa_ref
                else None
            ),
            cargo=(
                users_chemas.CargoSchema.model_validate(user.cargo_ref)
                if user.cargo_ref
                else None
            ),
            nome=user.nome,
            email=user.email,
            telefone=user.telefone,
            InfPlus=InfPlus,
        )
    )


@router.post("/refresh", response_model=users_chemas.AccessTokenOut)
def refresh_access_token(
    response: Response,
    refresh_token: str = Cookie(None),
    db: Session = Depends(database.get_db),
):
    if not refresh_token or not is_refresh_token_valid(db, refresh_token):
        raise HTTPException(
            status_code=422, detail="Refresh token inválido ou expirado"
        )
    payload = auth.decode_token(refresh_token)
    if not payload:
        raise HTTPException(status_code=402, detail="Token inválido")

    user_id = payload if isinstance(payload, str) else payload.get("sub")
    access_token = auth.create_access_token({"sub": str(user_id)})
    data_exp, isExpiring = refresh_token_time_left(db, refresh_token)
    # print(f"Tempo restante para expiração do refresh token: {data_exp}, Está expirando? {isExpiring}")
    if isExpiring:  # Se o token está prestes a expirar, renova
        refresh_token2 = auth.create_refresh_token({"sub": str(user_id)})
        update_refresh_token(db, refresh_token, refresh_token2)
        set_cookie(response, "refresh_token", refresh_token2, path="/auth/refresh")

    set_cookie(response, "access_token", access_token)
    set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh")

    return users_chemas.AccessTokenOut(access_token="", token_type="")


@router.get("/me", response_model=users_chemas.UserOut)
def get_logged_user(
    access_token: str = Cookie(None), db: Session = Depends(database.get_db)
):
    # 🚫 Sem token
    if access_token is None:
        raise HTTPException(status_code=401, detail="Access token não fornecido")

    # 🔎 Decodificar e validar token
    payload = auth.decode_token(access_token)
    if not payload:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")

    user_id = payload.get("sub") if isinstance(payload, dict) else payload
    if not user_id:
        raise HTTPException(status_code=401, detail="Token malformado")

    # 🔍 Buscar usuário
    user = db.get(user_model.User, int(user_id))
    if not user:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    # 🔁 Reativar conexão se existir
    rep = reativar_connection(user.id, db)
    InfPlus = rep["config"] if rep.get("success") else None

    # ✅ Retornar dados formatados
    return users_chemas.UserOut(
        id=user.id,
        apelido=user.apelido,
        empresa=(
            users_chemas.EmpresaSchema.model_validate(user.empresa_ref)
            if user.empresa_ref
            else None
        ),
        cargo=(
            users_chemas.CargoSchema.model_validate(user.cargo_ref)
            if user.cargo_ref
            else None
        ),
        nome=user.nome,
        email=user.email,
        telefone=user.telefone,
        InfPlus=InfPlus,
    )


@router.post("/logout")
def logout_user(
    response: Response,
    refresh_token: str = Cookie(None),
    db: Session = Depends(database.get_db),
):
    if refresh_token:
        revoke_token(db, refresh_token)
    payload = auth.decode_token(refresh_token)
    if payload:
        user_id = payload if isinstance(payload, str) else payload.get("sub")
        resp = desativar_connection(db, user_id)
        log_message("{resp}", "info")

    response.delete_cookie("refresh_token", path="/auth/refresh")
    response.delete_cookie("access_token", path="/")

    return {"message": "Logout efetuado com sucesso."}
