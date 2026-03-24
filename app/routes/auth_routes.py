import traceback
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, status, Response, Cookie, Request
from sqlalchemy.orm import Session

from app import database, auth
from app.cruds import user_crud
from app.models import user_model
from app.request_fingerprint import build_fingerprint
from app.schemas import users_chemas
from app.services.crypto_utils import aes_decrypt
from app.token_storage import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_DAYS,
    refresh_token_time_left,
    store_refresh_token,
    is_refresh_token_valid,
    revoke_token,
    update_refresh_token,
    assert_refresh_token_binding,   # ✅ IMPORTANTE
)
from app.config.dotenv import get_env
from app.ultils.ativar_session_bd import reativar_connection
from app.ultils.logger import log_message

router = APIRouter(prefix="/auth", tags=["Auth"])

COOKIE_SECURE = get_env("COOKIE_SECURE", "false").lower() == "true"
COOKIE_SAMESITE = get_env("COOKIE_SAMESITE", "lax") or "none"
COOKIE_DOMAIN = get_env("COOKIE_DOMAIN")

FINGERPRINT_SALT = get_env("FINGERPRINT_SALT", "change-me-please")


def _cookie_domain():
    return COOKIE_DOMAIN if COOKIE_DOMAIN and COOKIE_DOMAIN != "localhost" else None


def _to_seconds(value: int, unit: str) -> int:
    v = int(value)
    TEN_YEARS_SECONDS = 10 * 365 * 24 * 60 * 60
    if v > TEN_YEARS_SECONDS:
        return v

    if unit == "minutes":
        return int(timedelta(minutes=v).total_seconds())
    if unit == "days":
        return int(timedelta(days=v).total_seconds())
    return v


def set_cookie(response: Response, key: str, value: str, path: str = "/"):
    if key == "refresh_token":
        max_age = _to_seconds(REFRESH_TOKEN_EXPIRE_DAYS, "days")
    else:
        max_age = _to_seconds(ACCESS_TOKEN_EXPIRE_MINUTES, "minutes")

    response.set_cookie(
        key=key,
        value=value,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        max_age=max_age,
        path=path,
        domain=_cookie_domain(),
    )


def internal_error(e: Exception):
    log_message(f"❌ Erro interno: {str(e)}\n{traceback.format_exc()}", "error")
    raise HTTPException(status_code=500, detail="Erro interno no servidor.")


def build_user_out(user: user_model.User, info_extra=None) -> users_chemas.UserOut:
    return users_chemas.UserOut(
        id=str(user.id),
        nome=user.nome,
        apelido=user.apelido,
        email=user.email,
        telefone=user.telefone,
        empresa=users_chemas.EmpresaSchema.model_validate(user.empresa) if user.empresa else None,
        cargo=users_chemas.CargoSchema.model_validate(user.cargo) if user.cargo else None,
        roles=users_chemas.RoleSimpleSchema.model_validate(user.role) if user.role else None,
        permissions=list(user.permissions),
        info_extra=info_extra,
    )


def get_payload_from_token_or_401(token: str) -> dict:
    payload = auth.decode_token(token)
    if not payload or not isinstance(payload, dict):
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")
    return payload




def assert_access_token_binding(request: Request, access_token: str) -> dict:
    """
    Valida binding do access token com o fingerprint atual.
    (Isto não consulta BD; é só o JWT vs request atual.)
    """
    payload = get_payload_from_token_or_401(access_token)
    fp_now = build_fingerprint(request, FINGERPRINT_SALT)

    # Comparações severas (se quiseres tolerância, mexe aqui)
    if payload.get("fp") != fp_now.get("fp"):
        raise HTTPException(status_code=401, detail="Sessão inválida: fingerprint divergente")

    if payload.get("ip") != fp_now.get("user_ip_prefix"):
        raise HTTPException(status_code=401, detail="Sessão inválida: IP divergente")

    # Obs: user_agent no token pode estar normalizado/hashiado dependendo da tua implementação.
    # Aqui assumo que estás guardando o ua bruto no token como você fez.
    if payload.get("ua") != fp_now.get("user_agent"):
        raise HTTPException(status_code=401, detail="Sessão inválida: User-Agent divergente")

    return payload


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
        db_user = user_crud.create_user(db, user)

        return {
            **db_user.__dict__,
            "id": str(db_user.id),  # 🔥 aqui resolve
            "permissions": list(db_user.permissions),
            "role": db_user.role,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno ao criar usuário: {str(e)}"
        )



@router.post("/login", response_model=users_chemas.LoginResponse)
def login_user(
    credentials: users_chemas.UserLogin,
    request: Request,
    response: Response,
    db: Session = Depends(database.get_db),
):
    try:
        user = user_crud.get_user_by_email(db, credentials.email)
        if not user:
            raise HTTPException(status_code=401, detail="E-mail não encontrado")

        if not auth.verify_password(aes_decrypt(credentials.senha), user.hashed_password):
            raise HTTPException(status_code=401, detail="Senha incorreta")

        fp = build_fingerprint(request, FINGERPRINT_SALT)

        access_token = auth.create_access_token({
            "sub": str(user.id),
            "fp": fp["fp"],
            "ua": fp["user_agent"],
            "ip": fp["user_ip_prefix"],
        })

        refresh_token = auth.create_refresh_token({
            "sub": str(user.id),
            "fp": fp["fp"],
            "ua": fp["user_agent"],
            "ip": fp["user_ip_prefix"],
        })

        # ✅ CRÍTICO: guardar refresh token com fingerprint
        store_refresh_token(db, refresh_token, user.id, REFRESH_TOKEN_EXPIRE_DAYS, fp)

        set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh")
        set_cookie(response, "access_token", access_token, path="/")

        rep = reativar_connection(user.id, db)
        info_extra = rep.get("config") if rep.get("success") else None

        return users_chemas.LoginResponse(user=build_user_out(user, info_extra=info_extra))

    except HTTPException:
        raise
    except Exception as e:
        internal_error(e)


@router.post("/refresh", response_model=users_chemas.AccessTokenOut)
def refresh_access_token(
    request: Request,
    response: Response,
    refresh_token: str | None = Cookie(None, alias="refresh_token"),
    db: Session = Depends(database.get_db),
):
    try:
        if not refresh_token or not is_refresh_token_valid(db, refresh_token):
            raise HTTPException(status_code=422, detail="Refresh token inválido ou expirado")

        fp = build_fingerprint(request, FINGERPRINT_SALT)

        # ✅ valida binding SEVERO (refresh no BD vs request atual)
        try:
            assert_refresh_token_binding(db, refresh_token, fp)
        except ValueError as e:
            # opcional: revoga para matar a sessão imediatamente
            log_message(f"erro valida binding SEVERO {e} ", "error")
            revoke_token(db, refresh_token)
            raise HTTPException(status_code=401, detail=str(e))

        payload = get_payload_from_token_or_401(refresh_token)
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Refresh token inválido: sub ausente")

        # Access token novo carrega o binding atual
        access_token = auth.create_access_token({
            "sub": str(user_id),
            "fp": fp["fp"],
            "ua": fp["user_agent"],
            "ip": fp["user_ip_prefix"],
        })

        _, is_expiring = refresh_token_time_left(db, refresh_token)
        if is_expiring:
            new_refresh = auth.create_refresh_token({
                "sub": str(user_id),
                "fp": fp["fp"],
                "ua": fp["user_agent"],
                "ip": fp["user_ip_prefix"],
            })

            update_refresh_token(db, refresh_token, new_refresh)
            refresh_token = new_refresh

            # IMPORTANTÍSSIMO: quando muda o refresh token, mantém o binding coerente no BD.
            # Se o teu update_refresh_token só troca o token, o binding antigo continua válido (ok),
            # mas se quiseres "reiniciar binding", revoga o antigo e cria novo registro no store.
            # Aqui vou pelo mais seguro: cria novo registro e revoga o antigo.
            store_refresh_token(db, refresh_token, int(user_id), fp=fp)

        set_cookie(response, "access_token", access_token, path="/")
        set_cookie(response, "refresh_token", refresh_token, path="/auth/refresh")

        return users_chemas.AccessTokenOut(access_token="ok", token_type="bearer")

    except HTTPException as err :
        internal_error(err)
        raise 
    except Exception as e:
        internal_error(e)


@router.get("/me", response_model=users_chemas.UserOut)
def get_logged_user(
    request: Request,
    access_token: str | None = Cookie(None, alias="access_token"),
    db: Session = Depends(database.get_db),
):
    try:
        if not access_token:
            raise HTTPException(status_code=401, detail="Access token não fornecido")

        # ✅ severo: valida binding do access token
        payload = assert_access_token_binding(request, access_token)
        user_id = payload.get("sub")

        user = db.get(user_model.User, int(user_id))
        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")

        rep = reativar_connection(user.id, db)
        info_extra = rep.get("config") if rep.get("success") else None

        return build_user_out(user, info_extra=info_extra)

    except HTTPException:
        raise
    except Exception as e:
        internal_error(e)
        
def _delete_auth_cookies(response: Response):
    domain = _cookie_domain()

    # refresh pode ter sido setado em paths diferentes ao longo do tempo
    for path in ("/auth/refresh", "/auth", "/"):
        response.delete_cookie("refresh_token", path=path, domain=domain)

    # access normalmente é "/"
    response.delete_cookie("access_token", path="/", domain=domain)
    


@router.post("/logout")
def logout_user(
    request: Request,
    response: Response,
    refresh_token: str | None = Cookie(None, alias="refresh_token"),
    db: Session = Depends(database.get_db),
):
    try:
        if refresh_token:
            try:
                fp = build_fingerprint(request, FINGERPRINT_SALT)
                payload = assert_refresh_token_binding(request, refresh_token,fp)
                user_id = payload.get("sub")
                # se quiser: revoke_all_user_tokens(db, int(user_id))
            except HTTPException:
                # não trava logout
                pass

            revoke_token(db, refresh_token)

        _delete_auth_cookies(response)
        return {"message": "Logout efetuado com sucesso."}

    except Exception as e:
        internal_error(e)