import traceback
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, Response, Cookie, Request
from sqlalchemy.orm import Session

from app import database, auth
from app.cruds import user_crud
from app.models import user_model
from app.request_fingerprint import build_fingerprint
from app.schemas import users_schemas
from app.services.crypto_utils import aes_decrypt
from app.token_storage import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_DAYS,
    refresh_token_time_left,
    rotate_refresh_token,
    store_refresh_token,
    is_refresh_token_valid,
    revoke_token,
    assert_refresh_token_binding,  # ✅ IMPORTANTE
)
from app.config.dotenv import get_env
from app.ultils.ativar_session_bd import reativar_connection
from app.ultils.logger import log_message

router = APIRouter(prefix="/auth", tags=["Auth"])

COOKIE_SECURE = get_env("COOKIE_SECURE", "false").lower() == "true"
COOKIE_SAMESITE = get_env("COOKIE_SAMESITE", "lax") or "none"
COOKIE_DOMAIN = get_env("COOKIE_DOMAIN")
TRUST_PROXY_HEADERS = get_env("TRUST_PROXY_HEADERS", "false").lower() == "true"
FINGERPRINT_SALT = get_env("FINGERPRINT_SALT", "change-me-please")
ENV = get_env("ENV", "development").lower()


def _cookie_domain():
    if ENV == "production":
        return COOKIE_DOMAIN if COOKIE_DOMAIN else None
    return COOKIE_DOMAIN if COOKIE_DOMAIN and COOKIE_DOMAIN != "localhost" else None


def _to_seconds(value: Optional[int], unit: str) -> int:
    v = int(value or 10)
    TEN_YEARS_SECONDS = 10 * 365 * 24 * 60 * 60
    if v > TEN_YEARS_SECONDS:
        return v

    if unit == "minutes":
        return int(timedelta(minutes=v).total_seconds())
    if unit == "days":
        return int(timedelta(days=v).total_seconds())
    return v


def set_cookie(response: Response, key: str, value: str, path: str = "/"):
    try:
        if key == "refresh_token":
            max_age = _to_seconds(REFRESH_TOKEN_EXPIRE_DAYS, "days")
        else:
            max_age = _to_seconds(ACCESS_TOKEN_EXPIRE_MINUTES, "minutes")

        cookie_options = {
            "key": key,
            "value": value,
            "httponly": TRUST_PROXY_HEADERS,
            "secure": COOKIE_SECURE,
            "samesite": COOKIE_SAMESITE,
            "max_age": max_age,
            "path": path,
        }

        domain = _cookie_domain()
        if domain:
            cookie_options["domain"] = domain

        # 🔥 obrigatório quando SameSite=None
        if COOKIE_SAMESITE.lower() == "none":
            cookie_options["secure"] = COOKIE_SECURE

        response.set_cookie(**cookie_options)

    except Exception as e:
        log_message(f"💥 erro ao definir cookie {key}: {e}", "error")


def _delete_auth_cookies(response: Response):
    domain = _cookie_domain()

    cookie_options = {
        "domain": domain,
        "secure": COOKIE_SECURE,
        "httponly": TRUST_PROXY_HEADERS,
        "samesite": COOKIE_SAMESITE,
    }

    # 🔥 refresh pode ter múltiplos paths
    for path in ("/", "/auth", "/auth/refresh"):
        response.delete_cookie("refresh_token", path=path, **cookie_options)

    # 🔥 access token normalmente raiz
    response.delete_cookie("access_token", path="/", **cookie_options)

    log_message("🍪 Cookies de autenticação removidos", "info")


def internal_error(e: Exception):
    log_message(f"❌ Erro interno: {str(e)}\n{traceback.format_exc()}", "error")
    raise HTTPException(status_code=500, detail="Erro interno no servidor.")


def build_user_out(user: user_model.User, info_extra=None) -> users_schemas.UserOut:
    return users_schemas.UserOut(
        id=user.id,
        nome=user.nome,
        apelido=user.apelido,
        email=user.email,
        telefone=user.telefone,
        empresa=(
            users_schemas.EmpresaSchema.model_validate(user.empresa)
            if user.empresa
            else None
        ),
        cargo=(
            users_schemas.CargoSchema.model_validate(user.cargo) if user.cargo else None
        ),
        roles=(
            users_schemas.RoleSimpleSchema.model_validate(user.role)
            if user.role
            else None
        ),
        permissions=list(user.permissions),
        info_extra=info_extra,
    )


def get_payload_from_token_or_401(token: str) -> dict:
    try:
        payload = auth.decode_token(token)
        if not payload or not isinstance(payload, dict):
            raise ValueError("Payload inválido")
        return payload
    except Exception as e:
        log_message(f"❌ Token inválido: {e}", "warning")
        raise HTTPException(status_code=401, detail="Token inválido")


def assert_access_token_binding(request: Request, access_token: str) -> dict:
    payload = get_payload_from_token_or_401(access_token)
    fp_now = build_fingerprint(request, FINGERPRINT_SALT)

    # 🔒 1. fingerprint (obrigatório)
    if payload.get("fp") != fp_now.get("fp"):
        raise HTTPException(status_code=401, detail="Sessão inválida")

    # ⚠️ 2. IP (tolerante)
    ip_token = payload.get("ip")
    ip_now = fp_now.get("user_ip_prefix")

    if ip_token and ip_now and ip_token != ip_now:
        log_message(f"⚠️ IP divergente token={ip_token} atual={ip_now}", "warning")

    # ⚠️ 3. User-Agent (tolerante)
    ua_token = payload.get("ua")
    ua_now = fp_now.get("user_agent")

    if ua_token and ua_now and ua_token != ua_now:
        log_message(
            f"⚠️ UA divergente token={ua_token[:30]}... atual={ua_now[:30]}...",
            "warning",
        )

    return payload


@router.post(
    "/register",
    response_model=users_schemas.UserOut,
    status_code=status.HTTP_201_CREATED,
)
async def register_user(
    user: users_schemas.UserCreate,
    db: Session = Depends(database.get_db),
):
    try:
        db_user = user_crud.create_user(db, user)

        return {
            **db_user.__dict__,
            "id": db_user.id,  # 🔥 aqui resolve
            "permissions": list(db_user.permissions),
            "role": db_user.role,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro interno ao criar usuário: {str(e)}"
        )


@router.post("/login", response_model=users_schemas.LoginResponse)
async def login_user(
    credentials: users_schemas.UserLogin,
    request: Request,
    response: Response,
    db: Session = Depends(database.get_db),
):
    try:
        user = user_crud.get_user_by_email(db, credentials.email)
        if not user:
            raise HTTPException(status_code=401, detail="E-mail não encontrado")

        if not auth.verify_password(
            aes_decrypt(credentials.senha), user.hashed_password
        ):
            raise HTTPException(status_code=401, detail="Senha incorreta")

        fp = build_fingerprint(request, FINGERPRINT_SALT)

        access_token = auth.create_access_token(
            {
                "sub": str(user.id),
                "fp": fp["fp"],
                "ua": fp["user_agent"],
                "ip": fp["user_ip_prefix"],
            }
        )

        refresh_token = auth.create_refresh_token(
            {
                "sub": str(user.id),
                "fp": fp["fp"],
                "ua": fp["user_agent"],
                "ip": fp["user_ip_prefix"],
            }
        )

        # ✅ CRÍTICO: guardar refresh token com fingerprint
        store_refresh_token(db, refresh_token, user.id, REFRESH_TOKEN_EXPIRE_DAYS, fp)

        set_cookie(response, "refresh_token", refresh_token, path="/")
        set_cookie(response, "access_token", access_token, path="/")

        rep = reativar_connection(user.id, db)
        info_extra = rep.get("config") if rep.get("success") else None

        return users_schemas.LoginResponse(
            user=build_user_out(user, info_extra=info_extra)
        )

    except HTTPException:
        raise
    except Exception as e:
        internal_error(e)


@router.post("/refresh", response_model=users_schemas.AccessTokenOut)
async def refresh_access_token(
    request: Request,
    response: Response,
    refresh_token: str | None = Cookie(None, alias="refresh_token"),
    db: Session = Depends(database.get_db),
):
    try:
        if not refresh_token:
            raise HTTPException(status_code=401, detail="Sessão inválida")

        # 🔍 valida existência e estado
        if not is_refresh_token_valid(db, refresh_token):
            raise HTTPException(status_code=401, detail="Sessão expirada ou inválida")

        # 🔐 fingerprint atual
        fp = build_fingerprint(request, FINGERPRINT_SALT)

        # 🔒 valida binding (IP + UA)
        try:
            assert_refresh_token_binding(db, refresh_token, fp)
        except ValueError as e:
            log_message(f"🚨 Binding inválido: {e}", "error")
            revoke_token(db, refresh_token)
            raise HTTPException(status_code=401, detail="Sessão inválida")

        # 🔍 payload JWT
        payload = get_payload_from_token_or_401(refresh_token)
        user_id = payload.get("sub")

        if not user_id:
            revoke_token(db, refresh_token)
            raise HTTPException(status_code=401, detail="Token inválido")

        # 🆕 novo access token
        access_token = auth.create_access_token(
            {
                "sub": str(user_id),
                "fp": fp["fp"],
                "ua": fp["user_agent"],
                "ip": fp["user_ip_prefix"],
            }
        )

        # 🔄 verifica se precisa rotacionar refresh
        _, is_expiring = refresh_token_time_left(db, refresh_token)

        if is_expiring:
            new_refresh = auth.create_refresh_token(
                {
                    "sub": str(user_id),
                    "fp": fp["fp"],
                    "ua": fp["user_agent"],
                    "ip": fp["user_ip_prefix"],
                }
            )

            # ✅ ROTACIONA (correto)
            rotate_refresh_token(db, refresh_token, new_refresh, int(user_id), fp)

            refresh_token = new_refresh
            log_message("🔄 Refresh token rotacionado", "info")

        # 🍪 cookies seguras
        set_cookie(response, "access_token", access_token, path="/")
        set_cookie(response, "refresh_token", refresh_token, path="/")

        return users_schemas.AccessTokenOut(access_token="ok", token_type="bearer")

    except HTTPException as err:
        log_message(f"❌ HTTP error: {err.detail}", "warning")
        raise

    except Exception as e:
        log_message(f"💥 Erro inesperado: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro interno no servidor")


@router.get("/me", response_model=users_schemas.UserOut)
async def get_current_user(
    request: Request,
    access_token: str | None = Cookie(None, alias="access_token"),
    db: Session = Depends(database.get_db),
):
    if not access_token:
        raise HTTPException(status_code=401, detail="Não autenticado")

    try:
        payload = assert_access_token_binding(request, access_token)
        user_id = payload.get("sub")

        if not user_id:
            raise HTTPException(status_code=401, detail="Token inválido")

        user = db.get(user_model.User, int(user_id))
        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")

        return user

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"💥 erro auth: {e}", "error")
        raise HTTPException(status_code=401, detail="Sessão inválida")


@router.post("/logout")
async def logout_user(
    request: Request,
    response: Response,
    refresh_token: str | None = Cookie(None, alias="refresh_token"),
    db: Session = Depends(database.get_db),
):
    try:
        user_id = None

        if refresh_token:
            try:
                fp = build_fingerprint(request, FINGERPRINT_SALT)

                # 🔐 valida binding (sem quebrar fluxo)
                payload = assert_refresh_token_binding(db, refresh_token, fp)
                user_id = payload.get("sub")

            except Exception as e:
                # 🔥 NÃO trava logout
                log_message(f"⚠️ Logout com token inválido: {e}", "warning")

            finally:
                # 🔒 sempre revoga se existir
                revoke_token(db, refresh_token)

        # 🔥 opcional: logout global (todos dispositivos)
        # if user_id:
        #     revoke_all_user_tokens(db, int(user_id))

        # 🍪 remove cookies SEMPRE
        _delete_auth_cookies(response)

        log_message(f"👋 Logout realizado user_id={user_id}", "info")

        return {"message": "Logout efetuado com sucesso."}

    except Exception as e:
        log_message(f"💥 erro no logout: {e}", "error")

        # ⚠️ mesmo com erro, remove cookies (UX primeiro)
        _delete_auth_cookies(response)

        return {"message": "Logout efetuado."}
