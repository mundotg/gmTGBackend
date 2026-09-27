import traceback
from datetime import timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Response, Request, Security
from sqlalchemy.orm import Session

from app import database, auth
from app.config.api_security import cookie_scheme, refresh_cookie_scheme
from app.cruds import user_crud
from app.models import user_model
from app.request_fingerprint import build_fingerprint
from app.schemas import users_schemas
from app.services.crypto_utils import aes_decrypt, aes_encrypt
from app.token_storage import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_DAYS,
    refresh_token_time_left,
    rotate_refresh_token,
    store_refresh_token,
    is_refresh_token_valid,
    revoke_token,
    assert_refresh_token_binding,
)
from app.config.dotenv import get_env
from app.ultils.ativar_session_bd import reativar_connection
from app.ultils.logger import log_message
from app.ultils.rate_limit import limit_login_attempts

router = APIRouter(prefix="/auth", tags=["Auth"])

COOKIE_SECURE = get_env("COOKIE_SECURE", "false").lower() == "true"
COOKIE_SAMESITE = get_env("COOKIE_SAMESITE", "lax") or "none"
COOKIE_DOMAIN = get_env("COOKIE_DOMAIN")
TRUST_PROXY_HEADERS = get_env("TRUST_PROXY_HEADERS", "false").lower() == "true"
COOKIE_HTTPONLY = get_env("COOKIE_HTTPONLY", "true").lower() == "true"
FINGERPRINT_SALT = get_env("FINGERPRINT_SALT", "change-me-please")
ENV = get_env("ENV", "development").lower()


def _cookie_domain():
    return COOKIE_DOMAIN or None


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
            "httponly": COOKIE_HTTPONLY,
            "secure": COOKIE_SECURE,
            "samesite": COOKIE_SAMESITE,
            "max_age": max_age,
            "path": path,
        }

        domain = _cookie_domain()
        if domain:
            cookie_options["domain"] = domain

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
        "httponly": COOKIE_HTTPONLY,
        "samesite": COOKIE_SAMESITE,
    }

    for path in ("/", "/auth", "/auth/refresh", ""):
        response.delete_cookie("refresh_token", path=path, **cookie_options)

    response.delete_cookie("access_token", path="/", **cookie_options)
    response.delete_cookie("bk_access_token", path="/")

    log_message("🍪 Cookies de autenticação removidos", "info")


def internal_error(e: Exception):
    log_message(f"❌ Erro interno: {str(e)}\n{traceback.format_exc()}", "error")
    raise HTTPException(status_code=500, detail="Erro interno no servidor.")


def build_user_out(
    user: user_model.User, info_extra: Any = None
) -> users_schemas.UserOut2:

    roles_encriptadas = None
    if user.role:
        role_dict = {
            k: v for k, v in user.role.__dict__.items() if not k.startswith("_")
        }
        role_dict["name"] = aes_encrypt(user.role.name)
        role_schema = users_schemas.RoleSimpleSchema.model_validate(role_dict)
        roles_encriptadas = [role_schema]

    permissoes_encriptadas = (
        [aes_encrypt(str(perm)) for perm in user.permissions]
        if user.permissions
        else []
    )

    def _encrypt_if_exists(value: Any) -> str:
        return aes_encrypt(str(value)) if value else ""

    return users_schemas.UserOut2(
        id=aes_encrypt(str(user.id)),
        nome=_encrypt_if_exists(user.nome),
        apelido=_encrypt_if_exists(user.apelido),
        email=_encrypt_if_exists(user.email),
        telefone=_encrypt_if_exists(user.telefone),
        empresa=(
            users_schemas.EmpresaSchema.model_validate(user.empresa)
            if user.empresa
            else None
        ),
        cargo=(
            users_schemas.CargoSchema.model_validate(user.cargo) if user.cargo else None
        ),
        roles=roles_encriptadas,
        permissions=permissoes_encriptadas,
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

    # 🚀 Desencripta as variáveis do token antes da comparação
    try:
        token_fp = aes_decrypt(payload.get("fp")) if payload.get("fp") else None
        token_ip = aes_decrypt(payload.get("ip")) if payload.get("ip") else None
        token_ua = aes_decrypt(payload.get("ua")) if payload.get("ua") else None
    except Exception:
        raise HTTPException(status_code=401, detail="Token corrompido")

    if token_fp != fp_now.get("fp"):
        raise HTTPException(status_code=401, detail="Sessão inválida")

    ip_now = fp_now.get("user_ip_prefix")
    if token_ip and ip_now and token_ip != ip_now:
        log_message(f"⚠️ IP divergente token={token_ip} atual={ip_now}", "warning")

    ua_now = fp_now.get("user_agent")
    if token_ua and ua_now and token_ua != ua_now:
        log_message(
            f"⚠️ UA divergente token={token_ua[:30]}... atual={ua_now[:30]}...",
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
            "id": db_user.id,
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
        limit_login_attempts(request, credentials.email)

        user = user_crud.get_user_by_email(db, credentials.email)
        if not user or not auth.verify_password(
            aes_decrypt(credentials.senha), user.hashed_password
        ):
            raise HTTPException(status_code=401, detail="Credenciais inválidas")

        fp = build_fingerprint(request, FINGERPRINT_SALT)

        # 🚀 Tudo encriptado no JWT
        access_token = auth.create_access_token(
            {
                "sub": aes_encrypt(str(user.id)),
                "fp": aes_encrypt(fp["fp"]),
                "ua": aes_encrypt(fp["user_agent"]),
                "ip": aes_encrypt(fp["user_ip_prefix"]),
            }
        )

        refresh_token = auth.create_refresh_token(
            {
                "sub": aes_encrypt(str(user.id)),
                "fp": aes_encrypt(fp["fp"]),
                "ua": aes_encrypt(fp["user_agent"]),
                "ip": aes_encrypt(fp["user_ip_prefix"]),
            }
        )

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
    refresh_token: str | None = Security(refresh_cookie_scheme),
    db: Session = Depends(database.get_db),
):
    try:
        log_message(f"🔄 Refresh token request iniciado", "info")
        if not refresh_token:
            raise HTTPException(status_code=401, detail="Sessão inválida")

        if not is_refresh_token_valid(db, refresh_token):
            raise HTTPException(status_code=401, detail="Sessão expirada ou inválida")

        fp = build_fingerprint(request, FINGERPRINT_SALT)

        try:
            assert_refresh_token_binding(db, refresh_token, fp)
        except ValueError as e:
            log_message(f"🚨 Binding inválido: {e}", "error")
            revoke_token(db, refresh_token)
            raise HTTPException(status_code=401, detail="Sessão inválida")

        payload = get_payload_from_token_or_401(refresh_token)
        encrypted_user_id = payload.get("sub")

        if not encrypted_user_id:
            revoke_token(db, refresh_token)
            raise HTTPException(status_code=401, detail="Token inválido")

        try:
            decrypted_user_id = aes_decrypt(encrypted_user_id)
        except Exception:
            revoke_token(db, refresh_token)
            raise HTTPException(status_code=401, detail="Token corrompido")

        # 🚀 Volta a encriptar tudo para o novo Access Token
        access_token = auth.create_access_token(
            {
                "sub": aes_encrypt(decrypted_user_id),
                "fp": aes_encrypt(fp["fp"]),
                "ua": aes_encrypt(fp["user_agent"]),
                "ip": aes_encrypt(fp["user_ip_prefix"]),
            }
        )

        _, is_expiring = refresh_token_time_left(db, refresh_token)

        if is_expiring:
            # 🚀 Volta a encriptar tudo para o novo Refresh Token
            new_refresh = auth.create_refresh_token(
                {
                    "sub": aes_encrypt(decrypted_user_id),
                    "fp": aes_encrypt(fp["fp"]),
                    "ua": aes_encrypt(fp["user_agent"]),
                    "ip": aes_encrypt(fp["user_ip_prefix"]),
                }
            )
            rotate_refresh_token(db, refresh_token, new_refresh, int(decrypted_user_id), fp)
            refresh_token = new_refresh
            log_message(f"🔄 Refresh token rotacionado para utilizador {decrypted_user_id}", "info")

        set_cookie(response, "access_token", access_token, path="/")
        set_cookie(response, "refresh_token", refresh_token, path="/")

        return users_schemas.AccessTokenOut(access_token="ok", token_type="bearer")

    except HTTPException as err:
        log_message(f"❌ HTTP error: {err.detail}", "warning")
        raise
    except Exception as e:
        log_message(f"💥 Erro inesperado: {e}", "error")
        raise HTTPException(status_code=500, detail="Erro interno no servidor")


@router.get("/me", response_model=users_schemas.UserOut2)
async def get_current_user(
    request: Request,
    access_token: str | None = Security(cookie_scheme),
    db: Session = Depends(database.get_db),
):
    if not access_token:
        raise HTTPException(status_code=401, detail="Não autenticado")

    try:
        payload = assert_access_token_binding(request, access_token)
        encrypted_user_id = payload.get("sub")

        if not encrypted_user_id:
            raise HTTPException(status_code=401, detail="Token inválido")

        try:
            user_id = int(aes_decrypt(encrypted_user_id))
        except Exception:
            raise HTTPException(status_code=401, detail="Token corrompido")

        user = db.get(user_model.User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")
        
        rep = reativar_connection(user.id, db)
        info_extra = rep.get("config") if rep.get("success") else None
        
        return build_user_out(user, info_extra=info_extra)

    except HTTPException:
        raise
    except Exception as e:
        log_message(f"💥 erro auth: {e}", "error")
        raise HTTPException(status_code=401, detail="Sessão inválida")


@router.post("/logout")
async def logout_user(
    request: Request,
    response: Response,
    refresh_token: str | None = Security(refresh_cookie_scheme),
    db: Session = Depends(database.get_db),
):
    try:
        user_id = None

        if refresh_token:
            try:
                fp = build_fingerprint(request, FINGERPRINT_SALT)
                payload = assert_refresh_token_binding(db, refresh_token, fp)
                encrypted_user_id = payload.get("sub")
                
                if encrypted_user_id:
                    try:
                        user_id = aes_decrypt(encrypted_user_id)
                    except Exception:
                        pass

            except Exception as e:
                log_message(f"⚠️ Logout com token inválido: {e}", "warning")
            finally:
                revoke_token(db, refresh_token)

        _delete_auth_cookies(response)
        log_message(f"👋 Logout realizado user_id={user_id}", "info")

        return {"message": "Logout efetuado com sucesso."}

    except Exception as e:
        log_message(f"💥 erro no logout: {e}", "error")
        _delete_auth_cookies(response)
        return {"message": "Logout efetuado."}