import asyncio
import json
import secrets
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
from email_validator import validate_email, EmailNotValidError
from fastapi import APIRouter, Depends, HTTPException, status, Response, Request, Security, BackgroundTasks
from fastapi.responses import RedirectResponse
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
from app.ultils.servico_SMPP_smtp import send_email

router = APIRouter(prefix="/auth", tags=["Auth"])

COOKIE_SECURE = get_env("COOKIE_SECURE", "false").lower() == "true"
COOKIE_SAMESITE = get_env("COOKIE_SAMESITE", "lax") or "none"
COOKIE_DOMAIN = get_env("COOKIE_DOMAIN")
TRUST_PROXY_HEADERS = get_env("TRUST_PROXY_HEADERS", "false").lower() == "true"
COOKIE_HTTPONLY = get_env("COOKIE_HTTPONLY", "true").lower() == "true"
FINGERPRINT_SALT = get_env("FINGERPRINT_SALT", "change-me-please")
ENV = get_env("ENV", "development").lower()
FRONTEND_URL = get_env("FRONTEND_URL", "http://localhost:3000").rstrip("/")
API_BASE_URL = get_env("API_BASE_URL", get_env("BACKEND_URL", "http://localhost:8000")).rstrip("/")

# -----------------------------
# 🌐 Provedores OAuth2 / Social Login / Auth0
# -----------------------------
AUTH0_DOMAIN = get_env("AUTH0_DOMAIN", "").strip().rstrip("/")
AUTH0_BASE_URL = f"https://{AUTH0_DOMAIN}" if AUTH0_DOMAIN and not AUTH0_DOMAIN.startswith("http") else AUTH0_DOMAIN

OAUTH_PROVIDERS = {
    "google": {
        "client_id": get_env("GOOGLE_CLIENT_ID", ""),
        "client_secret": get_env("GOOGLE_CLIENT_SECRET", ""),
        "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "userinfo_url": "https://www.googleapis.com/oauth2/v2/userinfo",
        "scopes": ["openid", "email", "profile"],
    },
    "github": {
        "client_id": get_env("GITHUB_CLIENT_ID", ""),
        "client_secret": get_env("GITHUB_CLIENT_SECRET", ""),
        "auth_url": "https://github.com/login/oauth/authorize",
        "token_url": "https://github.com/login/oauth/access_token",
        "userinfo_url": "https://api.github.com/user",
        "emails_url": "https://api.github.com/user/emails",
        "scopes": ["read:user", "user:email"],
    },
    "gitlab": {
        "client_id": get_env("GITLAB_CLIENT_ID", ""),
        "client_secret": get_env("GITLAB_CLIENT_SECRET", ""),
        "auth_url": f"{get_env('GITLAB_URL', 'https://gitlab.com').rstrip('/')}/oauth/authorize",
        "token_url": f"{get_env('GITLAB_URL', 'https://gitlab.com').rstrip('/')}/oauth/token",
        "userinfo_url": f"{get_env('GITLAB_URL', 'https://gitlab.com').rstrip('/')}/api/v4/user",
        "scopes": ["read_user", "openid", "profile", "email"],
    },
    "microsoft": {
        "client_id": get_env("MICROSOFT_CLIENT_ID", ""),
        "client_secret": get_env("MICROSOFT_CLIENT_SECRET") or get_env("MICROSOFT_CLIENT_VALOR", ""),
        "auth_url": f"https://login.microsoftonline.com/{get_env('MICROSOFT_TENANT_ID', 'common')}/oauth2/v2.0/authorize",
        "token_url": f"https://login.microsoftonline.com/{get_env('MICROSOFT_TENANT_ID', 'common')}/oauth2/v2.0/token",
        "userinfo_url": "https://graph.microsoft.com/v1.0/me",
        "scopes": ["openid", "profile", "email", "User.Read"],
    },
}

if AUTH0_BASE_URL:
    OAUTH_PROVIDERS["auth0"] = {
        "client_id": get_env("AUTH0_CLIENT_ID", ""),
        "client_secret": get_env("AUTH0_CLIENT_SECRET", ""),
        "auth_url": f"{AUTH0_BASE_URL}/authorize",
        "token_url": f"{AUTH0_BASE_URL}/oauth/token",
        "userinfo_url": f"{AUTH0_BASE_URL}/userinfo",
        "scopes": ["openid", "profile", "email"],
    }


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


class VerifyEmailRequest(users_schemas.BaseModel):
    token: str


class ResendVerificationRequest(users_schemas.BaseModel):
    email: users_schemas.EmailStr


def send_registration_confirmation_email(
    recipient_email: str,
    user_name: str,
    empresa: Optional[str] = None,
    verification_token: Optional[str] = None,
) -> dict:
    """Envia um e-mail de confirmação de registo via SMTP com link seguro de verificação."""
    verification_url = (
        f"{FRONTEND_URL}/auth/verify-email?token={verification_token}"
        if verification_token
        else f"{FRONTEND_URL}/auth/login"
    )
    login_url = f"{FRONTEND_URL}/auth/login"

    try:
        resultado = send_email(
            to=recipient_email,
            subject="Confirmação de Registo - MustaInf",
            template_name="confirmacao_registo",
            context={
                "user_name": user_name,
                "recipient_email": recipient_email,
                "empresa": empresa,
                "verification_url": verification_url,
                "login_url": login_url,
                "platform_name": "MustaInf",
                "validity_hours": auth.EMAIL_VERIFICATION_EXPIRE_HOURS,
            },
        )
        if resultado.get("success"):
            log_message(f"📧 E-mail de confirmação de registo enviado para {recipient_email}", "success")
        else:
            log_message(f"⚠️ Não foi possível enviar e-mail de confirmação para {recipient_email}: {resultado.get('message')}", "warning")
        return resultado
    except Exception as exc:
        log_message(f"❌ Falha ao tentar enviar e-mail de registo via SMTP: {exc}", "error")
        return {"success": False, "message": str(exc), "error": str(exc)}


@router.post(
    "/register",
    response_model=users_schemas.UserOut,
    status_code=status.HTTP_201_CREATED,
)
async def register_user(
    user: users_schemas.UserCreate,
    response: Response,
    request: Request,
    db: Session = Depends(database.get_db),
):
    email_norm = (user.email or "").strip().lower()
    if not email_norm:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="E-mail é obrigatório.",
            headers={"X-Error-Code": "EMAIL_REQUIRED"},
        )

    # 🔍 Verifica se o registo é proveniente de um fluxo OAuth pendente (via header, cookie, query ou payload)
    raw_token = (
        request.headers.get("x-oauth-token")
        or request.cookies.get("oauth_pending")
        or request.query_params.get("oauth_token")
        or getattr(user, "oauth_token", None)
    )
    oauth_pending = None
    if raw_token:
        try:
            decrypted = aes_decrypt(raw_token)
            data = json.loads(decrypted)
            if data.get("email", "").strip().lower() == email_norm:
                oauth_pending = data
        except Exception as e:
            log_message(f"⚠️ Erro ao decifrar oauth_pending no registo: {e}", "warning")

    # 1. 🔍 Validação de sintaxe do e-mail
    try:
        validate_email(email_norm, check_deliverability=not bool(oauth_pending))
    except EmailNotValidError as exc:
        log_message(f"❌ E-mail inválido ou domínio inexistente no registo ({email_norm}): {exc}", "warning")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"E-mail inválido ou inexistente: {exc}",
            headers={"X-Error-Code": "INVALID_EMAIL"},
        )

    # 2. ⚡ Verifica se o utilizador já existe na base de dados (antes de criar)
    existing_user = user_crud.get_user_by_email(db, email_norm)

    # 3. 🌟 SE FOR REGISTO OAUTH PENDENTE (Auth0 / Google / GitHub / etc.)
    # NÃO precisa de verificação por e-mail: é verificado e ativado automaticamente!
    # E se o utilizador já existir, UTILIZA a conta existente sem recriar nada.
    if oauth_pending:
        log_message(f"✨ Concluindo registo OAuth ({oauth_pending.get('provider')}) para: {email_norm} (verificado automaticamente)", "info")
        try:
            if existing_user:
                # ♻️ Reutiliza o utilizador existente: atualiza dados cadastrais fornecidos
                if hasattr(user, "senha") and user.senha:
                    existing_user.hashed_password = auth.hash_password(user.senha)
                if user.nome:
                    existing_user.nome = user.nome.strip()
                if user.apelido:
                    existing_user.apelido = user.apelido.strip()
                if user.telefone:
                    existing_user.telefone = user.telefone.strip()
                if user.empresa:
                    emp = user_crud.get_or_create_empresa(db, user.empresa)
                    existing_user.empresa_id = emp.id if emp else None
                if user.cargo:
                    crg = user_crud.get_or_create_cargo(db, user.cargo)
                    existing_user.cargo_id = crg.id if crg else None
                existing_user.email_verified = True
                existing_user.is_active = True
                if oauth_pending.get("avatar_url") and not existing_user.avatar_url:
                    existing_user.avatar_url = oauth_pending["avatar_url"]
                db_user = existing_user
                db.flush()
            else:
                # Cria novo utilizador com verificação automática
                db_user = user_crud.create_user(db, user)
                db_user.email_verified = True
                db_user.is_active = True
                if oauth_pending.get("avatar_url") and not db_user.avatar_url:
                    db_user.avatar_url = oauth_pending["avatar_url"]
                db.flush()

            # Cria ou atualiza a conta OAuth na tabela oauth_accounts
            prov = oauth_pending.get("provider")
            prov_uid = str(oauth_pending.get("provider_user_id"))

            expires_at_dt = None
            if oauth_pending.get("expires_at"):
                try:
                    expires_at_dt = datetime.fromisoformat(oauth_pending["expires_at"])
                except Exception:
                    pass

            oauth_acc = (
                db.query(user_model.OAuthAccount)
                .filter(
                    user_model.OAuthAccount.provider == prov,
                    user_model.OAuthAccount.provider_user_id == prov_uid,
                )
                .first()
            )
            if oauth_acc:
                oauth_acc.user_id = db_user.id
                oauth_acc.email = email_norm
                oauth_acc.name = oauth_pending.get("name") or db_user.nome
                oauth_acc.avatar_url = oauth_pending.get("avatar_url") or oauth_acc.avatar_url
                if oauth_pending.get("access_token"):
                    oauth_acc.access_token = oauth_pending["access_token"]
                if oauth_pending.get("refresh_token"):
                    oauth_acc.refresh_token = oauth_pending["refresh_token"]
                if expires_at_dt:
                    oauth_acc.expires_at = expires_at_dt
                if oauth_pending.get("raw_data"):
                    oauth_acc.raw_data = oauth_pending["raw_data"]
            else:
                novo_oauth = user_model.OAuthAccount(
                    user_id=db_user.id,
                    provider=prov,
                    provider_user_id=prov_uid,
                    email=email_norm,
                    name=oauth_pending.get("name") or db_user.nome,
                    avatar_url=oauth_pending.get("avatar_url"),
                    access_token=oauth_pending.get("access_token"),
                    refresh_token=oauth_pending.get("refresh_token"),
                    expires_at=expires_at_dt,
                    raw_data=oauth_pending.get("raw_data"),
                )
                db.add(novo_oauth)

            db.commit()
            db.refresh(db_user)

            # Inicia sessão imediatamente (gera tokens, fingerprint e cookies de autenticação)
            fp = build_fingerprint(request, FINGERPRINT_SALT)
            access_token = auth.create_access_token(
                {
                    "sub": aes_encrypt(str(db_user.id)),
                    "fp": aes_encrypt(fp["fp"]),
                    "ua": aes_encrypt(fp["user_agent"]),
                    "ip": aes_encrypt(fp["user_ip_prefix"]),
                }
            )
            refresh_token = auth.create_refresh_token(
                {
                    "sub": aes_encrypt(str(db_user.id)),
                    "fp": aes_encrypt(fp["fp"]),
                    "ua": aes_encrypt(fp["user_agent"]),
                    "ip": aes_encrypt(fp["user_ip_prefix"]),
                }
            )
            store_refresh_token(db, refresh_token, db_user.id, REFRESH_TOKEN_EXPIRE_DAYS, fp)

            set_cookie(response, "refresh_token", refresh_token, path="/")
            set_cookie(response, "access_token", access_token, path="/")
            response.set_cookie(
                key="bk_access_token",
                value=db_user.email,
                max_age=60 * 60 * 24 * 7,
                path="/",
                httponly=False,
                secure=COOKIE_SECURE,
                samesite=COOKIE_SAMESITE,
            )
            response.delete_cookie(key="oauth_pending", path="/")
            response.headers["X-Verification-Action"] = "oauth_registered"

            log_message(f"🎉 Conta criada e vinculada com sucesso via OAuth ({prov}): {db_user.email}", "success")

            return {
                **db_user.__dict__,
                "id": db_user.id,
                "permissions": list(db_user.permissions),
                "role": db_user.role,
            }
        except ValueError as e:
            db.rollback()
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        except Exception as e:
            db.rollback()
            log_message(f"💥 Erro ao registrar usuário OAuth: {e}\n{traceback.format_exc()}", "error")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Erro interno ao criar usuário: {str(e)}",
            )

        # 4. ⚡ Registo Tradicional (sem OAuth): Verifica se o e-mail já existe na base de dados
    if existing_user:
        if existing_user.email_verified:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Este e-mail já se encontra registado e confirmado. Por favor, inicie sessão.",
                headers={"X-Error-Code": "ALREADY_REGISTERED_VERIFIED"},
            )

        # O e-mail já existe mas ainda NÃO foi verificado:
        # Consulta o token mais recente não utilizado
        latest_token = (
            db.query(user_model.EmailVerificationToken)
            .filter(
                user_model.EmailVerificationToken.user_id == existing_user.id,
                user_model.EmailVerificationToken.is_used == False,
            )
            .order_by(user_model.EmailVerificationToken.created_at.desc())
            .first()
        )

        now_utc = datetime.now(timezone.utc)
        token_expirou = True
        if latest_token and latest_token.expires_at:
            exp = (
                latest_token.expires_at
                if latest_token.expires_at.tzinfo
                else latest_token.expires_at.replace(tzinfo=timezone.utc)
            )
            token_expirou = now_utc > exp
        else:
            token_expirou = True

        # Se o token ainda NÃO expirou: pede para ele confirmar o e-mail
        if not token_expirou:
            log_message(
                f"ℹ️ Registo repetido para {email_norm}: conta aguarda confirmação e ligação anterior ainda se encontra válida.",
                "info",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Este e-mail já foi registado mas aguarda confirmação. Enviámos recentemente uma ligação de confirmação que ainda se encontra válida. Por favor, verifique a sua caixa de entrada (ou pasta de spam) e confirme o seu e-mail.",
                headers={"X-Error-Code": "ALREADY_REGISTERED_UNVERIFIED"},
            )

        # Caso o token de verificação tenha expirado: apenas envia um novo e-mail de verificação
        log_message(
            f"🔄 Registo para {email_norm}: token anterior expirado. A enviar novo e-mail de confirmação.",
            "info",
        )
        token_verificacao = auth.create_email_verification_token(email_norm)
        nome_destinatario = (user.nome or "").strip() or existing_user.nome or "Utilizador"
        nome_empresa = (
            user.empresa.nome
            if user.empresa
            else (existing_user.empresa.nome if getattr(existing_user, "empresa", None) else None)
        )

        resultado_email = await asyncio.to_thread(
            send_registration_confirmation_email,
            recipient_email=email_norm,
            user_name=nome_destinatario,
            empresa=nome_empresa,
            verification_token=token_verificacao,
        )

        if not resultado_email.get("success"):
            motivo = resultado_email.get("message") or "Servidor de e-mail rejeitou o destinatário."
            log_message(
                f"❌ Falha ao reenviar e-mail de confirmação no registo para {email_norm}: {motivo}",
                "warning",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"E-mail inválido ou não foi possível entregar a confirmação ({motivo}).",
                headers={"X-Error-Code": "EMAIL_DELIVERY_FAILED"},
            )

        # Regista o novo token na tabela email_verification_tokens com o novo prazo
        prazo = datetime.now(timezone.utc) + timedelta(hours=auth.EMAIL_VERIFICATION_EXPIRE_HOURS)
        novo_token_registro = user_model.EmailVerificationToken(
            token=token_verificacao,
            user_id=existing_user.id,
            expires_at=prazo,
            is_used=False,
        )
        db.add(novo_token_registro)

        # Atualiza a senha e dados cadastrais caso o utilizador os tenha fornecido neste novo envio
        if hasattr(user, "senha") and user.senha:
            existing_user.hashed_password = auth.hash_password(user.senha)
        if user.nome:
            existing_user.nome = user.nome.strip()
        if user.apelido:
            existing_user.apelido = user.apelido.strip()
        if user.telefone:
            existing_user.telefone = user.telefone.strip()

        db.commit()
        db.refresh(existing_user)

        log_message(f"✅ Novo e-mail de confirmação enviado para {email_norm} com sucesso (token renovado).", "success")

        response.headers["X-Verification-Action"] = "token_renewed"

        return {
            **existing_user.__dict__,
            "id": existing_user.id,
            "permissions": list(existing_user.permissions),
            "role": existing_user.role,
        }

    # 3. 📧 Envia PRIMEIRO o e-mail de confirmação com link de validação e certifica-se de que foi entregue/aceite
    nome_destinatario = (user.nome or "").strip() or "Utilizador"
    nome_empresa = user.empresa.nome if user.empresa else None
    token_verificacao = auth.create_email_verification_token(email_norm)

    resultado_email = await asyncio.to_thread(
        send_registration_confirmation_email,
        recipient_email=email_norm,
        user_name=nome_destinatario,
        empresa=nome_empresa,
        verification_token=token_verificacao,
    )

    if not resultado_email.get("success"):
        motivo = resultado_email.get("message") or "Servidor de e-mail rejeitou o destinatário."
        log_message(
            f"❌ Registo cancelado para {email_norm}. Falha na entrega do e-mail de confirmação: {motivo}",
            "warning",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"E-mail inválido ou não foi possível entregar a confirmação ({motivo}). O registo não foi concluído.",
            headers={"X-Error-Code": "EMAIL_DELIVERY_FAILED"},
        )

    # 4. 💾 Se o e-mail foi validado e entregue com sucesso, cria e salva o utilizador na BD
    try:
        db_user = user_crud.create_user(db, user)

        # 🎫 Regista o token na tabela email_verification_tokens com data de criação e prazo de validade
        prazo = datetime.now(timezone.utc) + timedelta(hours=auth.EMAIL_VERIFICATION_EXPIRE_HOURS)
        token_registro = user_model.EmailVerificationToken(
            token=token_verificacao,
            user_id=db_user.id,
            expires_at=prazo,
            is_used=False,
        )
        db.add(token_registro)
        db.commit()

        response.headers["X-Verification-Action"] = "new_user"

        return {
            **db_user.__dict__,
            "id": db_user.id,
            "permissions": list(db_user.permissions),
            "role": db_user.role,
        }
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno ao criar usuário: {str(e)}",
        )


@router.get("/verify-email")
def verify_email_get(
    token: str,
    db: Session = Depends(database.get_db),
):
    """Valida o e-mail do utilizador através do token assinado e regista a validação na base de dados."""
    try:
        email = auth.verify_email_token(token)
    except auth.TokenExpiredError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A ligação de confirmação expirou. O prazo de validade terminou. Por favor solicite um novo e-mail.",
        )
    except auth.TokenInvalidError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A ligação de confirmação é inválida ou foi corrompida. Por favor solicite um novo e-mail.",
        )

    user = user_crud.get_user_by_email(db, email)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Utilizador associado a este token não foi encontrado.",
        )

    # 🔍 Consulta o registro do token na tabela email_verification_tokens
    token_record = (
        db.query(user_model.EmailVerificationToken)
        .filter(user_model.EmailVerificationToken.token == token)
        .first()
    )

    now_utc = datetime.now(timezone.utc)

    if token_record:
        if token_record.is_used:
            return {
                "success": True,
                "already_verified": True,
                "message": "Este endereço de e-mail já foi confirmado anteriormente. A sua conta está ativa.",
                "email": user.email,
                "nome": user.nome,
                "validated_at": token_record.validated_at.isoformat() if token_record.validated_at else None,
            }

        exp = (
            token_record.expires_at
            if token_record.expires_at.tzinfo
            else token_record.expires_at.replace(tzinfo=timezone.utc)
        )
        if now_utc > exp:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A ligação de confirmação expirou. O prazo de validade terminou. Por favor solicite um novo e-mail.",
            )

        # ✅ Atualiza a data validada e marca como utilizado
        token_record.validated_at = now_utc
        token_record.is_used = True
    elif user.email_verified:
        return {
            "success": True,
            "already_verified": True,
            "message": "Este endereço de e-mail já foi confirmado anteriormente. A sua conta está ativa.",
            "email": user.email,
            "nome": user.nome,
        }

    user.email_verified = True
    user.is_active = True
    db.commit()
    log_message(f"✅ E-mail validado e ativado com sucesso para: {email}", "success")

    return {
        "success": True,
        "message": "E-mail confirmado com sucesso! A sua conta foi ativada.",
        "email": user.email,
        "nome": user.nome,
        "validated_at": token_record.validated_at.isoformat() if token_record and token_record.validated_at else now_utc.isoformat(),
    }


@router.post("/verify-email")
def verify_email_post(
    payload: VerifyEmailRequest,
    db: Session = Depends(database.get_db),
):
    """Valida o e-mail do utilizador via requisição POST JSON com o token."""
    return verify_email_get(token=payload.token, db=db)


@router.post("/resend-verification")
async def resend_verification_email(
    payload: ResendVerificationRequest,
    db: Session = Depends(database.get_db),
):
    """Reenvia o e-mail de ativação e confirmação com novo token seguro e regista na base de dados."""
    email_norm = payload.email.strip().lower()
    user = user_crud.get_user_by_email(db, email_norm)
    if not user:
        return {
            "success": True,
            "message": "Se o e-mail estiver registado, a mensagem de confirmação foi reenviada.",
        }

    if user.email_verified:
        return {
            "success": True,
            "already_verified": True,
            "message": "Este endereço de e-mail já se encontra confirmado e ativo.",
        }

    token = auth.create_email_verification_token(user.email)

    # 🎫 Salva o novo token na tabela email_verification_tokens com data de criação e prazo de validade
    prazo = datetime.now(timezone.utc) + timedelta(hours=auth.EMAIL_VERIFICATION_EXPIRE_HOURS)
    token_registro = user_model.EmailVerificationToken(
        token=token,
        user_id=user.id,
        expires_at=prazo,
        is_used=False,
    )
    db.add(token_registro)
    db.commit()

    nome_empresa = user.empresa.nome if getattr(user, "empresa", None) else None

    resultado = await asyncio.to_thread(
        send_registration_confirmation_email,
        recipient_email=user.email,
        user_name=user.nome,
        empresa=nome_empresa,
        verification_token=token,
    )

    if not resultado.get("success"):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Não foi possível reenviar o e-mail de confirmação: {resultado.get('message')}",
        )

    return {
        "success": True,
        "message": "E-mail de confirmação reenviado com sucesso. Verifique a sua caixa de entrada.",
    }


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

        # 🔒 Valida se o e-mail da conta foi confirmado
        if not user.email_verified:
            log_message(f"⚠️ Login bloqueado: e-mail não verificado ({user.email})", "warning")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="E-mail não verificado. Por favor confirme o seu e-mail através da ligação enviada para a sua caixa de entrada.",
            )

        # 🔒 Valida se a conta está ativa
        if not user.is_active:
            log_message(f"⚠️ Login bloqueado: conta inativa ({user.email})", "warning")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Esta conta está desativada. Por favor contacte o suporte.",
            )

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


# =====================================================================
# 🌐 FUNÇÕES AUXILIARES OAUTH2 / SOCIAL LOGIN
# =====================================================================

def _get_redirect_uri(request: Request, provider: str) -> str:
    """Calcula a URL de callback registrada no provedor."""
    base = API_BASE_URL or str(request.base_url).rstrip("/")
    return f"{base}/auth/{provider}/callback"


def buscar_ou_criar_usuario_oauth(db: Session, user_data: dict) -> user_model.User:
    """
    Busca ou cria o usuário na base de dados e persiste os dados e tokens
    do provedor externo na tabela `oauth_accounts`.
    """
    email_norm = (user_data.get("email") or "").strip().lower()
    provider = (user_data.get("provider") or "").strip().lower()
    provider_user_id = str(user_data.get("id") or "").strip()

    if not provider or not provider_user_id:
        raise ValueError("Dados de identificação do provedor OAuth inválidos ou ausentes.")

    # 1. 🔍 Verifica se esta conta do provedor externo já foi vinculada anteriormente
    oauth_acc = (
        db.query(user_model.OAuthAccount)
        .filter(
            user_model.OAuthAccount.provider == provider,
            user_model.OAuthAccount.provider_user_id == provider_user_id,
        )
        .first()
    )

    if oauth_acc:
        # Atualiza tokens e metadados mais recentes recebidos do provedor
        oauth_acc.email = email_norm or oauth_acc.email
        oauth_acc.name = user_data.get("name") or oauth_acc.name
        oauth_acc.avatar_url = user_data.get("avatar_url") or oauth_acc.avatar_url
        if user_data.get("access_token"):
            oauth_acc.access_token = user_data.get("access_token")
        if user_data.get("refresh_token"):
            oauth_acc.refresh_token = user_data.get("refresh_token")
        if user_data.get("expires_at"):
            oauth_acc.expires_at = user_data.get("expires_at")
        if user_data.get("raw_data"):
            oauth_acc.raw_data = user_data.get("raw_data")

        user = oauth_acc.user
        if user:
            changed = False
            if not user.email_verified:
                user.email_verified = True
                changed = True
            if not user.is_active:
                user.is_active = True
                changed = True
            if user_data.get("avatar_url") and not user.avatar_url:
                user.avatar_url = user_data["avatar_url"]
                changed = True
            if user_data.get("name") and (not user.nome or user.nome == email_norm.split("@")[0]):
                user.nome = user_data["name"].strip()
                changed = True

        db.commit()
        if user:
            db.refresh(user)
            log_message(f"👤 Conta OAuth existente sincronizada [{provider}]: {email_norm or provider_user_id}", "info")
            return user

    # 2. ⚡ Caso não tenha oauth_acc cadastrado, busca se já existe usuário com esse e-mail
    if not email_norm:
        raise ValueError("E-mail não retornado pelo provedor OAuth")

    user = user_crud.get_user_by_email(db, email_norm)
    if user:
        # Usuário já existe por e-mail: vincula a nova conta OAuth ao usuário existente
        if not user.email_verified:
            user.email_verified = True
        if not user.is_active:
            user.is_active = True
        if user_data.get("avatar_url") and not user.avatar_url:
            user.avatar_url = user_data["avatar_url"]
        if user_data.get("name") and (not user.nome or user.nome == email_norm.split("@")[0]):
            user.nome = user_data["name"].strip()
    else:
        # 3. 🎯 Novo usuário: associa o plano padrão gratuito e cria credenciais seguras
        plan = user_crud._default_signup_plan(db)
        random_password = secrets.token_urlsafe(32)
        hashed_pw = auth.hash_password(random_password)
        nome = (user_data.get("name") or "").strip() or email_norm.split("@")[0]

        user = user_model.User(
            nome=nome,
            apelido=email_norm.split("@")[0],
            email=email_norm,
            avatar_url=user_data.get("avatar_url"),
            hashed_password=hashed_pw,
            email_verified=True,
            is_active=True,
            concorda_termos=True,
            plan_id=plan.id,
        )
        db.add(user)
        db.flush()  # obtém user.id para FK

    # 4. 💾 Registra a nova conta na tabela oauth_accounts
    novo_oauth_acc = user_model.OAuthAccount(
        user_id=user.id,
        provider=provider,
        provider_user_id=provider_user_id,
        email=email_norm,
        name=user_data.get("name"),
        avatar_url=user_data.get("avatar_url"),
        access_token=user_data.get("access_token"),
        refresh_token=user_data.get("refresh_token"),
        expires_at=user_data.get("expires_at"),
        raw_data=user_data.get("raw_data"),
    )
    db.add(novo_oauth_acc)
    db.commit()
    db.refresh(user)

    log_message(f"✅ Nova conta OAuth ({provider}) salva na tabela oauth_accounts para usuário {user.email}", "success")
    return user


async def _initiate_oauth_login(provider: str, request: Request, next_url: Optional[str] = None) -> Response:
    """Gera URL de autorização e redireciona para o provedor com proteção de state anti-CSRF."""
    provider_key = provider.lower().replace("-", "_")
    if provider_key == "azure_ad":
        provider_key = "microsoft"

    cfg = OAUTH_PROVIDERS.get(provider_key)
    if not cfg or not cfg.get("client_id"):
        log_message(f"⚠️ Provedor '{provider_key}' chamado sem credenciais configuradas.", "warning")
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail=f"Provedor OAuth '{provider}' não está configurado. Defina as variáveis de ambiente necessárias (ex: {provider_key.upper()}_CLIENT_ID e {provider_key.upper()}_CLIENT_SECRET).",
        )

    state = secrets.token_urlsafe(32)
    redirect_uri = _get_redirect_uri(request, provider_key)

    params = {
        "client_id": cfg["client_id"],
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(cfg["scopes"]),
        "state": state,
    }

    if provider_key == "google":
        params["access_type"] = "offline"
        params["prompt"] = "select_account"
    elif provider_key == "microsoft":
        params["response_mode"] = "query"

    auth_url = f"{cfg['auth_url']}?{urlencode(params)}"
    log_message(f"🔀 Redirecionando para login {provider_key}: {cfg['auth_url']}", "info")

    response = RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)
    response.set_cookie(
        key=f"oauth_state_{provider_key}",
        value=state,
        max_age=600,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        path="/",
    )
    if next_url:
        response.set_cookie(
            key="oauth_next",
            value=next_url,
            max_age=600,
            httponly=True,
            secure=COOKIE_SECURE,
            samesite=COOKIE_SAMESITE,
            path="/",
        )
    return response


async def _handle_oauth_callback(
    provider: str,
    request: Request,
    response: Response,
    db: Session,
    code: Optional[str] = None,
    state: Optional[str] = None,
) -> Response:
    """Processa o callback de retorno de qualquer provedor OAuth, cria a sessão e redireciona."""
    provider_key = provider.lower().replace("-", "_")
    if provider_key == "azure_ad":
        provider_key = "microsoft"

    log_message(f"🔹 OAuth callback iniciado para [{provider_key}]", "info")
    log_message(f"🔹 Estado recebido: {state}", "info")
    log_message(f"🔹 Código recebido: {'presente' if code else 'ausente'}", "info")

    next_url_cookie = request.cookies.get("oauth_next") or "/home"
    frontend_base = FRONTEND_URL or "http://localhost:3000"
    fallback_redirect = f"{frontend_base.rstrip('/')}/auth/login"

    def _error_redirect(msg: str) -> Response:
        log_message(f"❌ Erro OAuth ({provider_key}): {msg}", "error")
        from urllib.parse import quote
        err_url = f"{fallback_redirect}?error={quote(msg)}&provider={provider_key}"
        res = RedirectResponse(url=err_url, status_code=status.HTTP_302_FOUND)
        res.delete_cookie(f"oauth_state_{provider_key}", path="/")
        res.delete_cookie("oauth_next", path="/")
        return res

    # 1. Validação do state anti-CSRF
    cookie_state = request.cookies.get(f"oauth_state_{provider_key}")
    if cookie_state and state and cookie_state != state:
        return _error_redirect("Estado OAuth inválido")

    # 2. Validação do código de autorização
    if not code:
        err_desc = (
            request.query_params.get("error_description")
            or request.query_params.get("error")
            or "Código de autenticação ausente"
        )
        return _error_redirect(err_desc)

    cfg = OAUTH_PROVIDERS.get(provider_key)
    if not cfg:
        return _error_redirect(f"Provedor {provider_key} não suportado")

    redirect_uri = _get_redirect_uri(request, provider_key)

    # 3. Troca de código por token de acesso
    user_data = {"provider": provider_key}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            token_payload = {
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
                "code": code,
                "redirect_uri": redirect_uri,
            }
            if provider_key != "github":
                token_payload["grant_type"] = "authorization_code"

            token_res = await client.post(
                cfg["token_url"],
                data=token_payload,
                headers={"Accept": "application/json"},
            )

            if token_res.status_code != 200:
                log_message(f"❌ Erro ao trocar código no {provider_key}: Status {token_res.status_code} - {token_res.text}", "error")
                return _error_redirect(f"Erro ao obter token do provedor {provider_key}")

            token_data = token_res.json()
            access_token_prov = token_data.get("access_token")
            if not access_token_prov:
                log_message(f"❌ Resposta de token sem access_token: {token_data}", "error")
                return _error_redirect("Token de acesso não retornado pelo provedor")

            # 4. Busca dados do perfil do usuário
            user_headers = {"Authorization": f"Bearer {access_token_prov}"}
            if provider_key == "github":
                user_headers["Accept"] = "application/vnd.github+json"

            user_res = await client.get(cfg["userinfo_url"], headers=user_headers)
            if user_res.status_code != 200:
                log_message(f"❌ Erro ao buscar perfil ({provider_key}): Status {user_res.status_code} - {user_res.text}", "error")
                return _error_redirect(f"Erro ao buscar perfil do {provider_key}")

            profile = user_res.json()

            # Normalização de cada provedor
            if provider_key == "google":
                user_data["id"] = str(profile.get("id"))
                user_data["email"] = profile.get("email")
                user_data["name"] = profile.get("name")
                user_data["avatar_url"] = profile.get("picture")

            elif provider_key == "github":
                user_data["id"] = str(profile.get("id"))
                user_data["name"] = profile.get("name") or profile.get("login")
                user_data["avatar_url"] = profile.get("avatar_url")
                user_data["email"] = profile.get("email")

                # Se o e-mail for privado no GitHub, consulta /user/emails
                if not user_data["email"]:
                    emails_res = await client.get(cfg["emails_url"], headers=user_headers)
                    if emails_res.status_code == 200:
                        emails = emails_res.json()
                        for em in emails:
                            if em.get("primary") and em.get("verified"):
                                user_data["email"] = em.get("email")
                                break
                            elif em.get("verified") and not user_data["email"]:
                                user_data["email"] = em.get("email")

            elif provider_key == "gitlab":
                user_data["id"] = str(profile.get("id"))
                user_data["email"] = profile.get("email")
                user_data["name"] = profile.get("name") or profile.get("username")
                user_data["avatar_url"] = profile.get("avatar_url")

            elif provider_key == "microsoft":
                user_data["id"] = str(profile.get("id"))
                user_data["name"] = profile.get("displayName")
                user_data["email"] = profile.get("mail") or profile.get("userPrincipalName")
                user_data["avatar_url"] = None

            elif provider_key == "auth0":
                user_data["id"] = str(profile.get("sub"))
                user_data["email"] = profile.get("email")
                user_data["name"] = profile.get("name") or profile.get("nickname")
                user_data["avatar_url"] = profile.get("picture")

            # Salva tokens e payload bruto recebidos do provedor
            user_data["access_token"] = access_token_prov
            user_data["refresh_token"] = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in")
            if expires_in and isinstance(expires_in, (int, float)):
                user_data["expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
            else:
                user_data["expires_at"] = None

            try:
                user_data["raw_data"] = json.dumps(profile)
            except Exception:
                user_data["raw_data"] = None

    except Exception as exc:
        log_message(f"💥 Erro na comunicação com {provider_key}: {exc}\n{traceback.format_exc()}", "error")
        return _error_redirect(f"Falha na comunicação com {provider_key}")

    email = (user_data.get("email") or "").strip().lower()
    if not email:
        return _error_redirect(f"O provedor {provider_key} não disponibilizou um e-mail válido")

    # 5. Verifica se o utilizador já existe no sistema (por conta OAuth ou por e-mail)
    provider = user_data["provider"]
    provider_user_id = str(user_data["id"])

    oauth_acc = (
        db.query(user_model.OAuthAccount)
        .filter(
            user_model.OAuthAccount.provider == provider,
            user_model.OAuthAccount.provider_user_id == provider_user_id,
        )
        .first()
    )
    existing_user = oauth_acc.user if oauth_acc else user_crud.get_user_by_email(db, email)

    if not existing_user:
        # 📝 Utilizador ainda não existe na base de dados!
        # Deve terminar de preencher o formulário no frontend (empresa, cargo, telefone, senha).
        log_message(
            f"📝 Novo utilizador detetado via {provider} ({email}). Redirecionando para preenchimento de registo no frontend.",
            "info",
        )
        expires_at_str = user_data["expires_at"].isoformat() if user_data.get("expires_at") else None
        oauth_pending_payload = {
            "provider": provider,
            "provider_user_id": provider_user_id,
            "email": email,
            "name": user_data.get("name"),
            "avatar_url": user_data.get("avatar_url"),
            "access_token": user_data.get("access_token"),
            "refresh_token": user_data.get("refresh_token"),
            "expires_at": expires_at_str,
            "raw_data": user_data.get("raw_data"),
        }

        full_name = (user_data.get("name") or "").strip()
        parts = full_name.split(" ", 1) if full_name else []
        first_name = parts[0] if parts else email.split("@")[0]
        last_name = parts[1] if len(parts) > 1 else ""

        encrypted_pending_token = aes_encrypt(json.dumps(oauth_pending_payload))
        reg_params = {
            "oauth": "1",
            "provider": provider,
            "email": email,
            "firstName": first_name,
            "lastName": last_name,
            "oauth_token": encrypted_pending_token,
        }
        if user_data.get("avatar_url"):
            reg_params["avatar_url"] = user_data["avatar_url"]

        reg_url = f"{frontend_base.rstrip('/')}/auth/register?{urlencode(reg_params)}"
        reg_redirect = RedirectResponse(url=reg_url, status_code=status.HTTP_302_FOUND)

        # Cookie cifrado (15 min) contendo tokens e dados do provedor para vincular no registo
        reg_redirect.set_cookie(
            key="oauth_pending",
            value=encrypted_pending_token,
            max_age=15 * 60,
            path="/",
            httponly=True,
            secure=COOKIE_SECURE,
            samesite=COOKIE_SAMESITE,
        )
        reg_redirect.delete_cookie(key=f"oauth_state_{provider_key}", path="/")
        reg_redirect.delete_cookie(key=f"oauth_next_{provider_key}", path="/")
        reg_redirect.delete_cookie(key="oauth_next", path="/")
        return reg_redirect

    # Se já existe utilizador: sincroniza/vincula a conta OAuth e segue para login
    try:
        user = buscar_ou_criar_usuario_oauth(db, user_data)
    except Exception as exc:
        log_message(f"❌ Erro ao buscar/criar usuário OAuth: {exc}\n{traceback.format_exc()}", "error")
        return _error_redirect("Erro ao processar conta de usuário")

    # 6. Gera fingerprint e tokens de sessão (JWT com AES)
    fp = build_fingerprint(request, FINGERPRINT_SALT)

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

    # Reativa conexão ativa da base de dados se houver
    try:
        reativar_connection(user.id, db)
    except Exception:
        pass

    # 7. Redirecionamento de sucesso para o Frontend (Fallback)
    target_path = next_url_cookie if next_url_cookie.startswith("/") else f"/{next_url_cookie}"
    if "/auth/login" in target_path or target_path == "/":
        target_path = "/home"

    redirect_target = f"{frontend_base.rstrip('/')}{target_path}"
    log_message(f"🔀 Redirecionando utilizador autenticado ({user.email}) para: {redirect_target}", "info")

    success_redirect = RedirectResponse(url=redirect_target, status_code=status.HTTP_302_FOUND)

    # 8. Define os cookies de autenticação
    set_cookie(success_redirect, "refresh_token", refresh_token, path="/")
    set_cookie(success_redirect, "access_token", access_token, path="/")

    # Cookie bk_access_token para integração com middleware Next.js
    success_redirect.set_cookie(
        key="bk_access_token",
        value=user.email,
        max_age=60 * 60 * 24 * 7,
        path="/",
        httponly=False,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
    )

    # Remove cookies temporários do fluxo OAuth
    success_redirect.delete_cookie(f"oauth_state_{provider_key}", path="/")
    success_redirect.delete_cookie("oauth_next", path="/")

    return success_redirect


# =====================================================================
# 🌐 ROTAS DE LOGIN OAUTH (REDIRECIONAMENTO)
# =====================================================================

@router.get("/google/login", summary="Iniciar login Google")
@router.get("/oauth2/google/login", include_in_schema=False)
@router.get("/google", include_in_schema=False)
async def google_login(request: Request, next: Optional[str] = None):
    return await _initiate_oauth_login("google", request, next)


@router.get("/github/login", summary="Iniciar login GitHub")
@router.get("/oauth2/github/login", include_in_schema=False)
@router.get("/github", include_in_schema=False)
async def github_login(request: Request, next: Optional[str] = None):
    return await _initiate_oauth_login("github", request, next)


@router.get("/gitlab/login", summary="Iniciar login GitLab")
@router.get("/oauth2/gitlab/login", include_in_schema=False)
@router.get("/gitlab", include_in_schema=False)
async def gitlab_login(request: Request, next: Optional[str] = None):
    return await _initiate_oauth_login("gitlab", request, next)


@router.get("/microsoft/login", summary="Iniciar login Microsoft")
@router.get("/oauth2/microsoft/login", include_in_schema=False)
@router.get("/microsoft", include_in_schema=False)
@router.get("/login/azure-ad", include_in_schema=False)
async def microsoft_login(request: Request, next: Optional[str] = None):
    return await _initiate_oauth_login("microsoft", request, next)


@router.get("/auth0/login", summary="Iniciar login Auth0")
@router.get("/oauth2/auth0/login", include_in_schema=False)
@router.get("/auth0", include_in_schema=False)
async def auth0_login(request: Request, next: Optional[str] = None):
    return await _initiate_oauth_login("auth0", request, next)


# Rota genérica de login
@router.get("/{provider}/login", summary="Iniciar OAuth por Provedor")
@router.get("/oauth2/{provider}/login", include_in_schema=False)
async def generic_oauth_login(provider: str, request: Request, next: Optional[str] = None):
    return await _initiate_oauth_login(provider, request, next)


# =====================================================================
# 🔀 ROTAS DE CALLBACK / FALLBACK OAUTH
# =====================================================================

@router.get("/google/callback", summary="Callback OAuth Google")
@router.get("/oauth2/google/callback", include_in_schema=False)
async def google_callback(
    request: Request,
    response: Response,
    code: Optional[str] = None,
    state: Optional[str] = None,
    db: Session = Depends(database.get_db),
):
    return await _handle_oauth_callback("google", request, response, db, code, state)


@router.get("/github/callback", summary="Callback OAuth GitHub")
@router.get("/oauth2/github/callback", include_in_schema=False)
async def github_callback(
    request: Request,
    response: Response,
    code: Optional[str] = None,
    state: Optional[str] = None,
    db: Session = Depends(database.get_db),
):
    return await _handle_oauth_callback("github", request, response, db, code, state)


@router.get("/gitlab/callback", summary="Callback OAuth GitLab")
@router.get("/oauth2/gitlab/callback", include_in_schema=False)
async def gitlab_callback(
    request: Request,
    response: Response,
    code: Optional[str] = None,
    state: Optional[str] = None,
    db: Session = Depends(database.get_db),
):
    return await _handle_oauth_callback("gitlab", request, response, db, code, state)


@router.get("/microsoft/callback", summary="Callback OAuth Microsoft")
@router.get("/oauth2/microsoft/callback", include_in_schema=False)
async def microsoft_callback(
    request: Request,
    response: Response,
    code: Optional[str] = None,
    state: Optional[str] = None,
    db: Session = Depends(database.get_db),
):
    return await _handle_oauth_callback("microsoft", request, response, db, code, state)


@router.get("/auth0/callback", summary="Callback OAuth Auth0")
@router.get("/oauth2/auth0/callback", include_in_schema=False)
async def auth0_callback(
    request: Request,
    response: Response,
    code: Optional[str] = None,
    state: Optional[str] = None,
    db: Session = Depends(database.get_db),
):
    return await _handle_oauth_callback("auth0", request, response, db, code, state)


@router.get("/oauth2/{provider}/callback", include_in_schema=False)
@router.get("/{provider}/callback", summary="Callback OAuth Genérico")
async def generic_oauth_callback(
    provider: str,
    request: Request,
    response: Response,
    code: Optional[str] = None,
    state: Optional[str] = None,
    db: Session = Depends(database.get_db),
):
    return await _handle_oauth_callback(provider, request, response, db, code, state)


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