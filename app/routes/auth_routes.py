import asyncio
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from email_validator import validate_email, EmailNotValidError
from fastapi import APIRouter, Depends, HTTPException, status, Response, Request, Security, BackgroundTasks
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
    db: Session = Depends(database.get_db),
):
    email_norm = (user.email or "").strip().lower()
    if not email_norm:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="E-mail é obrigatório.",
            headers={"X-Error-Code": "EMAIL_REQUIRED"},
        )

    # 1. 🔍 Validação rigorosa de sintaxe e entregabilidade do domínio (MX record)
    try:
        validate_email(email_norm, check_deliverability=True)
    except EmailNotValidError as exc:
        log_message(f"❌ E-mail inválido ou domínio inexistente no registo ({email_norm}): {exc}", "warning")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"E-mail inválido ou inexistente: {exc}",
            headers={"X-Error-Code": "INVALID_EMAIL"},
        )

    # 2. ⚡ Verifica se o e-mail já existe na base de dados
    existing_user = user_crud.get_user_by_email(db, email_norm)
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