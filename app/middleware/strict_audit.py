"""
🛡️ Modo de Auditoria Rigorosa (Strict Audit Mode).

Regista o corpo dos pedidos POST, PUT, PATCH e DELETE.
Palavras-passe e tokens são substituídos antes de gravar.

Só atua se a funcionalidade 'strict_audit' estiver ativada nas definições do sistema:
"Regista o corpo dos pedidos POST, PUT, PATCH e DELETE. Palavras-passe e tokens são substituídos antes de gravar."
"""

from __future__ import annotations

import json
import time
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.database import SessionLocal
from app.services import system_settings_service as definicoes
from app.ultils.logger import log_message

MÉTODOS_AUDITADOS = {"POST", "PUT", "PATCH", "DELETE"}

#: Nomes de campo cujo valor nunca é gravado. Comparados em minúsculas e por
#: inclusão, para apanhar `senha`, `nova_senha`, `password_confirm`, `access_token`, etc.
CAMPOS_SENSIVEIS = (
    "password",
    "senha",
    "token",
    "secret",
    "authorization",
    "api_key",
    "apikey",
    "hashed_password",
    "credential",
    "private_key",
    "access_token",
    "refresh_token",
    "client_secret",
    "auth",
    "code",
    "pin",
    "cvv",
)

REDIGIDO = "***"

#: Acima disto o corpo não é gravado inteiro — evita sobrecarga de memória
#: em uploads ou payloads gigantescos.
MAX_CORPO = 4000

#: Rotas que não devem ser auditadas (infraestrutura, monitorização, docs)
ROTAS_IGNORADAS = (
    "/health",
    "/metrics",
    "/favicon.ico",
    "/docs",
    "/redoc",
    "/openapi.json",
)


def _e_sensivel(nome: str) -> bool:
    """Verifica se o nome do campo corresponde a um dado sensível."""
    minusculo = str(nome).lower()
    return any(marca in minusculo for marca in CAMPOS_SENSIVEIS)


def _redigir(valor: Any) -> Any:
    """Percorre a estrutura recursivamente e substitui o valor dos campos sensíveis."""
    if isinstance(valor, dict):
        return {
            k: (REDIGIDO if _e_sensivel(k) else _redigir(v)) for k, v in valor.items()
        }
    if isinstance(valor, (list, tuple)):
        return [_redigir(v) for v in valor]
    if isinstance(valor, set):
        return {_redigir(v) for v in valor}
    if isinstance(valor, str):
        # Se for string contendo JSON codificado, tenta desserializar e redigir
        stripped = valor.strip()
        if (stripped.startswith("{") and stripped.endswith("}")) or (
            stripped.startswith("[") and stripped.endswith("]")
        ):
            try:
                parsed = json.loads(stripped)
                return json.dumps(_redigir(parsed), ensure_ascii=False, default=str)
            except Exception:
                pass
    return valor


def _redigir_form_urlencoded(texto: str) -> str:
    """Redige pares chave=valor em corpo urlencoded."""
    try:
        pares = parse_qsl(texto, keep_blank_values=True)
        if not pares:
            return texto
        redigidos = [(k, REDIGIDO if _e_sensivel(k) else v) for k, v in pares]
        return urlencode(redigidos)
    except Exception:
        return texto


def _redigir_corpo_bytes(corpo: bytes, content_type: str = "") -> str:
    """
    Decodifica e higieniza o corpo do pedido substituindo segredos por '***'.
    """
    if not corpo:
        return "<corpo vazio>"

    if len(corpo) > MAX_CORPO:
        return f"<corpo com {len(corpo)} bytes, não gravado por exceder o limite>"

    ct = (content_type or "").lower()

    try:
        texto = corpo.decode("utf-8")
    except UnicodeDecodeError:
        return f"<corpo binário com {len(corpo)} bytes>"

    # 1. JSON
    if "application/json" in ct or texto.strip().startswith(("{", "[")):
        try:
            dados = json.loads(texto)
            return json.dumps(_redigir(dados), ensure_ascii=False, default=str)[:MAX_CORPO]
        except (json.JSONDecodeError, ValueError):
            pass

    # 2. Form URL-Encoded
    if "application/x-www-form-urlencoded" in ct or ("=" in texto and not ct.startswith("multipart/")):
        try:
            return _redigir_form_urlencoded(texto)[:MAX_CORPO]
        except Exception:
            pass

    # 3. Multipart / uploads
    if "multipart/form-data" in ct:
        return f"<multipart/form-data com {len(corpo)} bytes>"

    # 4. Outros formatos de texto
    return texto[:MAX_CORPO]


def is_strict_audit_enabled() -> bool:
    """Verifica se o Modo de Auditoria Rigorosa está ativo nas definições do sistema."""
    db = SessionLocal()
    try:
        return bool(definicoes.obter(db, "strict_audit"))
    except Exception as e:
        log_message(f"[strict-audit] Erro ao ler definição 'strict_audit': {e}", "warning")
        return False
    finally:
        db.close()


class StrictAuditMiddleware(BaseHTTPMiddleware):
    """
    Middleware que implementa o Modo de Auditoria Rigorosa.
    Regista o corpo dos pedidos POST, PUT, PATCH e DELETE.
    Palavras-passe e tokens são substituídos antes de gravar.
    Só atua se a funcionalidade for ativada nas definições do sistema.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        # Só audita métodos de escrita/alteração de estado
        if request.method not in MÉTODOS_AUDITADOS:
            return await call_next(request)

        caminho = request.url.path

        # Ignora rotas internas/monitorização
        if any(caminho.startswith(p) for p in ROTAS_IGNORADAS):
            return await call_next(request)

        # Só atua se a definição 'strict_audit' estiver ativada nas definições do sistema
        if not is_strict_audit_enabled():
            return await call_next(request)

        # ── Modo de Auditoria Rigorosa ATIVO ──
        try:
            corpo = await request.body()
        except Exception as e:
            log_message(f"[strict-audit] Falha ao capturar corpo do pedido: {e}", "warning")
            return await call_next(request)

        # Repõe o stream do corpo para que os handlers seguintes possam lê-lo normalmente
        async def receive():
            return {"type": "http.request", "body": corpo, "more_body": False}

        request._receive = receive  # noqa: SLF001

        # Redige os segredos antes de qualquer gravação
        content_type = request.headers.get("content-type", "")
        resumo_corpo = _redigir_corpo_bytes(corpo, content_type)

        user_id, user_email = self._extrair_utilizador(request)
        client_ip = self._extrair_ip(request)
        request_id = getattr(request.state, "request_id", None) or request.headers.get("X-Request-ID", "-")

        started_at = time.perf_counter()
        try:
            response = await call_next(request)
            elapsed_ms = (time.perf_counter() - started_at) * 1000

            self._gravar_auditoria(
                request=request,
                request_id=request_id,
                status_code=response.status_code,
                elapsed_ms=elapsed_ms,
                user_id=user_id,
                user_email=user_email,
                client_ip=client_ip,
                resumo_corpo=resumo_corpo,
            )
            return response
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            self._gravar_auditoria(
                request=request,
                request_id=request_id,
                status_code=500,
                elapsed_ms=elapsed_ms,
                user_id=user_id,
                user_email=user_email,
                client_ip=client_ip,
                resumo_corpo=resumo_corpo,
                erro=str(exc),
            )
            raise

    def _extrair_utilizador(self, request: Request) -> tuple[Optional[int], Optional[str]]:
        """Extrai user_id e email a partir do token JWT no cabeçalho ou cookie."""
        try:
            from app.auth import decode_token

            token = None
            cabecalho = request.headers.get("Authorization", "")
            if cabecalho.lower().startswith("bearer "):
                token = cabecalho.split(" ", 1)[1].strip()
            elif "access_token" in request.cookies:
                token = request.cookies.get("access_token")

            if not token:
                return None, None

            payload = decode_token(token)
            if not payload:
                return None, None

            user_id = payload.get("user_id")
            sub = payload.get("sub")
            if user_id is None and sub is not None and str(sub).isdigit():
                user_id = int(sub)

            email = sub if (sub and "@" in str(sub)) else payload.get("email")
            uid = int(user_id) if user_id is not None and str(user_id).isdigit() else None
            return uid, str(email) if email else None
        except Exception:
            return None, None

    def _extrair_ip(self, request: Request) -> str:
        """Obtém o IP do cliente respeitando cabeçalhos de proxy."""
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip.strip()
        if request.client and request.client.host:
            return request.client.host
        return "127.0.0.1"

    def _gravar_auditoria(
        self,
        request: Request,
        request_id: str,
        status_code: int,
        elapsed_ms: float,
        user_id: Optional[int],
        user_email: Optional[str],
        client_ip: str,
        resumo_corpo: str,
        erro: Optional[str] = None,
    ) -> None:
        try:
            ident = f"user_id={user_id}" if user_id else (f"email={user_email}" if user_email else "anónimo")
            msg_erro = f" [ERRO: {erro}]" if erro else ""
            msg = (
                f"[auditoria] {request.method} {request.url.path} "
                f"(status={status_code}, {elapsed_ms:.0f}ms) "
                f"[{ident}] [ip={client_ip}] [req={request_id}]"
                f"{msg_erro} :: {resumo_corpo}"
            )

            level = "error" if (status_code >= 500 or erro) else "info"
            log_message(
                msg,
                level=level,
                source="strict_audit",
                user=user_id,
                withBd=False,
            )
        except Exception as e:
            log_message(f"[strict-audit] Falha ao registar auditoria: {e}", "warning")
