"""
🛡️ O que torna dois interruptores da aba Sistema reais.

**Modo Manutenção** — recusa pedidos de quem não é administrador, com 503.
Login, health e a própria aba de sistema continuam abertos, senão ligar a
manutenção trancaria também quem a tem de desligar.

**Auditoria Rigorosa** — regista o corpo dos pedidos que alteram estado.
Palavras-passe, tokens e afins são substituídos antes de gravar: uma trilha de
auditoria que guarda credenciais em claro deixa de ser um controlo e passa a ser
um problema de conformidade.

O custo por pedido é uma leitura de definição, servida do cache em processo do
`system_settings_service` — não é uma query à base de dados por chamada.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.database import SessionLocal
from app.services import system_settings_service as definicoes
from app.ultils.logger import log_message

#: Caminhos que continuam a responder em manutenção. Sem o login e sem a aba de
#: sistema, ligar a manutenção deixaria a instalação sem forma de a desligar.
ROTAS_SEMPRE_ABERTAS = (
    "/auth/login",
    "/auth/refresh",
    "/auth/logout",
    "/health",
    "/system/",
    "/users/rbac/me",
    "/docs",
    "/redoc",
    "/openapi.json",
)

MÉTODOS_AUDITADOS = {"POST", "PUT", "PATCH", "DELETE"}

#: Nomes de campo cujo valor nunca é gravado. Comparados em minúsculas e por
#: inclusão, para apanhar `senha`, `nova_senha`, `password_confirm`, etc.
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
)

REDIGIDO = "***"

#: Acima disto o corpo não é gravado inteiro — um restauro de backup pode trazer
#: megabytes, e a trilha não é sítio para os guardar.
MAX_CORPO = 4000


def _e_sensivel(nome: str) -> bool:
    minusculo = str(nome).lower()
    return any(marca in minusculo for marca in CAMPOS_SENSIVEIS)


def _redigir(valor: Any) -> Any:
    """Percorre a estrutura e substitui o valor dos campos sensíveis."""
    if isinstance(valor, dict):
        return {
            k: (REDIGIDO if _e_sensivel(k) else _redigir(v)) for k, v in valor.items()
        }
    if isinstance(valor, list):
        return [_redigir(v) for v in valor]
    return valor


def _rota_aberta(caminho: str, abertas: Iterable[str]) -> bool:
    return any(caminho.startswith(prefixo) for prefixo in abertas)


class SystemGuardMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        caminho = request.url.path

        # Health e afins saem já: são chamados por sondas, muitas vezes por
        # segundo, e não devem sequer tocar nas definições.
        if _rota_aberta(caminho, ("/health", "/favicon.ico")):
            return await call_next(request)

        db = SessionLocal()
        try:
            em_manutencao = definicoes.obter(db, "maintenance_mode")
            auditoria = (
                definicoes.obter(db, "strict_audit")
                if request.method in MÉTODOS_AUDITADOS
                else False
            )
        except Exception as e:  # noqa: BLE001
            # Se as definições não forem legíveis, o serviço continua a
            # funcionar normalmente: bloquear tudo por não se saber o estado
            # seria transformar uma falha de leitura numa paragem total.
            log_message(f"[system-guard] definições ilegíveis: {e}", "warning")
            em_manutencao = auditoria = False
        finally:
            db.close()

        if em_manutencao and not _rota_aberta(caminho, ROTAS_SEMPRE_ABERTAS):
            if not await self._e_admin(request):
                return JSONResponse(
                    status_code=503,
                    content={
                        "detail": "Sistema em manutenção. Tente novamente dentro de momentos.",
                        "maintenance": True,
                    },
                    headers={"Retry-After": "120"},
                )

        if auditoria:
            await self._registar_corpo(request)

        return await call_next(request)

    # ── manutenção ────────────────────────────────────────────────────────
    async def _e_admin(self, request: Request) -> bool:
        """
        Só o super admin atravessa a manutenção.

        Feito à mão e não por dependência do FastAPI porque o middleware corre
        antes de qualquer dependência ser resolvida. Qualquer falha conta como
        "não é admin" — em manutenção, o lado seguro é recusar.
        """
        try:
            from app.auth import decode_token
            from app.models.user_model import User
            from app.ultils.permissions import is_superadmin

            cabecalho = request.headers.get("Authorization", "")
            if not cabecalho.lower().startswith("bearer "):
                return False

            payload = decode_token(cabecalho.split(" ", 1)[1])
            user_id = payload.get("sub") or payload.get("user_id")
            if user_id is None:
                return False

            db = SessionLocal()
            try:
                utilizador = db.query(User).filter(User.id == int(user_id)).first()
                return bool(utilizador and is_superadmin(utilizador))
            finally:
                db.close()
        except Exception:  # noqa: BLE001
            return False

    # ── auditoria rigorosa ────────────────────────────────────────────────
    async def _registar_corpo(self, request: Request) -> None:
        try:
            corpo = await request.body()
            if not corpo:
                return

            # `await request.body()` esgota o stream. Sem repor, o handler
            # receberia um corpo vazio e o pedido falhava por causa da
            # auditoria — que é a última coisa que uma auditoria deve fazer.
            async def receive():
                return {"type": "http.request", "body": corpo, "more_body": False}

            request._receive = receive  # noqa: SLF001

            if len(corpo) > MAX_CORPO:
                resumo = f"<corpo com {len(corpo)} bytes, não gravado>"
            else:
                try:
                    resumo = json.dumps(
                        _redigir(json.loads(corpo)), ensure_ascii=False, default=str
                    )[:MAX_CORPO]
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Formulários e ficheiros: regista-se que houve corpo, não o conteúdo.
                    resumo = f"<corpo não-JSON, {len(corpo)} bytes>"

            log_message(
                f"[auditoria] {request.method} {request.url.path} :: {resumo}",
                "info",
                source="strict_audit",
            )
        except Exception as e:  # noqa: BLE001
            log_message(f"[system-guard] falha a auditar o corpo: {e}", "warning")
