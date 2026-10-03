"""
🛡️ Modo Manutenção (Maintenance Mode).

Crítico:
Recusa pedidos de quem não é administrador, com 503.
O login e os endpoints de health continuam abertos.

Só atua se a funcionalidade 'maintenance_mode' estiver ativada nas definições do sistema.
O custo por pedido é uma leitura servida do cache em memória do
system_settings_service (TTL 10s) — não é uma query à base de dados por chamada.
"""

from __future__ import annotations

from typing import Iterable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.database import SessionLocal
from app.services import system_settings_service as definicoes
from app.ultils.logger import log_message

#: Caminhos que continuam a responder mesmo em modo manutenção.
#: Sem o login e sem a aba de sistema, ligar a manutenção deixaria a instalação
#: sem forma de a desligar.
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
    "/favicon.ico",
)


def _rota_aberta(caminho: str, abertas: Iterable[str] = ROTAS_SEMPRE_ABERTAS) -> bool:
    """Verifica se o caminho do pedido faz parte das rotas sempre acessíveis."""
    return any(caminho.startswith(prefixo) for prefixo in abertas)


def is_maintenance_mode_enabled() -> bool:
    """
    Verifica se o Modo Manutenção está ativo nas definições do sistema.
    Usa o cache interno do system_settings_service para alta performance.
    Em caso de falha de conexão à BD, devolve False para evitar paragem total.
    """
    db = SessionLocal()
    try:
        return bool(definicoes.obter(db, "maintenance_mode"))
    except Exception as e:  # noqa: BLE001
        log_message(f"[maintenance-mode] Falha ao ler 'maintenance_mode': {e}", "warning")
        return False
    finally:
        db.close()


class MaintenanceModeMiddleware(BaseHTTPMiddleware):
    """
    Middleware que implementa o Modo Manutenção.

    Recusa pedidos de quem não é administrador com status HTTP 503.
    O login e os endpoints de health continuam abertos.
    Só atua se a funcionalidade for ativada nas definições do sistema.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        caminho = request.url.path

        # 1. Health checks e favicon saem já: chamados por sondas frequentes
        # e não devem sequer tocar na verificação de definições.
        if _rota_aberta(caminho, ("/health", "/favicon.ico")):
            return await call_next(request)

        # 2. Se o modo manutenção estiver desligado, passa com custo insignificante
        if not is_maintenance_mode_enabled():
            return await call_next(request)

        # 3. Rotas de sobrevivência (login, refresh, logout, aba de sistema, docs)
        if _rota_aberta(caminho, ROTAS_SEMPRE_ABERTAS):
            return await call_next(request)

        # 4. Só o super admin (admin:*) atravessa a manutenção
        if await self._e_admin(request):
            return await call_next(request)

        # 5. Pedido recusado: retorna 503 com cabeçalho Retry-After
        client_ip = self._extrair_ip(request)
        request_id = getattr(request.state, "request_id", None) or request.headers.get("X-Request-ID", "-")
        log_message(
            f"[maintenance] Pedido recusado (503): {request.method} {caminho} "
            f"[ip={client_ip}] [req={request_id}]",
            level="warning",
            source="maintenance_mode",
        )

        return JSONResponse(
            status_code=503,
            content={
                "detail": "Sistema em manutenção. Tente novamente dentro de momentos.",
                "maintenance": True,
            },
            headers={"Retry-After": "120"},
        )

    def _extrair_token(self, request: Request) -> Optional[str]:
        """Extrai o token JWT a partir do cabeçalho Authorization ou cookie access_token."""
        cabecalho = request.headers.get("Authorization", "")
        if cabecalho.lower().startswith("bearer "):
            return cabecalho.split(" ", 1)[1].strip()
        if "access_token" in request.cookies:
            return request.cookies.get("access_token")
        return None

    def _extrair_ip(self, request: Request) -> str:
        """Obtém o IP do cliente respeitando cabeçalhos de proxy reverso."""
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip.strip()
        if request.client and request.client.host:
            return request.client.host
        return "127.0.0.1"

    async def _e_admin(self, request: Request) -> bool:
        """
        Verifica se o pedido provém de um utilizador administrador (super admin).

        Executa antes de qualquer dependência do FastAPI ser resolvida.
        Qualquer falha (token ausente, expirado, utilizador inativo, sem admin:*)
        resulta em False.
        """
        token = self._extrair_token(request)
        if not token:
            return False

        try:
            from sqlalchemy.orm import joinedload

            from app.auth import decode_token
            from app.models.user_model import Role, User
            from app.ultils.permissions import is_superadmin

            payload = decode_token(token)
            if not payload:
                return False

            user_id = payload.get("sub") or payload.get("user_id")
            if user_id is None:
                return False

            db = SessionLocal()
            try:
                utilizador = (
                    db.query(User)
                    .options(
                        joinedload(User.role).joinedload(Role.permissions)
                    )
                    .filter(User.id == int(user_id))
                    .first()
                )
                if not utilizador or not utilizador.is_active:
                    return False
                return bool(is_superadmin(utilizador))
            finally:
                db.close()
        except Exception:  # noqa: BLE001
            return False
