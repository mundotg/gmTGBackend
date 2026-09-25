"""
Correlação de pedidos.

Cada pedido recebe um ID único que:
- é devolvido no header `X-Request-ID`,
- fica acessível em qualquer ponto do código via `get_request_id()`,
- aparece nos logs de erro.

Isto é o que permite pegar num 500 reportado por um utilizador e encontrar
exatamente a linha de log correspondente, em vez de procurar por timestamp.
"""

from __future__ import annotations

import time
import uuid
from contextvars import ContextVar

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.ultils.logger import log_message

# ContextVar em vez de variável global: cada pedido tem o seu valor,
# mesmo com centenas de corrotinas concorrentes no mesmo processo.
_request_id_ctx: ContextVar[str] = ContextVar("request_id", default="-")

REQUEST_ID_HEADER = "X-Request-ID"

# Rotas cujo tempo de resposta não interessa registar (ruído de monitorização).
_QUIET_PATHS = {"/health", "/health/live", "/health/ready", "/metrics", "/favicon.ico"}

# Acima disto o pedido é considerado lento e sobe para nível warning.
SLOW_REQUEST_MS = 3000


def get_request_id() -> str:
    """ID do pedido em curso, ou '-' se chamado fora de um pedido."""
    return _request_id_ctx.get()


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Atribui um ID a cada pedido e regista duração e estado da resposta."""

    async def dispatch(self, request: Request, call_next) -> Response:
        # Respeita o ID vindo do proxy/gateway, para manter a cadeia
        # de correlação entre serviços; senão gera um novo.
        request_id = request.headers.get(REQUEST_ID_HEADER) or uuid.uuid4().hex[:16]

        token = _request_id_ctx.set(request_id)
        request.state.request_id = request_id

        started = time.perf_counter()

        try:
            response = await call_next(request)

        except Exception:
            # O handler global trata a resposta; aqui só registamos a duração
            # para que o pedido falhado apareça na mesma linha temporal.
            elapsed_ms = (time.perf_counter() - started) * 1000
            log_message(
                f"[{request_id}] {request.method} {request.url.path} "
                f"→ exceção após {elapsed_ms:.0f}ms",
                "error",
                withBd=True,
            )
            raise

        finally:
            _request_id_ctx.reset(token)

        elapsed_ms = (time.perf_counter() - started) * 1000
        response.headers[REQUEST_ID_HEADER] = request_id

        if request.url.path not in _QUIET_PATHS:
            if response.status_code >= 500:
                level = "error"
            elif response.status_code >= 400 or elapsed_ms >= SLOW_REQUEST_MS:
                level = "warning"
            else:
                level = "info"

            log_message(
                f"[{request_id}] {request.method} {request.url.path} "
                f"→ {response.status_code} em {elapsed_ms:.0f}ms",
                level,
                # withBd=True: evita uma escrita na BD por cada pedido HTTP.
                withBd=True,
            )

        return response
