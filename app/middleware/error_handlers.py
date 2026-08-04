"""
Handlers globais de exceções.

Objetivo: nenhuma exceção não tratada chega ao cliente como stack trace.
O cliente recebe sempre um JSON estável com o `request_id`; o detalhe
técnico fica no log, associado a esse mesmo ID.

Formato de resposta (uniforme para todos os erros):

    {
      "detail": "mensagem legível",
      "request_id": "a1b2c3d4e5f6",
      "type": "http_error" | "validation_error" | "database_error" | "internal_error"
    }
"""

from __future__ import annotations

import traceback

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config.dotenv import get_env
from app.middleware.request_context import REQUEST_ID_HEADER, get_request_id
from app.ultils.logger import log_message

# Em desenvolvimento devolve-se o erro real ao cliente; em produção nunca,
# para não expor estrutura interna, nomes de tabelas ou credenciais.
_DEBUG_ERRORS = (get_env("ENV", "development") or "").lower() != "production"


def _resolve_request_id(request: Request) -> str:
    """
    Obtém o ID do pedido para a resposta de erro.

    Lê primeiro de `request.state`: quando uma exceção sobe através do
    middleware, o ContextVar já foi reposto antes de o handler correr, pelo
    que `get_request_id()` sozinho devolveria "-" — justamente nos casos em
    que o ID mais interessa.
    """
    return getattr(request.state, "request_id", None) or get_request_id()


def _error_response(
    request: Request,
    status_code: int,
    detail: str,
    error_type: str,
    extra: dict | None = None,
) -> JSONResponse:
    request_id = _resolve_request_id(request)

    body = {
        "detail": detail,
        "request_id": request_id,
        "type": error_type,
    }

    if extra:
        body.update(extra)

    return JSONResponse(
        status_code=status_code,
        content=body,
        # O middleware não chega a tocar nesta resposta (foi criada já fora
        # dele), por isso o header é posto aqui explicitamente.
        headers={REQUEST_ID_HEADER: request_id},
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Liga todos os handlers à aplicação."""

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        # Erros esperados (401, 404, 422 lançados à mão…): passam tal como são,
        # apenas ganham request_id. Não são logados como erro para não
        # encher o log com 404s normais.
        if exc.status_code >= 500:
            log_message(
                f"[{_resolve_request_id(request)}] {request.method} "
                f"{request.url.path} → HTTP {exc.status_code}: {exc.detail}",
                "error",
            )

        response = _error_response(
            request,
            exc.status_code,
            str(exc.detail),
            "http_error",
        )

        # Preserva headers da própria exceção (ex.: Retry-After do rate limit,
        # WWW-Authenticate do 401), que de outro modo se perderiam.
        for key, value in (getattr(exc, "headers", None) or {}).items():
            response.headers[key] = value

        return response

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        # Erros de validação do Pydantic: 422 com os campos em falta/errados.
        log_message(
            f"[{_resolve_request_id(request)}] Validação falhou em "
            f"{request.url.path}: {exc.errors()}",
            "warning",
            withBd=True,
        )

        return _error_response(
            request,
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "Dados inválidos no pedido.",
            "validation_error",
            {"errors": exc.errors()},
        )

    @app.exception_handler(SQLAlchemyError)
    async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError):
        # Falhas de base de dados nunca devem expor a query nem o schema.
        log_message(
            f"[{_resolve_request_id(request)}] Erro de base de dados em "
            f"{request.url.path}: {exc}\n{traceback.format_exc()}",
            "error",
        )

        return _error_response(
            request,
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Serviço de base de dados indisponível. Tenta novamente.",
            "database_error",
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        # Rede de segurança final: qualquer coisa que escape acima.
        log_message(
            f"[{_resolve_request_id(request)}] Exceção não tratada em "
            f"{request.method} {request.url.path}: {exc}\n{traceback.format_exc()}",
            "error",
        )

        detail = (
            f"{type(exc).__name__}: {exc}"
            if _DEBUG_ERRORS
            else "Erro interno no servidor."
        )

        return _error_response(
            request,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail,
            "internal_error",
        )
