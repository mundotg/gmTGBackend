"""Middlewares e handlers transversais da aplicação."""

from app.middleware.error_handlers import register_exception_handlers
from app.middleware.request_context import (
    RequestContextMiddleware,
    get_request_id,
)

__all__ = [
    "RequestContextMiddleware",
    "get_request_id",
    "register_exception_handlers",
]
