"""Middlewares e handlers transversais da aplicação."""

from app.middleware.error_handlers import register_exception_handlers
from app.middleware.maintenance_mode import (
    MaintenanceModeMiddleware,
    ROTAS_SEMPRE_ABERTAS,
    is_maintenance_mode_enabled,
)
from app.middleware.request_context import (
    RequestContextMiddleware,
    get_request_id,
)
from app.middleware.strict_audit import (
    StrictAuditMiddleware,
    is_strict_audit_enabled,
)
from app.middleware.system_guard import SystemGuardMiddleware

__all__ = [
    "MaintenanceModeMiddleware",
    "RequestContextMiddleware",
    "ROTAS_SEMPRE_ABERTAS",
    "StrictAuditMiddleware",
    "SystemGuardMiddleware",
    "get_request_id",
    "is_maintenance_mode_enabled",
    "is_strict_audit_enabled",
    "register_exception_handlers",
]
