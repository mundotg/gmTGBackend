"""
🛡️ System Guard (Retrocompatibilidade).

Anteriormente centralizava Modo Manutenção e Auditoria Rigorosa.
Para máxima clareza, manutenibilidade e performance, as funcionalidades
estão agora modularizadas em:
  - app/middleware/maintenance_mode.py (Modo Manutenção)
  - app/middleware/strict_audit.py (Modo de Auditoria Rigorosa)

Este módulo mantém as exportações para retrocompatibilidade.
"""

from __future__ import annotations

from app.middleware.maintenance_mode import (
    MaintenanceModeMiddleware,
    ROTAS_SEMPRE_ABERTAS,
    _rota_aberta,
    is_maintenance_mode_enabled,
)
from app.middleware.strict_audit import (
    CAMPOS_SENSIVEIS,
    MAX_CORPO,
    MÉTODOS_AUDITADOS,
    REDIGIDO,
    ROTAS_IGNORADAS,
    StrictAuditMiddleware,
    _e_sensivel,
    _redigir,
    _redigir_corpo_bytes,
    _redigir_form_urlencoded,
    is_strict_audit_enabled,
)

#: Alias para retrocompatibilidade
class SystemGuardMiddleware(MaintenanceModeMiddleware):
    """Alias compatível com a implementação anterior de SystemGuardMiddleware."""
    pass


__all__ = [
    "CAMPOS_SENSIVEIS",
    "MAX_CORPO",
    "MÉTODOS_AUDITADOS",
    "MaintenanceModeMiddleware",
    "REDIGIDO",
    "ROTAS_IGNORADAS",
    "ROTAS_SEMPRE_ABERTAS",
    "StrictAuditMiddleware",
    "SystemGuardMiddleware",
    "_e_sensivel",
    "_redigir",
    "_redigir_corpo_bytes",
    "_redigir_form_urlencoded",
    "_rota_aberta",
    "is_maintenance_mode_enabled",
    "is_strict_audit_enabled",
]
