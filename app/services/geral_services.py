"""
📄 Paginação genérica — o motor por trás de `GET /geral/paginate`.

⚠️ Esta é a única implementação. Existia uma cópia em `task_service` com um
mapa de modelos diferente (essa tinha project/task/sprint, esta não), e como a
rota usa esta, todo o módulo de projetos recebia 403 "Opção inválida". O mapa
abaixo é agora a união das duas e tem de acompanhar `OptionTipoModel` em
`app/schemas/geral_schema.py` — um nome que exista aqui e não lá é rejeitado
pela rota com 422, e o inverso dá 403.
"""

from typing import Any, Dict, Optional, Type

from sqlalchemy.orm import Session

from app.cruds.task_cruds import get_paginated_query
from app.models.connection_models import (
    ActiveConnection,
    ConnectionLog,
    DBConnection,
    DBHealthCheck,
)
from app.models.dbstructure_models import DBEnumField, DBField, DBStructure
from app.models.queryhistory_models import QueryHistory
from app.models.task_models import (
    AuditLog,
    Project,
    Sprint,
    Task,
    TaskStats,
    TypeProjecto,
)
from app.models.user_model import Cargo, Empresa, RefreshToken, Role, User
from app.schemas.geral_schema import OptionTipoModel
from app.ultils.logger import log_message


# Modelos expostos pela paginação genérica.
MODEL_MAP: Dict[str, Type] = {
    # 👥 Pessoas e organização
    "user": User,
    "Role": Role,
    "Empresa": Empresa,
    "Cargo": Cargo,
    "RefreshToken": RefreshToken,
    # 📋 Gestão de projetos
    "project": Project,
    "task": Task,
    "sprint": Sprint,
    "type_project": TypeProjecto,
    "TaskStats": TaskStats,
    "AuditLog": AuditLog,
    # 🔌 Conexões e base de dados
    "DBConnection": DBConnection,
    "ActiveConnection": ActiveConnection,
    "ConnectionLog": ConnectionLog,
    "DBHealthCheck": DBHealthCheck,
    "QueryHistory": QueryHistory,
    "DBStructure": DBStructure,
    "DBField": DBField,
    "DBEnumField": DBEnumField,
}


# Relações carregadas com `load_relations=True`, para o frontend receber os
# nomes (responsável, projeto, sprint) e não só os IDs.
RELATION_MAP: Dict[str, list[str]] = {
    "user": ["role", "created_projects", "assigned_tasks", "projects_participating"],
    "project": [
        "owner_user",
        "team_members",
        "task_stats",
        "type_project",
        "db_connection",
    ],
    "task": ["assigned_user", "delegated_user", "creator_user", "project", "sprint"],
    "sprint": ["created_by", "project", "tasks"],
    "type_project": [],
    "Role": ["permissions"],
    "AuditLog": ["user"],
    "DBConnection": ["structures"],
}


def get_paginacao_service(
    db: Session,
    search: Optional[str] = None,
    page: int = 1,
    limit: int = 5,
    options: OptionTipoModel = "user",
    filters: Optional[Dict[str, Any]] = None,
    load_relations: bool = False,
):
    """
    Devolve dados paginados de qualquer entidade registada em `MODEL_MAP`,
    com pesquisa textual, filtros e carregamento de relações.

    Returns:
        dict: {"items", "total", "page", "limit", "pages"}
    """
    if options not in MODEL_MAP:
        raise ValueError(
            f"Opção inválida: '{options}'. Use: {', '.join(MODEL_MAP.keys())}"
        )

    model = MODEL_MAP[options]
    relationships = RELATION_MAP.get(options, []) if load_relations else None

    result = get_paginated_query(
        db=db,
        model=model,
        search=search,
        filters=filters,
        page=page,
        limit=limit,
        relationships=relationships,
    )

    # Só o resumo: registar `result` inteiro enchia os logs com os próprios
    # dados (incluindo emails e metadados de conexões).
    log_message(
        f"📄 Paginação '{options}' filtros={filters} → "
        f"{result.get('total', 0)} registo(s), página {result.get('page')}",
        "info",
    )
    return result
