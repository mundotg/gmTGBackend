
from typing import Any, Dict,  Optional, Type
from typing_extensions import Literal
from sqlalchemy.orm import Session
from app.cruds.task_cruds import  get_paginated_query
from app.models.connection_models import ActiveConnection, ConnectionLog, DBConnection, DBHealthCheck
from app.models.dbstructure_models import DBEnumField, DBField, DBStructure
from app.models.queryhistory_models import QueryHistory
from app.models.user_model import Cargo, Empresa, RefreshToken, User
from app.schemas.geral_schema import OptionTipoModel
from app.ultils.logger import log_message

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
    Serviço genérico para retornar dados paginados com suporte a busca, filtros e relações.

    Args:
        db (Session): Sessão do banco de dados.
        search (str, opcional): Texto para pesquisa nas colunas string.
        page (int, opcional): Página atual. Padrão é 1.
        limit (int, opcional): Quantidade de itens por página. Padrão é 10.
        options (Literal): Define qual modelo será consultado.
        filters (dict, opcional): Filtros adicionais, ex: {"status": "ativo"}
        user_id (str, opcional): ID do usuário para filtros específicos
        load_relations (bool, opcional): Se deve carregar relações automaticamente

    Returns:
        dict: Resultado contendo items, total, página e total de páginas.
    """

    model_map: Dict[str, Type] = {
        "user": User,
        "ActiveConnection": ActiveConnection,
        "ConnectionLog": ConnectionLog,
        "DBHealthCheck": DBHealthCheck,
        "QueryHistory": QueryHistory,
        "DBStructure": DBStructure,
        "DBField": DBField,
        "DBEnumField": DBEnumField,
        "DBConnection": DBConnection,
        "RefreshToken"  : RefreshToken,
        "Empresa" : Empresa,
        "Cargo" : Cargo,
        
        
    }

    # ✅ Verificação de tipo válido
    if options not in model_map:
        raise ValueError(f"Opção inválida: '{options}'. Use: {', '.join(model_map.keys())}")

    model = model_map[options]

    # 🔗 Definir relações para cada modelo
    relation_map = {
        "user": ["role_ref", "created_projects", "assigned_tasks", "projects_participating"],
        "project": ["owner_user", "team_members",  "task_stats", "type_project", "db_connection"],
        "task": ["assigned_user", "delegated_user", "creator_user", "project", "sprint"],
        "sprint": ["created_by", "project", "task_stats"],
        "type_project": [],  # Sem relações
        "Role": ["users"],
        "AuditLog": ["user"],
        # "TaskStats": ["project", "sprint"],
        "DBConnection": ["structures"],
        "project_team_association": []  # Tabela de associação, sem relações
    }

    # Preparar relações para carregamento
    relationships = []
    if load_relations and options in relation_map:
        relationships = relation_map[options]

    # 🔍 Chama o método genérico de paginação e busca
    result = get_paginated_query(
        db=db,
        model=model,
        search=search,
        filters=filters,
        page=page,
        limit=limit,
        relationships=relationships if load_relations else None,
    )
    
    log_message(f"Resultado da paginação para '{options}{filters}': {result}")
    return result