import json
from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

# Importações dos teus CRUDS, Database e dependências
from app.cruds.dbstatistics_crud import create_statistics, get_statistics_by_connection
from app.cruds.queryhistory_crud import create_query_history
from app.cruds.geral_crud import (
    get_settings,
    update_appearance_settings_crud,
    update_language_settings_crud,
    update_settings,
)
from app.database import get_db
from app.services import geral_services

# Importações dos teus Schemas
from app.schemas.dbstatistics_schema import DBStatisticsCreate, DBStatisticsOut
from app.schemas.geral_schema import (
    SettingsUpdate,
    SettingsResponse,
    OptionTipoModel,
    UpdateAppearancePayload,
    UpdateLanguagePayload,
)
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryHistoryOut

# Utilitários
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

router = APIRouter(prefix="/geral", tags=["Geral"])


# ==========================================
# Query History
# ==========================================
@router.post(
    "/queries/",
    response_model=QueryHistoryOut,
    status_code=status.HTTP_201_CREATED,
    summary="Salvar histórico de Query",
)
def create_query(
    query_data: QueryHistoryCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        return create_query_history(db, user_id, query_data)
    except SQLAlchemyError as e:
        log_message(
            f"❌ Erro de BD ao salvar query (User {user_id}): {str(e)}", level="error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível guardar o histórico da consulta devido a um erro no servidor.",
        )
    except Exception as e:
        log_message(
            f"❌ Erro inesperado ao salvar query (User {user_id}): {str(e)}",
            level="error",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ocorreu um erro inesperado ao processar a requisição.",
        )


# ==========================================
# DB Statistics
# ==========================================
@router.post(
    "/statistics/",
    response_model=DBStatisticsOut,
    status_code=status.HTTP_201_CREATED,
    summary="Criar estatísticas de conexão",
)
async def create_statistic(
    stat_data: DBStatisticsCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        # Assumindo que a validação de propriedade (se a conexão pertence ao user)
        # está sendo feita dentro do CRUD `create_statistics`.
        return create_statistics(db, stat_data)
    except ValueError as ve:
        # Se o CRUD lançar ValueError (ex: conexão não pertence ao user)
        log_message(
            f"⚠️ Tentativa de criar estatística inválida (User {user_id}): {str(ve)}",
            level="warning",
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(ve))
    except Exception as e:
        log_message(
            f"❌ Erro ao criar estatística (User {user_id}): {str(e)}", level="error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível guardar as estatísticas da base de dados.",
        )


@router.get(
    "/statistics/{connection_id}",
    response_model=DBStatisticsOut,
    summary="Obter estatísticas de uma conexão",
)
async def get_statistic(
    connection_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        stats = get_statistics_by_connection(connection_id)
        if not stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Nenhuma estatística encontrada para esta conexão.",
            )
        # TODO: Garantir no CRUD que este user_id tem permissão para ver esta connection_id
        return stats
    except HTTPException:
        raise
    except Exception as e:
        log_message(
            f"❌ Erro ao buscar estatísticas (Conexão {connection_id}, User {user_id}): {str(e)}",
            level="error",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno ao carregar as métricas solicitadas.",
        )


# ==========================================
# User Settings
# ==========================================
@router.get(
    "/settings/me",
    response_model=SettingsResponse,
    summary="Obter preferências do utilizador",
)
async def get_user_settings(
    db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)
):
    try:
        settings = get_settings(db, user_id)
        if not settings:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Não foram encontradas definições para a sua conta.",
            )
        return settings
    except HTTPException:
        raise
    except Exception as e:
        log_message(
            f"❌ Erro ao buscar configurações (User {user_id}): {str(e)}", level="error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível carregar as suas preferências.",
        )


@router.patch(
    "/settings/me",
    response_model=SettingsResponse,
    summary="Atualizar preferências do utilizador",
)
async def update_user_settings(
    settings_data: SettingsUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        updated_settings = update_settings(db, user_id, settings_data)

        if not updated_settings:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="As suas configurações não foram encontradas no sistema.",
            )
        return updated_settings
    except HTTPException:
        raise
    except Exception as e:
        log_message(
            f"❌ Erro ao atualizar configurações (User {user_id}): {str(e)}",
            level="error",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Falha ao guardar as novas definições. Tente novamente mais tarde.",
        )


# ==========================================
# User Settings (Configurações do Utilizador)
# ==========================================
# Rota específica para alterar APARÊNCIA (Requisitada pelo Frontend)
@router.put(
    "/settings/appearance",
    response_model=SettingsResponse,
    summary="Atualizar preferências de aparência (Tema/Sidebar)",
)
async def update_appearance_settings(
    payload: UpdateAppearancePayload,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        # Reutilizamos a função update_settings do CRUD porque o Pydantic garante que
        # apenas 'theme' ou 'sidebar_collapsed' serão enviados para o update
        print(f"Payload recebido para update de aparência (User {user_id}): {payload}")
        updated_settings = update_appearance_settings_crud(db, user_id, payload)

        if not updated_settings:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Conta não localizada para aplicar as alterações de visual.",
            )
        return updated_settings
    except HTTPException:
        raise
    except Exception as e:
        log_message(
            f"❌ Erro ao atualizar aparência (User {user_id}): {str(e)}", level="error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ocorreu um erro ao guardar o seu tema visual.",
        )


# Rota específica para alterar IDIOMA (Requisitada pelo Frontend)
@router.put(
    "/settings/language",
    response_model=SettingsResponse,
    summary="Atualizar idioma principal do utilizador",
)
async def update_language_settings(
    payload: UpdateLanguagePayload,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        # Mapeamento do payload para o schema geral de Update
        updated_settings = update_language_settings_crud(db, user_id, payload)

        if not updated_settings:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Não foi possível guardar o idioma. Conta não encontrada.",
            )
        return updated_settings
    except HTTPException:
        raise
    except Exception as e:
        log_message(
            f"❌ Erro ao atualizar idioma (User {user_id}): {str(e)}", level="error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível alterar o idioma neste momento.",
        )


# ==========================================
# Paginação Global
# ==========================================
@router.get("/setting/paginate", summary="Listagem paginada e filtrada de entidades")
async def listar_elementos(
    tipo: OptionTipoModel = Query(
        ..., description="A entidade a listar (ex: user, project)"
    ),
    search: Optional[str] = Query(
        None, min_length=2, max_length=50, description="Pesquisa textual (min: 2 chars)"
    ),
    filtro: Optional[str] = Query(
        None, description="Filtros avançados em formato JSON"
    ),
    page: int = Query(1, ge=1, description="Número da página atual"),
    limit: int = Query(
        10, ge=1, le=100, description="Máximo de registos por página (Cap: 100)"
    ),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Motor de paginação universal.
    Processa pesquisa textual, limites seguros e filtros dinâmicos via JSON.
    """
    filters_dict: Dict[str, Any] = {}

    # 1. Validação segura do JSON de filtros
    if filtro:
        try:
            parsed_filters = json.loads(filtro)
            if not isinstance(parsed_filters, dict):
                raise ValueError("O JSON fornecido não é um objeto/dicionário.")

            # Sanitização: Garantir que não há injeção de dicionários aninhados profundos
            for k, v in parsed_filters.items():
                if isinstance(v, (dict, list)):
                    raise ValueError(
                        f"O filtro '{k}' contém tipos de dados complexos não permitidos."
                    )
                filters_dict[k] = v

        except json.JSONDecodeError:
            log_message(
                f"⚠️ JSON malformado na paginação (User {user_id}): {filtro}",
                level="warning",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A estrutura do filtro fornecido não é um JSON válido.",
            )
        except ValueError as ve:
            log_message(
                f"⚠️ Erro de segurança/validação no filtro (User {user_id}): {str(ve)}",
                level="warning",
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))

    # 2. Execução da paginação
    try:
        resultado = geral_services.get_paginacao_service(
            db=db,
            search=search,
            page=page,
            limit=limit,
            options=tipo,
            filters=filters_dict,
            load_relations=True,
        )
        return resultado

    except ValueError as ve:
        # Exemplo: O serviço rejeitou o 'tipo' ou o user_id não tem permissão
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(ve))
    except SQLAlchemyError as e:
        log_message(
            f"❌ Erro de BD na paginação de {tipo} (User {user_id}): {str(e)}",
            level="error",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ocorreu um erro ao processar os dados na base de dados.",
        )
    except Exception as e:
        log_message(
            f"❌ Erro inesperado na paginação de {tipo} (User {user_id}): {str(e)}",
            level="error",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno ao tentar listar os registos.",
        )
