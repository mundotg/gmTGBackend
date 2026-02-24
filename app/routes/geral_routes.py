import json
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.cruds.dbstatistics_crud import create_statistics, get_statistics_by_connection
from app.cruds.queryhistory_crud import create_query_history
from app.database import get_db
from app.cruds.geral_crud import create_settings, get_settings
from app.schemas.dbstatistics_schema import DBStatisticsCreate, DBStatisticsOut
from app.schemas.geral_schema import Settings, SettingsCreate, OptionTipoModel
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryHistoryOut
from app.services import geral_services
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

router = APIRouter(prefix="/geral", tags=["Geral"])


# ==========================================
# Query History
# ==========================================
@router.post("/queries/", response_model=QueryHistoryOut, summary="Salvar histórico de Query")
def create_query(
    query_data: QueryHistoryCreate, 
    db: Session = Depends(get_db), 
    user_id: int = Depends(get_current_user_id)
):
    try:
        return create_query_history(db, query_data)
    except Exception as e:
        log_message(f"❌ Erro ao salvar histórico de query (User {user_id}): {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno ao registrar a query.")


# ==========================================
# DB Statistics
# ==========================================
@router.post("/statistics/", response_model=DBStatisticsOut, summary="Criar estatísticas de conexão")
def create_statistic(
    stat_data: DBStatisticsCreate, 
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    # NOTA DE SEGURANÇA: Certifique-se de que a função create_statistics valide
    # internamente se o connection_id dentro de 'stat_data' pertence a este 'user_id'.
    try:
        return create_statistics(db, stat_data)
    except Exception as e:
        log_message(f"❌ Erro ao criar estatística (User {user_id}): {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro ao processar as estatísticas do banco de dados.")

@router.get("/statistics/{connection_id}", response_model=DBStatisticsOut, summary="Obter estatísticas")
def get_statistic(
    connection_id: int, 
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    try:
        # ATENÇÃO: É vital que sua função de CRUD valide o owner!
        # stats = get_statistics_by_connection(db, connection_id, user_id) 
        stats = get_statistics_by_connection(db, connection_id)
        if not stats:
            raise HTTPException(status_code=404, detail="Estatísticas não encontradas para esta conexão.")
        return stats
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro ao buscar estatísticas da conexão {connection_id}: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno ao carregar métricas.")


# ==========================================
# User Settings
# ==========================================
@router.post("/settings/", response_model=Settings, summary="Atualizar configurações do usuário")
def create_user_settings(
    settings_data: SettingsCreate, 
    db: Session = Depends(get_db), 
    user_id: int = Depends(get_current_user_id)
):
    try:
        return create_settings(db, user_id, settings_data)
    except Exception as e:
        log_message(f"❌ Erro ao salvar configurações (User {user_id}): {e}", level="error")
        raise HTTPException(status_code=500, detail="Falha ao salvar preferências do usuário.")

@router.get("/settings/", response_model=Settings, summary="Obter configurações do usuário")
def get_user_settings(
    db: Session = Depends(get_db), 
    user_id: int = Depends(get_current_user_id)
):
    try:
        settings = get_settings(db, user_id)
        if not settings:
            raise HTTPException(status_code=404, detail="Configurações não encontradas para este usuário.")
        return settings
    except HTTPException:
        raise
    except Exception as e:
        log_message(f"❌ Erro ao buscar configurações (User {user_id}): {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro ao carregar preferências.")


# ==========================================
# Paginação Global
# ==========================================
# Correção: Rota alterada de "/geral/paginate" para "/paginate" para evitar duplicação do prefixo
@router.get("/paginate", summary="Paginação Universal")
def listar_elementos(
    tipo: OptionTipoModel = Query("user", description="Tipo de entidade a ser paginada (ex: user, project, task, sprint)"), # type: ignore
    search: Optional[str] = Query(None, description="Texto para pesquisa em campos textuais"),
    filtro: Optional[str] = Query(None, description="Filtros avançados em formato JSON serializado"),
    page: int = Query(1, ge=1, description="Número da página"),
    limit: int = Query(10, ge=1, le=100, description="Registros por página (Max: 100)"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id) # Correção: alterado de str para int
):
    """
    Paginação genérica de entidades do sistema.
    Assegure-se de que o service 'get_paginacao_service' valide se o 'user_id'
    tem permissão para ver os dados da entidade solicitada.
    """
    filters = {}
    if filtro:
        try:
            parsed_filters = json.loads(filtro)
            if isinstance(parsed_filters, dict):
                filters = parsed_filters
            else:
                raise ValueError("O JSON não é um dicionário válido.")
        except (json.JSONDecodeError, ValueError) as e:
            log_message(f"⚠️ JSON inválido fornecido na paginação por User {user_id}: {filtro}", level="warning")
            raise HTTPException(status_code=400, detail="Formato de filtro JSON inválido.")
            
    try:
        return geral_services.get_paginacao_service(
            db=db,
            search=search,
            page=page,
            limit=limit,
            options=tipo,
            filters=filters,
            load_relations=True
        )
    except Exception as e:
        log_message(f"❌ Erro na paginação ({tipo}) solicitada pelo User {user_id}: {e}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno ao processar a listagem de dados.")