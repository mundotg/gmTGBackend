"""
🌐 Rotas de Linhagem de Dados (Data Lineage & Flow)

Disponibiliza endpoints REST para visualização de grafos de proveniência (upstream/downstream),
análise de impacto de alterações em tabelas e trilha de auditoria de fluxos de dados.
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.lineage_models import DataLineageEvent
from app.schemas.lineage_schemas import (
    LineageEventCreate,
    LineageEventResponse,
    LineageGraphResponse,
    TableImpactAnalysisResponse,
)
from app.services.lineage_service import LineageService
from app.ultils.get_id_by_token import get_current_user_id

router = APIRouter(prefix="/lineage", tags=["Data Lineage & Flow"])


@router.get(
    "/graph",
    response_model=LineageGraphResponse,
    summary="Obter Grafo Interativo de Linhagem",
    description="Retorna os nós (tabelas e transformações), arestas e métricas agregadas do ecossistema de dados.",
)
def get_lineage_graph(
    connection_id: Optional[int] = Query(None, description="Filtrar por ID de conexão"),
    table_name: Optional[str] = Query(None, description="Filtrar por nome de tabela"),
    search: Optional[str] = Query(None, description="Busca textual em tabelas, bancos ou regras"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        return LineageService.get_lineage_graph(
            db,
            user_id=user_id,
            connection_id=connection_id,
            table_name=table_name,
            search=search,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao sintetizar grafo de linhagem: {str(e)}",
        )


@router.get(
    "/table/{table_name}",
    response_model=TableImpactAnalysisResponse,
    summary="Análise de Impacto e Dependência de Tabela",
    description="Retorna quem alimenta esta tabela (upstream) e quais tabelas a consomem (downstream), com score de risco.",
)
def get_table_impact(
    table_name: str,
    connection_id: Optional[int] = Query(None, description="ID opcional da conexão da tabela"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        return LineageService.get_table_impact(
            db,
            table_name=table_name,
            connection_id=connection_id,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao analisar impacto da tabela '{table_name}': {str(e)}",
        )


@router.get(
    "/history",
    response_model=List[LineageEventResponse],
    summary="Trilha de Auditoria de Eventos de Linhagem",
    description="Retorna a lista cronológica de eventos de transferência, migração e transformação.",
)
def get_lineage_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    try:
        events = (
            db.query(DataLineageEvent)
            .order_by(DataLineageEvent.created_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        return events
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao recuperar histórico de linhagem: {str(e)}",
        )


@router.post(
    "/events",
    response_model=LineageEventResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Registrar Evento de Linhagem Manual ou Externo",
    description="Permite que pipelines ETL/ELT externos ou scripts customizados registrem transferências de dados no MustaInfo.",
)
def create_lineage_event(
    payload: LineageEventCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    event = LineageService.record_event_sync(
        db,
        user_id=user_id,
        source_conn_id=payload.source_connection_id,
        source_table=payload.source_table,
        target_conn_id=payload.target_connection_id,
        target_table=payload.target_table,
        source_query=payload.source_query,
        columns_mapping=payload.columns_mapping,
        transformation_type=payload.transformation_type or "direct_transfer",
        transformation_rule=payload.transformation_rule,
        rows_transferred=payload.rows_transferred or 0,
        duration_ms=payload.duration_ms or 0,
        status=payload.status or "success",
        error_message=payload.error_message,
        project_id=payload.project_id,
    )
    if not event:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Não foi possível registrar o evento de linhagem.",
        )
    return event


@router.post(
    "/seed-demo",
    summary="Semear Fluxos de Linhagem de Demonstração",
    description="Gera dados realistas de demonstração para testes no ambiente do usuário.",
)
def seed_demo_lineage(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    LineageService._seed_demo_events_if_empty(db, user_id)
    return {"message": "Fluxos de demonstração semeados com sucesso!"}
