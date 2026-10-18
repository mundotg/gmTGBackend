"""
🧪 Testes unitários para o LineageService e endpoints de Linhagem de Dados (Data Lineage & Flow).
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models.lineage_models import DataLineageEvent
from app.models.connection_models import DBConnection
from app.services.lineage_service import LineageService


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


def test_record_lineage_event_sync(db_session):
    event = LineageService.record_event_sync(
        db_session,
        user_id=1,
        source_conn_id=None,
        source_table="pedidos",
        target_conn_id=None,
        target_table="analytics_pedidos",
        source_query="SELECT * FROM pedidos WHERE status = 'pago'",
        columns_mapping=[{"source": "id", "target": "pedido_id"}],
        transformation_type="query_filter",
        transformation_rule="Filtro: status = 'pago'",
        rows_transferred=1500,
        duration_ms=450,
        status="success",
    )

    assert event is not None
    assert event.source_table == "pedidos"
    assert event.target_table == "analytics_pedidos"
    assert event.rows_transferred == 1500
    assert event.status == "success"


def test_get_lineage_graph(db_session):
    # Cria dois eventos sequenciais
    LineageService.record_event_sync(
        db_session,
        user_id=1,
        source_conn_id=1,
        source_table="clientes",
        target_conn_id=2,
        target_table="dw_clientes",
        rows_transferred=500,
    )
    LineageService.record_event_sync(
        db_session,
        user_id=1,
        source_conn_id=2,
        source_table="dw_clientes",
        target_conn_id=3,
        target_table="report_clientes",
        rows_transferred=500,
    )

    graph = LineageService.get_lineage_graph(db_session, user_id=1)

    assert graph is not None
    assert len(graph.nodes) >= 4  # 3 tabelas + 2 transformações
    assert len(graph.edges) >= 4
    assert graph.summary.total_volume_records >= 1000
    assert graph.summary.active_flows_count >= 2


def test_table_impact_analysis(db_session):
    # Fonte -> Intermediária -> Destino
    LineageService.record_event_sync(
        db_session,
        user_id=1,
        source_conn_id=1,
        source_table="produtos",
        target_conn_id=2,
        target_table="analytics_produtos",
        transformation_rule="Carga diária",
        rows_transferred=120,
    )

    # Analisa o impacto na tabela origem "produtos"
    impact_origem = LineageService.get_table_impact(db_session, "produtos")
    assert impact_origem.table_name == "produtos"
    assert len(impact_origem.downstream_dependents) == 1
    assert impact_origem.downstream_dependents[0]["target_table"] == "analytics_produtos"
    assert impact_origem.risk_assessment in ["MEDIUM", "HIGH", "CRITICAL"]

    # Analisa o impacto na tabela destino "analytics_produtos"
    impact_destino = LineageService.get_table_impact(db_session, "analytics_produtos")
    assert impact_destino.table_name == "analytics_produtos"
    assert len(impact_destino.upstream_sources) == 1
    assert impact_destino.upstream_sources[0]["source_table"] == "produtos"
