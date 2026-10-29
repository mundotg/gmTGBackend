import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models import user_model, integration_models
from app.services.integration_service import IntegrationService
from app.schemas.integration_schemas import (
    BigQueryConfigIn,
    BigQueryTestIn,
    WebhookConfigIn,
)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sessao = sessionmaker(bind=engine)()

    # Criar empresa de teste
    empresa = user_model.Empresa(
        id=1,
        nome="Empresa Teste QA",
        nif="999888777",
        endereco="Rua Teste, 123",
        is_active=True,
    )
    sessao.add(empresa)
    sessao.commit()

    try:
        yield sessao
    finally:
        sessao.close()


def test_catalogo_padrao_inclui_bigquery(db):
    items = IntegrationService.get_all_integrations(db, empresa_id=1)
    assert len(items) >= 7

    bq = next((i for i in items if i.id == "bigquery"), None)
    assert bq is not None
    assert bq.name == "Google BigQuery"
    assert bq.category == "Data Warehouse & Cloud"
    assert bq.connected is False


def test_salvar_e_obter_config_bigquery(db):
    sa_json = json.dumps({
        "type": "service_account",
        "project_id": "test-gcp-project",
        "client_email": "sa-test@test-gcp-project.iam.gserviceaccount.com",
    })

    cfg_in = BigQueryConfigIn(
        project_id="test-gcp-project",
        dataset_id="analytics_dw_test",
        region="southamerica-east1",
        service_account_json=sa_json,
        dry_run_enabled=True,
        max_bytes_billed_gb=15,
        cost_alert_threshold_usd=2.5,
        auto_sync_lineage=True,
        export_query_audit=True,
    )

    saved = IntegrationService.save_bigquery_config(db, empresa_id=1, data=cfg_in)
    assert saved.project_id == "test-gcp-project"
    assert saved.dataset_id == "analytics_dw_test"
    assert saved.has_credentials is True
    assert saved.client_email == "sa-test@test-gcp-project.iam.gserviceaccount.com"
    assert saved.max_bytes_billed_gb == 15

    # Obter novamente e verificar persistência
    retrieved = IntegrationService.get_bigquery_config(db, empresa_id=1)
    assert retrieved.project_id == "test-gcp-project"
    assert retrieved.dataset_id == "analytics_dw_test"
    assert retrieved.region == "southamerica-east1"

    # Verificar catálogo com status conectado
    items = IntegrationService.get_all_integrations(db, empresa_id=1)
    bq = next((i for i in items if i.id == "bigquery"), None)
    assert bq is not None
    assert bq.connected is True


def test_testar_conexao_bigquery():
    import asyncio

    test_in = BigQueryTestIn(
        project_id="MustaInfo-dw-prod",
        dataset_id="marketing_dw",
        region="us-central1",
        service_account_json=json.dumps({"type": "service_account"}),
    )

    res = asyncio.run(IntegrationService.test_bigquery_connection(test_in))
    assert res.success is True
    assert res.latency_ms > 0
    assert "marketing_dw" in res.datasets
    assert "bigquery.jobs.create" in res.permissions_validated


def test_toggle_integracao(db):
    res = IntegrationService.toggle_integration(db, empresa_id=1, provider="slack", active=True)
    assert res["connected"] is True

    items = IntegrationService.get_all_integrations(db, empresa_id=1)
    slack = next((i for i in items if i.id == "slack"), None)
    assert slack is not None
    assert slack.connected is True

    # Desconectar
    res2 = IntegrationService.toggle_integration(db, empresa_id=1, provider="slack", active=False)
    assert res2["connected"] is False


def test_configuracao_e_disparo_webhook(db):
    import asyncio

    wb_in = WebhookConfigIn(
        url="https://api.empresa.com/v1/MustaInfo-events",
        secret="whsec_teste1234567890",
        events=["query.executed", "dry_run.alert"],
        is_active=True,
    )

    saved_wb = IntegrationService.save_webhook_config(db, empresa_id=1, data=wb_in)
    assert saved_wb.url == "https://api.empresa.com/v1/MustaInfo-events"
    assert saved_wb.secret == "whsec_teste1234567890"
    assert "dry_run.alert" in saved_wb.events

    # Teste de disparo
    res_test = asyncio.run(
        IntegrationService.test_webhook_delivery(
            db=db,
            empresa_id=1,
            url=saved_wb.url,
            secret=saved_wb.secret,
            actor_email="tester@MustaInfo.com",
        )
    )

    assert res_test.status == 200
    assert res_test.success is True
    assert res_test.payload["event"] == "test.ping"
    assert res_test.payload["actor"] == "tester@MustaInfo.com"
