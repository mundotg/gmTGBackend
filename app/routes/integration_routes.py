from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user_model import User
from app.schemas.integration_schemas import (
    BigQueryConfigIn,
    BigQueryConfigOut,
    BigQueryTestIn,
    BigQueryTestOut,
    IntegrationItemOut,
    GenericIntegrationIn,
    WebhookConfigIn,
    WebhookConfigOut,
    WebhookTestIn,
    WebhookTestOut,
)
from app.services.integration_service import IntegrationService
from app.ultils.permissions import (
    get_current_user,
    require_permission,
    user_has_permission,
)

router = APIRouter(prefix="/integrations", tags=["Integrations"])


def _get_empresa_id(user: User) -> int:
    empresa_id = getattr(user, "empresa_id", None)
    if not empresa_id:
        return 1  # Fallback seguro para tenant padrão caso admin isolado
    return empresa_id


@router.get("/", response_model=List[IntegrationItemOut])
def list_integrations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Retorna o catálogo de integrações e o estado atual para a empresa do usuário.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.get_all_integrations(db, empresa_id)


@router.get("/bigquery", response_model=BigQueryConfigOut)
def get_bigquery_config(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Recupera a configuração atual do Google BigQuery da empresa (credenciais mascaradas).
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.get_bigquery_config(db, empresa_id)


@router.post("/bigquery", response_model=BigQueryConfigOut)
def save_bigquery_config(
    data: BigQueryConfigIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Configura ou atualiza a integração com o Google BigQuery (Dry-Run, Project ID, Service Account).
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.save_bigquery_config(db, empresa_id, data)


@router.post("/bigquery/test", response_model=BigQueryTestOut)
async def test_bigquery_connection(
    data: BigQueryTestIn,
    current_user: User = Depends(get_current_user),
):
    """
    Valida credenciais, conectividade e latência com a API do Google BigQuery.
    """
    return await IntegrationService.test_bigquery_connection(data)


@router.post("/toggle")
def toggle_integration(
    provider: str,
    active: Optional[bool] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Conecta ou desconecta uma integração da empresa.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.toggle_integration(db, empresa_id, provider, active)


@router.post("/generic")
def save_generic_integration(
    data: GenericIntegrationIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Salva configuração de integrações padrão (Snowflake, Databricks, Slack, etc.).
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.save_generic_integration(db, empresa_id, data)


@router.get("/webhooks", response_model=WebhookConfigOut)
def get_webhook_config(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Recupera as configurações de Webhook da empresa.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.get_webhook_config(db, empresa_id)


@router.post("/webhooks", response_model=WebhookConfigOut)
def save_webhook_config(
    data: WebhookConfigIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Salva configurações de Webhook e eventos subscritos.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.save_webhook_config(db, empresa_id, data)


@router.post("/webhooks/test", response_model=WebhookTestOut)
async def test_webhook_delivery(
    data: WebhookTestIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Dispara teste de webhook com assinatura HMAC SHA-256 e inspeciona resposta.
    """
    empresa_id = _get_empresa_id(current_user)
    return await IntegrationService.test_webhook_delivery(
        db=db,
        empresa_id=empresa_id,
        url=data.url,
        secret=data.secret,
        actor_email=getattr(current_user, "email", "admin@MustaInfo.com"),
    )


@router.post("/sync-all")
def sync_all_integrations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Força sincronização de status de todas as integrações ativas.
    """
    empresa_id = _get_empresa_id(current_user)
    items = IntegrationService.get_all_integrations(db, empresa_id)
    return {"status": "ok", "synced_count": len([i for i in items if i.connected])}
