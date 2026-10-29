from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user_model import User
from app.schemas.integration_schemas import (
    BigQueryConfigIn,
    BigQueryConfigOut,
    BigQueryTestIn,
    BigQueryTestOut,
    GenericIntegrationIn,
    GenericIntegrationTestIn,
    GenericIntegrationTestOut,
    IntegrationItemOut,
    IntegrationLogOut,
    IntegrationStaticConfigOut,
    WebhookConfigIn,
    WebhookConfigOut,
    WebhookTestIn,
    WebhookTestOut,
)
from app.services.integration_service import IntegrationService
from app.ultils.permissions import get_current_user

router = APIRouter(prefix="/integrations", tags=["Integrations"])


def _get_empresa_id(user: User) -> int:
    empresa_id = getattr(user, "empresa_id", None)
    if not empresa_id:
        return 1
    return empresa_id


@router.get("/static-config", response_model=IntegrationStaticConfigOut)
def get_static_config(
    current_user: User = Depends(get_current_user),
):
    """
    Retorna as configurações estáticas de integrações carregadas das variáveis de ambiente (.env).
    """
    return IntegrationService.get_static_config()


@router.get("/", response_model=List[IntegrationItemOut])
def list_integrations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Retorna o catálogo de integrações e o estado atual persistido na base de dados para a empresa do utilizador.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.get_all_integrations(db, empresa_id)


@router.get("/logs", response_model=List[IntegrationLogOut])
def list_integration_logs(
    level: Optional[str] = Query(default=None, description="Filtro de severidade: INFO, WARNING, ERROR, CRITICAL"),
    provider: Optional[str] = Query(default=None, description="Filtro de provedor: bigquery, github, webhook, etc."),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Lista os logs de eventos e erros de integrações registados na tabela `integration_logs`.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.get_integration_logs(
        db=db,
        empresa_id=empresa_id,
        level=level,
        provider=provider,
        limit=limit,
    )


@router.delete("/logs")
def clear_integration_logs(
    provider: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Limpa o histórico de logs de integrações da empresa na base de dados.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.clear_integration_logs(db=db, empresa_id=empresa_id, provider=provider)


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
    Configura ou atualiza a integração com o Google BigQuery na base de dados.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.save_bigquery_config(
        db=db,
        empresa_id=empresa_id,
        data=data,
        user_id=getattr(current_user, "id", None),
    )


@router.post("/bigquery/test", response_model=BigQueryTestOut)
async def test_bigquery_connection(
    data: BigQueryTestIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Valida credenciais, conectividade HTTPS/TLS e latência com a API do Google BigQuery.
    """
    empresa_id = _get_empresa_id(current_user)
    return await IntegrationService.test_bigquery_connection(
        db=db,
        empresa_id=empresa_id,
        data=data,
        user_id=getattr(current_user, "id", None),
    )


@router.post("/toggle")
def toggle_integration(
    provider: str,
    active: Optional[bool] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Conecta ou desconecta uma integração da empresa na base de dados.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.toggle_integration(
        db=db,
        empresa_id=empresa_id,
        provider=provider,
        active=active,
        user_id=getattr(current_user, "id", None),
    )


@router.post("/generic")
async def save_generic_integration(
    data: GenericIntegrationIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Valida (RFC 3986 / RFC 9110 / SSRF) e salva configuração de integrações na base de dados.
    """
    empresa_id = _get_empresa_id(current_user)
    return await IntegrationService.save_generic_integration(
        db=db,
        empresa_id=empresa_id,
        data=data,
        user_id=getattr(current_user, "id", None),
    )


@router.post("/generic/test", response_model=GenericIntegrationTestOut)
async def test_generic_integration(
    data: GenericIntegrationTestIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Executa teste real de conectividade HTTPS/TLS e conformidade RFC 3986 / SSRF para um provedor.
    """
    empresa_id = _get_empresa_id(current_user)
    return await IntegrationService.test_generic_integration(
        db=db,
        empresa_id=empresa_id,
        data=data,
        user_id=getattr(current_user, "id", None),
    )


@router.get("/webhooks", response_model=WebhookConfigOut)
def get_webhook_config(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Recupera as configurações de Webhook da empresa da base de dados.
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
    Valida e salva configurações de Webhook e eventos subscritos na base de dados.
    """
    empresa_id = _get_empresa_id(current_user)
    return IntegrationService.save_webhook_config(
        db=db,
        empresa_id=empresa_id,
        data=data,
        user_id=getattr(current_user, "id", None),
    )


@router.post("/webhooks/test", response_model=WebhookTestOut)
async def test_webhook_delivery(
    data: WebhookTestIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Dispara teste real de Webhook com assinatura HMAC SHA-256 e regista o resultado nos logs.
    """
    empresa_id = _get_empresa_id(current_user)
    return await IntegrationService.test_webhook_delivery(
        db=db,
        empresa_id=empresa_id,
        url=data.url,
        secret=data.secret,
        actor_email=getattr(current_user, "email", "admin@mustainfo.org"),
        user_id=getattr(current_user, "id", None),
    )


@router.post("/sync-all")
async def sync_all_integrations(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Força verificação e sincronização de todas as integrações ativas na base de dados.
    """
    empresa_id = _get_empresa_id(current_user)
    return await IntegrationService.sync_all_active_integrations(
        db=db,
        empresa_id=empresa_id,
        user_id=getattr(current_user, "id", None),
    )
