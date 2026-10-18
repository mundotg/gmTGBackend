from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class BigQueryConfigIn(BaseModel):
    project_id: str = Field(..., description="GCP Project ID do BigQuery")
    dataset_id: str = Field(..., description="Dataset padrão de análise")
    region: str = Field(default="us-central1", description="Região GCP")
    service_account_json: Optional[str] = Field(default=None, description="JSON da Service Account GCP")
    dry_run_enabled: bool = Field(default=True, description="Ativar Dry-Run antes de qualquer query")
    max_bytes_billed_gb: int = Field(default=10, description="Teto de bytes varridos por consulta em GB")
    cost_alert_threshold_usd: float = Field(default=1.0, description="Alerta de custo em USD")
    auto_sync_lineage: bool = Field(default=True, description="Sincronizar esquemas com a Linhagem")
    export_query_audit: bool = Field(default=True, description="Exportar auditoria de consultas")


class BigQueryConfigOut(BaseModel):
    project_id: str
    dataset_id: str
    region: str
    has_credentials: bool
    client_email: Optional[str] = None
    dry_run_enabled: bool
    max_bytes_billed_gb: int
    cost_alert_threshold_usd: float
    auto_sync_lineage: bool
    export_query_audit: bool


class BigQueryTestIn(BaseModel):
    project_id: str
    dataset_id: Optional[str] = None
    region: Optional[str] = "us-central1"
    service_account_json: Optional[str] = None


class BigQueryTestOut(BaseModel):
    success: bool
    latency_ms: int
    message: str
    datasets: List[str] = []
    permissions_validated: List[str] = []


class IntegrationItemOut(BaseModel):
    id: str
    name: str
    provider: str
    description: str
    category: str
    badge: Optional[str] = None
    connected: bool
    last_sync: Optional[str] = None
    metrics: Optional[List[Dict[str, str]]] = None
    config: Optional[Dict[str, Any]] = None


class GenericIntegrationIn(BaseModel):
    provider: str
    name: Optional[str] = None
    category: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    credentials: Optional[Dict[str, Any]] = None


class WebhookConfigIn(BaseModel):
    url: str
    secret: Optional[str] = None
    events: List[str] = []
    is_active: bool = True


class WebhookConfigOut(BaseModel):
    url: str
    secret: str
    events: List[str]
    is_active: bool
    last_triggered_at: Optional[str] = None
    last_status_code: Optional[int] = None


class WebhookTestIn(BaseModel):
    url: Optional[str] = None
    secret: Optional[str] = None


class WebhookTestOut(BaseModel):
    status: int
    success: bool
    message: str
    latency_ms: int
    payload: Dict[str, Any]
