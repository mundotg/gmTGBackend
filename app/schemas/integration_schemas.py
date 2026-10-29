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
    status: str = "disconnected"
    last_error: Optional[str] = None
    last_latency_ms: Optional[int] = None


class BigQueryTestIn(BaseModel):
    project_id: str
    dataset_id: Optional[str] = None
    region: Optional[str] = "us-central1"
    service_account_json: Optional[str] = None


class BigQueryTestOut(BaseModel):
    success: bool
    latency_ms: int
    message: str
    error_code: Optional[str] = None
    datasets: List[str] = []
    permissions_validated: List[str] = []


class IntegrationItemOut(BaseModel):
    id: str
    name: str
    provider: str
    description: str
    category: str
    badge: Optional[str] = None
    brand_color: str = "#4285F4"
    docs_url: Optional[str] = None
    featured: bool = False
    connected: bool
    status: str = "disconnected"
    endpoint_url: Optional[str] = None
    last_sync: Optional[str] = None
    last_error: Optional[str] = None
    error_count: int = 0
    last_latency_ms: Optional[int] = None
    metrics: Optional[List[Dict[str, str]]] = None
    config: Optional[Dict[str, Any]] = None


class GenericIntegrationIn(BaseModel):
    provider: str
    name: Optional[str] = None
    category: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    credentials: Optional[Dict[str, Any]] = None
    verify_connection: bool = True


class GenericIntegrationTestIn(BaseModel):
    provider: str
    host: str
    token: Optional[str] = None
    extra_config: Optional[Dict[str, Any]] = None


class GenericIntegrationTestOut(BaseModel):
    success: bool
    status_code: Optional[int] = None
    latency_ms: int
    error_code: Optional[str] = None
    message: str
    resolved_endpoint: Optional[str] = None


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
    last_error: Optional[str] = None


class WebhookTestIn(BaseModel):
    url: Optional[str] = None
    secret: Optional[str] = None


class WebhookTestOut(BaseModel):
    status: int
    success: bool
    error_code: Optional[str] = None
    message: str
    latency_ms: int
    payload: Dict[str, Any]


class IntegrationLogOut(BaseModel):
    id: int
    provider: str
    action: str
    level: str
    status_code: Optional[int] = None
    error_code: Optional[str] = None
    endpoint_url: Optional[str] = None
    latency_ms: Optional[int] = None
    message: str
    details: Optional[Dict[str, Any]] = None
    created_at: str


class IntegrationStaticConfigOut(BaseModel):
    enforce_https: bool
    allow_private_networks: bool
    http_timeout_seconds: int
    max_retries: int
    bq_default_region: str
    bq_default_max_bytes_gb: int
    bq_default_cost_alert_usd: float
    bq_cost_per_tb_usd: float
    gcp_regions: List[Dict[str, str]]
    webhook_signature_header: str
    webhook_timestamp_header: str
    webhook_events_catalog: List[Dict[str, str]]
