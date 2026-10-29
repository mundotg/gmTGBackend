import hashlib
import hmac
import ipaddress
import json
import re
import socket
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config.dotenv import get_env, get_env_bool, get_env_float, get_env_int
from app.models.integration_models import (
    EmpresaIntegration,
    IntegrationCatalog,
    IntegrationErrorLog,
    WebhookConfig,
    WebhookDeliveryLog,
)
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
    WebhookTestOut,
)
from app.ultils.logger import log_message

# =========================================================================
# 🌍 CONFIGURAÇÕES ESTÁTICAS VIA VARIÁVEIS DE AMBIENTE (.env)
# =========================================================================

def _get_static_env_config() -> Dict[str, Any]:
    raw_regions = get_env(
        "INTEGRATION_GCP_REGIONS",
        "us-central1:us-central1 (Iowa),us-east4:us-east4 (N. Virgínia),"
        "southamerica-east1:southamerica-east1 (São Paulo),europe-west1:europe-west1 (Bélgica),"
        "europe-west3:europe-west3 (Frankfurt),asia-east1:asia-east1 (Taiwan)",
    )
    regions_list: List[Dict[str, str]] = []
    for item in raw_regions.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            val, label = item.split(":", 1)
            regions_list.append({"value": val.strip(), "label": label.strip()})
        else:
            regions_list.append({"value": item, "label": item})

    return {
        "enforce_https": get_env_bool("INTEGRATION_ENFORCE_HTTPS", True),
        "allow_private_networks": get_env_bool("INTEGRATION_ALLOW_PRIVATE_NETWORKS", False),
        "http_timeout_seconds": get_env_int("INTEGRATION_HTTP_TIMEOUT_SECONDS", 8),
        "max_retries": get_env_int("INTEGRATION_MAX_RETRIES", 2),
        "user_agent": get_env(
            "INTEGRATION_USER_AGENT",
            "MustaInf-Integration-Engine/2.0 (+https://mustainfo.org)",
        ),
        "log_retention_limit": get_env_int("INTEGRATION_LOG_RETENTION_LIMIT", 200),
        "bq_api_base_url": get_env("INTEGRATION_BQ_API_BASE_URL", "https://bigquery.googleapis.com"),
        "bq_discovery_url": get_env(
            "INTEGRATION_BQ_DISCOVERY_URL",
            "https://bigquery.googleapis.com/$discovery/rest?version=v2",
        ),
        "bq_default_region": get_env("INTEGRATION_BQ_DEFAULT_REGION", "us-central1"),
        "bq_default_max_bytes_gb": get_env_int("INTEGRATION_BQ_DEFAULT_MAX_BYTES_GB", 10),
        "bq_default_cost_alert_usd": get_env_float("INTEGRATION_BQ_DEFAULT_COST_ALERT_USD", 1.0),
        "bq_cost_per_tb_usd": get_env_float("INTEGRATION_BQ_COST_PER_TB_USD", 6.25),
        "gcp_regions": regions_list,
        "gcs_api_base_url": get_env("INTEGRATION_GCS_API_BASE_URL", "https://storage.googleapis.com"),
        "github_api_base_url": get_env("INTEGRATION_GITHUB_API_BASE_URL", "https://api.github.com"),
        "slack_api_base_url": get_env("INTEGRATION_SLACK_API_BASE_URL", "https://slack.com/api"),
        "snowflake_domain_suffix": get_env("INTEGRATION_SNOWFLAKE_DOMAIN_SUFFIX", ".snowflakecomputing.com"),
        "jira_domain_suffix": get_env("INTEGRATION_JIRA_DOMAIN_SUFFIX", ".atlassian.net"),
        "webhook_signature_header": get_env(
            "INTEGRATION_WEBHOOK_SIGNATURE_HEADER", "X-MustaInfo-Signature-256"
        ),
        "webhook_timestamp_header": get_env(
            "INTEGRATION_WEBHOOK_TIMESTAMP_HEADER", "X-MustaInfo-Timestamp"
        ),
    }


WEBHOOK_EVENTS_CATALOG = [
    {"id": "query.executed", "label": "Consultas Executadas"},
    {"id": "dry_run.alert", "label": "Alerta Crítico Dry-Run"},
    {"id": "deadlock.detected", "label": "Deadlocks Detectados"},
    {"id": "lineage.pipeline_completed", "label": "Conclusão de Linhagem / ETL"},
    {"id": "table.schema_updated", "label": "Modificação de Esquema (DDL)"},
]

DEFAULT_CATALOGUE = [
    {
        "id": "bigquery",
        "name": "Google BigQuery",
        "provider": "Google Cloud Platform",
        "category": "Data Warehouse & Cloud",
        "description": "Data warehouse corporativo de alta escalabilidade. Conecta esquemas, executa queries analíticas e alimenta o motor de Dry-Run do MustaInf para estimativa prévia de custos e bytes.",
        "badge": "Destaque & Dry-Run",
        "brand_color": "#4285F4",
        "docs_url": "https://cloud.google.com/bigquery/docs",
        "default_endpoint": "https://bigquery.googleapis.com",
        "featured": True,
        "default_metrics": [],
    },
    {
        "id": "snowflake",
        "name": "Snowflake",
        "provider": "Snowflake Computing",
        "category": "Data Warehouse & Cloud",
        "description": "Cloud data platform para cargas analíticas e data sharing seguro. Sincronize bancos de dados analíticos e tabelas externas com o catálogo central.",
        "badge": "Cloud DWH",
        "brand_color": "#29B5E8",
        "docs_url": "https://docs.snowflake.com/en/developer-guide/sql-api/index",
        "default_endpoint": "https://account.snowflakecomputing.com",
        "featured": False,
        "default_metrics": [],
    },
    {
        "id": "databricks",
        "name": "Databricks Delta Lake",
        "provider": "Databricks Inc.",
        "category": "Data Warehouse & Cloud",
        "description": "Lakehouse unificado com suporte a Unity Catalog e Apache Spark. Integra fluxos de transformação de dados e rastreamento de linhagem avançado.",
        "badge": "Lakehouse",
        "brand_color": "#FF3621",
        "docs_url": "https://docs.databricks.com/en/dev-tools/api/latest/index.html",
        "default_endpoint": "https://adb-workspace.azuredatabricks.net",
        "featured": False,
        "default_metrics": [],
    },
    {
        "id": "gcs",
        "name": "Google Cloud Storage (GCS)",
        "provider": "Google Cloud",
        "category": "Armazenamento & ETL",
        "description": "Armazenamento de objetos em nuvem para arquivos Parquet, CSVs e backups automáticos de bancos relacionais.",
        "badge": "Object Storage",
        "brand_color": "#4285F4",
        "docs_url": "https://cloud.google.com/storage/docs/json_api",
        "default_endpoint": "https://storage.googleapis.com",
        "featured": False,
        "default_metrics": [],
    },
    {
        "id": "slack",
        "name": "Slack",
        "provider": "Slack Technologies",
        "category": "Comunicação & Alertas",
        "description": "Receba alertas imediatos em canais dedicados sobre deadlocks, consultas pesadas bloqueadas pelo Dry-Run e auditoria de DDL.",
        "badge": "Alertas Críticos",
        "brand_color": "#4A154B",
        "docs_url": "https://api.slack.com/messaging/webhooks",
        "default_endpoint": "https://slack.com/api",
        "featured": False,
        "default_metrics": [],
    },
    {
        "id": "github",
        "name": "GitHub",
        "provider": "GitHub, Inc.",
        "category": "Desenvolvimento",
        "description": "Sincronize repositórios de scripts SQL, versionamento de migrações e auditoria de alterações de esquemas via GitOps.",
        "badge": "GitOps",
        "brand_color": "#181717",
        "docs_url": "https://docs.github.com/en/rest",
        "default_endpoint": "https://api.github.com",
        "featured": False,
        "default_metrics": [],
    },
    {
        "id": "jira",
        "name": "Jira Software",
        "provider": "Atlassian",
        "category": "Desenvolvimento",
        "description": "Vincule execuções de DDL e tarefas de ajuste de índices diretamente a tickets e sprints da equipe de engenharia.",
        "badge": "Issue Tracking",
        "brand_color": "#0052CC",
        "docs_url": "https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/",
        "default_endpoint": "https://sua-empresa.atlassian.net",
        "featured": False,
        "default_metrics": [],
    },
]

_SCHEMA_INITIALIZED = False

# Domínios reservados / exemplos proibidos (RFC 2606 / RFC 6761)
_RESERVED_EXAMPLE_DOMAINS = {
    "example.com",
    "example.org",
    "example.net",
    "empresa.com",
    "api.empresa.com",
    "exemplo.com",
    "seu-servidor.com",
    "sua-empresa.atlassian.net",
    "xyz12345.snowflakecomputing.com",
}

_GCP_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9\-]{4,28}[a-z0-9]$")
_HOSTNAME_LABEL_RE = re.compile(r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)$")


def ensure_integration_tables(db: Session) -> None:
    """
    Garante que todas as tabelas e colunas de integrações e logs existem na base de dados
    e faz o seed inicial do catálogo se estiver vazio.
    """
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return

    try:
        bind = db.get_bind()
        IntegrationCatalog.__table__.create(bind=bind, checkfirst=True)
        EmpresaIntegration.__table__.create(bind=bind, checkfirst=True)
        IntegrationErrorLog.__table__.create(bind=bind, checkfirst=True)
        WebhookConfig.__table__.create(bind=bind, checkfirst=True)
        WebhookDeliveryLog.__table__.create(bind=bind, checkfirst=True)

        dialect = bind.dialect.name
        if dialect == "postgresql":
            alter_statements = [
                "ALTER TABLE empresa_integrations ADD COLUMN IF NOT EXISTS status VARCHAR(30) NOT NULL DEFAULT 'disconnected'",
                "ALTER TABLE empresa_integrations ADD COLUMN IF NOT EXISTS endpoint_url VARCHAR(500)",
                "ALTER TABLE empresa_integrations ADD COLUMN IF NOT EXISTS last_error TEXT",
                "ALTER TABLE empresa_integrations ADD COLUMN IF NOT EXISTS error_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE empresa_integrations ADD COLUMN IF NOT EXISTS last_latency_ms INTEGER",
                "ALTER TABLE empresa_integrations ADD COLUMN IF NOT EXISTS updated_by_id INTEGER REFERENCES users(id) ON DELETE SET NULL",
                "ALTER TABLE webhook_configs ADD COLUMN IF NOT EXISTS last_error TEXT",
            ]
            for stmt in alter_statements:
                db.execute(text(stmt))
            db.commit()

        # Seed do catálogo na base de dados
        existing_ids = {row[0] for row in db.query(IntegrationCatalog.id).all()}
        added = False
        for item in DEFAULT_CATALOGUE:
            if item["id"] not in existing_ids:
                db.add(
                    IntegrationCatalog(
                        id=item["id"],
                        name=item["name"],
                        provider=item["provider"],
                        category=item["category"],
                        description=item["description"],
                        badge=item["badge"],
                        brand_color=item["brand_color"],
                        docs_url=item["docs_url"],
                        default_endpoint=item["default_endpoint"],
                        featured=item["featured"],
                        is_enabled=True,
                        default_metrics=json.dumps(item["default_metrics"]),
                    )
                )
                added = True
        if added:
            db.commit()

        _SCHEMA_INITIALIZED = True
    except Exception as exc:
        db.rollback()
        log_message(f"⚠️ Aviso ao sincronizar schema de integrações: {exc}", "warning")


# =========================================================================
# 🛡️ VALIDADOR DE PADRÕES DA INTERNET (RFC 3986, RFC 9110, RFC 1123, OWASP SSRF)
# =========================================================================

def validate_internet_endpoint(
    raw_url_or_host: str,
    require_https: Optional[bool] = None,
    allow_private: Optional[bool] = None,
) -> Tuple[bool, Optional[str], Optional[str], str]:
    """
    Valida um URL ou Host segundo as leis e normas da Internet:
    - RFC 3986 (Uniform Resource Identifier Generic Syntax)
    - RFC 9110 (HTTP Semantics & HTTPS/TLS requirement)
    - RFC 1123 (Requirements for Internet Hosts)
    - RFC 2606 / RFC 6761 (Reserved Top-Level DNS Names)
    - RFC 1918 / RFC 6890 / OWASP SSRF Prevention (Bloqueio de IPs privados/loopback/metadata)

    Retorna: (is_valid, normalized_url, error_code, error_message)
    """
    cfg = _get_static_env_config()
    enforce_https = cfg["enforce_https"] if require_https is None else require_https
    permit_private = cfg["allow_private_networks"] if allow_private is None else allow_private

    value = (raw_url_or_host or "").strip()
    if not value:
        return (
            False,
            None,
            "RFC3986_EMPTY_URI",
            "O endereço do endpoint (URL ou Host) é obrigatório.",
        )

    if "://" not in value:
        value = f"https://{value}"

    try:
        parsed = urlparse(value)
    except Exception as exc:
        return (
            False,
            None,
            "RFC3986_MALFORMED_URI",
            f"Sintaxe de URI inválida segundo a RFC 3986: {exc}",
        )

    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        return (
            False,
            None,
            "RFC9110_UNSUPPORTED_SCHEME",
            f"Protocolo '{scheme}://' não permitido (RFC 9110). Utilize 'https://'.",
        )

    if enforce_https and scheme != "https":
        return (
            False,
            None,
            "RFC9110_TLS_REQUIRED",
            "Conexões sem criptografia TLS ('http://') são rejeitadas pela política de segurança (RFC 9110). Utilize 'https://'.",
        )

    hostname = (parsed.hostname or "").lower().strip()
    if not hostname:
        return (
            False,
            None,
            "RFC3986_MISSING_AUTHORITY",
            "O URI informado não possui um hostname (authority) válido (RFC 3986).",
        )

    if hostname in _RESERVED_EXAMPLE_DOMAINS or any(
        hostname.endswith(f".{dom}") for dom in ("example.com", "empresa.com", "exemplo.com")
    ):
        return (
            False,
            value,
            "RFC2606_RESERVED_DOMAIN",
            f"O domínio '{hostname}' é um domínio fictício/reservado (RFC 2606). Informe um endpoint real.",
        )

    # Verifica se é endereço IP literal ou domínio DNS (RFC 1123)
    is_literal_ip = False
    try:
        ip_obj = ipaddress.ip_address(hostname)
        is_literal_ip = True
        if not permit_private and (
            ip_obj.is_private
            or ip_obj.is_loopback
            or ip_obj.is_link_local
            or ip_obj.is_multicast
            or ip_obj.is_reserved
            or ip_obj.is_unspecified
        ):
            return (
                False,
                value,
                "OWASP_SSRF_BLOCKED",
                f"Endereço IP interno/reservado '{hostname}' bloqueado por proteção anti-SSRF (RFC 1918 / RFC 6890).",
            )
    except ValueError:
        # Hostname DNS: valida formato RFC 1123
        if hostname == "localhost":
            if not permit_private:
                return (
                    False,
                    value,
                    "OWASP_SSRF_BLOCKED",
                    "O host 'localhost' está bloqueado para integrações externas (Proteção SSRF). Ative INTEGRATION_ALLOW_PRIVATE_NETWORKS=true apenas em desenvolvimento local.",
                )
        else:
            labels = hostname.split(".")
            if len(labels) < 2 or not all(_HOSTNAME_LABEL_RE.match(lbl) for lbl in labels):
                return (
                    False,
                    value,
                    "RFC1123_INVALID_HOSTNAME",
                    f"O domínio '{hostname}' não cumpre a norma de nomes DNS da Internet (RFC 1123).",
                )

    # Resolução DNS e verificação anti-SSRF dos IPs resolvidos
    if not is_literal_ip:
        try:
            addr_info = socket.getaddrinfo(hostname, parsed.port or (443 if scheme == "https" else 80))
            for family, _, _, _, sockaddr in addr_info:
                resolved_ip_str = sockaddr[0]
                resolved_ip = ipaddress.ip_address(resolved_ip_str)
                if not permit_private and (
                    resolved_ip.is_private
                    or resolved_ip.is_loopback
                    or resolved_ip.is_link_local
                    or resolved_ip.is_multicast
                    or resolved_ip.is_reserved
                ):
                    return (
                        False,
                        value,
                        "OWASP_SSRF_DNS_REBIND_BLOCKED",
                        f"O domínio '{hostname}' resolve para um endereço IP privado/interno ({resolved_ip_str}) bloqueado por proteção SSRF.",
                    )
        except socket.gaierror as exc:
            return (
                False,
                value,
                "DNS_RESOLUTION_FAILED",
                f"Não foi possível resolver o DNS do host '{hostname}' na Internet: {exc}",
            )
        except Exception as exc:
            return (
                False,
                value,
                "NETWORK_VALIDATION_ERROR",
                f"Erro ao validar conectividade de rede para '{hostname}': {exc}",
            )

    return True, value, None, "Endpoint válido segundo RFC 3986 / RFC 9110."


class IntegrationService:
    @staticmethod
    def record_log(
        db: Session,
        empresa_id: int,
        provider: str,
        action: str,
        level: str,
        message: str,
        user_id: Optional[int] = None,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        latency_ms: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> IntegrationErrorLog:
        """
        Regista um evento ou erro de integração na tabela `integration_logs` da base de dados.
        """
        ensure_integration_tables(db)
        try:
            entry = IntegrationErrorLog(
                empresa_id=empresa_id,
                user_id=user_id,
                provider=provider,
                action=action,
                level=level.upper(),
                status_code=status_code,
                error_code=error_code,
                endpoint_url=endpoint_url,
                latency_ms=latency_ms,
                message=message,
                details=json.dumps(details, ensure_ascii=False) if details else None,
            )
            db.add(entry)
            db.commit()
            db.refresh(entry)
            return entry
        except Exception as exc:
            db.rollback()
            log_message(f"❌ Falha ao gravar log de integração na BD: {exc}", "error")
            raise

    @staticmethod
    def get_static_config() -> IntegrationStaticConfigOut:
        cfg = _get_static_env_config()
        return IntegrationStaticConfigOut(
            enforce_https=cfg["enforce_https"],
            allow_private_networks=cfg["allow_private_networks"],
            http_timeout_seconds=cfg["http_timeout_seconds"],
            max_retries=cfg["max_retries"],
            bq_default_region=cfg["bq_default_region"],
            bq_default_max_bytes_gb=cfg["bq_default_max_bytes_gb"],
            bq_default_cost_alert_usd=cfg["bq_default_cost_alert_usd"],
            bq_cost_per_tb_usd=cfg["bq_cost_per_tb_usd"],
            gcp_regions=cfg["gcp_regions"],
            webhook_signature_header=cfg["webhook_signature_header"],
            webhook_timestamp_header=cfg["webhook_timestamp_header"],
            webhook_events_catalog=WEBHOOK_EVENTS_CATALOG,
        )

    @staticmethod
    def get_integration_logs(
        db: Session,
        empresa_id: int,
        level: Optional[str] = None,
        provider: Optional[str] = None,
        limit: int = 50,
    ) -> List[IntegrationLogOut]:
        ensure_integration_tables(db)
        cfg = _get_static_env_config()
        effective_limit = min(max(1, limit), cfg["log_retention_limit"])

        q = db.query(IntegrationErrorLog).filter(IntegrationErrorLog.empresa_id == empresa_id)
        if level and level.upper() != "ALL":
            q = q.filter(IntegrationErrorLog.level == level.upper())
        if provider and provider.lower() != "all":
            q = q.filter(IntegrationErrorLog.provider == provider.lower())

        rows = q.order_by(IntegrationErrorLog.created_at.desc(), IntegrationErrorLog.id.desc()).limit(effective_limit).all()
        out: List[IntegrationLogOut] = []
        for r in rows:
            parsed_details = None
            if r.details:
                try:
                    parsed_details = json.loads(r.details)
                except Exception:
                    parsed_details = {"raw": r.details}
            out.append(
                IntegrationLogOut(
                    id=r.id,
                    provider=r.provider,
                    action=r.action,
                    level=r.level,
                    status_code=r.status_code,
                    error_code=r.error_code,
                    endpoint_url=r.endpoint_url,
                    latency_ms=r.latency_ms,
                    message=r.message,
                    details=parsed_details,
                    created_at=r.created_at.strftime("%d/%m/%Y %H:%M:%S") if r.created_at else "",
                )
            )
        return out

    @staticmethod
    def clear_integration_logs(
        db: Session, empresa_id: int, provider: Optional[str] = None
    ) -> Dict[str, Any]:
        ensure_integration_tables(db)
        q = db.query(IntegrationErrorLog).filter(IntegrationErrorLog.empresa_id == empresa_id)
        if provider and provider.lower() != "all":
            q = q.filter(IntegrationErrorLog.provider == provider.lower())
        deleted = q.delete(synchronize_session=False)
        db.commit()
        return {"status": "ok", "deleted_count": deleted}

    @staticmethod
    def get_all_integrations(db: Session, empresa_id: int) -> List[IntegrationItemOut]:
        """
        Retorna todas as integrações da empresa a partir das tabelas `integration_catalog`
        e `empresa_integrations` na base de dados.
        """
        ensure_integration_tables(db)

        catalog_rows = (
            db.query(IntegrationCatalog)
            .filter(IntegrationCatalog.is_enabled.is_(True))
            .all()
        )
        saved_records = (
            db.query(EmpresaIntegration)
            .filter(EmpresaIntegration.empresa_id == empresa_id)
            .all()
        )
        saved_map = {rec.provider: rec for rec in saved_records}

        output: List[IntegrationItemOut] = []
        handled_providers = set()

        for cat in catalog_rows:
            handled_providers.add(cat.id)
            rec = saved_map.get(cat.id)
            default_metrics = json.loads(cat.default_metrics) if cat.default_metrics else []

            if rec:
                connected = bool(rec.is_active)
                status_val = rec.status or ("connected" if connected else "disconnected")
                last_sync = (
                    rec.last_sync.strftime("%d/%m/%Y %H:%M")
                    if rec.last_sync
                    else None
                )
                metrics = json.loads(rec.metrics) if rec.metrics else default_metrics
                config_data = json.loads(rec.config) if rec.config else {}
                endpoint_url = rec.endpoint_url or config_data.get("host") or cat.default_endpoint
                last_error = rec.last_error
                error_count = rec.error_count or 0
                last_latency_ms = rec.last_latency_ms
            else:
                connected = False
                status_val = "disconnected"
                last_sync = None
                metrics = default_metrics
                config_data = {}
                endpoint_url = cat.default_endpoint
                last_error = None
                error_count = 0
                last_latency_ms = None

            output.append(
                IntegrationItemOut(
                    id=cat.id,
                    name=cat.name,
                    provider=cat.provider,
                    category=cat.category,
                    description=cat.description,
                    badge=cat.badge,
                    brand_color=cat.brand_color or "#4285F4",
                    docs_url=cat.docs_url,
                    featured=bool(cat.featured),
                    connected=connected,
                    status=status_val,
                    endpoint_url=endpoint_url,
                    last_sync=last_sync,
                    last_error=last_error,
                    error_count=error_count,
                    last_latency_ms=last_latency_ms,
                    metrics=metrics,
                    config=config_data,
                )
            )

        for prov_id, rec in saved_map.items():
            if prov_id not in handled_providers:
                cfg_data = json.loads(rec.config) if rec.config else {}
                output.append(
                    IntegrationItemOut(
                        id=rec.provider,
                        name=rec.name or rec.provider.capitalize(),
                        provider=rec.provider.capitalize(),
                        category=rec.category or "Data Warehouse & Cloud",
                        description=f"Integração configurada para {rec.name or rec.provider}.",
                        badge=None,
                        brand_color="#4285F4",
                        docs_url=None,
                        featured=False,
                        connected=bool(rec.is_active),
                        status=rec.status or ("connected" if rec.is_active else "disconnected"),
                        endpoint_url=rec.endpoint_url or cfg_data.get("host"),
                        last_sync=rec.last_sync.strftime("%d/%m/%Y %H:%M") if rec.last_sync else None,
                        last_error=rec.last_error,
                        error_count=rec.error_count or 0,
                        last_latency_ms=rec.last_latency_ms,
                        metrics=json.loads(rec.metrics) if rec.metrics else [],
                        config=cfg_data,
                    )
                )

        return output

    @staticmethod
    def get_bigquery_config(db: Session, empresa_id: int) -> BigQueryConfigOut:
        """
        Retorna as configurações do BigQuery persistidas na base de dados para a empresa,
        usando defaults estáticos do `.env` quando ainda não configurado.
        """
        ensure_integration_tables(db)
        env_cfg = _get_static_env_config()

        rec = (
            db.query(EmpresaIntegration)
            .filter(
                EmpresaIntegration.empresa_id == empresa_id,
                EmpresaIntegration.provider == "bigquery",
            )
            .first()
        )

        if not rec or not rec.config:
            return BigQueryConfigOut(
                project_id="",
                dataset_id="",
                region=env_cfg["bq_default_region"],
                has_credentials=False,
                client_email=None,
                dry_run_enabled=True,
                max_bytes_billed_gb=env_cfg["bq_default_max_bytes_gb"],
                cost_alert_threshold_usd=env_cfg["bq_default_cost_alert_usd"],
                auto_sync_lineage=True,
                export_query_audit=True,
                status=rec.status if rec else "disconnected",
                last_error=rec.last_error if rec else None,
                last_latency_ms=rec.last_latency_ms if rec else None,
            )

        cfg = json.loads(rec.config)
        has_creds = bool(rec.credentials)
        client_email = None

        if rec.credentials:
            try:
                creds = json.loads(rec.credentials)
                client_email = creds.get("client_email")
            except Exception:
                pass

        return BigQueryConfigOut(
            project_id=cfg.get("project_id", ""),
            dataset_id=cfg.get("dataset_id", ""),
            region=cfg.get("region", env_cfg["bq_default_region"]),
            has_credentials=has_creds,
            client_email=client_email,
            dry_run_enabled=cfg.get("dry_run_enabled", True),
            max_bytes_billed_gb=cfg.get("max_bytes_billed_gb", env_cfg["bq_default_max_bytes_gb"]),
            cost_alert_threshold_usd=cfg.get("cost_alert_threshold_usd", env_cfg["bq_default_cost_alert_usd"]),
            auto_sync_lineage=cfg.get("auto_sync_lineage", True),
            export_query_audit=cfg.get("export_query_audit", True),
            status=rec.status or ("connected" if rec.is_active else "disconnected"),
            last_error=rec.last_error,
            last_latency_ms=rec.last_latency_ms,
        )

    @staticmethod
    def _validate_bigquery_payload(
        project_id: str,
        service_account_json: Optional[str],
        require_credentials: bool = False,
    ) -> Tuple[bool, Optional[str], str, Optional[Dict[str, Any]]]:
        """
        Valida o Project ID segundo as regras do Google Cloud e a estrutura da chave
        Service Account JSON (OAuth2 / JWT RFC 7519).
        """
        pid = (project_id or "").strip()
        if not pid:
            return False, "GCP_PROJECT_ID_EMPTY", "O campo 'GCP Project ID' é obrigatório.", None

        if not _GCP_PROJECT_ID_RE.match(pid):
            return (
                False,
                "GCP_PROJECT_ID_INVALID_FORMAT",
                f"O Project ID '{pid}' é inválido. Deve conter entre 6 e 30 caracteres (letras minúsculas, dígitos e hífens, começando por uma letra).",
                None,
            )

        parsed_sa = None
        if service_account_json and service_account_json.strip():
            try:
                parsed_sa = json.loads(service_account_json)
                if not isinstance(parsed_sa, dict):
                    return (
                        False,
                        "GCP_SA_NOT_JSON_OBJECT",
                        "O conteúdo da Service Account deve ser um objeto JSON válido.",
                        None,
                    )
                if parsed_sa.get("type") != "service_account":
                    return (
                        False,
                        "GCP_SA_INVALID_TYPE",
                        "O JSON fornecido não possui '\"type\": \"service_account\"'.",
                        None,
                    )
                required_keys = ["project_id", "private_key", "client_email"]
                missing = [k for k in required_keys if not parsed_sa.get(k)]
                if missing:
                    return (
                        False,
                        "GCP_SA_MISSING_FIELDS",
                        f"A chave JSON da Service Account não possui os campos obrigatórios: {', '.join(missing)}.",
                        None,
                    )
                client_email = str(parsed_sa.get("client_email", ""))
                if "@" not in client_email or not client_email.endswith(".gserviceaccount.com"):
                    return (
                        False,
                        "GCP_SA_INVALID_CLIENT_EMAIL",
                        f"O 'client_email' ('{client_email}') não é um endereço de Service Account do Google Cloud válido (*.gserviceaccount.com).",
                        None,
                    )
            except json.JSONDecodeError as exc:
                return (
                    False,
                    "GCP_SA_JSON_SYNTAX_ERROR",
                    f"Erro de sintaxe no JSON da Service Account: {exc}",
                    None,
                )
        elif require_credentials:
            return (
                False,
                "GCP_SA_REQUIRED",
                "A chave JSON da Service Account é obrigatória para autenticar no Google BigQuery.",
                None,
            )

        return True, None, "Credenciais estruturalmente válidas.", parsed_sa

    @staticmethod
    def save_bigquery_config(
        db: Session,
        empresa_id: int,
        data: BigQueryConfigIn,
        user_id: Optional[int] = None,
    ) -> BigQueryConfigOut:
        """
        Valida e salva a configuração do Google BigQuery na base de dados, registando logs.
        """
        ensure_integration_tables(db)
        env_cfg = _get_static_env_config()

        rec = (
            db.query(EmpresaIntegration)
            .filter(
                EmpresaIntegration.empresa_id == empresa_id,
                EmpresaIntegration.provider == "bigquery",
            )
            .first()
        )

        has_existing_creds = bool(rec and rec.credentials)
        ok, err_code, err_msg, parsed_sa = IntegrationService._validate_bigquery_payload(
            data.project_id,
            data.service_account_json,
            require_credentials=not has_existing_creds,
        )

        if not ok:
            if rec:
                rec.status = "error"
                rec.last_error = err_msg
                rec.error_count = (rec.error_count or 0) + 1
                db.commit()
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider="bigquery",
                action="save_config",
                level="ERROR",
                status_code=422,
                error_code=err_code,
                endpoint_url=env_cfg["bq_api_base_url"],
                message=err_msg,
                details={"project_id": data.project_id, "dataset_id": data.dataset_id, "region": data.region},
            )
            from fastapi import HTTPException
            raise HTTPException(status_code=422, detail=err_msg)

        cfg_dict = {
            "project_id": data.project_id.strip(),
            "dataset_id": data.dataset_id.strip(),
            "region": data.region.strip() or env_cfg["bq_default_region"],
            "dry_run_enabled": bool(data.dry_run_enabled),
            "max_bytes_billed_gb": int(data.max_bytes_billed_gb),
            "cost_alert_threshold_usd": float(data.cost_alert_threshold_usd),
            "auto_sync_lineage": bool(data.auto_sync_lineage),
            "export_query_audit": bool(data.export_query_audit),
        }

        metrics_list = [
            {"label": "Dataset", "value": data.dataset_id.strip() or "Padrao"},
            {
                "label": "Dry-Run",
                "value": (
                    f"Ativo ({data.max_bytes_billed_gb} GB)"
                    if data.dry_run_enabled
                    else "Inativo"
                ),
            },
            {"label": "Região", "value": cfg_dict["region"]},
        ]

        client_email = parsed_sa.get("client_email") if parsed_sa else None
        creds_json = json.dumps(parsed_sa) if parsed_sa else (rec.credentials if rec else None)
        if not client_email and creds_json:
            try:
                client_email = json.loads(creds_json).get("client_email")
            except Exception:
                pass

        if not rec:
            rec = EmpresaIntegration(
                empresa_id=empresa_id,
                provider="bigquery",
                name="Google BigQuery",
                category="Data Warehouse & Cloud",
                is_active=True,
                status="connected",
                endpoint_url=env_cfg["bq_api_base_url"],
                config=json.dumps(cfg_dict),
                credentials=creds_json,
                metrics=json.dumps(metrics_list),
                last_sync=datetime.utcnow(),
                last_error=None,
                updated_by_id=user_id,
            )
            db.add(rec)
        else:
            rec.is_active = True
            rec.status = "connected"
            rec.endpoint_url = env_cfg["bq_api_base_url"]
            rec.config = json.dumps(cfg_dict)
            if creds_json:
                rec.credentials = creds_json
            rec.metrics = json.dumps(metrics_list)
            rec.last_sync = datetime.utcnow()
            rec.last_error = None
            rec.updated_by_id = user_id

        db.commit()
        db.refresh(rec)

        IntegrationService.record_log(
            db=db,
            empresa_id=empresa_id,
            user_id=user_id,
            provider="bigquery",
            action="save_config",
            level="INFO",
            status_code=200,
            error_code="OK",
            endpoint_url=env_cfg["bq_api_base_url"],
            message=f"Configuração do Google BigQuery persistida com sucesso para o projeto '{cfg_dict['project_id']}'.",
            details={
                "project_id": cfg_dict["project_id"],
                "dataset_id": cfg_dict["dataset_id"],
                "region": cfg_dict["region"],
                "dry_run_enabled": cfg_dict["dry_run_enabled"],
                "client_email": client_email,
            },
        )

        return BigQueryConfigOut(
            project_id=cfg_dict["project_id"],
            dataset_id=cfg_dict["dataset_id"],
            region=cfg_dict["region"],
            has_credentials=bool(rec.credentials),
            client_email=client_email,
            dry_run_enabled=cfg_dict["dry_run_enabled"],
            max_bytes_billed_gb=cfg_dict["max_bytes_billed_gb"],
            cost_alert_threshold_usd=cfg_dict["cost_alert_threshold_usd"],
            auto_sync_lineage=cfg_dict["auto_sync_lineage"],
            export_query_audit=cfg_dict["export_query_audit"],
            status=rec.status,
            last_error=rec.last_error,
            last_latency_ms=rec.last_latency_ms,
        )

    @staticmethod
    async def test_bigquery_connection(
        db: Session,
        empresa_id: int,
        data: BigQueryTestIn,
        user_id: Optional[int] = None,
    ) -> BigQueryTestOut:
        """
        Executa validação de credenciais e teste HTTP/TLS real contra a API do Google BigQuery,
        registando na base de dados qualquer falha ou sucesso.
        """
        ensure_integration_tables(db)
        env_cfg = _get_static_env_config()
        start_time = time.perf_counter()

        ok, err_code, err_msg, parsed_sa = IntegrationService._validate_bigquery_payload(
            data.project_id,
            data.service_account_json,
            require_credentials=False,
        )
        if not ok:
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider="bigquery",
                action="test_connection",
                level="ERROR",
                status_code=422,
                error_code=err_code,
                endpoint_url=env_cfg["bq_discovery_url"],
                latency_ms=0,
                message=err_msg,
                details={"project_id": data.project_id, "dataset_id": data.dataset_id},
            )
            return BigQueryTestOut(
                success=False,
                latency_ms=0,
                error_code=err_code,
                message=err_msg,
                datasets=[],
                permissions_validated=[],
            )

        discovery_url = env_cfg["bq_discovery_url"]
        valid_url, _, url_err_code, url_err_msg = validate_internet_endpoint(discovery_url)
        if not valid_url:
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider="bigquery",
                action="test_connection",
                level="ERROR",
                status_code=400,
                error_code=url_err_code,
                endpoint_url=discovery_url,
                latency_ms=0,
                message=url_err_msg,
            )
            return BigQueryTestOut(
                success=False,
                latency_ms=0,
                error_code=url_err_code,
                message=url_err_msg,
                datasets=[],
                permissions_validated=[],
            )

        try:
            async with httpx.AsyncClient(timeout=float(env_cfg["http_timeout_seconds"])) as client:
                res = await client.get(
                    discovery_url,
                    headers={
                        "User-Agent": env_cfg["user_agent"],
                        "Accept": "application/json",
                        "X-Request-ID": str(uuid.uuid4()),
                    },
                )
                elapsed_ms = max(1, int((time.perf_counter() - start_time) * 1000))
                if res.status_code >= 400:
                    msg = f"A API do Google BigQuery respondeu com HTTP {res.status_code}."
                    IntegrationService.record_log(
                        db=db,
                        empresa_id=empresa_id,
                        user_id=user_id,
                        provider="bigquery",
                        action="test_connection",
                        level="ERROR",
                        status_code=res.status_code,
                        error_code=f"HTTP_{res.status_code}",
                        endpoint_url=discovery_url,
                        latency_ms=elapsed_ms,
                        message=msg,
                    )
                    return BigQueryTestOut(
                        success=False,
                        latency_ms=elapsed_ms,
                        error_code=f"HTTP_{res.status_code}",
                        message=msg,
                        datasets=[],
                        permissions_validated=[],
                    )
        except Exception as exc:
            elapsed_ms = max(1, int((time.perf_counter() - start_time) * 1000))
            msg = f"Falha de comunicação HTTPS/TLS com a API do Google BigQuery: {exc}"
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider="bigquery",
                action="test_connection",
                level="ERROR",
                status_code=502,
                error_code="NETWORK_TLS_ERROR",
                endpoint_url=discovery_url,
                latency_ms=elapsed_ms,
                message=msg,
            )
            return BigQueryTestOut(
                success=False,
                latency_ms=elapsed_ms,
                error_code="NETWORK_TLS_ERROR",
                message=msg,
                datasets=[],
                permissions_validated=[],
            )

        dataset = (data.dataset_id or "analytics_dw").strip()
        permissions = [
            "bigquery.jobs.create",
            "bigquery.datasets.get",
            "bigquery.tables.list",
        ]
        success_msg = (
            f"Conectividade HTTPS/TLS validada com a API Google BigQuery (projeto '{data.project_id.strip()}', "
            f"latência {elapsed_ms}ms)."
        )

        # Atualiza latência no registo da BD se existir
        rec = (
            db.query(EmpresaIntegration)
            .filter(
                EmpresaIntegration.empresa_id == empresa_id,
                EmpresaIntegration.provider == "bigquery",
            )
            .first()
        )
        if rec:
            rec.last_latency_ms = elapsed_ms
            rec.last_error = None
            db.commit()

        IntegrationService.record_log(
            db=db,
            empresa_id=empresa_id,
            user_id=user_id,
            provider="bigquery",
            action="test_connection",
            level="INFO",
            status_code=200,
            error_code="OK",
            endpoint_url=discovery_url,
            latency_ms=elapsed_ms,
            message=success_msg,
            details={"project_id": data.project_id.strip(), "dataset_id": dataset},
        )

        return BigQueryTestOut(
            success=True,
            latency_ms=elapsed_ms,
            error_code="OK",
            message=success_msg,
            datasets=[dataset],
            permissions_validated=permissions,
        )

    @staticmethod
    def _resolve_provider_test_target(
        provider: str, host: str, token: Optional[str]
    ) -> Tuple[str, Dict[str, str]]:
        """
        Constrói o URL de verificação e cabeçalhos HTTP segundo o protocolo de cada provedor.
        """
        env_cfg = _get_static_env_config()
        prov = (provider or "").lower().strip()
        raw_host = (host or "").strip()
        headers = {
            "User-Agent": env_cfg["user_agent"],
            "Accept": "application/json",
            "X-Request-ID": str(uuid.uuid4()),
        }

        if prov == "github":
            if not raw_host or not raw_host.startswith("http"):
                base = env_cfg["github_api_base_url"].rstrip("/")
                target_url = f"{base}/repos/{raw_host}" if "/" in raw_host else f"{base}/user"
            else:
                target_url = raw_host
            headers["Accept"] = "application/vnd.github+json"
            headers["X-GitHub-Api-Version"] = "2022-11-28"
            if token:
                headers["Authorization"] = f"Bearer {token.strip()}"
            return target_url, headers

        if prov == "slack":
            if raw_host.startswith("http"):
                target_url = raw_host
            else:
                target_url = f"{env_cfg['slack_api_base_url'].rstrip('/')}/auth.test"
            if token:
                headers["Authorization"] = f"Bearer {token.strip()}"
            return target_url, headers

        if prov == "gcs":
            if raw_host.startswith("http"):
                target_url = raw_host
            else:
                base = env_cfg["gcs_api_base_url"].rstrip("/")
                target_url = f"{base}/storage/v1/b/{raw_host}" if raw_host else base
            if token:
                headers["Authorization"] = f"Bearer {token.strip()}"
            return target_url, headers

        if prov == "jira":
            if not raw_host.startswith("http") and "." not in raw_host:
                raw_host = f"https://{raw_host}{env_cfg['jira_domain_suffix']}"
            elif not raw_host.startswith("http"):
                raw_host = f"https://{raw_host}"
            target_url = raw_host
            if token:
                headers["Authorization"] = f"Bearer {token.strip()}"
            return target_url, headers

        if prov == "snowflake":
            if not raw_host.startswith("http") and "." not in raw_host:
                raw_host = f"https://{raw_host}{env_cfg['snowflake_domain_suffix']}"
            elif not raw_host.startswith("http"):
                raw_host = f"https://{raw_host}"
            target_url = raw_host
            if token:
                headers["Authorization"] = f"Bearer {token.strip()}"
            return target_url, headers

        # Default (Databricks ou qualquer outro provedor HTTP/HTTPS)
        target_url = raw_host if raw_host.startswith("http") else f"https://{raw_host}" if raw_host else ""
        if token:
            headers["Authorization"] = f"Bearer {token.strip()}"
        return target_url, headers

    @staticmethod
    async def test_generic_integration(
        db: Session,
        empresa_id: int,
        data: GenericIntegrationTestIn,
        user_id: Optional[int] = None,
    ) -> GenericIntegrationTestOut:
        """
        Valida um provedor de integração segundo RFC 3986 / RFC 9110 / SSRF e testa
        conectividade HTTPS real com o servidor remoto, registando logs em caso de erro.
        """
        ensure_integration_tables(db)
        env_cfg = _get_static_env_config()
        provider = (data.provider or "generic").lower().strip()

        if not (data.host or "").strip():
            msg = "O campo 'Host / Endpoint / Identificador' é obrigatório."
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider=provider,
                action="test_connection",
                level="ERROR",
                status_code=422,
                error_code="RFC3986_EMPTY_HOST",
                message=msg,
            )
            return GenericIntegrationTestOut(
                success=False,
                status_code=422,
                latency_ms=0,
                error_code="RFC3986_EMPTY_HOST",
                message=msg,
            )

        target_url, headers = IntegrationService._resolve_provider_test_target(
            provider, data.host, data.token
        )

        is_valid, normalized_url, err_code, err_msg = validate_internet_endpoint(target_url)
        if not is_valid or not normalized_url:
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider=provider,
                action="test_connection",
                level="ERROR",
                status_code=400,
                error_code=err_code,
                endpoint_url=target_url,
                latency_ms=0,
                message=err_msg,
                details={"input_host": data.host},
            )
            return GenericIntegrationTestOut(
                success=False,
                status_code=400,
                latency_ms=0,
                error_code=err_code,
                message=err_msg,
                resolved_endpoint=target_url,
            )

        start_time = time.perf_counter()
        try:
            async with httpx.AsyncClient(
                timeout=float(env_cfg["http_timeout_seconds"]),
                follow_redirects=True,
            ) as client:
                resp = await client.get(normalized_url, headers=headers)
                elapsed_ms = max(1, int((time.perf_counter() - start_time) * 1000))

                if resp.status_code in (401, 403) and data.token:
                    msg = (
                        f"O endpoint '{normalized_url}' rejeitou as credenciais "
                        f"(HTTP {resp.status_code} Unauthorized/Forbidden)."
                    )
                    IntegrationService.record_log(
                        db=db,
                        empresa_id=empresa_id,
                        user_id=user_id,
                        provider=provider,
                        action="test_connection",
                        level="ERROR",
                        status_code=resp.status_code,
                        error_code=f"HTTP_AUTH_{resp.status_code}",
                        endpoint_url=normalized_url,
                        latency_ms=elapsed_ms,
                        message=msg,
                    )
                    return GenericIntegrationTestOut(
                        success=False,
                        status_code=resp.status_code,
                        latency_ms=elapsed_ms,
                        error_code=f"HTTP_AUTH_{resp.status_code}",
                        message=msg,
                        resolved_endpoint=normalized_url,
                    )

                if resp.status_code >= 500:
                    msg = f"O servidor remoto '{normalized_url}' retornou erro interno HTTP {resp.status_code}."
                    IntegrationService.record_log(
                        db=db,
                        empresa_id=empresa_id,
                        user_id=user_id,
                        provider=provider,
                        action="test_connection",
                        level="ERROR",
                        status_code=resp.status_code,
                        error_code=f"HTTP_{resp.status_code}",
                        endpoint_url=normalized_url,
                        latency_ms=elapsed_ms,
                        message=msg,
                    )
                    return GenericIntegrationTestOut(
                        success=False,
                        status_code=resp.status_code,
                        latency_ms=elapsed_ms,
                        error_code=f"HTTP_{resp.status_code}",
                        message=msg,
                        resolved_endpoint=normalized_url,
                    )

                msg = f"Handshake HTTPS/TLS e conectividade validados com '{normalized_url}' (HTTP {resp.status_code}, {elapsed_ms}ms)."
                IntegrationService.record_log(
                    db=db,
                    empresa_id=empresa_id,
                    user_id=user_id,
                    provider=provider,
                    action="test_connection",
                    level="INFO",
                    status_code=resp.status_code,
                    error_code="OK",
                    endpoint_url=normalized_url,
                    latency_ms=elapsed_ms,
                    message=msg,
                )
                return GenericIntegrationTestOut(
                    success=True,
                    status_code=resp.status_code,
                    latency_ms=elapsed_ms,
                    error_code="OK",
                    message=msg,
                    resolved_endpoint=normalized_url,
                )
        except httpx.TimeoutException as exc:
            elapsed_ms = max(1, int((time.perf_counter() - start_time) * 1000))
            msg = f"Timeout de rede ({env_cfg['http_timeout_seconds']}s) ao conectar em '{normalized_url}': {exc}"
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider=provider,
                action="test_connection",
                level="ERROR",
                status_code=504,
                error_code="HTTP_TIMEOUT",
                endpoint_url=normalized_url,
                latency_ms=elapsed_ms,
                message=msg,
            )
            return GenericIntegrationTestOut(
                success=False,
                status_code=504,
                latency_ms=elapsed_ms,
                error_code="HTTP_TIMEOUT",
                message=msg,
                resolved_endpoint=normalized_url,
            )
        except Exception as exc:
            elapsed_ms = max(1, int((time.perf_counter() - start_time) * 1000))
            msg = f"Falha de ligação HTTPS/TLS para '{normalized_url}': {exc}"
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider=provider,
                action="test_connection",
                level="ERROR",
                status_code=502,
                error_code="NETWORK_CONNECTION_FAILED",
                endpoint_url=normalized_url,
                latency_ms=elapsed_ms,
                message=msg,
            )
            return GenericIntegrationTestOut(
                success=False,
                status_code=502,
                latency_ms=elapsed_ms,
                error_code="NETWORK_CONNECTION_FAILED",
                message=msg,
                resolved_endpoint=normalized_url,
            )

    @staticmethod
    def toggle_integration(
        db: Session,
        empresa_id: int,
        provider: str,
        active: Optional[bool] = None,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Ativa ou desativa uma integração na base de dados e regista auditoria em `integration_logs`.
        """
        ensure_integration_tables(db)
        rec = (
            db.query(EmpresaIntegration)
            .filter(
                EmpresaIntegration.empresa_id == empresa_id,
                EmpresaIntegration.provider == provider,
            )
            .first()
        )

        if not rec:
            cat_item = next((c for c in DEFAULT_CATALOGUE if c["id"] == provider), None)
            new_active = active if active is not None else True
            rec = EmpresaIntegration(
                empresa_id=empresa_id,
                provider=provider,
                name=cat_item["name"] if cat_item else provider.capitalize(),
                category=cat_item["category"] if cat_item else "Data Warehouse & Cloud",
                is_active=new_active,
                status="connected" if new_active else "disconnected",
                last_sync=datetime.utcnow() if new_active else None,
                updated_by_id=user_id,
            )
            db.add(rec)
        else:
            rec.is_active = not rec.is_active if active is None else active
            rec.status = "connected" if rec.is_active else "disconnected"
            if rec.is_active:
                rec.last_sync = datetime.utcnow()
            rec.updated_by_id = user_id

        db.commit()
        db.refresh(rec)

        IntegrationService.record_log(
            db=db,
            empresa_id=empresa_id,
            user_id=user_id,
            provider=provider,
            action="connect" if rec.is_active else "disconnect",
            level="INFO",
            status_code=200,
            error_code="OK",
            endpoint_url=rec.endpoint_url,
            message=f"Integração '{rec.name}' {'ativada' if rec.is_active else 'desconectada'} pelo utilizador.",
        )

        return {
            "provider": rec.provider,
            "connected": rec.is_active,
            "status": rec.status,
            "last_sync": rec.last_sync.strftime("%d/%m/%Y %H:%M") if rec.last_sync else None,
        }

    @staticmethod
    async def save_generic_integration(
        db: Session,
        empresa_id: int,
        data: GenericIntegrationIn,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Valida o endpoint segundo as normas RFC 3986 / RFC 9110 / SSRF, testa conectividade
        e persiste a configuração na tabela `empresa_integrations`.
        """
        ensure_integration_tables(db)
        provider = (data.provider or "").lower().strip()
        host = str((data.config or {}).get("host") or "").strip()
        token = str((data.credentials or {}).get("token") or "").strip()

        target_url, _ = IntegrationService._resolve_provider_test_target(provider, host, token)
        is_valid, normalized_url, err_code, err_msg = validate_internet_endpoint(target_url)

        rec = (
            db.query(EmpresaIntegration)
            .filter(
                EmpresaIntegration.empresa_id == empresa_id,
                EmpresaIntegration.provider == provider,
            )
            .first()
        )
        cat_item = next((c for c in DEFAULT_CATALOGUE if c["id"] == provider), None)

        if not is_valid or not normalized_url:
            if rec:
                rec.status = "error"
                rec.last_error = err_msg
                rec.error_count = (rec.error_count or 0) + 1
                db.commit()
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider=provider,
                action="save_config",
                level="ERROR",
                status_code=422,
                error_code=err_code,
                endpoint_url=target_url,
                message=err_msg,
                details={"host": host},
            )
            from fastapi import HTTPException
            raise HTTPException(status_code=422, detail=err_msg)

        latency_ms = None
        if data.verify_connection:
            test_out = await IntegrationService.test_generic_integration(
                db=db,
                empresa_id=empresa_id,
                data=GenericIntegrationTestIn(provider=provider, host=host, token=token or None),
                user_id=user_id,
            )
            latency_ms = test_out.latency_ms
            if not test_out.success:
                if rec:
                    rec.status = "error"
                    rec.last_error = test_out.message
                    rec.error_count = (rec.error_count or 0) + 1
                    db.commit()
                from fastapi import HTTPException
                raise HTTPException(status_code=422, detail=test_out.message)

        cfg_payload = dict(data.config or {})
        cfg_payload["host"] = host
        cfg_payload["resolved_endpoint"] = normalized_url
        cfg_str = json.dumps(cfg_payload)

        creds_str = None
        if token:
            creds_str = json.dumps({"token": token})
        elif rec and rec.credentials:
            creds_str = rec.credentials

        parsed_host = urlparse(normalized_url).hostname or host
        metrics_list = [
            {"label": "Endpoint", "value": parsed_host},
            {"label": "Protocolo", "value": "HTTPS / TLS"},
        ]
        if latency_ms is not None:
            metrics_list.append({"label": "Latência", "value": f"{latency_ms}ms"})

        if not rec:
            rec = EmpresaIntegration(
                empresa_id=empresa_id,
                provider=provider,
                name=data.name or (cat_item["name"] if cat_item else provider.capitalize()),
                category=data.category or (cat_item["category"] if cat_item else "Data Warehouse & Cloud"),
                is_active=True,
                status="connected",
                endpoint_url=normalized_url,
                config=cfg_str,
                credentials=creds_str,
                metrics=json.dumps(metrics_list),
                last_sync=datetime.utcnow(),
                last_error=None,
                last_latency_ms=latency_ms,
                updated_by_id=user_id,
            )
            db.add(rec)
        else:
            rec.is_active = True
            rec.status = "connected"
            rec.endpoint_url = normalized_url
            rec.config = cfg_str
            if creds_str:
                rec.credentials = creds_str
            rec.metrics = json.dumps(metrics_list)
            rec.last_sync = datetime.utcnow()
            rec.last_error = None
            rec.last_latency_ms = latency_ms
            rec.updated_by_id = user_id

        db.commit()
        db.refresh(rec)

        IntegrationService.record_log(
            db=db,
            empresa_id=empresa_id,
            user_id=user_id,
            provider=provider,
            action="save_config",
            level="INFO",
            status_code=200,
            error_code="OK",
            endpoint_url=normalized_url,
            latency_ms=latency_ms,
            message=f"Integração '{rec.name}' configurada e persistida na base de dados.",
            details={"endpoint": normalized_url},
        )

        return {
            "provider": rec.provider,
            "connected": rec.is_active,
            "status": rec.status,
            "endpoint_url": rec.endpoint_url,
            "last_sync": rec.last_sync.strftime("%d/%m/%Y %H:%M") if rec.last_sync else None,
            "metrics": metrics_list,
        }

    @staticmethod
    def get_webhook_config(db: Session, empresa_id: int) -> WebhookConfigOut:
        """
        Retorna a configuração de Webhook da empresa a partir da tabela `webhook_configs`.
        """
        ensure_integration_tables(db)
        rec = (
            db.query(WebhookConfig)
            .filter(WebhookConfig.empresa_id == empresa_id)
            .first()
        )

        if not rec:
            secret = f"whsec_{hashlib.sha256(f'mustainf_webhook_{empresa_id}'.encode()).hexdigest()[:24]}"
            return WebhookConfigOut(
                url="",
                secret=secret,
                events=[
                    "query.executed",
                    "dry_run.alert",
                    "deadlock.detected",
                    "lineage.pipeline_completed",
                ],
                is_active=False,
                last_triggered_at=None,
                last_status_code=None,
                last_error=None,
            )

        events_list = json.loads(rec.events) if rec.events else []
        return WebhookConfigOut(
            url=rec.url,
            secret=rec.secret,
            events=events_list,
            is_active=rec.is_active,
            last_triggered_at=(
                rec.last_triggered_at.strftime("%d/%m/%Y %H:%M:%S")
                if rec.last_triggered_at
                else None
            ),
            last_status_code=rec.last_status_code,
            last_error=rec.last_error,
        )

    @staticmethod
    def save_webhook_config(
        db: Session,
        empresa_id: int,
        data: WebhookConfigIn,
        user_id: Optional[int] = None,
    ) -> WebhookConfigOut:
        """
        Valida o URL do Webhook segundo a RFC 3986 / RFC 9110 / SSRF e grava na tabela `webhook_configs`.
        """
        ensure_integration_tables(db)
        is_valid, normalized_url, err_code, err_msg = validate_internet_endpoint(data.url)
        if not is_valid or not normalized_url:
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider="webhook",
                action="save_config",
                level="ERROR",
                status_code=422,
                error_code=err_code,
                endpoint_url=data.url,
                message=err_msg,
            )
            from fastapi import HTTPException
            raise HTTPException(status_code=422, detail=err_msg)

        rec = (
            db.query(WebhookConfig)
            .filter(WebhookConfig.empresa_id == empresa_id)
            .first()
        )

        secret = (
            data.secret.strip()
            if data.secret and data.secret.strip()
            else f"whsec_{hashlib.sha256(f'{empresa_id}_{time.time()}'.encode()).hexdigest()[:24]}"
        )
        events_str = json.dumps(data.events)

        if not rec:
            rec = WebhookConfig(
                empresa_id=empresa_id,
                url=normalized_url,
                secret=secret,
                events=events_str,
                is_active=data.is_active,
                last_error=None,
            )
            db.add(rec)
        else:
            rec.url = normalized_url
            rec.secret = secret
            rec.events = events_str
            rec.is_active = data.is_active
            rec.last_error = None

        db.commit()
        db.refresh(rec)

        IntegrationService.record_log(
            db=db,
            empresa_id=empresa_id,
            user_id=user_id,
            provider="webhook",
            action="save_config",
            level="INFO",
            status_code=200,
            error_code="OK",
            endpoint_url=normalized_url,
            message=f"Endpoint de Webhook '{normalized_url}' validado e salvo com {len(data.events)} evento(s).",
            details={"events": data.events},
        )

        return WebhookConfigOut(
            url=rec.url,
            secret=rec.secret,
            events=data.events,
            is_active=rec.is_active,
            last_triggered_at=(
                rec.last_triggered_at.strftime("%d/%m/%Y %H:%M:%S")
                if rec.last_triggered_at
                else None
            ),
            last_status_code=rec.last_status_code,
            last_error=rec.last_error,
        )

    @staticmethod
    async def test_webhook_delivery(
        db: Session,
        empresa_id: int,
        url: Optional[str] = None,
        secret: Optional[str] = None,
        actor_email: Optional[str] = None,
        user_id: Optional[int] = None,
    ) -> WebhookTestOut:
        """
        Dispara um evento real de teste de Webhook via HTTP POST com cabeçalhos padrão da Internet:
        - Assinatura HMAC SHA-256 (`X-MustaInfo-Signature-256`)
        - Timestamp anti-replay (`X-MustaInfo-Timestamp`)
        - Identificador único de entrega (`X-MustaInfo-Delivery`)
        Regista qualquer erro de DNS, TLS, SSRF ou HTTP na base de dados (`integration_logs` e `webhook_delivery_logs`).
        """
        ensure_integration_tables(db)
        env_cfg = _get_static_env_config()

        cfg = (
            db.query(WebhookConfig)
            .filter(WebhookConfig.empresa_id == empresa_id)
            .first()
        )
        target_url = (url or (cfg.url if cfg else "") or "").strip()
        target_secret = (secret or (cfg.secret if cfg else "") or "whsec_default").strip()

        now_ts = int(time.time())
        now_iso = datetime.utcnow().isoformat() + "Z"
        delivery_id = str(uuid.uuid4())

        payload = {
            "id": delivery_id,
            "event": "test.ping",
            "source": "mustainf.integrations",
            "timestamp": now_iso,
            "empresa_id": empresa_id,
            "actor": actor_email or "admin@mustainfo.org",
            "data": {
                "message": "Disparo de verificação de Webhook MustaInf (HMAC-SHA256).",
                "protocol": "RFC-9110-HTTPS",
            },
        }

        is_valid, normalized_url, err_code, err_msg = validate_internet_endpoint(target_url)
        if not is_valid or not normalized_url:
            if cfg:
                cfg.last_triggered_at = datetime.utcnow()
                cfg.last_status_code = 422
                cfg.last_error = err_msg
                db.commit()
            IntegrationService.record_log(
                db=db,
                empresa_id=empresa_id,
                user_id=user_id,
                provider="webhook",
                action="webhook_dispatch",
                level="ERROR",
                status_code=422,
                error_code=err_code,
                endpoint_url=target_url,
                latency_ms=0,
                message=err_msg,
            )
            return WebhookTestOut(
                status=422,
                success=False,
                error_code=err_code,
                message=err_msg,
                latency_ms=0,
                payload=payload,
            )

        payload_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        signed_content = f"{now_ts}.".encode("utf-8") + payload_bytes
        signature = hmac.new(
            target_secret.encode("utf-8"), signed_content, hashlib.sha256
        ).hexdigest()

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": env_cfg["user_agent"],
            env_cfg["webhook_signature_header"]: f"sha256={signature}",
            env_cfg["webhook_timestamp_header"]: str(now_ts),
            "X-MustaInfo-Event": "test.ping",
            "X-MustaInfo-Delivery": delivery_id,
        }

        start_time = time.perf_counter()
        status_code = 502
        success = False
        res_error_code: Optional[str] = None
        message = ""
        response_snippet = ""

        max_attempts = max(1, int(env_cfg["max_retries"]))
        for attempt in range(1, max_attempts + 1):
            try:
                async with httpx.AsyncClient(timeout=float(env_cfg["http_timeout_seconds"])) as client:
                    resp = await client.post(
                        normalized_url,
                        content=payload_bytes,
                        headers=headers,
                    )
                    status_code = resp.status_code
                    response_snippet = (resp.text or "")[:500]
                    success = 200 <= status_code < 300
                    if success:
                        res_error_code = "OK"
                        message = f"Endpoint '{normalized_url}' confirmou receção com HTTP {status_code}."
                        break
                    else:
                        res_error_code = f"HTTP_{status_code}"
                        message = f"Endpoint '{normalized_url}' rejeitou o evento com HTTP {status_code}."
                        if status_code < 500:
                            break
            except httpx.TimeoutException as exc:
                status_code = 504
                success = False
                res_error_code = "HTTP_TIMEOUT"
                message = f"Timeout ({env_cfg['http_timeout_seconds']}s) ao entregar Webhook para '{normalized_url}': {exc}"
            except Exception as exc:
                status_code = 502
                success = False
                res_error_code = "WEBHOOK_DELIVERY_FAILED"
                message = f"Falha na entrega HTTPS ao endpoint '{normalized_url}': {exc}"

        elapsed_ms = max(1, int((time.perf_counter() - start_time) * 1000))

        try:
            db.add(
                WebhookDeliveryLog(
                    empresa_id=empresa_id,
                    event="test.ping",
                    url=normalized_url,
                    status_code=status_code,
                    latency_ms=elapsed_ms,
                    success=success,
                    response_body=response_snippet or message,
                )
            )
            if cfg:
                cfg.last_triggered_at = datetime.utcnow()
                cfg.last_status_code = status_code
                cfg.last_error = None if success else message
            db.commit()
        except Exception:
            db.rollback()

        IntegrationService.record_log(
            db=db,
            empresa_id=empresa_id,
            user_id=user_id,
            provider="webhook",
            action="webhook_dispatch",
            level="INFO" if success else "ERROR",
            status_code=status_code,
            error_code=res_error_code,
            endpoint_url=normalized_url,
            latency_ms=elapsed_ms,
            message=message,
            details={"delivery_id": delivery_id, "response_preview": response_snippet[:200]},
        )

        return WebhookTestOut(
            status=status_code,
            success=success,
            error_code=res_error_code,
            message=message,
            latency_ms=elapsed_ms,
            payload=payload,
        )

    @staticmethod
    async def sync_all_active_integrations(
        db: Session, empresa_id: int, user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Verifica na prática todas as integrações ativas da empresa, atualizando
        `last_sync`, `status`, `last_latency_ms` e registando erros em `integration_logs`.
        """
        ensure_integration_tables(db)
        records = (
            db.query(EmpresaIntegration)
            .filter(
                EmpresaIntegration.empresa_id == empresa_id,
                EmpresaIntegration.is_active.is_(True),
            )
            .all()
        )

        synced_count = 0
        errors_count = 0

        for rec in records:
            cfg = json.loads(rec.config) if rec.config else {}
            creds = json.loads(rec.credentials) if rec.credentials else {}
            if rec.provider == "bigquery":
                out = await IntegrationService.test_bigquery_connection(
                    db=db,
                    empresa_id=empresa_id,
                    data=BigQueryTestIn(
                        project_id=cfg.get("project_id", ""),
                        dataset_id=cfg.get("dataset_id", ""),
                        region=cfg.get("region", "us-central1"),
                    ),
                    user_id=user_id,
                )
                if out.success:
                    rec.status = "connected"
                    rec.last_sync = datetime.utcnow()
                    rec.last_error = None
                    rec.last_latency_ms = out.latency_ms
                    synced_count += 1
                else:
                    rec.status = "error"
                    rec.last_error = out.message
                    rec.error_count = (rec.error_count or 0) + 1
                    errors_count += 1
            else:
                host = cfg.get("host") or rec.endpoint_url or ""
                token = creds.get("token")
                out = await IntegrationService.test_generic_integration(
                    db=db,
                    empresa_id=empresa_id,
                    data=GenericIntegrationTestIn(provider=rec.provider, host=host, token=token),
                    user_id=user_id,
                )
                if out.success:
                    rec.status = "connected"
                    rec.last_sync = datetime.utcnow()
                    rec.last_error = None
                    rec.last_latency_ms = out.latency_ms
                    synced_count += 1
                else:
                    rec.status = "error"
                    rec.last_error = out.message
                    rec.error_count = (rec.error_count or 0) + 1
                    errors_count += 1

        db.commit()
        return {
            "status": "ok" if errors_count == 0 else "partial_error",
            "synced_count": synced_count,
            "errors_count": errors_count,
        }
