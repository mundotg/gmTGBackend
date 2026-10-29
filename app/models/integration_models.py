from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    Text,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.database import Base


class IntegrationCatalog(Base):
    """
    Catálogo de integrações suportadas pela plataforma, persistido na base de dados
    e inicializado a partir de variáveis de ambiente e padrões estáticos.
    """
    __tablename__ = "integration_catalog"

    id = Column(String(50), primary_key=True, index=True)  # ex: bigquery, snowflake, github
    name = Column(String(100), nullable=False)
    provider = Column(String(100), nullable=False)
    category = Column(String(60), nullable=False, index=True)
    description = Column(Text, nullable=False)
    badge = Column(String(60), nullable=True)
    brand_color = Column(String(20), nullable=False, default="#4285F4")
    docs_url = Column(String(500), nullable=True)
    default_endpoint = Column(String(500), nullable=True)
    featured = Column(Boolean, default=False, nullable=False)
    is_enabled = Column(Boolean, default=True, nullable=False)
    default_metrics = Column(Text, nullable=True)  # JSON array
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<IntegrationCatalog(id={self.id}, name={self.name}, category={self.category})>"


class EmpresaIntegration(Base):
    """
    Tabela de integrações de Data Warehouse, Cloud, Alertas e DevOps da empresa.
    Regista configuração, credenciais, estado de conectividade e último erro.
    """
    __tablename__ = "empresa_integrations"

    id = Column(Integer, primary_key=True, index=True)
    empresa_id = Column(
        Integer,
        ForeignKey("empresas.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    provider = Column(String(50), nullable=False, index=True)
    name = Column(String(100), nullable=False)
    category = Column(String(50), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    status = Column(String(30), default="disconnected", nullable=False)  # connected | disconnected | error | degraded
    endpoint_url = Column(String(500), nullable=True)
    config = Column(Text, nullable=True)  # JSON com dados não-sensíveis (ex: project_id, dataset, region, host)
    credentials = Column(Text, nullable=True)  # JSON com credenciais (service_account_json, tokens)
    metrics = Column(Text, nullable=True)  # JSON com estatísticas ao vivo
    last_sync = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)
    error_count = Column(Integer, default=0, nullable=False)
    last_latency_ms = Column(Integer, nullable=True)
    updated_by_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("empresa_id", "provider", name="uq_empresa_provider"),
    )

    def __repr__(self):
        return (
            f"<EmpresaIntegration(id={self.id}, empresa_id={self.empresa_id}, "
            f"provider={self.provider}, is_active={self.is_active}, status={self.status})>"
        )


class IntegrationErrorLog(Base):
    """
    Tabela de auditoria e logs de erros de integrações (conexões, testes HTTP/TLS,
    validações RFC 3986 / RFC 9110, bloqueios SSRF e falhas de Webhooks).
    """
    __tablename__ = "integration_logs"

    id = Column(Integer, primary_key=True, index=True)
    empresa_id = Column(
        Integer,
        ForeignKey("empresas.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    provider = Column(String(50), nullable=False, index=True)
    action = Column(String(60), nullable=False, index=True)  # test_connection, save_config, toggle, sync, webhook_dispatch
    level = Column(String(20), nullable=False, default="ERROR", index=True)  # INFO, WARNING, ERROR, CRITICAL
    status_code = Column(Integer, nullable=True)
    error_code = Column(String(60), nullable=True)  # ex: RFC3986_INVALID_URI, OWASP_SSRF_BLOCKED, HTTP_TIMEOUT, AUTH_ERROR
    endpoint_url = Column(String(500), nullable=True)
    latency_ms = Column(Integer, nullable=True)
    message = Column(Text, nullable=False)
    details = Column(Text, nullable=True)  # JSON com detalhes técnicos (sem segredos)
    created_at = Column(DateTime, default=func.now(), nullable=False, index=True)

    __table_args__ = (
        Index("ix_integration_logs_empresa_created", "empresa_id", "created_at"),
    )

    def __repr__(self):
        return (
            f"<IntegrationErrorLog(id={self.id}, provider={self.provider}, "
            f"level={self.level}, error_code={self.error_code})>"
        )


class WebhookConfig(Base):
    """
    Configuração de Webhooks de saída para eventos e alertas de dados.
    """
    __tablename__ = "webhook_configs"

    id = Column(Integer, primary_key=True, index=True)
    empresa_id = Column(
        Integer,
        ForeignKey("empresas.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    url = Column(String(500), nullable=False)
    secret = Column(String(100), nullable=False)
    events = Column(Text, nullable=True)  # JSON list de eventos subscritos
    is_active = Column(Boolean, default=True, nullable=False)
    last_triggered_at = Column(DateTime, nullable=True)
    last_status_code = Column(Integer, nullable=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<WebhookConfig(id={self.id}, empresa_id={self.empresa_id}, url={self.url}, is_active={self.is_active})>"


class WebhookDeliveryLog(Base):
    """
    Histórico de disparos de Webhook para auditoria de eventos e testes.
    """
    __tablename__ = "webhook_delivery_logs"

    id = Column(Integer, primary_key=True, index=True)
    empresa_id = Column(
        Integer,
        ForeignKey("empresas.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event = Column(String(50), nullable=False)
    url = Column(String(500), nullable=False)
    status_code = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    success = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)

    def __repr__(self):
        return f"<WebhookDeliveryLog(id={self.id}, event={self.event}, status_code={self.status_code})>"
