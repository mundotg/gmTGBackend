from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    Text,
    DateTime,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.database import Base


class EmpresaIntegration(Base):
    """
    Tabela de integrações de Data Warehouse e Cloud da empresa (BigQuery, Snowflake, etc.).
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
    config = Column(Text, nullable=True)  # JSON com dados não-sensíveis (ex: project_id, dataset, region)
    credentials = Column(Text, nullable=True)  # JSON com credenciais (service_account_json, tokens)
    metrics = Column(Text, nullable=True)  # JSON com estatísticas ao vivo
    last_sync = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("empresa_id", "provider", name="uq_empresa_provider"),
    )

    def __repr__(self):
        return f"<EmpresaIntegration(id={self.id}, empresa_id={self.empresa_id}, provider={self.provider}, is_active={self.is_active})>"


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
