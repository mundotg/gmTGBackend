"""
Configurações para os serviços de SMPP (SMS) e SMTP (E-mail).

Permite carregar definições a partir do ambiente (.env) ou sobrescrever programaticamente.
Utiliza Pydantic para validação e serialização segura com mascaramento de senhas.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator

from app.config.dotenv import get_env, get_env_bool, get_env_int


class SMTPConfig(BaseModel):
    """
    Configuração do serviço SMTP (Simple Mail Transfer Protocol) para envio de e-mails.
    """
    host: str = Field(default_factory=lambda: get_env("SMTP_HOST", "localhost"), description="Servidor SMTP")
    port: int = Field(default_factory=lambda: get_env_int("SMTP_PORT", 587) or 587, description="Porta SMTP (25, 465, 587)")
    username: Optional[str] = Field(default_factory=lambda: get_env("SMTP_USER") or get_env("SMTP_USERNAME"), description="Usuário/E-mail de autenticação")
    password: Optional[str] = Field(default_factory=lambda: get_env("SMTP_PASSWORD") or get_env("SMTP_PASS"), description="Senha do servidor SMTP")
    use_tls: bool = Field(default_factory=lambda: get_env_bool("SMTP_USE_TLS", True), description="Usar STARTTLS (normalmente porta 587)")
    use_ssl: bool = Field(default_factory=lambda: get_env_bool("SMTP_USE_SSL", False), description="Usar SSL direto (normalmente porta 465)")
    from_email: str = Field(default_factory=lambda: get_env("SMTP_FROM_EMAIL") or get_env("SMTP_USER") or "noreply@mustainf.com", description="E-mail de envio padrão")
    from_name: str = Field(default_factory=lambda: get_env("SMTP_FROM_NAME", "MustaInf Notificações"), description="Nome do remetente exibido")
    reply_to: Optional[str] = Field(default_factory=lambda: get_env("SMTP_REPLY_TO"), description="E-mail de resposta (Reply-To)")
    timeout: int = Field(default_factory=lambda: get_env_int("SMTP_TIMEOUT", 30) or 30, description="Tempo limite em segundos")
    max_retries: int = Field(default_factory=lambda: get_env_int("SMTP_MAX_RETRIES", 3) or 3, description="Tentativas máximas em caso de falha transitória")
    retry_delay: float = Field(default=2.0, description="Intervalo entre tentativas em segundos")
    enabled: bool = Field(default_factory=lambda: get_env_bool("SMTP_ENABLED", True), description="Ativa/desativa envio de e-mails")

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("Porta SMTP inválida. Deve estar entre 1 e 65535.")
        return v

    def masked_dict(self) -> Dict[str, Any]:
        """Retorna as configurações mascarando a senha para logs e inspeção segura."""
        data = self.model_dump()
        if data.get("password"):
            data["password"] = "********"
        return data

    def is_configured(self) -> bool:
        """Verifica se as configurações mínimas para envio estão presentes."""
        return bool(self.host and self.from_email)


class SMPPConfig(BaseModel):
    """
    Configuração do serviço SMPP (Short Message Peer-to-Peer) para envio de SMS.
    """
    host: str = Field(default_factory=lambda: get_env("SMPP_HOST") or get_env("SMSC_HOST", "localhost"), description="Endereço do SMSC (Short Message Service Center)")
    port: int = Field(default_factory=lambda: get_env_int("SMPP_PORT") or get_env_int("SMSC_PORT", 2775) or 2775, description="Porta SMPP (padrão 2775)")
    system_id: str = Field(default_factory=lambda: get_env("SMPP_SYSTEM_ID", ""), description="Identificador do sistema no SMSC")
    password: str = Field(default_factory=lambda: get_env("SMPP_PASSWORD", ""), description="Senha de autenticação no SMSC")
    system_type: str = Field(default_factory=lambda: get_env("SMPP_SYSTEM_TYPE", ""), description="Tipo de sistema (opcional conforme operadora)")
    interface_version: str = Field(default_factory=lambda: get_env("SMPP_INTERFACE_VERSION", "3.4"), description="Versão da interface SMPP (normalmente 3.4)")
    bind_type: str = Field(default_factory=lambda: get_env("SMPP_BIND_TYPE", "transceiver"), description="Tipo de bind: transceiver, transmitter ou receiver")
    
    # Endereço de Origem (Remetente)
    source_addr: str = Field(default_factory=lambda: get_env("SMPP_SOURCE_ADDR") or get_env("SMPP_SENDER_ID", "MustaInf"), description="Remetente do SMS (Alfanumérico ou Numérico)")
    source_addr_ton: int = Field(default_factory=lambda: get_env_int("SMPP_SOURCE_TON", 5) or 5, description="Type of Number do remetente (1=Internacional, 5=Alfanumérico)")
    source_addr_npi: int = Field(default_factory=lambda: get_env_int("SMPP_SOURCE_NPI", 0) or 0, description="Numbering Plan Indicator do remetente (0=Unknown, 1=ISDN/E.164)")

    # Endereço de Destino (Destinatário padrão)
    dest_addr_ton: int = Field(default_factory=lambda: get_env_int("SMPP_DEST_TON", 1) or 1, description="Type of Number do destinatário (1=Internacional E.164)")
    dest_addr_npi: int = Field(default_factory=lambda: get_env_int("SMPP_DEST_NPI", 1) or 1, description="Numbering Plan Indicator do destinatário (1=ISDN/E.164)")

    # Codificação de Dados
    # 0 = SMSC Default (GSM 7-bit), 8 = UCS2 (UTF-16-BE para caracteres especiais e acentos)
    data_coding: int = Field(default_factory=lambda: get_env_int("SMPP_DATA_CODING", 0) or 0, description="Data Coding Scheme (0=GSM 7-bit, 8=UCS2)")
    
    enquire_link_interval: int = Field(default_factory=lambda: get_env_int("SMPP_ENQUIRE_LINK_INTERVAL", 30) or 30, description="Intervalo de keepalive (enquire_link) em segundos")
    timeout: int = Field(default_factory=lambda: get_env_int("SMPP_TIMEOUT", 30) or 30, description="Tempo limite do socket em segundos")
    max_retries: int = Field(default_factory=lambda: get_env_int("SMPP_MAX_RETRIES", 3) or 3, description="Tentativas de reconexão/reenvio")
    retry_delay: float = Field(default=2.0, description="Intervalo entre tentativas em segundos")
    enabled: bool = Field(default_factory=lambda: get_env_bool("SMPP_ENABLED", True), description="Ativa/desativa envio de SMS via SMPP")

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("Porta SMPP inválida. Deve estar entre 1 e 65535.")
        return v

    @field_validator("bind_type")
    @classmethod
    def validate_bind_type(cls, v: str) -> str:
        clean = v.lower().strip()
        allowed = {"transceiver", "transmitter", "receiver"}
        if clean not in allowed:
            raise ValueError(f"bind_type inválido '{v}'. Valores permitidos: {', '.join(allowed)}")
        return clean

    def masked_dict(self) -> Dict[str, Any]:
        """Retorna as configurações mascarando a senha para logs e inspeção segura."""
        data = self.model_dump()
        if data.get("password"):
            data["password"] = "********"
        return data

    def is_configured(self) -> bool:
        """Verifica se as configurações mínimas para bind no SMSC estão presentes."""
        return bool(self.host and self.system_id and self.password)


class MessagingConfig(BaseModel):
    """
    Configuração agregada dos serviços de comunicação da aplicação.
    """
    smtp: SMTPConfig = Field(default_factory=SMTPConfig)
    smpp: SMPPConfig = Field(default_factory=SMPPConfig)
    app_name: str = Field(default_factory=lambda: get_env("APP_NAME", "MustaInf"))
    environment: str = Field(default_factory=lambda: get_env("ENV", "development"))

    def masked_dict(self) -> Dict[str, Any]:
        """Retorna a configuração completa mascarada."""
        return {
            "app_name": self.app_name,
            "environment": self.environment,
            "smtp": self.smtp.masked_dict(),
            "smpp": self.smpp.masked_dict(),
        }


# Instâncias e funções utilitárias de carregamento
def get_smtp_config() -> SMTPConfig:
    """Retorna uma instância atualizada de SMTPConfig baseada nas variáveis de ambiente."""
    return SMTPConfig()


def get_smpp_config() -> SMPPConfig:
    """Retorna uma instância atualizada de SMPPConfig baseada nas variáveis de ambiente."""
    return SMPPConfig()


def get_messaging_config() -> MessagingConfig:
    """Retorna uma instância agregada de MessagingConfig."""
    return MessagingConfig()
