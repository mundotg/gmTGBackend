"""
Módulo de Serviços de Comunicação SMPP e SMTP.

Fornece configurações tipadas, clientes de conexão nativos e gerenciador de
mensagens para envio de E-mails (SMTP) e SMS (SMPP) na aplicação.
"""

from app.ultils.servico_SMPP_smtp.config import (
    MessagingConfig,
    SMPPConfig,
    SMTPConfig,
    get_messaging_config,
    get_smpp_config,
    get_smtp_config,
)
from app.ultils.servico_SMPP_smtp.manager import (
    MessagingManager,
    get_messaging_manager,
)
from app.ultils.servico_SMPP_smtp.smpp_service import (
    SMPPError,
    SMPPService,
    get_smpp_service,
)
from app.ultils.servico_SMPP_smtp.smtp_service import (
    SMTPService,
    get_smtp_service,
)


from app.ultils.servico_SMPP_smtp.template_service import (
    TemplateService,
    get_template_service,
    render_email_template,
    render_sms_template,
)


# Funções de conveniência prontas para importação direta
def send_email(*args, **kwargs):
    """Envia um e-mail utilizando o serviço SMTP padrão (suporta template_name)."""
    return get_smtp_service().send_email(*args, **kwargs)


def send_email_template(to, subject, template_name, context=None, **kwargs):
    """Envia um e-mail renderizando o template especificado da pasta de templates."""
    return get_smtp_service().send_email_template(
        to=to,
        subject=subject,
        template_name=template_name,
        context=context,
        **kwargs,
    )


async def async_send_email(*args, **kwargs):
    """Envia um e-mail assincronamente sem bloquear a thread principal."""
    return await get_smtp_service().async_send_email(*args, **kwargs)


def send_sms(*args, **kwargs):
    """Envia um SMS utilizando o serviço SMPP padrão (suporta template_name)."""
    return get_smpp_service().send_sms(*args, **kwargs)


def send_sms_template(to, template_name, context=None, **kwargs):
    """Envia um SMS renderizando o template especificado da pasta de templates."""
    return get_smpp_service().send_sms_template(
        to=to,
        template_name=template_name,
        context=context,
        **kwargs,
    )


async def async_send_sms(*args, **kwargs):
    """Envia um SMS assincronamente sem bloquear a thread principal."""
    return await get_smpp_service().async_send_sms(*args, **kwargs)


def test_smtp_connection():
    """Testa a conectividade com o servidor SMTP configurado."""
    return get_smtp_service().test_connection()


def test_smpp_connection():
    """Testa a conectividade e bind com o SMSC SMPP configurado."""
    return get_smpp_service().test_connection()


def test_all_connections():
    """Testa a conectividade de ambos os serviços."""
    return get_messaging_manager().test_all_connections()


__all__ = [
    # Configurações
    "SMTPConfig",
    "SMPPConfig",
    "MessagingConfig",
    "get_smtp_config",
    "get_smpp_config",
    "get_messaging_config",
    # Serviços
    "SMTPService",
    "get_smtp_service",
    "SMPPService",
    "SMPPError",
    "get_smpp_service",
    "MessagingManager",
    "get_messaging_manager",
    "TemplateService",
    "get_template_service",
    # Templates
    "render_email_template",
    "render_sms_template",
    # Funções de conveniência
    "send_email",
    "send_email_template",
    "async_send_email",
    "send_sms",
    "send_sms_template",
    "async_send_sms",
    "test_smtp_connection",
    "test_smpp_connection",
    "test_all_connections",
]
