"""
Gerenciador unificado de mensagens (E-mail e SMS) da aplicação.

Fornece uma interface de alto nível para envio de notificações, alertas,
códigos de autenticação (OTP/2FA) e verificação de integridade dos serviços.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Literal, Optional, Union

from app.ultils.logger import log_message
from app.ultils.servico_SMPP_smtp.config import MessagingConfig, get_messaging_config
from app.ultils.servico_SMPP_smtp.smtp_service import SMTPService, get_smtp_service
from app.ultils.servico_SMPP_smtp.smpp_service import SMPPService, get_smpp_service


NotificationChannel = Literal["email", "sms", "both"]


class MessagingManager:
    """
    Controlador central de mensageria da aplicação MustaInf.
    """

    def __init__(
        self,
        config: Optional[MessagingConfig] = None,
        smtp_service: Optional[SMTPService] = None,
        smpp_service: Optional[SMPPService] = None,
    ):
        self.config = config or get_messaging_config()
        self.smtp = smtp_service or get_smtp_service(self.config.smtp)
        self.smpp = smpp_service or get_smpp_service(self.config.smpp)

    def send_email(self, *args, **kwargs) -> Dict[str, Any]:
        """Envia um e-mail através do serviço SMTP configurado."""
        return self.smtp.send_email(*args, **kwargs)

    def send_email_template(self, *args, **kwargs) -> Dict[str, Any]:
        """Envia um e-mail renderizando um template a partir da pasta de templates."""
        return self.smtp.send_email_template(*args, **kwargs)

    async def async_send_email(self, *args, **kwargs) -> Dict[str, Any]:
        """Envia um e-mail de forma assíncrona (não bloqueante)."""
        return await self.smtp.async_send_email(*args, **kwargs)

    def send_sms(self, *args, **kwargs) -> Dict[str, Any]:
        """Envia um SMS através do serviço SMPP configurado."""
        return self.smpp.send_sms(*args, **kwargs)

    def send_sms_template(self, *args, **kwargs) -> Dict[str, Any]:
        """Envia um SMS renderizando um template a partir da pasta de templates."""
        return self.smpp.send_sms_template(*args, **kwargs)

    async def async_send_sms(self, *args, **kwargs) -> Dict[str, Any]:
        """Envia um SMS de forma assíncrona (não bloqueante)."""
        return await self.smpp.async_send_sms(*args, **kwargs)

    def send_notification(
        self,
        channel: NotificationChannel,
        message: Optional[str] = None,
        subject: Optional[str] = None,
        recipient_email: Optional[Union[str, List[str]]] = None,
        recipient_phone: Optional[Union[str, List[str]]] = None,
        html_message: Optional[str] = None,
        template_name: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        attachments: Optional[List[Any]] = None,
    ) -> Dict[str, Any]:
        """
        Envia uma notificação multicanal (E-mail, SMS ou ambos).

        Args:
            channel: Canal de envio ('email', 'sms', 'both')
            message: Conteúdo em texto puro
            subject: Assunto (obrigatório para e-mail, opcional para SMS)
            recipient_email: Endereço(s) de e-mail de destino
            recipient_phone: Número(s) de telefone de destino
            html_message: Versão HTML do e-mail (opcional)
            attachments: Lista de anexos para o e-mail (opcional)

        Returns:
            Dict detalhado com o resultado do envio em cada canal selecionado.
        """
        results: Dict[str, Any] = {"success": True, "channels": {}}

        if channel in ("email", "both"):
            if not recipient_email:
                results["channels"]["email"] = {
                    "success": False,
                    "message": "Nenhum e-mail de destino informado para o canal 'email'.",
                }
                results["success"] = False
            else:
                email_res = self.send_email(
                    to=recipient_email,
                    subject=subject or f"[{self.config.app_name}] Notificação do Sistema",
                    body_text=message,
                    body_html=html_message,
                    template_name=template_name,
                    context=context,
                    attachments=attachments,
                )
                results["channels"]["email"] = email_res
                if not email_res.get("success"):
                    results["success"] = False

        if channel in ("sms", "both"):
            if not recipient_phone:
                results["channels"]["sms"] = {
                    "success": False,
                    "message": "Nenhum telefone de destino informado para o canal 'sms'.",
                }
                results["success"] = False
            else:
                sms_res = self.send_sms(
                    to=recipient_phone,
                    text=message,
                    template_name=template_name,
                    context=context,
                )
                results["channels"]["sms"] = sms_res
                if not sms_res.get("success"):
                    results["success"] = False

        return results

    async def async_send_notification(self, *args, **kwargs) -> Dict[str, Any]:
        """Versão assíncrona para envio de notificação multicanal."""
        return await asyncio.to_thread(self.send_notification, *args, **kwargs)

    def test_all_connections(self) -> Dict[str, Any]:
        """
        Verifica o estado de saúde e conectividade de ambos os serviços (SMTP e SMPP).
        """
        smtp_health = self.smtp.test_connection()
        smpp_health = self.smpp.test_connection()

        overall_ok = smtp_health.get("success", False) and smpp_health.get("success", False)
        return {
            "overall_status": "ok" if overall_ok else "partial_or_degraded",
            "smtp": smtp_health,
            "smpp": smpp_health,
            "config": self.config.masked_dict(),
        }

    def get_status(self) -> Dict[str, Any]:
        """Retorna o estado das configurações dos serviços."""
        return {
            "app_name": self.config.app_name,
            "smtp_enabled": self.config.smtp.enabled,
            "smtp_configured": self.config.smtp.is_configured(),
            "smpp_enabled": self.config.smpp.enabled,
            "smpp_configured": self.config.smpp.is_configured(),
            "config": self.config.masked_dict(),
        }


# Instância singleton padrão
_messaging_manager: Optional[MessagingManager] = None


def get_messaging_manager() -> MessagingManager:
    """Retorna uma instância única de MessagingManager."""
    global _messaging_manager
    if _messaging_manager is None:
        _messaging_manager = MessagingManager()
    return _messaging_manager
