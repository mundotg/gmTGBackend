"""
Serviço de envio de e-mails via SMTP (Simple Mail Transfer Protocol).

Implementa envio síncrono e assíncrono, suporte a HTML, texto simples,
anexos de arquivos, múltiplos destinatários (Para, Cc, Cco) e verificação de conexão.
"""

from __future__ import annotations

import asyncio
import os
import smtplib
import ssl
import time
from email import encoders
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from app.ultils.logger import log_message
from app.ultils.servico_SMPP_smtp.config import SMTPConfig, get_smtp_config
from app.ultils.servico_SMPP_smtp.template_service import render_email_template


class SMTPService:
    """
    Cliente para envio de e-mails através do protocolo SMTP.
    """

    def __init__(self, config: Optional[SMTPConfig] = None):
        self.config = config or get_smtp_config()

    def _create_smtp_client(self) -> Union[smtplib.SMTP, smtplib.SMTP_SSL]:
        """Cria e configura o objeto de conexão SMTP conforme TLS/SSL configurados."""
        timeout = self.config.timeout

        if self.config.use_ssl:
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(self.config.host, self.config.port, timeout=timeout, context=context)
        else:
            server = smtplib.SMTP(self.config.host, self.config.port, timeout=timeout)
            if self.config.use_tls:
                context = ssl.create_default_context()
                server.starttls(context=context)

        if self.config.username and self.config.password:
            server.login(self.config.username, self.config.password)

        return server

    def test_connection(self) -> Dict[str, Any]:
        """
        Testa a conectividade e autenticação com o servidor SMTP.
        
        Retorna:
            Dict com status 'success', mensagem explicativa e detalhes da conexão.
        """
        if not self.config.enabled:
            return {
                "success": False,
                "message": "O serviço SMTP está desabilitado na configuração (SMTP_ENABLED=False).",
                "config": self.config.masked_dict(),
            }

        start_time = time.time()
        try:
            with self._create_smtp_client() as server:
                status_code, response_msg = server.noop()
                elapsed = round(time.time() - start_time, 3)
                log_message(
                    f"Conexão SMTP com {self.config.host}:{self.config.port} testada com sucesso ({elapsed}s)",
                    level="info",
                    source="SMTPService",
                )
                return {
                    "success": True,
                    "message": f"Conexão SMTP estabelecida com sucesso ({elapsed}s).",
                    "details": {
                        "host": self.config.host,
                        "port": self.config.port,
                        "tls": self.config.use_tls,
                        "ssl": self.config.use_ssl,
                        "authenticated": bool(self.config.username),
                        "response_code": status_code,
                        "response_message": response_msg.decode("utf-8", errors="ignore") if isinstance(response_msg, bytes) else str(response_msg),
                        "latency_seconds": elapsed,
                    },
                }
        except smtplib.SMTPAuthenticationError as e:
            msg = f"Falha de autenticação SMTP com o servidor {self.config.host}:{self.config.port}: {e}"
            log_message(msg, level="error", source="SMTPService")
            return {"success": False, "message": msg, "error": str(e), "error_type": "AuthenticationError"}
        except (smtplib.SMTPConnectError, ConnectionRefusedError, TimeoutError, OSError) as e:
            msg = f"Não foi possível conectar ao servidor SMTP {self.config.host}:{self.config.port}: {e}"
            log_message(msg, level="error", source="SMTPService")
            return {"success": False, "message": msg, "error": str(e), "error_type": "ConnectionError"}
        except Exception as e:
            msg = f"Erro inesperado ao testar conexão SMTP: {e}"
            log_message(msg, level="error", source="SMTPService")
            return {"success": False, "message": msg, "error": str(e), "error_type": type(e).__name__}

    def _build_mime_message(
        self,
        to: List[str],
        subject: str,
        body_text: Optional[str] = None,
        body_html: Optional[str] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
        attachments: Optional[List[Union[str, Path, Dict[str, Any]]]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> MIMEMultipart:
        """Monta o envelope MIME do e-mail com suporte a texto, HTML e anexos."""
        sender_email = from_email or self.config.from_email
        # No Gmail, se o From for de outro domínio sem SPF (ex: @mustainf.com),
        # provedores corporativos rejeitam ou enviam para o Spam por falha de SPF/DMARC.
        if self.config.username and "@gmail.com" in self.config.username.lower():
            if not sender_email or "@gmail.com" not in sender_email.lower():
                sender_email = self.config.username

        sender_name = from_name or self.config.from_name

        msg = MIMEMultipart("mixed")
        msg["From"] = formataddr((str(Header(sender_name, "utf-8")), sender_email))
        msg["To"] = ", ".join(to)
        msg["Subject"] = Header(subject, "utf-8").encode()
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid(domain=sender_email.split("@")[-1] if "@" in sender_email else None)

        if reply_to or self.config.reply_to:
            msg["Reply-To"] = reply_to or self.config.reply_to

        if cc:
            msg["Cc"] = ", ".join(cc)

        if headers:
            for k, v in headers.items():
                msg[k] = v

        # Parte de conteúdo (Texto ou HTML alternativo)
        alt_part = MIMEMultipart("alternative")
        if body_text:
            alt_part.attach(MIMEText(body_text, "plain", "utf-8"))
        if body_html:
            alt_part.attach(MIMEText(body_html, "html", "utf-8"))

        msg.attach(alt_part)

        # Anexos
        if attachments:
            for att in attachments:
                self._attach_file(msg, att)

        return msg

    def _attach_file(self, msg: MIMEMultipart, attachment: Union[str, Path, Dict[str, Any]]) -> None:
        """Adiciona um anexo à mensagem MIME."""
        filename = "anexo"
        data: bytes = b""

        if isinstance(attachment, (str, Path)):
            path = Path(attachment)
            if not path.is_file():
                log_message(f"Arquivo anexo não encontrado: {path}", level="warning", source="SMTPService")
                return
            filename = path.name
            with open(path, "rb") as f:
                data = f.read()
        elif isinstance(attachment, dict):
            filename = attachment.get("filename", "anexo.dat")
            content = attachment.get("content", b"")
            if isinstance(content, str):
                data = content.encode("utf-8")
            elif isinstance(content, bytes):
                data = content
            else:
                return

        part = MIMEBase("application", "octet-stream")
        part.set_payload(data)
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{Header(filename, "utf-8").encode()}"',
        )
        msg.attach(part)

    def send_email(
        self,
        to: Union[str, List[str]],
        subject: str,
        body_text: Optional[str] = None,
        body_html: Optional[str] = None,
        template_name: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        attachments: Optional[List[Union[str, Path, Dict[str, Any]]]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Envia um e-mail com retentativas automáticas em caso de falhas transitórias.
        Suporta renderização direta de templates HTML/TXT a partir do parâmetro template_name.

        Retorna:
            Dict com status 'success', 'message_id' e destinatários notificados.
        """
        if not self.config.enabled:
            return {
                "success": False,
                "message": "Serviço SMTP desabilitado na configuração (SMTP_ENABLED=False).",
            }

        # Se um nome de template foi fornecido, renderiza HTML e texto via TemplateService
        if template_name:
            tmpl_context = dict(context or {})
            if "recipient_email" not in tmpl_context:
                tmpl_context["recipient_email"] = to if isinstance(to, str) else ", ".join(to)

            rendered_html, rendered_text = render_email_template(template_name, tmpl_context)
            if rendered_html and not body_html:
                body_html = rendered_html
            if rendered_text and not body_text:
                body_text = rendered_text

        # Normaliza listas de destinatários
        to_list = [to] if isinstance(to, str) else list(to)
        cc_list = [cc] if isinstance(cc, str) else (list(cc) if cc else [])
        bcc_list = [bcc] if isinstance(bcc, str) else (list(bcc) if bcc else [])

        all_recipients = to_list + cc_list + bcc_list
        if not all_recipients:
            return {"success": False, "message": "Nenhum destinatário informado."}

        mime_msg = self._build_mime_message(
            to=to_list,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            from_email=from_email,
            from_name=from_name,
            reply_to=reply_to,
            cc=cc_list if cc_list else None,
            bcc=bcc_list if bcc_list else None,
            attachments=attachments,
            headers=headers,
        )

        sender = from_email or self.config.from_email
        if self.config.username and "@gmail.com" in self.config.username.lower():
            if not sender or "@gmail.com" not in sender.lower():
                sender = self.config.username

        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                with self._create_smtp_client() as server:
                    refused = server.sendmail(sender, all_recipients, mime_msg.as_string())
                    if refused:
                        refused_info = ", ".join([f"{rcpt}: {err}" for rcpt, err in refused.items()])
                        log_message(f"Destinatários recusados pelo servidor: {refused_info}", level="warning", source="SMTPService")
                        return {
                            "success": False,
                            "message": f"Destinatário recusado pelo servidor de e-mail: {refused_info}",
                            "error": refused_info,
                            "is_invalid_recipient": True,
                            "attempts": attempt,
                        }

                message_id = mime_msg.get("Message-ID", "")
                log_message(
                    f"E-mail '{subject}' enviado com sucesso para {len(all_recipients)} destinatários (Tentativa {attempt})",
                    level="success",
                    source="SMTPService",
                )
                return {
                    "success": True,
                    "message_id": message_id,
                    "to": to_list,
                    "cc": cc_list,
                    "bcc": bcc_list,
                    "subject": subject,
                    "attempts": attempt,
                }
            except smtplib.SMTPRecipientsRefused as e:
                err_msg = f"Destinatário recusado pelo servidor SMTP: {e}"
                log_message(err_msg, level="warning", source="SMTPService")
                return {
                    "success": False,
                    "message": "E-mail inválido ou inexistente.",
                    "error": str(e),
                    "is_invalid_recipient": True,
                    "attempts": attempt,
                }
            except (smtplib.SMTPException, OSError, TimeoutError) as e:
                last_error = e
                # Se for erro 5xx permanente, não adianta tentar novamente
                if isinstance(e, smtplib.SMTPResponseException) and 500 <= e.smtp_code < 600:
                    err_msg = f"Erro permanente no servidor SMTP ({e.smtp_code}): {e.smtp_error}"
                    log_message(err_msg, level="warning", source="SMTPService")
                    return {
                        "success": False,
                        "message": f"E-mail recusado pelo servidor ({e.smtp_code}): {e.smtp_error}",
                        "error": str(e),
                        "is_invalid_recipient": True,
                        "attempts": attempt,
                    }
                log_message(
                    f"Falha ao enviar e-mail (tentativa {attempt}/{self.config.max_retries}): {e}",
                    level="warning",
                    source="SMTPService",
                )
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_delay * attempt)

        err_msg = f"Falha definitiva ao enviar e-mail após {self.config.max_retries} tentativas: {last_error}"
        log_message(err_msg, level="error", source="SMTPService")
        return {
            "success": False,
            "message": err_msg,
            "error": str(last_error),
            "attempts": self.config.max_retries,
        }

    def send_email_template(
        self,
        to: Union[str, List[str]],
        subject: str,
        template_name: str,
        context: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Envia um e-mail renderizando o template especificado da pasta de templates.
        """
        return self.send_email(
            to=to,
            subject=subject,
            template_name=template_name,
            context=context,
            **kwargs,
        )

    async def async_send_email(self, *args, **kwargs) -> Dict[str, Any]:
        """Versão assíncrona não bloqueante para uso em endpoints FastAPI ou tarefas em background."""
        return await asyncio.to_thread(self.send_email, *args, **kwargs)


# Instância global reutilizável
_default_smtp_service: Optional[SMTPService] = None


def get_smtp_service(config: Optional[SMTPConfig] = None) -> SMTPService:
    """Retorna uma instância de SMTPService."""
    global _default_smtp_service
    if config is not None:
        return SMTPService(config)
    if _default_smtp_service is None:
        _default_smtp_service = SMTPService()
    return _default_smtp_service
