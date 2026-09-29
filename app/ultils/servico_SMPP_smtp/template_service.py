"""
Serviço de gerenciamento e renderização de templates de comunicação (E-mail e SMS).

Utiliza o motor Jinja2 para interpolação de variáveis, herança de layouts e
formatação dinâmica de mensagens.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import jinja2

from app.ultils.logger import log_message


DEFAULT_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"


class TemplateService:
    """
    Carregador e renderizador central de templates de E-mail e SMS.
    """

    def __init__(self, templates_dir: Optional[Path] = None):
        self.templates_dir = Path(templates_dir) if templates_dir else DEFAULT_TEMPLATES_DIR
        self._env: Optional[jinja2.Environment] = None
        self._init_environment()

    def _init_environment(self) -> None:
        """Inicializa o ambiente Jinja2 com o carregador de arquivos."""
        if not self.templates_dir.exists():
            self.templates_dir.mkdir(parents=True, exist_ok=True)

        loader = jinja2.FileSystemLoader(str(self.templates_dir))
        self._env = jinja2.Environment(
            loader=loader,
            autoescape=jinja2.select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Variáveis globais úteis disponíveis em todos os templates
        self._env.globals.update(
            {
                "platform_name": "MustaInf",
                "current_year": datetime.now().year,
                "now": datetime.now,
                "support_email": "suporte@mustainf.com",
            }
        )

    def _normalize_email_template_paths(self, template_name: str) -> Tuple[str, str]:
        """
        Normaliza o nome do template para localizar os arquivos HTML e TXT correspondentes.
        Ex: 'confirmacao_registo' -> ('emails/confirmacao_registo.html', 'emails/confirmacao_registo.txt')
        """
        clean_name = template_name.strip()
        if clean_name.endswith(".html"):
            base_name = clean_name[:-5]
        elif clean_name.endswith(".txt"):
            base_name = clean_name[:-4]
        else:
            base_name = clean_name

        if base_name.startswith("emails/"):
            html_path = f"{base_name}.html"
            txt_path = f"{base_name}.txt"
        else:
            html_path = f"emails/{base_name}.html"
            txt_path = f"emails/{base_name}.txt"

        return html_path, txt_path

    def _normalize_sms_template_path(self, template_name: str) -> str:
        """
        Normaliza o nome do template para localizar o arquivo de SMS em texto.
        Ex: 'confirmacao_registo' -> 'sms/confirmacao_registo.txt'
        """
        clean_name = template_name.strip()
        if clean_name.endswith(".txt"):
            base_name = clean_name[:-4]
        else:
            base_name = clean_name

        if base_name.startswith("sms/"):
            return f"{base_name}.txt"
        return f"sms/{base_name}.txt"

    def _html_to_plain_text(self, html: str) -> str:
        """Converte conteúdo HTML básico em texto puro legível como fallback."""
        text = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
        text = re.sub(r"<style.*?>.*?</style>", "", text, flags=re.DOTALL)
        text = re.sub(r"<script.*?>.*?</script>", "", text, flags=re.DOTALL)
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</p>", "\n\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</div>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        lines = [line.strip() for line in text.split("\n")]
        return "\n".join(line for line in lines if line)

    def render_email(
        self,
        template_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Renderiza os formatos HTML e TXT de um template de e-mail.

        Retorna:
            Tupla (body_html, body_text). Se um deles não existir, o outro é retornado com fallback.
        """
        if self._env is None:
            self._init_environment()

        ctx = context or {}
        html_path, txt_path = self._normalize_email_template_paths(template_name)

        body_html: Optional[str] = None
        body_text: Optional[str] = None

        # 1. Renderiza template HTML
        try:
            template = self._env.get_template(html_path)
            body_html = template.render(**ctx)
        except jinja2.TemplateNotFound:
            log_message(f"Template HTML '{html_path}' não encontrado.", level="warning", source="TemplateService")
        except Exception as e:
            log_message(f"Erro ao renderizar template HTML '{html_path}': {e}", level="error", source="TemplateService")
            raise

        # 2. Renderiza template TXT
        try:
            template_txt = self._env.get_template(txt_path)
            body_text = template_txt.render(**ctx)
        except jinja2.TemplateNotFound:
            # Fallback automático: se tiver HTML, gera o texto a partir dele
            if body_html:
                body_text = self._html_to_plain_text(body_html)
        except Exception as e:
            log_message(f"Erro ao renderizar template TXT '{txt_path}': {e}", level="error", source="TemplateService")
            raise

        if not body_html and not body_text:
            raise FileNotFoundError(
                f"Nenhum template encontrado para '{template_name}' (procurado: {html_path} e {txt_path})"
            )

        return body_html, body_text

    def render_sms(
        self,
        template_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Renderiza um template de SMS em texto.

        Retorna:
            Mensagem formatada pronta para envio via SMPP.
        """
        if self._env is None:
            self._init_environment()

        ctx = context or {}
        sms_path = self._normalize_sms_template_path(template_name)

        try:
            template = self._env.get_template(sms_path)
            return template.render(**ctx).strip()
        except jinja2.TemplateNotFound:
            log_message(f"Template de SMS '{sms_path}' não encontrado.", level="error", source="TemplateService")
            raise FileNotFoundError(f"Template de SMS '{sms_path}' não encontrado.")
        except Exception as e:
            log_message(f"Erro ao renderizar template de SMS '{sms_path}': {e}", level="error", source="TemplateService")
            raise

    def render_string(self, source: str, context: Optional[Dict[str, Any]] = None) -> str:
        """Renderiza uma string como template Jinja2."""
        if self._env is None:
            self._init_environment()
        template = self._env.from_string(source)
        return template.render(**(context or {}))


_default_template_service: Optional[TemplateService] = None


def get_template_service(templates_dir: Optional[Path] = None) -> TemplateService:
    """Retorna a instância singleton do serviço de templates."""
    global _default_template_service
    if templates_dir is not None:
        return TemplateService(templates_dir)
    if _default_template_service is None:
        _default_template_service = TemplateService()
    return _default_template_service


def render_email_template(template_name: str, context: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Optional[str]]:
    """Função de conveniência para renderizar template de e-mail (HTML e Texto)."""
    return get_template_service().render_email(template_name, context)


def render_sms_template(template_name: str, context: Optional[Dict[str, Any]] = None) -> str:
    """Função de conveniência para renderizar template de SMS."""
    return get_template_service().render_sms(template_name, context)
