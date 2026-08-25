from pydantic import BaseModel, ConfigDict, Field
from typing import Literal, Optional
from datetime import datetime

# ==========================================
# TYPES E ENUMS GERAIS
# ==========================================
# Entidades que `GET /geral/paginate` aceita no parâmetro `tipo`.
#
# ⚠️ Tem de acompanhar `MODEL_MAP` em app/services/geral_services.py: um nome
# aqui que lá não exista dá 403, e um nome só lá é rejeitado com 422 antes de
# chegar ao serviço. Removido "project_team_association" — era uma Table de
# associação, não um modelo mapeado, e a paginação rebentava com ela.
OptionTipoModel = Literal[
    # 👥 Pessoas e organização
    "user",
    "Role",
    "Empresa",
    "Cargo",
    "RefreshToken",
    # 📋 Gestão de projetos
    "project",
    "task",
    "sprint",
    "type_project",
    "TaskStats",
    "AuditLog",
    # 🔌 Conexões e base de dados
    "DBConnection",
    "ActiveConnection",
    "ConnectionLog",
    "DBHealthCheck",
    "QueryHistory",
    "DBStructure",
    "DBField",
    "DBEnumField",
]


# ==========================================
# SCHEMAS DE SETTINGS (CONFIGURAÇÕES DO UTILIZADOR)
# ==========================================

class SettingsBase(BaseModel):
    """
    Campos partilhados para leitura e criação das configurações.
    Alinhado com as opções do frontend (incluindo idiomas nacionais de Angola).
    """
    theme: str = Field(default="light", pattern="^(light|dark|system)$")
    language: str = Field(default="pt", pattern="^(pt|en|fr|cn|km-AO|umb-AO)$")
    sidebar_collapsed: bool = Field(default=False)
    preferred_db_type: Optional[str] = Field(default=None)
    email_notifications: bool = Field(default=True)
    app_notifications: bool = Field(default=True)
    timezone: str = Field(default="UTC")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "theme": "system",
                "language": "pt",
                "sidebar_collapsed": False,
                "preferred_db_type": "PostgreSQL",
                "email_notifications": True,
                "app_notifications": True,
                "timezone": "Africa/Luanda"
            }
        }
    )


class SettingsCreate(SettingsBase):
    """
    Schema usado no momento da criação (se o utilizador quiser enviar definições específicas logo no registo, 
    embora o habitual seja criar automaticamente com os defaults do SettingsBase).
    """
    pass


class SettingsUpdate(BaseModel):
    """
    Schema para Atualização (PATCH). Todos os campos são opcionais.
    """
    theme: Optional[str] = Field(None, pattern="^(light|dark|system)$")
    language: Optional[str] = Field(None, pattern="^(pt|en|fr|cn|km-AO|umb-AO)$")
    sidebar_collapsed: Optional[bool] = None
    preferred_db_type: Optional[str] = None
    email_notifications: Optional[bool] = None
    app_notifications: Optional[bool] = None
    timezone: Optional[str] = None


class SettingsResponse(SettingsBase):
    """
    Schema de Resposta. O que o Backend devolve para o Frontend.
    Inclui os campos gerados pela base de dados.
    """
    id: int
    user_id: int
    created_at: datetime
    updated_at: datetime

    # Permite que o Pydantic leia diretamente da instância do modelo SQLAlchemy
    model_config = ConfigDict(from_attributes=True)


class UpdateAppearancePayload(BaseModel):
    theme: Optional[str] = Field(None, pattern="^(light|dark|system)$")
    sidebar_collapsed: Optional[bool] = None

class UpdateLanguagePayload(BaseModel):
    language: str = Field(..., pattern="^(pt|en|fr|cn|km-AO|umb-AO)$")
