from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator



# -----------------------------
# ENUM - Papel (Role)
# -----------------------------
class UserRoleEnum(str, Enum):
    admin = "admin"
    user = "user"
    manager = "manager"
    membro = "membro"
    gerente = "gerente"


# -----------------------------
# SCHEMA - Role
# -----------------------------
class RoleSchema(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid4()))
    nome: UserRoleEnum = Field(default=UserRoleEnum.membro)
    descricao: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

# -----------------------------
# REQUEST - Criação de Usuário
# -----------------------------
class UsuarioCreateSchema(BaseModel):
    nome: str = Field(..., min_length=2)
    email: EmailStr
    senha: str = Field(..., min_length=6)
    avatarUrl: Optional[str] = None
    role_id : Optional[str] = None
    role: Optional[RoleSchema] = Field(
        default_factory=lambda: RoleSchema(nome=UserRoleEnum.membro),
        description="Função ou papel do usuário (ex: admin, membro, gerente)"
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
# -----------------------------
# SCHEMA - Usuário
# -----------------------------
class UsuarioSchema(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid4()))
    user_id: Optional[str] = None
    nome: str
    email: EmailStr
    role: Optional[RoleSchema] = Field(default_factory=lambda: RoleSchema())
    avatarUrl: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    projects_participating: List[str] = Field(default_factory=list)
    created_projects: List[str] = Field(default_factory=list)
    assigned_tasks: List[str] = Field(default_factory=list)
    delegated_tasks: List[str] = Field(default_factory=list)
    created_tasks: List[str] = Field(default_factory=list)

    @field_validator('email')
    def normalize_email(cls, v): return v.strip().lower()

    @field_validator('nome')
    def normalize_nome(cls, v): return ' '.join(v.strip().title().split())

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# REQUEST - Criação de Usuário
# -----------------------------
class UsuarioCreateSchema(BaseModel):
    nome: str = Field(..., min_length=2)
    email: EmailStr
    senha: str = Field(..., min_length=6)
    avatarUrl: Optional[str] = None
    role_id : Optional[str] = None
    role: Optional[RoleSchema] = Field(
        default_factory=lambda: RoleSchema(nome=UserRoleEnum.membro),
        description="Função ou papel do usuário (ex: admin, membro, gerente)"
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# REQUEST - Atualização de Usuário
# -----------------------------
class UsuarioUpdateSchema(BaseModel):
    nome: Optional[str] = Field(None, min_length=2, description="Nome atualizado do usuário")
    email: Optional[EmailStr] = Field(None, description="Novo email, se aplicável")
    senha: Optional[str] = Field(None, min_length=6, description="Nova senha, se for alterada")
    role: Optional[RoleSchema] = Field(None, description="Nova função do usuário")
    avatarUrl: Optional[str] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# REQUEST - Login
# -----------------------------
class UsuarioLoginSchema(BaseModel):
    email: EmailStr = Field(..., description="Email do usuário para login")
    senha: str = Field(..., min_length=6, description="Senha do usuário")

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# RESPONSE - Usuário Completo
# -----------------------------
class UsuarioResponseSchema(BaseModel):
    id: str
    nome: str
    email: EmailStr
    role: Optional[RoleSchema] = None
    avatarUrl: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    projects_participating: List[str] = Field(default_factory=list)
    created_projects: List[str] = Field(default_factory=list)
    assigned_tasks: List[str] = Field(default_factory=list)
    delegated_tasks: List[str] = Field(default_factory=list)
    created_tasks: List[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# MINI SCHEMAS (para respostas leves)
# -----------------------------
class ProjectMiniSchema(BaseModel):
    id: str
    name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class TaskMiniSchema(BaseModel):
    id: str
    title: Optional[str] = None
    status: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class UsuarioMiniSchema(BaseModel):
    id: str
    nome: str
    email: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
    
class SprintMiniSchema(BaseModel):
    """Campos e validações comuns entre todos os schemas de Sprint."""

    name: str = Field(..., min_length=3, max_length=100, description="Nome da sprint")
    start_date: datetime = Field(
        default_factory=datetime.utcnow, description="Data de início da sprint"
    )
    end_date: datetime = Field(..., description="Data de término da sprint")
    goal: Optional[str] = Field(
        default=None, max_length=255, description="Objetivo principal da sprint"
    )
# -----------------------------
# RESPONSE - Login com Tokens
# -----------------------------
class LoginResponseSchema(BaseModel):
    id: str
    nome: str
    email: str
    role: Optional[RoleSchema]= None
    avatarUrl: Optional[str] = None

    projects_participating: List[ProjectMiniSchema] = Field(default_factory=list)
    created_projects: List[ProjectMiniSchema] = Field(default_factory=list)
    assigned_tasks: List[TaskMiniSchema] = Field(default_factory=list)
    delegated_tasks: List[TaskMiniSchema] = Field(default_factory=list)
    created_tasks: List[TaskMiniSchema] = Field(default_factory=list)

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 3600  # 1h padrão
    expires_at: datetime = Field(default_factory=lambda: datetime.utcnow() + timedelta(hours=1))
    last_login: datetime = Field(default_factory=datetime.utcnow)

    @field_validator('expires_at', mode='before')
    def sync_expires_at(cls, v, info):
        expires_in = info.data.get('expires_in', 3600)
        return datetime.utcnow() + timedelta(seconds=expires_in)

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
