from datetime import datetime, timedelta
from enum import Enum
from typing import Optional
from uuid import uuid4
from pydantic import BaseModel, BeforeValidator, ConfigDict, EmailStr, Field
from typing_extensions import Annotated


# -----------------------------
# TIPO PARTILHADO - ID
# -----------------------------
def _to_str_id(value):
    """
    As chaves primárias são Integer na base de dados, mas o frontend trata
    todos os IDs como string (compara com ===). O Pydantic v2 não converte
    int→str sozinho: sem isto, ler qualquer tarefa/projeto do ORM rebentava
    com "Input should be a valid string".
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(value)
    return value


StrId = Annotated[str, BeforeValidator(_to_str_id)]
OptStrId = Annotated[Optional[str], BeforeValidator(_to_str_id)]


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
# REQUEST - Login
# -----------------------------
class UserLoginSchema(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)

    model_config = ConfigDict(from_attributes=True)




# -----------------------------
# MINI SCHEMAS (para respostas leves)
# -----------------------------
class ProjectMiniSchema(BaseModel):
    id: StrId
    name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class TaskMiniSchema(BaseModel):
    id: StrId
    title: Optional[str] = None
    status: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class UsuarioMiniSchema(BaseModel):
    id: StrId
    nome: str
    email: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class SprintMiniSchema(BaseModel):
    """Resumo de uma sprint, para embutir em respostas de tarefa/projeto."""

    # Faltava o `id`: quem recebia uma tarefa com sprint não tinha como
    # identificá-la, e o agrupamento por sprint no frontend não fechava.
    id: OptStrId = None
    name: str = Field(..., min_length=1, max_length=100, description="Nome da sprint")
    start_date: Optional[datetime] = Field(
        default=None, description="Data de início da sprint"
    )
    end_date: Optional[datetime] = Field(
        default=None, description="Data de término da sprint"
    )
    goal: Optional[str] = Field(
        default=None, max_length=255, description="Objetivo principal da sprint"
    )
    is_active: Optional[bool] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

