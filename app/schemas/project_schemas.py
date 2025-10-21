# -----------------------------
# PROJECT
# -----------------------------
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field




# ------------------------------------------------
# CONEXÃO MINI SCHEMA
# ------------------------------------------------
class DBConnectionMiniSchema(BaseModel):
    id: int
    name: str
    type: str

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# ------------------------------------------------
# TIPO DE PROJETO
# ------------------------------------------------
class TypeProjectoSchema(BaseModel):
    id: str
    name: str
    description: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

from app.schemas.userTask_schemas import UsuarioMiniSchema,TaskMiniSchema,SprintMiniSchema
# ------------------------------------------------
# PROJECT SCHEMA BASE
# ------------------------------------------------
class ProjectSchema(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    owner_id: str = Field(..., alias="ownerId")
    team: Optional[List[str]] = Field(default_factory=list)  # IDs dos usuários
    tasks: Optional[List[TaskMiniSchema]] = Field(default_factory=list)
    sprints: Optional["SprintMiniSchema"] = None
    type_project: Optional[TypeProjectoSchema] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    due_date: Optional[datetime] = None
    id_conexao_db: Optional[int] = Field(None, alias="connectionId")
    connection: Optional[DBConnectionMiniSchema] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# ------------------------------------------------
# PROJECT RESPONSE (com usuário completo)
# ------------------------------------------------
class ProjectResponseSchema(ProjectSchema):
    """Retorno completo de projeto, com informações detalhadas."""
    owner: Optional[UsuarioMiniSchema] = None
    team_members: Optional[List[UsuarioMiniSchema]] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
