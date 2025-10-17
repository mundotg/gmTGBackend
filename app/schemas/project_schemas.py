# -----------------------------
# PROJECT
# -----------------------------

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.sprint_schemas import SprintSchema
from app.schemas.task_schema import TaskSchema
from app.schemas.userTask_schemas  import UsuarioResponseSchema


class DBConnectionMiniSchema(BaseModel):
    id: int
    name: str
    type: str

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
    
class TypeProjectoSchema(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    
    
class ProjectSchema(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    owner_id: str = Field(..., alias="ownerId")
    team: Optional[List[str]] = Field(default_factory=list)  # IDs dos usuários
    tasks: Optional[List[TaskSchema]] = Field(default_factory=list)
    sprint: Optional[SprintSchema] = None
    type_project: Optional[TypeProjectoSchema] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    due_date: Optional[datetime] = None
    id_conexao_db: Optional[int] = Field(None, alias="connectionId")
    connection: Optional[DBConnectionMiniSchema] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
    
class ProjectResponseSchema(ProjectSchema):
    owner: Optional["UsuarioResponseSchema"] = None