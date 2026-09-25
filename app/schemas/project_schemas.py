# -----------------------------
# PROJECT
# -----------------------------
from datetime import datetime
from typing import List, Optional
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from app.schemas.userTask_schemas import (
    OptStrId,
    ProjectMiniSchema,
    SprintMiniSchema,
    StrId,
    TaskMiniSchema,
    UsuarioMiniSchema,
)




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
    id: OptStrId = None
    name: str
    description: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

# ------------------------------------------------
# PROJECT SCHEMA BASE
# ------------------------------------------------
class ProjectSchema(BaseModel):
    id: OptStrId = None
    name: str
    description: Optional[str] = None
    # Opcional: o dono é sempre o utilizador autenticado, definido no serviço.
    # AliasChoices porque este schema serve de entrada (recebe `ownerId`) e de
    # saída (tem de encontrar `project.owner_id` no ORM).
    owner_id: OptStrId = Field(
        None, validation_alias=AliasChoices("ownerId", "owner_id")
    )
    # IDs dos membros da equipa. O frontend envia-os ora como número ora como
    # string conforme a origem, por isso aceitam-se os dois.
    team: List[StrId] = Field(default_factory=list)
    tasks: Optional[List[TaskMiniSchema]] = Field(default_factory=list)
    # É uma coleção no ORM (`Project.sprints`): como um único objeto, qualquer
    # projeto com mais do que uma sprint falhava a validação.
    sprints: List[SprintMiniSchema] = Field(default_factory=list)
    type_project: Optional[TypeProjectoSchema] = None
    created_at: Optional[datetime] = None
    due_date: Optional[datetime] = None
    is_active: Optional[bool] = True
    id_conexao_db: Optional[int] = Field(
        None, validation_alias=AliasChoices("connectionId", "id_conexao_db")
    )
    # No ORM a relação chama-se `db_connection`; o frontend envia e espera
    # `connection`. Sem aceitar os dois nomes, a conexão associada nunca
    # aparecia na resposta.
    connection: Optional[DBConnectionMiniSchema] = Field(
        None, validation_alias=AliasChoices("connection", "db_connection")
    )

    model_config = ConfigDict(
        from_attributes=True, populate_by_name=True, extra="ignore"
    )


# ------------------------------------------------
# PROJECT RESPONSE (com usuário completo)
# ------------------------------------------------
class ProjectResponseSchema(ProjectSchema):
    """Retorno completo de projeto, com informações detalhadas."""

    # No ORM o dono é `owner_user`.
    owner: Optional[UsuarioMiniSchema] = Field(
        None, validation_alias=AliasChoices("owner", "owner_user")
    )
    team_members: Optional[List[UsuarioMiniSchema]] = Field(default_factory=list)

    model_config = ConfigDict(
        from_attributes=True, populate_by_name=True, extra="ignore"
    )
