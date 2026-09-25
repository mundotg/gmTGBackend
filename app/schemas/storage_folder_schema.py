"""Schemas das pastas do storage.

A senha nunca sai daqui para fora: `FolderOut` expõe apenas `is_locked`, para a
UI poder mostrar o cadeado sem que o hash chegue perto da resposta.
"""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FolderCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    parent_id: Optional[UUID] = None
    password: Optional[str] = Field(
        None, description="Senha opcional; protege toda a subárvore."
    )

    @field_validator("name")
    @classmethod
    def _nome_limpo(cls, v: str) -> str:
        nome = v.strip()
        if not nome:
            raise ValueError("O nome da pasta não pode estar vazio.")
        # `/` e `\` partiriam qualquer caminho apresentado na UI.
        if "/" in nome or "\\" in nome:
            raise ValueError("O nome da pasta não pode conter '/' nem '\\'.")
        return nome


class FolderUpdate(BaseModel):
    """Campos opcionais: só se aplica o que vier preenchido."""

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    parent_id: Optional[UUID] = None
    move_to_root: bool = Field(
        False,
        description="Move para a raiz (parent_id=None não distingue 'ausente' de 'raiz').",
    )

    @field_validator("name")
    @classmethod
    def _nome_limpo(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        nome = v.strip()
        if not nome:
            raise ValueError("O nome da pasta não pode estar vazio.")
        if "/" in nome or "\\" in nome:
            raise ValueError("O nome da pasta não pode conter '/' nem '\\'.")
        return nome


class FileMove(BaseModel):
    """Destino de um ficheiro. `move_to_root` existe porque `folder_id=None`
    não distingue 'não mexer' de 'mover para a raiz'."""

    folder_id: Optional[UUID] = None
    move_to_root: bool = False


class FolderPasswordUpdate(BaseModel):
    current_password: Optional[str] = Field(
        None, description="Obrigatória se a pasta já tiver senha."
    )
    new_password: Optional[str] = Field(
        None, description="Vazio ou ausente remove a senha."
    )


class FolderUnlock(BaseModel):
    password: str


class FolderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    parent_id: Optional[UUID] = None
    is_locked: bool = False
    created_at: Optional[datetime] = None


class FolderTreeNode(FolderOut):
    # `is_locked` diz que TEM senha; `is_unlocked` diz que está aberta agora.
    # Sem os dois, o cliente teria de adivinhar pelo conteúdo — e uma pasta
    # aberta mas vazia era indistinguível de uma bloqueada.
    is_unlocked: bool = True
    children: List["FolderTreeNode"] = []
    file_count: int = 0


FolderTreeNode.model_rebuild()
