import enum

from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Literal, Optional


class LogModel(BaseModel):
    message: str
    level: Literal["info", "error", "success", "warning"] = "info"
    timestamp: datetime = datetime.now()


class LogOut(BaseModel):
    """
    Um registo da tabela `logs` numa resposta da API.

    Sem isto, as rotas declaravam `ResponseWrapper[list]` / `[dict]` e
    devolviam objectos SQLAlchemy: o Pydantic v2 não sabe serializá-los e
    rebentava com `PydanticSerializationError: Unable to serialize unknown
    type: <class 'app.models.log_models.Log'>` — um 500 em cada chamada.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int
    message: str
    # `str` e não `Literal`: o nível vem da base e pode ter valores antigos
    # ou fora da lista. Numa resposta de leitura, rejeitar um registo já
    # gravado só troca um 500 por outro.
    level: str
    source: Optional[str] = None
    user: Optional[int] = None
    created_at: Optional[datetime] = None


class LogLevel(enum.Enum):
    info = "info"
    error = "error"
    warning = "warning"
    success = "success"
