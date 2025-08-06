from typing import Optional, Dict, Any, Generic, TypeVar
from pydantic import BaseModel, RootModel

# Tipo genérico para suportar qualquer tipo de dado no `ResponseWrapper`
T = TypeVar("T")

class ResponseWrapper(BaseModel, Generic[T]):
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None


class TableRow(RootModel[Dict[str, Any]]):
    """Representa uma linha genérica de qualquer tabela."""
    pass
