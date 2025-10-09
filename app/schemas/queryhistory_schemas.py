from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Any, Dict, List, Literal, Optional
from datetime import datetime, timezone
from enum import Enum


# --------- ENUM ---------

class QueryType(str, Enum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    OTHER = "OTHER"


# --------- BASES ---------

class QueryHistoryBase(BaseModel):
    """Base schema para histórico de consulta"""
    user_id: int = Field(..., description="ID do usuário")
    db_connection_id: int = Field(..., description="ID da conexão de banco")
    query: str = Field(..., min_length=1, description="Query SQL executada")
    query_type: Optional[QueryType] = Field(None, description="Tipo da query")
    duration_ms: Optional[int] = Field(None, ge=0, description="Duração em ms")
    result_preview: Optional[str] = Field(None, description="Preview dos resultados")
    error_message: Optional[str] = Field(None, description="Mensagem de erro")
    is_favorite: bool = Field(False, description="Se é favorita")
    tags: Optional[str] = Field(None, description="Tags para categorização")

    model_config = ConfigDict(from_attributes=True)


# --------- AUXILIARES ---------

class TableInfo(BaseModel):
    name: str = Field(..., title="Nome da Tabela")
    rowcount: int = Field(..., title="Linhas", description="Número de linhas da tabela")


class DatabaseMetadata(BaseModel):
    connectionName: str
    databaseName: str
    serverVersion: str
    tableCount: int
    viewCount: int
    procedureCount: int
    functionCount: int
    triggerCount: int
    indexCount: int
    tableNames: List[TableInfo]





# --------- ASYNC MODELS ---------

class QueryHistoryCreateAsync(QueryHistoryBase):
    executed_at: datetime = Field(default_factory=lambda: datetime.now())
    updated_at: datetime = Field(default_factory=lambda: datetime.now())

    @field_validator('executed_at', 'updated_at', mode='before')
    def ensure_naive_datetime(cls, v):
        if v is None:
            return datetime.now()
        if isinstance(v, datetime):
            # Remove tzinfo, deixando naive
            return v.replace(tzinfo=None)
        return v


class QueryHistoryUpdateAsync(BaseModel):
    is_favorite: Optional[bool] = None
    tags: Optional[str] = None
    error_message: Optional[str] = None
    result_preview: Optional[str] = None
    duration_ms: Optional[int] = Field(None, ge=0)
    updated_at: datetime = Field(default_factory=lambda: datetime.now().replace(tzinfo=None))

    @field_validator('updated_at', mode='before')
    def ensure_naive_datetime(cls, v):
        if v is None:
            return datetime.now().replace(tzinfo=None)
        if isinstance(v, datetime) and v.tzinfo is not None:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        return v

    model_config = ConfigDict(
        json_encoders={datetime: lambda v: v.isoformat() if v else None}
    )


class QueryHistoryResponseAsync(QueryHistoryBase):
    id: int
    executed_at: datetime
    updated_at: datetime
    created_at: Optional[datetime] = None

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None}
    )


# --------- Criação e Atualização ---------

class QueryHistoryCreate(QueryHistoryBase):
    user_id: int = Field(..., title="ID do Usuário")
    db_connection_id: int = Field(..., title="ID da Conexão")


class QueryHistoryUpdate(QueryHistoryBase):
    # Permite update parcial
    query: Optional[str] = Field(None, title="Query SQL")
    query_type: Optional[str] = Field(None)
    duration_ms: Optional[int] = Field(None)
    result_preview: Optional[str] = Field(None)
    error_message: Optional[str] = Field(None)
    is_favorite: Optional[bool] = Field(None)
    tags: Optional[str] = Field(None)


# --------- Resposta ---------

class QueryHistoryOut(BaseModel):
    id: int
    user_id: int
    db_connection_id: int
    query: str
    query_type: Optional[str] = None
    executed_at: datetime
    duration_ms: Optional[int] = None
    result_preview: Optional[str] = None
    error_message: Optional[str] = None
    is_favorite: bool
    tags: Optional[str] = None
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)  # Para suportar ORM
