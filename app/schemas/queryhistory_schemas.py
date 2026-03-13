from dataclasses import dataclass

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
    COUNT = "COUNT"
    ADDCOLUMN = "ADD COLUMN"
    REMOVECOLUMN = "REMOVE COLUMN"
    ALTERCOLUMN = "ALTER COLUMN"
    ADDFK = "ADD FK"
    REMOVEFK = "REMOVE FK"
    CREATETABLE = "CREATETABLE"
    ALTERTABLE = "ALTERTABLE"
    DROPTABLE = "DROPTABLE"
    


# --------- BASE ---------

class QueryHistoryBase(BaseModel):
    """Base schema para histórico de consulta"""
    user_id: Optional[int] = Field(None, description="ID do usuário (pode ser nulo se excluído)")
    db_connection_id: Optional[int] = Field(None, description="ID da conexão de banco (pode ser nulo)")
    query: str = Field(..., min_length=1, description="Query SQL executada")
    query_type: Optional[QueryType] = Field(None, description="Tipo da query SQL")
    duration_ms: Optional[int] = Field(None, ge=0, description="Duração da execução em milissegundos")
    result_preview: Optional[str] = Field(None, description="Prévia dos resultados")
    error_message: Optional[str] = Field(None, description="Mensagem de erro")
    is_favorite: bool = Field(False, description="Indica se foi marcada como favorita")
    tags: Optional[str] = Field(None, description="Tags para categorização")
    app_source: Optional[str] = Field(None, description="Origem da execução (ex: API, Console, UI)")
    client_ip: Optional[str] = Field(None, description="Endereço IP do cliente")
    executed_by: Optional[str] = Field(None, description="Nome do executor (usuário ou sistema)")
    meta_info: Optional[dict] = Field(default_factory=dict, description="Metadados adicionais")
    modified_by: Optional[str] = Field(None, description="Usuário que modificou o registro")

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
    executed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator('executed_at', 'updated_at', mode='before')
    def ensure_utc_datetime(cls, v):
        """Garante que as datas sejam timezone-aware (UTC)"""
        if v is None:
            return datetime.now(timezone.utc)
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc)


class QueryHistoryUpdateAsync(BaseModel):
    is_favorite: Optional[bool] = None
    tags: Optional[str] = None
    error_message: Optional[str] = None
    result_preview: Optional[str] = None
    duration_ms: Optional[int] = Field(None, ge=0)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    modified_by: Optional[str] = None

    @field_validator('updated_at', mode='before')
    def ensure_utc_datetime(cls, v):
        if v is None:
            return datetime.now(timezone.utc)
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc)

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


# --------- CRUD Sync ---------

class QueryHistoryCreate(QueryHistoryBase):
    """Criação síncrona"""
    executed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class QueryHistoryUpdate(BaseModel):
    """Atualização síncrona parcial"""
    query: Optional[str] = None
    query_type: Optional[QueryType] = None
    duration_ms: Optional[int] = Field(None, ge=0)
    result_preview: Optional[str] = None
    error_message: Optional[str] = None
    is_favorite: Optional[bool] = None
    tags: Optional[str] = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    modified_by: Optional[str] = None


# --------- OUTPUT ---------

class QueryHistoryOut(QueryHistoryBase):
    id: int
    executed_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
    
    
@dataclass
class QueryExecutionResult:
    """Resultado da execução de uma query."""
    success: bool
    query: str
    duration_ms: int
    cached: bool = False
    error_message: Optional[str] = None
    # Para queries SELECT
    columns: Optional[List[str]] = None
    preview: Optional[List[Dict]] = None
    params: Optional[Dict[str, Any]] = None
    count: Optional[int] = None
