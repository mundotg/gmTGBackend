from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field, ConfigDict, field_validator


# =========================
# ENUM
# =========================

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

    ADD_COLUMN = "ADD COLUMN"
    REMOVE_COLUMN = "REMOVE COLUMN"
    ALTER_COLUMN = "ALTER COLUMN"

    ADD_FK = "ADD FK"
    REMOVE_FK = "REMOVE FK"

    CREATE_TABLE = "CREATETABLE"
    ALTER_TABLE = "ALTERTABLE"
    DROP_TABLE = "DROPTABLE"


# =========================
# UTILS
# =========================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(v: Optional[datetime]) -> datetime:
    if v is None:
        return utc_now()
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return utc_now()


# =========================
# BASE
# =========================

class QueryHistoryBase(BaseModel):
    """Base schema para histórico de consulta"""

    user_id: Optional[int] = None
    db_connection_id: Optional[int] = None

    query: str = Field(..., min_length=1)
    query_type: Optional[QueryType] = None

    duration_ms: Optional[int] = Field(None, ge=0)

    result_preview: Optional[str] = None
    error_message: Optional[str] = None

    is_favorite: bool = False
    tags: Optional[str] = None

    app_source: Optional[str] = None
    client_ip: Optional[str] = None
    executed_by: Optional[str] = None

    meta_info: Dict[str, Any] = Field(default_factory=dict)
    modified_by: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# =========================
# AUXILIAR
# =========================

class TableInfo(BaseModel):
    name: str
    rowcount: int


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


# =========================
# ASYNC MODELS
# =========================

class QueryHistoryCreateAsync(QueryHistoryBase):
    executed_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("executed_at", "updated_at", mode="before")
    @classmethod
    def validate_dates(cls, v):
        return ensure_utc(v)


class QueryHistoryUpdateAsync(BaseModel):
    is_favorite: Optional[bool] = None
    tags: Optional[str] = None
    error_message: Optional[str] = None
    result_preview: Optional[str] = None
    duration_ms: Optional[int] = Field(None, ge=0)

    updated_at: datetime = Field(default_factory=utc_now)
    modified_by: Optional[str] = None

    @field_validator("updated_at", mode="before")
    @classmethod
    def validate_updated(cls, v):
        return ensure_utc(v)

    model_config = ConfigDict(
        json_encoders={datetime: lambda v: v.isoformat()}
    )


class QueryHistoryResponseAsync(QueryHistoryBase):
    id: int
    executed_at: datetime
    updated_at: datetime
    created_at: Optional[datetime] = None

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat()}
    )


# =========================
# SYNC MODELS
# =========================

class QueryHistoryCreate(QueryHistoryBase):
    executed_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class QueryHistoryUpdate(BaseModel):
    query: Optional[str] = None
    query_type: Optional[QueryType] = None

    duration_ms: Optional[int] = Field(None, ge=0)

    result_preview: Optional[str] = None
    error_message: Optional[str] = None

    is_favorite: Optional[bool] = None
    tags: Optional[str] = None

    updated_at: datetime = Field(default_factory=utc_now)
    modified_by: Optional[str] = None


# =========================
# OUTPUT
# =========================

class QueryHistoryOut(QueryHistoryBase):
    id: int
    executed_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# =========================
# EXECUTION RESULT
# =========================

@dataclass
class QueryExecutionResult:
    """Resultado da execução de uma query."""

    success: bool
    query: str
    duration_ms: int

    cached: bool = False
    error_message: Optional[str] = None

    columns: Optional[List[str]] = None
    preview: Optional[List[Dict[str, Any]]] = None
    params: Optional[Dict[str, Any]] = None
    count: Optional[int] = None