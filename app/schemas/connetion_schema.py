from typing_extensions import Annotated
from pydantic import BaseModel, ConfigDict, StringConstraints
from typing import Optional, List
from datetime import datetime
from enum import Enum

class DbConnectionOutput(BaseModel):
    message: Optional[str] 
    connection_id: Optional[int]  
    connect: bool = False

    model_config = ConfigDict(from_attributes=True) 
    
class ActiveConnectionBase(BaseModel):
    user_id: int
    connection_id: int

class ActiveConnectionCreate(ActiveConnectionBase):
    pass

class ActiveConnectionResponse(ActiveConnectionBase):
    activated_at: datetime

    model_config = ConfigDict(from_attributes=True) 
       

class ConnectionStatus(str, Enum):
    connected = "connected"
    disconnected = "disconnected"
    error = "error"


class LogStatus(str, Enum):
    success = "success"
    error = "error"
    warning = "warning"
    info = "info"


class ConnectionLogBase(BaseModel):
    connection: str
    action: Optional[str] = None
    timestamp: Optional[datetime] = None
    status: LogStatus


class ConnectionLogCreate(ConnectionLogBase):
    id: str


class ConnectionLogRead(ConnectionLogBase):
    id: str

    class Config:
        from_attributes = True


class SavedConnectionBase(BaseModel):
    id:int
    name: str
    type: str
    host: str
    database: str
    last_used: Optional[datetime] = None
    status: ConnectionStatus
    model_config = ConfigDict(from_attributes=True) 


class SavedConnectionCreate(SavedConnectionBase):
    id: str


class SavedConnectionRead(SavedConnectionBase):
    id: str
    logs: Optional[List[ConnectionLogRead]] = []

    model_config = ConfigDict(from_attributes=True) 


class ConnectionPaginationOutput(BaseModel):
    page: int
    limit: int
    total: int
    results: List[SavedConnectionBase]
class ConnectionPassUserOut(BaseModel):
    password: str
    username: str
    service: Optional[str] = None  # Oracle
    sslmode: Optional[str] = None  # PostgreSQL
    trustServerCertificate: Optional[str] = None

        
from typing import Optional, Annotated
from pydantic import BaseModel, StringConstraints

class DBConnectionBase(BaseModel):
    name: Annotated[str, StringConstraints(min_length=2, max_length=100)]
    type: Annotated[str, StringConstraints(min_length=2, max_length=50)]
    host: str
    port: int
    username: str
    password: str
    database_name: str
    status: Optional[str] = "available"
    
    # Campos específicos por tipo de banco
    service: Optional[str] = None  # Oracle
    sslmode: Optional[str] = None  # PostgreSQL
    trustServerCertificate: Optional[str] = None  # SQL Server

    class Config:
        json_schema_extra = {
            "example": {
                "name": "PostgreSQL Principal",
                "type": "PostgreSQL",
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
                "database_name": "app_db",
                "sslmode": "require",  # exemplo para PostgreSQL
                "status": "available"
            }
        }

        
class ConnectionLogOut(BaseModel):
    id: int
    connection_id: int
    action: str
    status: str
    timestamp: datetime

    model_config = ConfigDict(from_attributes=True) 

class ConnectionLogPaginationOutput(BaseModel):
    page: int
    limit: int
    total: int
    results: List[ConnectionLogOut]
