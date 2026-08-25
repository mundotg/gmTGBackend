from typing_extensions import Annotated
from pydantic import BaseModel, ConfigDict, StringConstraints, model_validator
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
    # Connection string, quando a conexão foi criada no modo URL. Vai cifrada
    # no envelope de transporte, como a password — sem isto, editar uma
    # conexão por URL no formulário deixava o campo vazio.
    url: Optional[str] = None

        

class DBConnectionBase(BaseModel):
    """
    Dados de uma conexão. Duas formas de a descrever, à escolha do utilizador:

    1. **Campos separados** — host, porta, utilizador, password, base.
    2. **URL** (`url`) — a connection string completa do fornecedor, cifrada
       pelo frontend. Nesse caso os campos separados podem vir vazios: o
       backend deriva host/porta/base da própria URL só para preencher as
       colunas e a listagem, e liga usando a URL tal como está.

    Os campos separados deixaram de ser obrigatórios por causa da opção 2, mas
    continua a ser obrigatório indicar **um dos dois** (ver validador).
    """

    name: Annotated[str, StringConstraints(min_length=2, max_length=100)]
    type: Annotated[str, StringConstraints(min_length=2, max_length=50)]
    host: str = ""
    port: int = 0
    username: str = ""
    password: str = ""
    database_name: str = ""
    status: Optional[str] = "available"

    # Connection string completa, cifrada com o envelope de transporte
    # (aes_encrypt no frontend). None/"" = modo campos separados.
    url: Optional[str] = None

    # Campos específicos por tipo de banco
    service: Optional[str] = None  # Oracle
    sslmode: Optional[str] = None  # PostgreSQL
    trustServerCertificate: Optional[str] = None  # SQL Server

    @model_validator(mode="after")
    def _exige_url_ou_campos(self):
        if not (self.url or "").strip() and not (self.host or "").strip():
            raise ValueError(
                "Indique o host (ou o caminho, no SQLite) ou então uma URL de conexão."
            )
        return self

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
                "status": "available",
            }
        }
class ConnectionRequest(BaseModel):
    conn_data: DBConnectionBase
    tipo:str = "con" 
        
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
    
class DatasetOpenResponse(BaseModel):
    success: bool
    source: str
    filename: str
    connection_id: int
    table_name: str
    total_rows: int
    total_columns: int
    columns: list[str]
    preview: list[dict]
    message: str


# =========================================================
# 🤝 Partilha de conexões
# =========================================================
class ConnectionAccessLevel(str, Enum):
    """Do mais fraco para o mais forte. `manage` permite repartilhar."""

    read = "read"
    write = "write"
    manage = "manage"


class ConnectionShareOut(BaseModel):
    id: int
    connection_id: int
    user_id: int
    user_nome: Optional[str] = None
    user_email: Optional[str] = None
    access_level: ConnectionAccessLevel
    granted_by_id: Optional[int] = None
    granted_by_nome: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ConnectionShareCreate(BaseModel):
    user_id: int
    access_level: ConnectionAccessLevel = ConnectionAccessLevel.read


class ConnectionShareUpdate(BaseModel):
    access_level: ConnectionAccessLevel


class ConnectionAccessOut(BaseModel):
    """Resumo do que o utilizador atual pode fazer numa conexão."""

    connection_id: int
    connection_name: Optional[str] = None
    owner_id: Optional[int] = None
    owner_nome: Optional[str] = None
    is_owner: bool = False
    access_level: Optional[ConnectionAccessLevel] = None
    can_read: bool = False
    can_write: bool = False
    can_share: bool = False
    can_delete: bool = False
    shares: List[ConnectionShareOut] = []
