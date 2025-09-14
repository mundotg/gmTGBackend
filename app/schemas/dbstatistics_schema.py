from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional

class DBStatisticsBase(BaseModel):
    tables_connected: int = 0
    queries_today: int = 0
    records_analyzed: int = 0
    
from typing import TypedDict

class DBStatisticsDict(TypedDict):
    server_version: str
    connection_name: str
    db_connection_id: int
    table_count: int
    view_count: int
    procedure_count: int
    function_count: int
    trigger_count: int
    index_count: int
    tables_connected: int
    queries_today: int
    records_analyzed: int
    

    
class DBStatisticsCreate(BaseModel):
    db_connection_id: int
    server_version: Optional[str] = ""
    tables_connected: Optional[int] = 0
    table_count: Optional[int] = 0
    view_count: Optional[int] = 0
    procedure_count: Optional[int] = 0
    function_count: Optional[int] = 0
    trigger_count: Optional[int] = 0
    index_count: Optional[int] = 0
    queries_today: Optional[int] = 0
    records_analyzed: Optional[int] = 0
    last_query_at: Optional[datetime] = None

class DBStatisticsUpdate(DBStatisticsBase):
    last_query_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class DBStatisticsOut(DBStatisticsBase):
    id: int
    db_connection_id: int
    updated_at: Optional[datetime]
    last_query_at: Optional[datetime]

    model_config = ConfigDict(from_attributes=True) 
    
class ConnectionStatisticsOverview(BaseModel):
    statistics: DBStatisticsCreate
    total_structured_tables: int
