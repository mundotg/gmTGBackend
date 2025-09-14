from pydantic import BaseModel, Field, ConfigDict
from typing import Any, Dict, List, Literal, Optional
from datetime import datetime


# --------- Base ---------

class QueryHistoryBase(BaseModel):
    query: str = Field(..., title="Query SQL", description="Consulta SQL executada.")
    query_type: Optional[str] = Field(None, title="Tipo de Query", description="Tipo da query, ex: SELECT, INSERT, etc.")
    duration_ms: Optional[int] = Field(None, title="Duração (ms)", description="Tempo de execução em milissegundos.")
    result_preview: Optional[str] = Field(None, title="Prévia do Resultado", description="Texto ou JSON com os primeiros registros retornados.")
    error_message: Optional[str] = Field(None, title="Erro", description="Mensagem de erro, se houver.")
    is_favorite: Optional[bool] = Field(False, title="Favorita", description="Se a query foi marcada como favorita.")
    tags: Optional[str] = Field(None, title="Tags", description="Tags separadas por vírgula, para facilitar agrupamento.")


# --------- Estruturas auxiliares ---------

class TableInfo(BaseModel):
    name: str = Field(..., title="Nome da Tabela")
    rowcount: int = Field(..., title="Linhas", description="Número de linhas da tabela.")


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

class CondicaoFiltro(BaseModel):
    table_name_fil: str = Field(..., title="Tabela")
    column: str = Field(..., title="Coluna")
    operator: str = Field(..., title="Operador")
    value: str = Field(..., title="Valor")
    value2: Optional[str] = Field(None, title="Valor 2 (para BETWEEN)")
    column_type: str = Field(..., title="Tipo da Coluna (ex: varchar, int)")
    logicalOperator: Optional[Literal['AND', 'OR']] = Field("AND", title="Operador Lógico")
    value_type: Optional[Literal['string', 'number', 'date', 'boolean']] = Field("string", title="Tipo de Valor")
    length: Optional[int] = None
    is_nullable: Optional[bool] = None


class JoinOption(BaseModel):
    table: str
    type: str
    on: str 


class OrderByOption(BaseModel):
    column: str
    direction: str  # 'ASC' ou 'DESC'


class DistinctList(BaseModel):
    useDistinct: bool = False
    distinct_columns: List[str] = Field(default_factory=list)


class QueryPayload(BaseModel):
    baseTable: str
    joins: Optional[List[JoinOption]] = None
    table_list: Optional[List[str]] = None
    select: Optional[List[str]] = []
    aliaisTables: Optional[Dict[str, str]] = None   # agora suporta
    where: Optional[List[CondicaoFiltro]] = None
    orderBy: Optional[List[OrderByOption]] = None   # lista, não único
    limit: Optional[int] = None
    distinct: Optional[DistinctList] = None
    offset: Optional[int] = None
    isCountQuery: Optional[bool] = False

    
class ChangedField(BaseModel):
    value: str
    type_column: str
    

class UpdateRequest(BaseModel):
    updatedRow: Dict[str, Dict[str, ChangedField]]
    tables_primary_keys_values: Dict[str, Dict[str, str]]
    
class InsertRequest(BaseModel):
    createdRow: Dict[str, Dict[str, ChangedField]]
    
class CampoPadronizado(BaseModel):
    campo: str
    valor: Optional[str] = None
    id: str = "text"

class ConfiguracaoTabela(BaseModel):
    schema_name: Optional[str] = ""
    tabela: str
    quantidade: int = 1
    camposPadronizados: Optional[List[CampoPadronizado]] | None = None

class AutoCreateRequest(BaseModel):
    configs: List[ConfiguracaoTabela]

