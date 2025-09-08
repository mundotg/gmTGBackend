# app/schemas/db_structure.py
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, List
from datetime import datetime


# =========================
#        DBField
# =========================
class DBFieldBase(BaseModel):
    name: str = Field(..., description="Nome da coluna no banco de dados")
    type: str = Field(..., description="Tipo de dados da coluna (ex: VARCHAR, INTEGER)")
    is_nullable: Optional[bool] = Field(default=None, description="Indica se o campo permite nulo")
    default_value: Optional[str] = Field(default=None, description="Valor padrão da coluna")
    is_primary_key: Optional[bool] = Field(default=False, description="É chave primária?")
    is_foreign_key: Optional[bool] = Field(default=False, description="É chave estrangeira?")
    referenced_table: Optional[str] = Field(default=None, description="Nome da tabela referenciada (se for chave estrangeira)")
    field_references: Optional[str] = Field(default=None, description="Nome do campo que referencia (se for chave estrangeira)")
    is_unique: Optional[bool] = Field(default=False, description="É valor único?")
    is_auto_increment: Optional[bool] = Field(default=False, description="É autoincrementável?")
    comment: Optional[str] = Field(default=None, description="Comentário da coluna")
    length: Optional[int] = Field(default=None, description="Tamanho do campo, se aplicável")
    precision: Optional[int] = Field(default=None, description="Precisão decimal, se aplicável")
    scale: Optional[int] = Field(default=None, description="Escala decimal, se aplicável")




class DBFieldCreate(DBFieldBase):
    pass

class DBFieldOut(DBFieldBase):
    id: int
    structure_id: int

    model_config = ConfigDict(from_attributes=True)


# =========================
#       DBStructure
# =========================
class DBStructureBase(BaseModel):
    table_name: str
    schema_name: Optional[str] = None
    description: Optional[str] = None


class DBStructureCreate(DBStructureBase):
    db_connection_id: int
    fields: List[DBFieldCreate] = []


class DBStructureOut(DBStructureBase):
    id: int
    created_at: datetime
    fields: List[DBFieldOut] = []

    model_config = ConfigDict(from_attributes=True)
    
class CampoEnumSincronizado(BaseModel):
    campo: str
    valores_encontrados: List[str]
    valores_adicionados: List[str]



from typing import List, Optional
from pydantic import BaseModel, Field

class CampoDetalhado(BaseModel):
    nome: str
    tipo: str
    is_nullable: bool
    is_primary_key: bool
    is_foreign_key: Optional[bool] = False
    is_auto_increment: Optional[bool] = False
    referenced_table: Optional[str] = None
    field_references: Optional[str] = None
    is_unique: Optional[bool] = False
    default: Optional[str] = None
    comentario: Optional[str] = None
    length: Optional[int] = None
    enum_valores_encontrados: List[str] = Field(default_factory=list)
    enum_valores_adicionados: List[str] = Field(default_factory=list)



class MetadataTableResponse(BaseModel):
    message: str
    executado_em: datetime
    connection_id: int
    schema_name: str
    table_name: str
    total_colunas: int
    colunas: List[CampoDetalhado]

