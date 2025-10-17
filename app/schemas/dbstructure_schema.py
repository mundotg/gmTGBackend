# app/schemas/db_structure.py
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict


# =========================
#        DBField
# =========================
class DBFieldBase(BaseModel):
    """
    Representa a estrutura de um campo (coluna) em uma tabela do banco de dados.
    """
    name: str = Field(..., description="Nome da coluna no banco de dados")
    type: str = Field(..., description="Tipo de dados da coluna (ex: VARCHAR, INTEGER)")
    is_nullable: Optional[bool] = Field(default=None, description="Indica se o campo permite nulo")
    default_value: Optional[str] = Field(default=None, description="Valor padrão da coluna")
    is_primary_key: bool = Field(default=False, description="É chave primária?")
    is_foreign_key: bool = Field(default=False, description="É chave estrangeira?")
    referenced_table: Optional[str] = Field(default=None, description="Nome da tabela referenciada (se for chave estrangeira)")
    referenced_field: Optional[str] = Field(default=None, description="Nome do campo referenciado (se for chave estrangeira)")
    fk_on_delete: Optional[str] = Field(default=None, description="Ação ON DELETE (se for chave estrangeira)")
    fk_on_update: Optional[str] = Field(default=None, description="Ação ON UPDATE (se for chave estrangeira)")
    is_unique: bool = Field(default=False, description="É valor único?")
    is_auto_increment: bool = Field(default=False, description="É autoincrementável?")
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
    """
    Representa a estrutura de uma tabela do banco de dados.
    """
    table_name: str = Field(..., description="Nome da tabela")
    schema_name: Optional[str] = Field(default=None, description="Nome do schema (se aplicável)")
    description: Optional[str] = Field(default=None, description="Descrição da tabela")


class DBStructureCreate(DBStructureBase):
    db_connection_id: int = Field(..., description="ID da conexão do banco de dados")
    fields: List[DBFieldCreate] = Field(default_factory=list, description="Lista de campos da tabela")


class DBStructureOut(DBStructureBase):
    id: int
    created_at: datetime
    fields: List[DBFieldOut] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


# =========================
#     Sincronização Enum
# =========================
class CampoEnumSincronizado(BaseModel):
    """
    Representa um campo que teve sincronização de valores ENUM.
    """
    campo: str = Field(..., description="Nome do campo ENUM")
    valores_encontrados: List[str] = Field(default_factory=list, description="Valores ENUM encontrados no banco")
    valores_adicionados: List[str] = Field(default_factory=list, description="Novos valores adicionados")


# =========================
#     Metadata de Tabela
# =========================
class CampoDetalhado(BaseModel):
    """
    Representa informações detalhadas de uma coluna retornada pela análise de metadados.
    """
    nome: str
    tipo: str
    is_nullable: bool
    is_primary_key: bool
    is_foreign_key: bool = False
    is_auto_increment: bool = False
    referenced_table: Optional[str] = None
    field_references: Optional[str] = None
    on_delete_action: Optional[str] = None
    on_update_action: Optional[str] = None
    is_unique: bool = False
    default: Optional[str] = None
    comentario: Optional[str] = None
    length: Optional[int] = None
    enum_valores_encontrados: List[str] = Field(default_factory=list)


class MetadataTableResponse(BaseModel):
    """
    Estrutura de resposta com informações completas da tabela e seus campos.
    """
    message: str
    executado_em: datetime
    connection_id: int
    schema_name: str
    table_name: str
    total_colunas: int
    colunas: List[CampoDetalhado] = Field(default_factory=list)
