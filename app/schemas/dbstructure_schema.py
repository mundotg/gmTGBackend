# app/schemas/db_structure.py
from datetime import datetime
from typing import Literal, Optional, List
from pydantic import BaseModel, Field, ConfigDict, field_validator

from app.models.dbstructure_models import _STATUS_CHOICES, STATUS_ACTIVE


# =========================
#       DBEnumField
# =========================
class DBEnumFieldBase(BaseModel):
    """
    Representa um valor possível de um campo ENUM.
    """

    value: str = Field(..., description="Valor ENUM definido para o campo")
    is_active: Optional[bool] = Field(
        default=True, description="Indica se o valor ENUM está ativo"
    )


class DBEnumFieldOut(DBEnumFieldBase):
    field_id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


type StatusType = Literal["active", "inactive", "deleted", "error"]


# =========================
#          DBField
# =========================
# =========================
#          DBField
# =========================
class DBFieldBase(BaseModel):
    """
    Representa uma coluna (campo) pertencente a uma tabela de banco de dados.
    """

    name: str = Field(..., description="Nome do campo no banco de dados")
    type: str = Field(..., description="Tipo de dados do campo (ex: VARCHAR, INTEGER)")
    is_nullable: Optional[bool] = Field(
        default=True, description="Indica se o campo permite nulo"
    )
    is_primary_key: Optional[bool] = Field(
        default=False, description="É chave primária?"
    )
    is_unique: Optional[bool] = Field(default=False, description="É valor único?")
    is_auto_increment: Optional[bool] = Field(
        default=False, description="É autoincrementável?"
    )

    # 🔥 ADICIONADO: is_unsigned
    is_unsigned: Optional[bool] = Field(
        default=False, description="É um número sem sinal (Unsigned)?"
    )

    is_foreign_key: Optional[bool] = Field(
        default=False, description="É chave estrangeira?"
    )
    referenced_table: Optional[str] = Field(
        default=None, description="Tabela referenciada (se for chave estrangeira)"
    )
    referenced_field: Optional[str] = Field(
        default=None, description="Campo referenciado (se for chave estrangeira)"
    )
    referenced_field_id: Optional[int] = Field(
        default=None, description="ID do campo referenciado (FK)"
    )
    fk_on_delete: Optional[str] = Field(
        default="NO ACTION", description="Ação ON DELETE"
    )
    fk_on_update: Optional[str] = Field(
        default="NO ACTION", description="Ação ON UPDATE"
    )
    default_value: Optional[str] = Field(
        default=None, description="Valor padrão do campo"
    )
    comment: Optional[str] = Field(default=None, description="Comentário do campo")
    length: Optional[int] = Field(
        default=None, description="Tamanho do campo, se aplicável"
    )
    precision: Optional[int] = Field(
        default=None, description="Precisão decimal, se aplicável"
    )
    scale: Optional[int] = Field(
        default=None, description="Escala decimal, se aplicável"
    )
    status: StatusType = Field(
        default="active", description="Status do campo (ativo, inativo, deletado)"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        value = value.strip() if value else ""
        if not value:
            raise ValueError("O nome do campo não pode estar vazio.")
        return value


class DBFieldCreate(DBFieldBase):
    pass


class DBFieldOut(DBFieldBase):
    id: int
    structure_id: int
    created_at: datetime
    updated_at: datetime
    enum_values: List[DBEnumFieldOut] = Field(
        default_factory=list, description="Lista de valores ENUM associados"
    )

    model_config = ConfigDict(from_attributes=True)


class FieldsBulkRequest(BaseModel):
    table_names: List[str] = Field(
        ..., description="Lista de nomes de tabelas para obter os campos"
    )


# =========================
#         DBStructure
# =========================
class DBStructureBase(BaseModel):
    """
    Representa uma tabela do banco de dados, com seus metadados e campos.
    """

    db_connection_id: int = Field(..., description="ID da conexão com o banco de dados")
    table_name: str = Field(..., description="Nome da tabela")
    schema_name: Optional[str] = Field(
        default=None, description="Schema da tabela (pode ser nulo)"
    )
    description: Optional[str] = Field(default=None, description="Descrição da tabela")
    is_deleted: Optional[bool] = Field(
        default=False, description="Indica se a estrutura foi marcada como deletada"
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        value = value.strip() if value else ""
        if not value:
            raise ValueError("O nome da tabela não pode estar vazio.")
        return value


class TableDDLRequest(BaseModel):
    """
    Payload recebido do Frontend para criar/editar/eliminar uma tabela no banco alvo.
    """

    connection_id: int = Field(..., description="ID da conexão do banco de dados alvo")

    # Nome/schema da tabela alvo
    table_name: str = Field(
        ..., description="Nome da tabela (novo nome em caso de rename)"
    )
    schema_name: Optional[str] = Field(
        default=None, description="Schema do banco (ex: public, dbo)"
    )

    # Para edição/rename
    original_table_name: Optional[str] = Field(
        default=None, description="Nome original da tabela (para rename)"
    )
    original_schema_name: Optional[str] = Field(
        default=None, description="Schema original (para mover schema)"
    )

    # Metadados
    description: Optional[str] = Field(
        default=None, description="Descrição da tabela (metadata)"
    )

    # Opções DDL (dialect-dependent)
    if_not_exists: bool = Field(
        default=True, description="Criar com IF NOT EXISTS (quando suportado)"
    )
    temporary: bool = Field(
        default=False, description="Criar como TEMPORARY (quando suportado)"
    )
    engine: Optional[str] = Field(default=None, description="Engine (MySQL/MariaDB)")
    charset: Optional[str] = Field(default=None, description="Charset (MySQL/MariaDB)")
    collation: Optional[str] = Field(
        default=None, description="Collation (MySQL/MariaDB)"
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        value = value.strip() if value else ""
        if not value:
            raise ValueError("O nome da tabela não pode estar vazio.")
        return value


class BulkDropTablesRequest(BaseModel):
    tables: List[str] = Field(
        ..., description="Lista de nomes de tabelas para eliminar"
    )
    schema_name: List[str] = Field(..., description="Schema comum (se aplicável)")


class DBStructureCreate(DBStructureBase):
    fields: List[DBFieldCreate] = Field(
        default_factory=list, description="Campos da tabela"
    )


class DBStructureOut(DBStructureBase):
    id: int
    created_at: datetime
    updated_at: datetime
    fields: List[DBFieldOut] = Field(
        default_factory=list, description="Lista de campos pertencentes à tabela"
    )

    model_config = ConfigDict(from_attributes=True)


# =========================
#     Sincronização Enum
# =========================
class CampoEnumSincronizado(BaseModel):
    """
    Representa um campo que teve sincronização de valores ENUM.
    """

    campo: str = Field(..., description="Nome do campo ENUM")
    valores_encontrados: List[str] = Field(
        default_factory=list, description="Valores ENUM encontrados no banco"
    )
    valores_adicionados: List[str] = Field(
        default_factory=list, description="Novos valores adicionados"
    )


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

    # 🔥 ADICIONADOS: Para manter sincronia com a interface do Frontend
    is_unsigned: Optional[bool] = False
    precision: Optional[int] = None
    scale: Optional[int] = None

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


class FieldDDLRequest(DBFieldBase):
    """
    Payload recebido do Frontend para criar ou editar uma coluna no banco alvo.
    """

    connection_id: int = Field(..., description="ID da conexão do banco de dados alvo")
    table_id: Optional[int] = Field(default=None, description="id strutures")
    table_name: str = Field(
        ..., description="Nome da tabela onde a coluna será inserida/alterada"
    )
    schema_name: Optional[str] = Field(
        default=None, description="Schema do banco (ex: public, dbo)"
    )
    original_name: Optional[str] = Field(
        default=None,
        description="Nome original da coluna (necessário apenas para edição/rename)",
    )

    # 🔥 ADICIONADO: Para o backend receber a array de ENUMs vinda do payload do frontend
    enum_values: Optional[List[str]] = Field(
        default_factory=list, description="Valores ENUM, se o tipo for ENUM"
    )
