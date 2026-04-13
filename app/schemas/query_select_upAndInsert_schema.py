from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional, Union

from app.schemas.dbstructure_schema import CampoDetalhado


class Pattern(BaseModel):
    prefix: Optional[Literal["%", "_", ""]] = Field("", title="Prefixo do LIKE")
    suffix: Optional[Literal["%", "_", ""]] = Field("", title="Sufixo do LIKE")


# Condição individual do JOIN
class JoinCondition(BaseModel):
    table: Optional[str] = None
    leftColumn: str
    operator: str
    rightColumn: Optional[str] = None
    rightValue: Optional[str] = None  # para valores literais
    valueColumnType: Optional[str] = (
        None  # tipo da coluna/valor: int, float, date, string...
    )
    useValue: bool  # se True → usa rightValue em vez de rightColumn
    logicalOperator: Optional[Literal["AND", "OR"]] = None
    caseSensitive: Optional[bool] = None  # se aplicável
    collation: Optional[str] = None  # ex: "utf8_general_ci"
    functionLeft: Optional[str] = None  # ex: UPPER, LOWER, TRIM
    functionRight: Optional[str] = None
    pattern: Optional[Pattern] = Field(
        None, title="Padrão para operadores LIKE (prefixo/sufixo)"
    )


# Tipo de JOIN
JoinType = Literal["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"]


# Estrutura para grupos de condições (suporte a parênteses)
class GroupStart(BaseModel):
    initIndex: int
    is_: bool  # "is" é palavra reservada em Python → renomeei para "is_"


class GroupEnd(BaseModel):
    endIndex: int
    is_: bool


class JoinOption(BaseModel):
    table: str
    type: str  # Pode validar com Enum se quiser
    on: str


class OrderByOption(BaseModel):
    column: str = Field(
        ...,
        title="Coluna para Ordenação",
        description="Coluna pela qual ordenar os resultados.",
    )
    direction: str = Field(
        ...,
        title="Direção da Ordenação",
        description="Direção da ordenação: 'asc' para ascendente ou 'desc' para descendente.",
    )


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


# Opção de JOIN avançada com múltiplas condições
class AdvancedJoinOption(BaseModel):
    conditions: List[JoinCondition]
    alias: Optional[str] = None
    typeJoin: JoinType
    groupStart: Optional[List[GroupStart]] = None
    groupEnd: Optional[List[GroupEnd]] = None


class DistinctList(BaseModel):
    useDistinct: bool = Field(
        False,
        title="Usar DISTINCT",
        description="Se True, aplica DISTINCT na consulta.",
    )
    distinct_columns: List[str] = Field(
        default_factory=list,
        title="Colunas Distintas",
        description="Lista de colunas para aplicar DISTINCT na consulta.",
    )


class CondicaoFiltro(BaseModel):
    table_name_fil: str = Field(..., title="Tabela")
    column: str = Field(..., title="Coluna")
    operator: str = Field(..., title="Operador")
    value: str = Field(..., title="Valor")
    value2: Optional[str] = Field(None, title="Valor 2 (para BETWEEN)")
    column_type: str = Field(..., title="Tipo da Coluna (ex: varchar, int)")
    logicalOperator: Literal["AND", "OR"] = Field("AND", title="Operador Lógico")
    value_type: Literal["string", "number", "date", "boolean"] = Field(
        "string", title="Tipo de Valor"
    )
    length: Optional[int] = None
    is_nullable: Optional[bool] = None

    # 🔥 aqui está a correção
    pattern: Optional[Pattern] = Field(
        None, title="Padrão para operadores LIKE (prefixo/sufixo)"
    )


class QueryPayload(BaseModel):
    baseTable: str
    joins: Optional[dict[str, AdvancedJoinOption]] = None
    table_list: Optional[List[str]] = None
    select: Optional[List[str]] = Field(default_factory=list)
    aliaisTables: Optional[Dict[str, str]] = None
    where: Optional[List[CondicaoFiltro]] = None
    orderBy: Optional[List[OrderByOption]] = None
    limit: Optional[int] = None
    distinct: Optional[DistinctList] = None
    offset: Optional[int] = None
    isCountQuery: bool = False


class QueryResultType(BaseModel):
    success: bool
    query: str
    params: Dict[str, Union[int, str, bool]]
    totalResults: Optional[int] = None
    duration_ms: float
    columns: List[str]
    tabela_coluna: Optional[Dict[str, List[CampoDetalhado]]] = None
    preview: List[Dict[str, Any]]
    QueryPayload: Optional["QueryPayload"] = None


class ParametrosRelatorioSchema(BaseModel):
    formato: str = Field(
        ..., description="Formato de saída do relatório (ex: pdf, csv, excel)"
    )
    incluirDetalhes: Optional[bool] = Field(
        default=False, description="Define se o relatório incluirá detalhes adicionais"
    )

    class Config:
        json_schema_extra = {"example": {"formato": "excel", "incluirDetalhes": True}}
