from typing import Any, Dict, List, Optional, Union, Literal
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from datetime import datetime
from enum import Enum
import uuid

# ==========================================================
# ENUMS E TIPOS BASE
# ==========================================================

class DatabaseType(str, Enum):
    """Tipos de banco de dados suportados"""
    POSTGRES = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    SQLSERVER = "sqlserver"
    ORACLE = "oracle"

class OperatorType(str, Enum):
    """Operadores SQL genéricos compatíveis com todos os bancos"""
    EQUAL = "="
    NOT_EQUAL = "!="
    GREATER = ">"
    LESS = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    BETWEEN = "BETWEEN"

class LogicalOperator(str, Enum):
    """Operadores lógicos para combinar condições"""
    AND = "AND"
    OR = "OR"

class DeleteStrategy(str, Enum):
    """Estratégia de deleção"""
    SIMPLE = "simple"
    CASCADE = "cascade"
    SOFT_DELETE = "soft_delete"
    BATCH = "batch"

# ==========================================================
# MODELO DE CONDIÇÃO WHERE
# ==========================================================

class WhereCondition(BaseModel):
    """Condição WHERE individual para construção da query"""
    model_config = ConfigDict(extra='forbid')
    
    column: str = Field(..., min_length=1, max_length=100, description="Nome da coluna")
    operator: OperatorType = Field(..., description="Operador de comparação")
    value: Optional[Union[str, int, float, bool, List[Any]]] = Field(None, description="Valor(es) para comparação")
    logical_operator: LogicalOperator = Field(LogicalOperator.AND, description="Operador lógico com a próxima condição")

    @field_validator('column')
    @classmethod
    def validate_column_name(cls, v: str) -> str:
        if not v.replace('_', '').replace('.', '').isalnum():
            raise ValueError(f"Nome de coluna inválido: '{v}'")
        return v

    @field_validator('value')
    @classmethod
    def validate_value_for_operator(cls, v: Any, info) -> Any:
        operator = info.data.get('operator')
        null_operators = [OperatorType.IS_NULL, OperatorType.IS_NOT_NULL]
        if operator in null_operators:
            if v is not None:
                raise ValueError(f"Operador '{operator}' não aceita valor")
            return None
        if v is None:
            raise ValueError(f"Operador '{operator}' requer um valor")
        if operator in [OperatorType.IN, OperatorType.NOT_IN]:
            if not isinstance(v, list) or len(v) == 0:
                raise ValueError(f"Operador '{operator}' requer uma lista não-vazia")
        if operator == OperatorType.BETWEEN:
            if not isinstance(v, list) or len(v) != 2:
                raise ValueError("BETWEEN requer lista com exatamente 2 valores")
        return v

# ==========================================================
# REQUEST - DELEÇÃO GENÉRICA
# ==========================================================

class DeleteRequest(BaseModel):
    """Request para operação DELETE genérica multi-database"""
    model_config = ConfigDict(extra='forbid')
    
    operation_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="ID único da operação")
    database_type: DatabaseType = Field(..., description="Tipo do banco de dados")
    table: str = Field(..., min_length=1, max_length=100, description="Nome da tabela")
    db_schema: Optional[str] = Field(None, description="Schema/Database (ex: 'dbo' no SQL Server)")
    conditions: List[WhereCondition] = Field(..., min_length=1, description="Condições WHERE obrigatórias")
    strategy: DeleteStrategy = Field(DeleteStrategy.SIMPLE, description="Estratégia de deleção")
    dry_run: bool = Field(True, description="Se True, apenas simula sem executar")
    max_rows: int = Field(1000, ge=1, le=100000, description="Limite máximo de linhas a deletar")
    require_conditions: bool = Field(True, description="Se True, requer condição WHERE")
    soft_delete_column: Optional[str] = Field(None, description="Coluna para soft delete")
    soft_delete_value: Optional[Union[datetime, bool, str]] = Field(None, description="Valor para soft delete")
    batch_size: int = Field(100, ge=1, le=10000, description="Tamanho do lote para deleção em batch")
    timeout_seconds: int = Field(30, ge=1, le=300, description="Timeout em segundos")
    reason: str = Field(..., min_length=10, max_length=1000, description="Motivo da deleção")
    user_id: str = Field(..., description="ID do usuário solicitante")
    user_name: Optional[str] = Field(None)
    ip_address: Optional[str] = Field(None)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator('table')
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not v.replace('_', '').isalnum():
            raise ValueError(f"Nome de tabela inválido: '{v}'")
        reserved = {'user', 'group', 'order', 'table', 'select', 'delete', 'insert', 'update', 'drop', 'create', 'alter'}
        if v.lower() in reserved:
            raise ValueError(f"'{v}' é palavra reservada SQL")
        return v

    @model_validator(mode='after')
    def validate_soft_delete_config(self) -> 'DeleteRequest':
        if self.strategy == DeleteStrategy.SOFT_DELETE:
            if not self.soft_delete_column:
                raise ValueError("soft_delete_column obrigatório para SOFT_DELETE")
            if self.soft_delete_value is None:
                raise ValueError("soft_delete_value obrigatório para SOFT_DELETE")
        return self

    @model_validator(mode='after')
    def validate_conditions_safety(self) -> 'DeleteRequest':
        if self.require_conditions and len(self.conditions) == 0:
            raise ValueError("Pelo menos uma condição WHERE é obrigatória")
        broad_conditions = sum(
            1 for cond in self.conditions 
            if cond.operator in [OperatorType.LIKE, OperatorType.NOT_LIKE, OperatorType.IS_NULL, OperatorType.IS_NOT_NULL]
        )
        if broad_conditions >= 2 and self.max_rows > 10000:
            raise ValueError("Muitas condições amplas com limite alto.")
        return self

# ==========================================================
# REQUEST - DELEÇÃO POR IDs
# ==========================================================

class DeleteByIdsRequest(BaseModel):
    """Request simplificado para deleção por lista de IDs"""
    model_config = ConfigDict(extra='forbid')
    
    operation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    database_type: DatabaseType = Field(..., description="Tipo do banco")
    table: str = Field(..., description="Nome da tabela")
    db_schema: Optional[str] = Field(None, description="Schema/Database")
    id_column: str = Field("id", description="Nome da coluna de ID")
    ids: List[Union[str, int, uuid.UUID]] = Field(..., min_length=1, max_length=10000, description="Lista de IDs")
    strategy: DeleteStrategy = Field(DeleteStrategy.SIMPLE)
    soft_delete_column: Optional[str] = Field(None)
    soft_delete_value: Optional[Union[datetime, bool, str]] = Field(None)
    dry_run: bool = Field(True)
    batch_size: int = Field(500, ge=1, le=5000)
    verify_existence: bool = Field(True)
    reason: str = Field(..., min_length=10, max_length=1000)
    user_id: str = Field(...)
    user_name: Optional[str] = Field(None)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator('ids')
    @classmethod
    def validate_ids_list(cls, v: List[Any]) -> List[Any]:
        if len(v) != len(set(map(str, v))):
            raise ValueError("IDs duplicados encontrados")
        return v

# ==========================================================
# RESPONSE - RESULTADO DA DELEÇÃO
# ==========================================================

class DeleteResponse(BaseModel):
    """Response detalhado da operação de deleção"""
    model_config = ConfigDict(extra='forbid')
    
    success: bool
    operation_id: str
    deleted_count: int = 0
    affected_rows: int = 0
    execution_time_ms: float
    database_type: DatabaseType
    sql_preview: str
    sql_executed: Optional[str] = None
    parameters_used: Dict[str, Any] = Field(default_factory=dict)
    dry_run: bool
    strategy_used: DeleteStrategy
    batch_count: int = 0
    message: str
    warnings: List[str] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)
    error: Optional[str] = None
    error_code: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    table: str
    db_schema: Optional[str] = None
    user_id: str
    reason: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    request_snapshot: Optional[Dict[str, Any]] = None

# ==========================================================
# RESPONSE - VALIDAÇÃO PRÉ-DELEÇÃO
# ==========================================================

class PreDeleteValidationResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    
    operation_id: str
    valid: bool
    can_proceed: bool
    estimated_count: int = 0
    sql_preview: str
    affected_tables: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    blockers: List[str] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)
    risk_level: Literal["low", "medium", "high", "critical"]
    requires_confirmation: bool = False
    timestamp: datetime = Field(default_factory=datetime.utcnow)
